# 独立定时指标采集 Pod

[返回项目 README](../README.md)

`azure-metrics-scraper` 是与 exporter 分开运行的 Go 进程，通过 HTTP 定时拉取 exporter 返回的 Prometheus 文本，或平台指标 / PromQL 窗口 JSON，不需要部署 Prometheus 服务。

采集器的代码、测试、配置文件、Dockerfile、部署清单和说明统一放在本目录，与 exporter 共用仓库根目录的 Go module，但独立构建镜像。以下命令均从 `scraper` 目录执行；若当前在仓库根目录，先执行 `cd .\scraper`。

配置 `event_hub` 后，采集器按数据源封装消息：PromQL JSON 转换为 AKS 控制平面业务 JSON，Azure Monitor 平台 JSON 转换为按资源分组的平台指标消息，共用 Azure Event Hubs Go SDK 和 SAS 认证发送层。日志只在服务确认后记录 `Event Hub delivery completed`；不会输出密钥或完整指标正文。未配置 `event_hub` 时保留仅采集模式，明确记录丢弃 batch 的日志。数据不落盘，不提供断点续传或历史补采。

## 配置

本地调试使用 [scraper.yaml](scraper.yaml)，默认访问 `127.0.0.1:8080`；Kubernetes 使用[部署清单](deploy/metrics-scraper.yaml) 中的 ConfigMap，默认访问 `azure-metrics:8080`。两份配置独立维护，不会自动同步，镜像不内置配置文件。配置使用 Prometheus 风格的静态 job 子集，不是完整的 Prometheus 配置兼容实现，以下为本地配置示例：

```yaml
global:
  scrape_interval: 1m
  scrape_timeout: 30s
scrape_configs:
  - job_name: azure-control-plane
    metrics_path: /probe/metrics/promql
    honor_timestamps: true
    params:
      query: ["apiserver_request_total"]
      # 根据工作区入库延迟按需启用。
      # query_delay: ["20m"]
    static_configs:
      - targets: ["127.0.0.1:8080"]
        labels:
          environment: development
  - job_name: exporter-self
    scrape_interval: 30s
    scrape_timeout: 5s
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:8080"]
```

- 支持 `global.scrape_interval` / `scrape_timeout`；默认分别为 `1m` / `10s`。job 可以覆盖，两者必须为正的 Go duration，显式超时不能大于周期；继承的超时超过 job 周期时自动缩短到周期。省略全局超时时，默认超时也不超过全局周期。
- job 名称必须非空且唯一。支持 `scheme`（`http` / `https`）、`metrics_path`（默认 `/metrics`）、`params`（值为字符串列表，可多值）和 `static_configs.targets` / `labels`。
- target 使用 `host[:port]`，不包含协议、路径或认证信息；IPv6 使用方括号。相同 job 内 target 不可重复。
- 默认将 `params` 原样 URL 编码后发送到 exporter；启用 `rolling_range` 的 PromQL job 会在每次请求时额外生成 `start/end/step`。这里只请求 exporter，不直接请求 Azure。PromQL 的 `query_delay` 等规则见 [PROMQL 文档](../PROMQL.md)。
- 立即执行第一次抓取，然后按每个 job 的周期调度。不同 target 独立运行，同一 target 不重叠抓取；失败记录错误日志，下一个周期继续，绝不复用旧结果或补零。
- 限制单次响应为 16 MiB；只接受 HTTP 200 和合法的 Prometheus 文本或本项目的指标窗口 JSON（允许空结果）。JSON 的 `source` 支持平台指标的 `azure-monitor` 和 PromQL 的 `azure-prometheus`。不跟随 HTTP 重定向。请求和响应读取均受超时与退出信号约束。
- 原始响应及源时间戳不做修改；只支持 `honor_timestamps: true`（或省略）。job、target、静态 labels、采集时间、Content-Type 和原始响应进入 batch；两类窗口 JSON 均保留解析后的全部序列和采样点，留给后续发送逻辑使用。**静态 labels 是 batch 元数据，不会注入或改写指标，也不实现 Prometheus 的 job/instance 标签合并规则。**
- 不实现服务发现、relabel、认证配置、本地 PromQL 计算、TSDB、历史补采或完整 OpenMetrics/protobuf 协议。未知配置字段、重复 YAML 键、非法周期等会在启动时直接报错退出，不会静默忽略。
- 配置在启动时读取，不支持热加载。本地修改后重启进程；Kubernetes 修改 ConfigMap 后执行 `apply` 和 `rollout restart`，无需重新构建镜像。

对于 `30s` probe 抓取超时，应协调 exporter 的写超时，例如设置 `--server.timeout.write=120s`，避免 exporter 默认 `10s` 提前结束响应。

## 平台指标 JSON 采集

在原有平台指标 job 中增加 `params.format: ["json"]` 即可，不需要新增调度框架或在 scraper 中累积历史窗口：

```yaml
- job_name: azure-platform-window
  scrape_interval: 3m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/resource
  params:
    subscription: ["<subscription-id>"]
    target: ["/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.ContainerService/managedClusters/<cluster>"]
    metric: ["node_cpu_usage_percentage"]
    aggregation: ["average"]
    timespan: ["PT3M"]
    interval: ["PT1M"]
    format: ["json"]
  static_configs:
    - targets: ["127.0.0.1:8080"]
```

将此 job 加入本地配置的 `scrape_configs`，或 Kubernetes ConfigMap 的对应位置，替换订阅和资源 ID；在 Kubernetes 中 target 地址改为 exporter Service。原有 PromQL 和 `/metrics` job 不受影响。不要把含占位符的 job 直接用于生产。

scraper 为 `format=json` 请求发送 `Accept: application/json`，校验响应 Content-Type、schemaVersion、source、序列结构和采样点。若 exporter 仍返回旧的文本响应，会明确报错，不静默退回单点模式。JSON 成功日志额外记录 `series`、`samples`；`metric_families` 为不同输出指标名的数量，不是序列或采样点数量。batch 同时保留原始 JSON 和解析后的窗口，不丢弃历史点。

平台指标只采集 exporter 已返回的窗口，不提供持久化或历史补采。启用 `event_hub` 后，此 job 可与 PromQL JSON jobs 在同一实例中发送到同一个 Hub，使用独立的 `source: "azure-monitor"` 消息格式，不伪装成 AKS 控制平面指标。`/metrics` 文本 job 仍需使用不配置 `event_hub` 的独立实例。完整接口参数和字段约定见[平台指标 JSON](../README.md#platform-window-json)。

## PromQL JSON 采集

PromQL 与平台指标使用相同的 `params.format: ["json"]`，无需增加新的 scraper 输出格式选项。仅查询参数按数据源区分：平台指标使用 `timespan/interval`，PromQL 使用 `start/end/step`。

```yaml
- job_name: azure-control-plane-window
  scrape_interval: 3m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  params:
    query: ['sum(rate(apiserver_request_total{cluster="prod"}[5m])) by (cluster, code)']
    start: ["2026-10-21T12:00:00Z"]
    end: ["2026-10-21T12:03:00Z"]
    step: ["15s"]
    format: ["json"]
  static_configs:
    - targets: ["127.0.0.1:8080"]
```

上例用于调试固定历史窗口，重复执行仍查询同一窗口；scraper 不自动改写 `start/end`。JSON 的 schemaVersion 和 name/labels/samples 结构与平台指标相同，`source` 为 `azure-prometheus`，日志中的序列数和采样点数采用同一统计方式。完整时间戳规则、指标名及标签语义见 [PromQL JSON 文档](../PROMQL.md#json-window)。

### PromQL 滚动查询范围

本地配置和 Kubernetes ConfigMap 的默认 PromQL job 已启用滚动范围。`rolling_range` 是 scraper 的 job 配置，不放在 `params` 中，也不会作为参数传给 exporter：

```yaml
- job_name: azure-control-plane
  scrape_interval: 1m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  rolling_range:
    window: 1m
    step: 15s
    delay: 0s
  params:
    query: ["apiserver_request_total"]
    format: ["json"]
  static_configs:
    - targets: ["127.0.0.1:8080"]
```

- 每次实际开始抓取时，取一次当前 UTC 时间，计算 `end = now - delay`、`start = end - window`，生成 `params.start/end/step`。按 PromQL 毫秒精度截去亚毫秒，不按整分钟或 `step` 网格取整；不会修改共享配置或前一批的查询参数。
- `window` 默认 `1m`，独立于 `scrape_interval`，可调整为 `3m` 等；`step` 默认 `15s`，必须为正且不超过 window；`delay` 默认 `0s`，必须非负。三者均为 Go duration，精度为整毫秒。`rolling_range: {}` 启用默认值，完全省略该字段则保留原有查询行为。
- 例如在 `2026-10-22T00:12:24Z` 抓取，默认生成 `start=2026-10-22T00:11:24Z`、`end=2026-10-22T00:12:24Z`、`step=15s`。下一次抓取重新计算。配置 `delay: 20m` 时，整个范围向过去移动 20 分钟，不是等待 20 分钟再发送。
- 仅支持 `/probe/metrics/promql`，必须有一个非空 `params.query`；不能与 `params.start/end/step/time/query_delay` 同时配置，包括空列表或空字符串。冲突在启动时直接报错。入库延迟使用 `rolling_range.delay`，不是即时查询的 `params.query_delay`。
- 不启用 `rolling_range` 时，原有即时查询、即时 `query_delay`、固定 `start/end/step` 和平台指标 `timespan/interval` 都保持不变。若需要固定范围或即时查询，请移除整个 `rolling_range`。
- 范围查询包含起止端点；原始选择器还可能在多个求值点选中相同源样本。相邻窗口可能重复边界样本，且调度抖动可能产生重叠或间隙。查询周期大于 window、请求/发送耗时、暂停或失败也可能产生缺口；本功能不保证连续覆盖，不记忆上次 end、不自动补采。
- `step` 是 PromQL 求值步长，不保证抓取上游每个原始样本。查询步长大于上游采样周期时可能跳过部分源样本；JSON 仅保证保留本次查询实际返回的全部点，不补零、不插值、不重新打时间戳。
- 成功日志记录 `query_start/query_end/query_step`。Event Hub 消息的 `windowStart/windowEnd` 仍取实际源样本时间的最小值/最大值，可能与请求范围不同；`scrapeInterval` 仍表示抓取周期。JSON 正文结构和发送逻辑不变。

## 本地运行

先确保本地 exporter 已启动；本目录的 [scraper.yaml](scraper.yaml) 默认 target 为 `127.0.0.1:8080`，若 exporter 在其他地址则修改各 job 的 target，然后运行：

```powershell
go run .\cmd\metrics-scraper --config.file=.\scraper.yaml --web.listen-address=:8081
```

访问 exporter 不需要 Azure 凭据；查询 Azure 指标的身份仍由 exporter 管理。启用 Event Hub 后，先按下节设置 SAS 连接字符串。`/healthz` 和 `/readyz` 表示进程及配置已就绪，不代表上游抓取成功或 Event Hub 已连通。采集和发送失败需查看日志。Ctrl+C 或 SIGTERM 会取消正在执行的请求和发送并关闭生产者连接。

## Kubernetes 部署

[部署清单](deploy/metrics-scraper.yaml) 包含 ConfigMap 和单副本 Deployment，配置只读挂载到 `/etc/azure-metrics-scraper/scraper.yaml`。`Recreate` 策略用于避免常规滚动更新时两个版本同时采集；不要横向扩容，否则会重复采集。

先修改部署清单 ConfigMap 中的 target 为现有 exporter 的 Kubernetes Service 地址（跨 namespace 使用完整 DNS），再使用本目录独立的 [Dockerfile](Dockerfile) 构建并推送镜像。虽然命令在 `scraper` 目录执行，构建上下文仍需指定为上一级仓库根目录，以读取共享的 Go module：

```powershell
docker build -f .\Dockerfile -t <registry>/azure-metrics-scraper:v1 ..
docker push <registry>/azure-metrics-scraper:v1
```

将清单中的 `image` 替换为该镜像。不是新建 exporter，也不会创建 Event Hub 资源。采集器镜像与 exporter 镜像独立，根目录 Dockerfile 仍只构建 exporter。

```powershell
kubectl apply -n <namespace> -f .\deploy\metrics-scraper.yaml
kubectl rollout status -n <namespace> deployment/azure-metrics-scraper
kubectl logs -n <namespace> deployment/azure-metrics-scraper --follow
```

后续修改部署清单中的 ConfigMap 后，应用配置并重启采集器以重新读取：

```powershell
kubectl apply -n <namespace> -f .\deploy\metrics-scraper.yaml
kubectl rollout restart -n <namespace> deployment/azure-metrics-scraper
kubectl rollout status -n <namespace> deployment/azure-metrics-scraper
```

无需 Service 或额外 Azure RBAC；健康探针直接访问 Pod。启用 Event Hub 时，部署前需在同一 namespace 中准备名为 `azure-metrics-eventhub` 的 Kubernetes Secret，键为 `connection-string`，通过现有 Secret 管理工具注入，不要提交到仓库。清单使用 `secretKeyRef` 注入环境变量。集群网络策略需要允许访问 exporter、DNS 及 Event Hubs 的 TLS AMQP 端口 5671。

## Event Hub SAS 推送

本地配置及 Kubernetes ConfigMap 已启用以下目标，不会创建或修改 Azure 资源：

```yaml
event_hub:
  namespace: shein-aks-metrics.servicebus.windows.net
  name: shein-aks-metrics
```

在启动采集器的同一终端中，安全读取具有 **Send** 权限的 SAS policy 连接字符串（必须含 `EntityPath=shein-aks-metrics`），无需 Listen / Manage 权限：

```powershell
$secret = Read-Host "Event Hub SAS connection string" -AsSecureString
$env:AZURE_EVENTHUB_CONNECTION_STRING = [System.Net.NetworkCredential]::new("", $secret).Password
go run .\cmd\metrics-scraper --config.file=.\scraper.yaml --web.listen-address=:8081
```

连接字符串的结构为 `Endpoint=sb://shein-aks-metrics.servicebus.windows.net/;SharedAccessKeyName=<send-policy>;SharedAccessKey=<key>;EntityPath=shein-aks-metrics`。不要将真实字符串放入 YAML、命令行参数、日志或聊天。缺失凭据、目标不匹配、配置文本或不支持的端点时启动失败，不静默丢弃数据。修改凭据后重启进程。

PromQL 消息与示例业务 JSON 使用相同字段结构：

```json
{
  "schemaVersion": "1.0",
  "source": "aks-controlplane",
  "cluster": "sk-rundeck",
  "region": "",
  "windowStart": "2026-10-21T15:00:26Z",
  "windowEnd": "2026-10-21T15:00:26Z",
  "scrapeInterval": "1m0s",
  "metrics": [
    {
      "name": "apiserver_request_total",
      "promql": "apiserver_request_total",
      "labels": {"cluster": "sk-rundeck", "code": "200"},
      "samples": [["2026-10-21T15:00:26Z", 1313]]
    }
  ]
}
```

上例仅展示部分标签；实际发送保留全部原始标签、指标名和采样点，不将 counter 改成 rate，也不伪造示例中的业务指标名。

- 所有 Event Hub jobs 必须使用 `format: ["json"]`。PromQL job 使用 `/probe/metrics/promql` 及一个 `query`；同一查询的全部结果序列都携带实际查询表达式 `promql`。平台 job 支持 `/probe/metrics`、`/probe/metrics/resource`、`/probe/metrics/list`、`/probe/metrics/scrape`、`/probe/metrics/resourcegraph`；原有查询参数原样传给 exporter。响应 `source` 与请求端点不匹配时拒绝发送。
- PromQL 按 `(cluster, region)` 分组。`cluster` 优先来自序列标签，其次是 job 的静态 labels；两者都缺失则报错。两类数据的 region 都优先读取序列的 `region` / `location`，其次读取静态 labels 的同名字段；仍缺失则使用空字符串并记录警告，不从工作区名称或资源 ID 猜测。
- `windowStart` / `windowEnd` 是组内实际源采样时间的最小值 / 最大值；即时查询可能相等，也可能包含不同源时间。`scrapeInterval` 是 job 的抓取周期，不是查询步长；保留亚秒精度。
- 空结果或无采样点的序列不发送，不补零、不生成伪造窗口，会记录无事件日志（整个结果为空时）。既不累积最近三分钟，也不自动执行示例中的全部 PromQL。
- 每条事件正文不超过 200 KiB，使用 256 KiB 的 SDK batch 上限预留 AMQP 开销。大组按序列拆分；单条序列过大时按 samples 拆分，每条消息仍是完整有效 JSON，并保留原组窗口元数据。不能拆分的超大单点会在发送前报错。所有消息预先编码和校验，避免编码错误导致部分发送。
- PromQL 同一 cluster 使用相同 partition key。平台指标使用资源 ID 的小写形式计算 SHA-256 作为 partition key，避免长 ARM ID 超过长度限制。消息的 AMQP Content-Type 为 `application/json`。不保证不同 job 的发送顺序，也不提供消费端确认或 exactly-once。
- HTTP 抓取继续受 `scrape_timeout` 限制；每批发送另有 30 秒总超时，使用 SDK 默认有界重试。同一 target 同步发送形成背压，不积累无界后台队列；target 的一次采集加发送可能超过抓取周期，但不会重叠。
- 失败明确记录已确认事件数量；若前几条已成功而后续失败，可能出现部分发送或重试重复。无本地持久化、离线重放或应用层整批重试，下一轮采集新数据，不重发旧批。消费者应按指标名、完整标签及源时间戳进行幂等处理。

### Azure Monitor 平台指标消息

将上文[平台指标 JSON job](#平台指标-json-采集) 加入 `scrape_configs` 并填写真实订阅和资源 ID 即可，无需新增凭据或生产者。例如：

```json
{
  "schemaVersion": "1.0",
  "source": "azure-monitor",
  "resourceId": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm",
  "query": {
    "path": "/probe/metrics/resource",
    "params": {
      "subscription": ["sub"],
      "target": ["/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm"],
      "metric": ["Percentage CPU"],
      "aggregation": ["average"],
      "timespan": ["PT3M"],
      "interval": ["PT1M"],
      "format": ["json"]
    }
  },
  "region": "",
  "windowStart": "2026-10-21T15:00:00Z",
  "windowEnd": "2026-10-21T15:01:00Z",
  "scrapeInterval": "1m0s",
  "metrics": [
    {
      "name": "azure_cpu",
      "labels": {
        "resourceID": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm",
        "aggregation": "average",
        "unit": "Percent",
        "dimension": "node-a"
      },
      "samples": [["2026-10-21T15:00:00Z", 25.5], ["2026-10-21T15:01:00Z", 26.5]]
    }
  ]
}
```

- 平台消息不生成顶层 `cluster` 或指标的 `promql`；原始 labels 中已有同名标签时仍保留。`query.path` 和 `query.params` 记录实际 exporter 端点及全部请求参数，包括多值列表。不要在查询参数中存放凭据。
- 按 `(resourceID 的小写形式, region)` 分组，但不改写原始资源 ID 或标签。保留原始指标名、聚合类型、单位、维度和全部历史采样点；不同聚合仍由 labels 区分，不补零、不丢失历史点。
- 资源 ID 优先取序列的 `resourceID` 标签；仅 `/probe/metrics/resource` 且请求中恰好一个 `target` 时可回退到该 target。自动发现、订阅查询或多 target 请求不猜测资源归属。资源 ID 缺失时省略顶层 `resourceId` 并明确记录警告；仍保留全部序列和查询上下文，使用 job、exporter target、查询端点及参数的 SHA-256 作为备用 partition key。
- 与 PromQL 共用窗口时间规则、200 KiB 正文限制、拆分、30 秒发送超时和有界重试。每个拆分消息都携带完整查询上下文及原组窗口。

### 真实发送验证

确保 exporter 正在运行，并在当前终端设置连接字符串后，从 `scraper` 目录执行：

```powershell
$env:AZURE_EVENTHUB_LIVE_TEST = "1"
go test ./internal/scraper -run '^TestLiveEventHubDelivery$' -count=1 -v
Remove-Item Env:\AZURE_EVENTHUB_LIVE_TEST
```

此显式启用的测试会真实采集并发送一次，空指标结果或发送失败都会失败；普通 `go test` 不连接 Azure。成功日志只证明 Event Hubs 服务已确认写入，不证明下游消费者已处理。SDK 参考：[Azure Event Hubs Go SDK](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2)。

## 验证

```powershell
go test ./...
```

此命令覆盖采集器代码；若需验证整个仓库，在仓库根目录运行 `go test ./...`。
