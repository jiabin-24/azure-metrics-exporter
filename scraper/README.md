# 独立定时指标采集器

[返回项目 README](../README.md)

定时从 exporter 采集指标并发送到 Azure Event Hubs。以下 PowerShell 命令均在仓库根目录执行。

## 本地启动

### 1. 启动 exporter

按[项目配置](../GUIDE.md#configuration)设置 Azure 身份，授予各 workspace 的 `Monitoring Data Reader` 权限。需要自动补充资源区域时，还需具有对应 Azure 资源的读取权限。

修改 [workspace 配置文件](../config/prometheus-workspaces.yaml)，填写真实查询终结点：

```yaml
workspaces:
  workspace-a:
    query_endpoint: https://<workspace-a>.<region>.prometheus.monitor.azure.com
```

在第一个终端启动，必须显式指定 workspace 文件：

```powershell
$env:AZURE_PROMETHEUS_WORKSPACES_FILE = ".\config\prometheus-workspaces.yaml"
go run . --server.timeout.write=120s
```

### 2. 启动 scraper

统一使用 [scraper.yaml](scraper.yaml)，默认配置面向 Kubernetes。修改其中的 Event Hub namespace、name；本地单实例启动时，将 `&exporter_targets` 定义的地址改为 `127.0.0.1:8080`，并移除或注释整个 `cluster` 配置。改回 Kubernetes 部署前，恢复 `azure-metrics:8080` 和 memberlist 配置。

在第二个终端输入具有 **Send** 权限的 SAS 连接字符串并启动：

```powershell
$secret = Read-Host "Event Hub SAS connection string" -AsSecureString
$env:AZURE_EVENTHUB_CONNECTION_STRING = [System.Net.NetworkCredential]::new("", $secret).Password
go run .\scraper\cmd\metrics-scraper --config.file=.\scraper\scraper.yaml --web.listen-address=:8081
```

连接字符串的 namespace 必须与配置一致，并包含 `EntityPath=<event_hub.name>`。缺少时追加，不要重复填写或将密钥提交到仓库。

仅调试采集时可移除 `event_hub` 配置；此时数据会被丢弃，不发送、不落盘。

### 3. 检查状态

```powershell
Invoke-RestMethod "http://127.0.0.1:8080/probe/metrics/promql/workspaces"
Invoke-WebRequest "http://127.0.0.1:8081/healthz"
Invoke-WebRequest "http://127.0.0.1:8081/readyz"
```

采集和发送是否成功以日志为准。配置修改后需重启；增删 workspace 后也需重启 scraper。

## 核心配置

本地和 Kubernetes 共用唯一的 [scraper.yaml](scraper.yaml)，Kustomize 直接从此文件生成 ConfigMap，不维护额外的部署配置副本。

| 配置项 | 用法 |
|--------|------|
| `scrape_interval` | 抓取周期，默认 `1m`，可在 job 中覆盖 |
| `scrape_timeout` | 抓取超时，不得大于抓取周期 |
| `honor_timestamps` | 默认 `true`；可在 `global` 设置、job 覆盖；`false` 仅用于 PromQL，保留上游查询求值时间 |
| `rolling_range` | 可在 `global` 设置，job 按字段覆盖；`enabled: false` 关闭滚动查询 |
| `targets` | exporter 的 `host:port`；HTTPS 另设 `scheme: https` |
| `metric_name` | 可选，指定单条 PromQL JSON 查询的输出指标名 |
| `params.query` | PromQL 表达式，每个任务一条 |
| `params.format` | 发送到 Event Hub 时使用 `["json"]` |
| `event_hub.batch_window` | 单实例跨 job 汇聚等待窗口，默认 `5s`，必须为正时长 |
| `static_configs.labels` | 静态元数据，例如 `cluster`、`region` |

### 时间戳语义

scraper 始终保留 exporter 返回的指标时间戳，不替换为 scraper 的本次抓取时间。`honor_timestamps: true`（默认）保持现有逻辑：PromQL 独立原始指标选择器使用源样本时间，计算表达式使用查询求值时间；平台指标使用 Azure 聚合时间桶的时间。

范围查询的一次响应包含多个历史点，若全部改成本次抓取时间，会丢失各点的时间关系。`rolling_range.step` 只对齐查询求值网格，不保证源样本时间对齐。

`honor_timestamps: false` 让 PromQL exporter 不再用源时间替换上游响应的时间戳。普通范围查询的每个点保留自身求值时间；滚动查询配置 `step: 15s` 时落在每分钟 `00/15/30/45`，`delay` 在确定查询范围时生效。缺失点不补齐，不按点数倒推，不插值；重复值仍可出现在不同求值时刻。即时查询使用实际求值时间（含 `time` 或 `query_delay`），不自动对齐到 step。即时查询返回的原始范围向量仍携带原始样本时间，见 [PromQL 文档](../PROMQL.md#采样时间默认启用)。

这个选项不同于标准 Prometheus 的同名配置：`false` 不是将历史点统一打上本次抓取时间。请在 `global` 或 job 中设置，不要填写 `params.honor_timestamps`。非 PromQL job 不支持 `false`；全局设为 `false` 时，这些 job 必须覆盖为 `true`。Event Hub 的 `windowStart/windowEnd` 仍表示实际返回样本的最早/最晚时间，不保证等于请求边界。

### PromQL JSON 采集

每个任务自动采集全部已配置的 workspace，不要填写 `workspaces` 或 `params.workspace`。

```yaml
global:
  honor_timestamps: true
  rolling_range:
    window: 1m
    step: 15s
    delay: 0s

scrape_configs:
  - job_name: apiserver_request_rate_by_code
    metric_name: apiserver_request_rate_by_code
    metrics_path: /probe/metrics/promql
    params:
      query: ['sum(rate(apiserver_request_total[5m])) by (cluster, code)']
      format: ["json"]
    static_configs:
      - targets: &exporter_targets ["127.0.0.1:8080"]
```

其他任务使用 `targets: *exporter_targets` 复用地址。输出缺少 `cluster` 时，须在静态 `labels` 中补充；也可填写 `region`。

自定义静态标签示例（可选，不包含在默认配置中）：

```yaml
static_configs:
  - targets: ["127.0.0.1:8080"]
    labels:
      environment: development
```

`environment` 仅保存在采集批次元数据中，不会自动添加到指标标签或最终 Event Hub 消息，也不会切换运行环境。当前导出逻辑仅使用静态标签中的 `cluster`、`region`、`location` 补充导出信息。

### PromQL 滚动查询范围

`global.rolling_range` 为所有 job 提供默认值；job 的 `rolling_range` 按字段覆盖，未填写的字段继承全局值。例如 job 只配置 `rolling_range: {delay: 20m}` 时，`window` 和 `step` 继续继承；显式设置 `delay: 0s` 可清除全局延迟。未在全局或 job 配置 `rolling_range` 时，保持原有查询行为。

- `enabled`：默认启用；job 设置 `rolling_range: {enabled: false}` 可关闭继承的滚动范围。全局关闭后，job 可设置 `enabled: true` 重新启用。
- `window`：查询窗口，默认 `1m`。
- `step`：查询步长，默认 `15s`，起止时间按此对齐。
- `delay`：入库延迟，默认 `0s`，可按需要设置为 `20m` 等。

启用滚动范围时，不要与 `params.start/end/step/time/query_delay` 同时使用。改用即时查询或固定范围时，在 job 设置 `rolling_range: {enabled: false}`，再按需设置查询参数。滚动范围仅适用于 `/probe/metrics/promql` 的单条查询；其他端点的 job 也需关闭继承的滚动范围。更多参数见 [PromQL 文档](../PROMQL.md)。

### 平台指标 JSON 采集

参考 [scraper.yaml](scraper.yaml) 中的平台任务注释，填写订阅、资源、指标及 `format: ["json"]`，可与 PromQL 共用 Event Hub。参数见[平台指标文档](../GUIDE.md#platform-window-json)。

## 多 workspace 与 memberlist 分片

单实例不配置 `cluster`。多实例使用相同任务配置并启用：

```yaml
cluster:
  name: azure-metrics-scraper
  bind_address: 0.0.0.0:7946
  join: ["scraper-seed:7946"]
```

- `join` 填写可达的种子地址或发现 DNS；不要让所有实例都以空 `join` 启动。
- 实例间双向放行 **TCP/UDP 7946**，必要时设置 `advertise_address`。
- 可用 `SCRAPER_CLUSTER_NODE_NAME`、`SCRAPER_CLUSTER_BIND_ADDRESS`、`SCRAPER_CLUSTER_ADVERTISE_ADDRESS`、`SCRAPER_CLUSTER_JOIN` 覆盖配置；多个种子用逗号分隔。
- 非可信网络设置所有实例共用的 `SCRAPER_CLUSTER_SECRET_KEY`，使用 base64 编码的 16、24 或 32 字节 AES 密钥。

## Docker Compose

镜像发布工作流会同时构建并推送 `niuai/azure-metrics-scraper`，支持 `linux/amd64` 和 `linux/arm64`，标签规则与 exporter 一致。发布后可使用 `niuai/azure-metrics-scraper:<tag>`。

也可在本地构建镜像：

```powershell
docker build -f .\scraper\Dockerfile -t azure-metrics-scraper:local .
```

在配置中启用 `cluster: {}`，将 exporter 地址改为容器可访问的地址。将以下片段加入仓库根目录的 Compose 文件，并确保与 exporter 网络互通：

```yaml
services:
  scraper:
    image: azure-metrics-scraper:local
    command: ["--config.file=/etc/scraper.yaml"]
    volumes:
      - ./scraper/scraper.yaml:/etc/scraper.yaml:ro
    environment:
      AZURE_EVENTHUB_CONNECTION_STRING: ${AZURE_EVENTHUB_CONNECTION_STRING:?Set the Event Hub connection string}
      SCRAPER_CLUSTER_JOIN: "scraper:7946"
```

设置连接字符串后运行 `docker compose up -d --scale scraper=2`。不要设置固定 `container_name`。

## Kubernetes 部署

部署文件统一位于仓库根目录的 [deploy](../deploy/README.md)，包含 exporter 和 scraper 两个服务。按[部署指南](../deploy/README.md)配置 workspace、Azure 身份和 Event Hub Secret，然后一次性部署，实现定时采集并推送到 Event Hub。

scraper 的采集参数统一位于 [scraper.yaml](scraper.yaml)，调整 [metrics-scraper.yaml](../deploy/metrics-scraper.yaml) 中 Deployment 的 `replicas` 可修改副本数。
