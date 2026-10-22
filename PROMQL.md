# PromQL 查询与 AKS 控制平面指标

[返回 README](README.md)

本项目的 `/probe/metrics/promql` 端点将 PromQL 即时或范围查询转发到 Azure Monitor 工作区，默认转换为 Prometheus 抓取格式；与平台指标接口一致，可通过 `format=json` 输出完整 JSON 时间窗口。它适用于通过 Azure Monitor 托管 Prometheus 收集的 AKS 控制平面指标。

目录：

* [查询终结点与权限](#endpoint-and-auth)
* [查询参数与结果](#query-parameters)
* [完整时间窗口 JSON](#json-window)
* [AKS 高级控制平面指标](#aks-control-plane)
    * [etcd 专项指标与 job](#etcd-metrics)
* [Windows PowerShell 本地启动](#local-startup)
* [不部署 Prometheus 的定时采集 Pod](scraper/README.md)
* [验证与排查](#verification)
* [官方参考](#references)

<a id="endpoint-and-auth"></a>
## 查询终结点与权限

从 **Azure Monitor 工作区**概述页复制 Prometheus 查询终结点，填写 [workspace 配置文件](config/prometheus-workspaces.example.yaml)，并设置文件路径：

```text
AZURE_PROMETHEUS_WORKSPACES_FILE=<path-to-workspaces.yaml>
```

不要使用 Log Analytics 工作区地址或数据收集终结点（DCE），也不要追加 `/api/v1/query` 或 `/api/v1/query_range`，程序会自动选择路径。

运行导出器所用的 Azure 身份需要在目标 Azure Monitor 工作区上拥有 **Monitoring Data Reader** 角色。导出器会使用现有 Azure 凭据获取 Prometheus API 所需的令牌，无需在 Prometheus job 中配置 Azure 令牌。身份验证方式参见 [README 配置](README.md#configuration)；本地 Azure CLI 登录方式见下方示例。

单个和多个 workspace 统一使用配置文件；单个 workspace 只需保留一个条目：

```yaml
workspaces:
  workspace-a:
    query_endpoint: https://<workspace-a>.<region>.prometheus.monitor.azure.com
  workspace-b:
    query_endpoint: https://<workspace-b>.<region>.prometheus.monitor.azure.com
```

也可使用 `--azure.prometheus.workspaces-file` 指定路径。启动时严格校验别名、重复键、未知字段和 HTTPS origin；修改后需重启。每个目标 workspace 都需要上述读取权限。每次请求必须指定一个 `workspace` 别名，即使只配置一个 workspace；缺失、空值、重复值或未知别名返回 HTTP 400，合法别名请求但服务尚未配置工作区时返回 HTTP 503。别名以 ASCII 字母或数字开头，总长 1–128 字符，后续可包含字母、数字、`.`、`_`、`-`。

不接受请求直接传 endpoint URL；`workspace` 不转发给 Azure，成功响应携带 `X-Azure-Monitor-Workspace` 标头，不修改 JSON 指标标签。旧的 `AZURE_PROMETHEUS_QUERY_ENDPOINT` / `--azure.prometheus.query-endpoint` 已移除，没有默认 endpoint 回退。多个 AKS 集群写入同一 workspace 时仍可用 PromQL 标签筛选。

exporter 的多个副本应配置相同 workspace 映射和权限，并放在同一个 Service 后。独立 scraper 支持[多 workspace 与 memberlist 分片](scraper/README.md#多-workspace-与-memberlist-分片)，在现有进程内发现成员并自动重新分配任务，不需要 Kubernetes API、Redis 或 etcd 协调。

最小抓取示例（放入 Prometheus 的 `scrape_configs`）：

```yaml
- job_name: azure-control-plane
  scrape_interval: 1m
  scrape_timeout: 30s
  honor_timestamps: true
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query: ["apiserver_request_total"]
  static_configs:
  - targets: ["azure-metrics:8080"]
```

`azure-metrics:8080` 是导出器地址，不是 AKS API Server 或工作区地址。使用上述 `30s` 抓取超时时，导出器应设置 `--server.timeout.write=120s`，避免默认 `10s` 写超时提前结束请求；也可根据实际延迟协调调小查询和抓取超时。`scrape_timeout` 不得大于 `scrape_interval`。

<a id="query-parameters"></a>
## 查询参数与结果

也可以在 `/query` Web UI 的 endpoint 下拉框中选择 `/probe/metrics/promql`，填写 `workspace`、`query` 执行即时查询，可用 `time` 指定测试求值时间，或用 `query_delay` 动态延迟求值；范围查询需同时填写 `start`、`end`、`step`，且不能填写 `time` 或 `query_delay`。页面会显示查询响应并生成对应的 Prometheus 抓取配置。固定 `time` 仅用于调试历史结果，不应放入持续监控当前状态的 job。

| GET 参数 | 必填 | 描述 |
|----------|------|------|
| `query` | 是 | PromQL 表达式 |
| `workspace` | 是 | 配置文件中的一个别名，单个 workspace 也必须指定；不接受重复值或 URL |
| `time` | 否 | 即时查询的固定求值时间，支持 RFC3339 或 Unix 秒；未配置 `time` 和 `query_delay` 时使用本次请求开始查询时的时间。不能与 `query_delay` 或范围查询参数同时使用 |
| `query_delay` | 否 | 即时查询的动态延迟，每次请求按当前时间减去该时长求值。使用非负 Go duration，如 `20m`、`1h`、`1h30m`、`30s`；`0s` 等同不延迟。不接受空值、重复值、负数、无单位的非零数字或 `1d`。不能与 `time`、`start`、`end`、`step` 同时使用 |
| `start` | 范围查询必填 | 开始时间，支持 RFC3339 时间或 Unix 时间戳（秒） |
| `end` | 范围查询必填 | 结束时间，格式与 `start` 相同 |
| `step` | 范围查询必填 | 查询步长，支持秒数或时间长度，例如 `60`、`1m` |
| `format` | 否 | 与平台指标接口一致：省略或 `prometheus` 输出原有文本，`json` 输出完整时间窗口。只允许一个非空值，其他值或重复参数返回 HTTP 400 |

不传 `start`、`end`、`step` 时使用 `/api/v1/query` 即时查询；传入其中任一参数时，必须同时提供三个非空参数，并使用 `/api/v1/query_range` 范围查询。参数格式、时间先后顺序和步长有效性由上游 Prometheus API 校验，错误会返回给调用方。

例如，以下参数查询指定一小时内每分钟计算的 API Server 请求速率：

```yaml
params:
  workspace: ["workspace-a"]
  query: ["sum(rate(apiserver_request_total[5m]))"]
  start: ["2026-10-19T12:00:00Z"]
  end: ["2026-10-19T13:00:00Z"]
  step: ["1m"]
```

默认文本模式下，范围查询返回矩阵时，每条时间序列仍只导出上游返回的最后一个数据点，不回填整段历史数据。文本输出默认携带显式时间戳（Unix 毫秒），不再让 Prometheus 使用本次抓取时间。`format=json` 则保留全部返回点，见下节。固定时间范围被重复抓取时，会重复输出相同时间点，不能将其用于持续监控当前状态。

<a id="json-window"></a>
### 完整时间窗口 JSON

在 scraper job 顶层选择 `workspaces`，并在 `params` 中设置 JSON 输出及查询参数：

```yaml
workspaces: ["workspace-a"]
params:
  query: ['sum(rate(apiserver_request_total{cluster="prod"}[5m])) by (cluster, code)']
  start: ["2026-10-21T12:00:00Z"]
  end: ["2026-10-21T12:03:00Z"]
  step: ["15s"]
  format: ["json"]
```

返回 `Content-Type: application/json; charset=utf-8`，字段结构与平台 JSON 一致，`source` 用于区分数据源：

```json
{
  "schemaVersion": "1.0",
  "source": "azure-prometheus",
  "metrics": [
    {
      "name": "azure_promql_query_result",
      "labels": {"cluster": "prod", "code": "200"},
      "samples": [
        ["2026-10-21T12:00:00Z", 12.5],
        ["2026-10-21T12:00:15Z", 13.0]
      ]
    }
  ]
}
```

示例仅展示两个采样点。范围查询保留每条序列的全部浮点采样点，即时查询的 vector/scalar 返回单点，原始范围向量的 matrix 也保留全部点；无数据时 `metrics` 为 `[]`。各序列的样本按时间升序输出，不补零、不插值，JSON 数值不是字符串。

- 时间规则与原接口一致：独立原始选择器额外查询 `timestamp(...)`，按“指标名 + 标签 + 求值时间”逐点匹配源时间；表达式保留上游求值时间。源时间匹配失败会报错，不静默丢点或回退到本地时间。
- 原始样本被多个求值步选中时，JSON 中可能出现重复的源时间戳；不会将其重打为规则的 15 秒网格。时间输出为 UTC RFC3339，PromQL 时间解析精度仍为毫秒。
- JSON 保留原始标签名，例如 `microsoft.resourceid`，不执行文本模式的下划线规范化。结果没有 `__name__` 时仍使用 `azure_promql_query_result`，不会自动推导业务别名。
- `NaN` / `±Inf` 无法表示为标准 JSON number，JSON 模式返回失败，不替换成零；native histogram 样本暂不支持，出现时也明确报错。
- `format` 是 exporter 参数，不转发给 Azure。`query_delay` 仍只支持即时查询；JSON 模式不会放宽它与 `time`、`start`、`end`、`step` 的互斥规则。
- 这里的固定 `start/end` 适合调试历史窗口，不会自动更新。独立 scraper 可通过 job 级别的 [`rolling_range`](scraper/README.md#promql-滚动查询范围) 每次动态生成查询范围，并将 JSON 发送到 Event Hub；`rolling_range` 不是 exporter HTTP 参数，也不能与固定范围或即时查询时间参数同时使用。
- Prometheus 本身不能将此 JSON 当作抓取文本；应由 [scraper](scraper/README.md#promql-json-采集) 等客户端读取。原有 Prometheus jobs 不添加 `format=json` 即可保持不变。

### 入库延迟与动态查询时间

Azure 数据可能尚未入库，导致当前即时查询为空，而历史范围查询有数据。可在 job 中配置 `query_delay`，无需在 PromQL 中添加 `offset`：

```yaml
- job_name: azure-aks-etcd-network
  scrape_interval: 1m
  scrape_timeout: 30s
  honor_timestamps: true
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query:
    - 'sum(rate(etcd_network_client_grpc_sent_bytes_total{cluster="sk-rundeck"}[1m]))'
    query_delay: ["20m"]
  static_configs:
  - targets: ["azure-metrics:8080"]
```

此示例的集群标签和 `20m` 需按实际环境调整；使用 `30s` 抓取超时时，仍需协调导出器写超时，例如 `--server.timeout.write=120s`。Web UI 也可填写 `query_delay`，但必须清空 `time`、`start`、`end` 和 `step`。

例如在 `00:30` 抓取时，导出器向上游发送 `time=00:10`；下一分钟抓取自动推进到 `00:11`。`query_delay` 是导出器参数，不转发给 Azure。原始选择器的值查询和源时间戳查询共用同一个延迟后的求值时间。

- 原始指标仍输出源采样时间；`rate`、`sum` 等表达式输出延迟后的求值时间，不标记为当前抓取时间。它与只在表达式中添加 `offset` 不同；不要为同一延迟同时配置两者。
- 保持 `honor_timestamps: true`，不要将历史值重打为当前时间。未配置 `query_delay` 的 job 行为不变；非法或冲突参数返回 HTTP 400。
- 延迟值不是自动探测的，应留出实际摄取延迟的安全余量。超过该余量仍可能返回空结果；空结果不补零，也不复用旧值，不保证历史补采。
- Prometheus 默认即时查询回看窗口通常为 5 分钟；20 分钟前的样本即使成功入库，也不会自动出现在当前即时查询中。历史图和告警必须考虑此延迟，例如对本地结果使用相应的查询偏移，并明确展示数据滞后；不要用空值补零掩盖缺失。

### 采样时间（默认启用）

默认使用上游时间，而不是本地抓取时间。以下说明针对默认文本模式，Prometheus 保持 `honor_timestamps: true`（默认值）即可；JSON 完整窗口模式见[上节](#json-window)。

| 查询形式 | 输出时间 |
|----------|----------|
| 独立原始指标，如 `apiserver_request_total`、`etcd_server_has_leader{cluster="prod"}`，以及固定指标名列表 | 原始采样时间；额外请求一次 `timestamp(...)` |
| 即时查询的原始范围向量，如 `apiserver_request_total[5m]` | 最后一个原始样本的时间 |
| `rate(...)`、`sum(...)` 等其他表达式 | 上游查询求值时间 |

例如：`10:02` 查到 `10:00` 的原始样本，会按 `10:00` 输出，而不是标记为 `10:02`。

注意：
- 带括号、`offset`、`@` 等表达式不自动追溯原始采样时间；范围查询仍只输出最后一点。
- 两次请求使用相同求值时间，但不是事务快照。获取时间失败或匹配失败会报错，不回退到本地时间。
- 不提供历史补采；旧样本可能不再显示，乱序或同时间不同值可能被拒绝。
- Azure 平台指标也保留源时间，缓存不刷新时间，见 [平台指标源时间戳](README.md#源时间戳)；`/metrics` 自身指标不变。

查询结果中的 `__name__` 会作为导出的指标名称；如果结果没有指标名（例如聚合表达式），则使用 `azure_promql_query_result`。端点支持数值型即时向量、标量和范围矩阵结果，不支持字符串或原生直方图结果；所有结果以 gauge 导出。

为兼容传统 Prometheus 文本格式，上游标签名中的非 ASCII 字母、数字、下划线字符会逐个替换为 `_`，例如 `microsoft.resourceid` 导出为 `microsoft_resourceid`。数字开头的名称会加 `_` 前缀，空名称转换为 `_`；合法名称和标签值保持不变。若同一时间序列中两个标签转换后重名（例如同时包含 `microsoft.resourceid` 和 `microsoft_resourceid`），或转换后名称为保留的 `__name__`，查询会明确报错，不会覆盖或丢弃标签。此规则适用于即时和范围查询，不改变指标名称的校验。

现有 Azure Metrics 的 `timespan`、`interval`、`aggregation`、`name`、`metric`、`subscription` 等参数不用于此端点。聚合、指标选择和资源筛选应在 PromQL 表达式中指定。Azure Monitor 托管 Prometheus 还要求时序查询包含指标名称选择器，其他限制请参阅[官方 PromQL API 文档](https://learn.microsoft.com/azure/azure-monitor/metrics/prometheus-api-promql#api-limitations)。

### 固定指标名列表兼容

导出器支持将独立选择器中的固定指标名列表转换为 Azure 可接受的显式指标名查询。例如：

```promql
{__name__=~"etcd_server_has_leader|etcd_server_proposals_pending",cluster="example"}
```

发送给 Azure 的查询为：

```promql
etcd_server_has_leader{cluster="example"}
or on (__name__) etcd_server_proposals_pending{cluster="example"}
```

其他标签条件原样保留；相同指标名重复出现时只展开一次。即时和范围查询均适用，不额外请求指标名称元数据。`on (__name__)` 防止不同指标因为其他标签相同而被普通 `or` 丢弃。

此兼容仅支持整个查询为独立 `{...}` 选择器、使用双引号 `__name__=~"metric_a|metric_b"` 的形式。列表项必须是完整合法的指标名；通配符、分组、锚点及空分支（例如 `etcd_.+`、`(a|b)`、`^a$`、`a|`）会明确返回 HTTP 400。嵌入 `sum(...)`、`rate(...)` 等表达式、带指标名前缀的选择器及其他写法不做转换，仍由 Azure 校验；这些场景请显式编写各指标查询或拆分 job。本功能不保证上游支持所有多指标表达式，也不改变 Azure 自身的限制。

<a id="aks-control-plane"></a>
## AKS 高级控制平面指标

### 1. 区分平台指标与高级控制平面指标

AKS 默认提供的部分 Azure Monitor 平台指标，可以通过原有 `/probe/metrics` 或 `/probe/metrics/resource` 等端点读取。API Server、etcd、Scheduler、Controller Manager、Cluster Autoscaler 的详细 Prometheus 指标，则需要走以下链路：

```text
AKS 控制平面 -> Azure Monitor 托管 Prometheus -> Azure Monitor 工作区
                                                        |
                                                 PromQL 查询 API
                                                        |
                                              azure-metrics-exporter
                                                        |
                                            自建 Prometheus scrape job
```

仅添加导出器 job 不会开启 AKS 侧的采集，也不能通过 Azure Metrics 的 `metricNamespace` 切换出这些指标。本项目是查询转发端，不会直接抓取托管控制平面内部的 `/metrics`。

### 2. 在 AKS 侧启用采集

前提：AKS 使用托管身份，且已启用 Azure Monitor 托管 Prometheus 并关联目标 Azure Monitor 工作区。当前官方控制平面指标文档列明该功能不支持 Azure Private Link；部署前应确认适用区域、网络和服务限制。

已有托管 Prometheus 的集群，执行：

```powershell
az aks update --resource-group "<aks-resource-group>" --name "<aks-name>" --enable-control-plane-metrics
```

若尚未启用托管 Prometheus，可先关联工作区，再启用控制平面指标：

```powershell
az aks update --resource-group "<aks-resource-group>" --name "<aks-name>" --enable-azure-monitor-metrics --azure-monitor-workspace-resource-id "<azure-monitor-workspace-resource-id>"
az aks update --resource-group "<aks-resource-group>" --name "<aks-name>" --enable-control-plane-metrics
```

以上命令会修改集群配置，应由具备相应权限的管理员执行。采集启用后，等待数分钟，再在目标工作区查询确认。

### 3. 启用额外组件与指标

默认控制平面目标为 `apiserver` 和 `etcd`，且默认启用最小摄取配置（minimal ingestion profile）。Scheduler、Controller Manager 和 Cluster Autoscaler 需要显式启用；仅修改 Prometheus job 的查询不会让缺失的指标自动入库。

使用官方 [ama-metrics-settings-configmap.yaml 模板](https://github.com/Azure/prometheus-collector/blob/main/otelcollector/configmaps/ama-metrics-settings-configmap.yaml)。以下为 **schema v2** 的相关配置示例，展示如何采集所选组件的全部可用指标：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ama-metrics-settings-configmap
  namespace: kube-system
data:
  schema-version: v2
  config-version: ver1
  controlplane-metrics: |-
    default-targets-scrape-enabled: |-
      apiserver = true
      etcd = true
      kube-scheduler = true
      kube-controller-manager = true
      cluster-autoscaler = true
    minimal-ingestion-profile: |-
      enabled = false
```

如果集群已有此 ConfigMap，请将上述 `controlplane-metrics` 内容合并到现有配置，保留其他设置（例如 `cluster-metrics`、集群别名和已有 keep list），不要直接覆盖整个 ConfigMap。旧 schema v1 应按官方文档迁移到 v2，不要混用旧的 `controlplane-*` 键名。

将合并后的完整配置保存为 `configmap-controlplane.yaml`，再应用到正确的集群上下文：

```powershell
kubectl config current-context
kubectl apply -f .\configmap-controlplane.yaml
```

关闭 minimal ingestion 且没有目标 keep list 时，会摄取所选目标的全部可用指标，可能显著增加时序数量和费用。生产环境通常建议保持 `enabled = true`，并在 `controlplane-metrics` 内追加按目标划分的 keep list，只补充所需指标。例如为 API Server 增补请求计数和经典直方图：

```yaml
    default-targets-metrics-keep-list: |-
      apiserver = "apiserver_request_total|apiserver_request_duration_seconds_bucket|apiserver_request_duration_seconds_sum|apiserver_request_duration_seconds_count"
```

此片段应与上面的 `minimal-ingestion-profile` 同级；修改已有 keep list 时应合并指标，而不是删除原有条目。经典直方图需保留整个 `_bucket`、`_sum`、`_count` 家族，不能只配置不带后缀的基名。其他目标按同样方式配置 `etcd`、`kube-scheduler`、`kube-controller-manager` 或 `cluster-autoscaler`。实际可用指标取决于 AKS/Kubernetes 版本和已启用功能，例如未使用 Cluster Autoscaler 的集群不能依靠打开采集开关产生其业务指标。

### 4. 配置导出器与 Prometheus job

按[查询终结点与权限](#endpoint-and-auth)配置导出器。下例不限定集群，查询当前工作区内所有已有对应指标的集群；聚合表达式保留 `cluster` 分组，分别输出各集群的结果。请确认上游存在能区分集群的 `cluster` 标签，且不同集群未使用相同别名，避免聚合时混合数据。此处不跨工作区，也不会自动启用尚未配置的 AKS 侧采集。

以下可直接作为 `scrape_configs` 的列表项使用，无需配置 `start`、`end`、`step`：

```yaml
- job_name: azure-aks-control-plane-selected
  scrape_interval: 1m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query:
    - '{__name__=~"apiserver_current_inflight_requests|etcd_server_has_leader|scheduler_pending_pods|workqueue_depth|cluster_autoscaler_unschedulable_pods_count"}'
  static_configs:
  - targets: ["azure-metrics:8080"]
  metric_relabel_configs:
  - source_labels: [__name__]
    target_label: metric
  - target_label: __name__
    replacement: azure_aks_control_plane

- job_name: azure-aks-apiserver-request-rate
  scrape_interval: 1m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query:
    - 'sum by (cluster, code) (rate(apiserver_request_total[5m]))'
  static_configs:
  - targets: ["azure-metrics:8080"]
  metric_relabel_configs:
  - source_labels: [__name__]
    regex: azure_promql_query_result
    target_label: __name__
    replacement: azure_aks_apiserver_requests_per_second

- job_name: azure-aks-apiserver-latency-p99
  scrape_interval: 1m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query:
    - 'histogram_quantile(0.99, sum by (cluster, le) (rate(apiserver_request_duration_seconds_bucket{verb!~"WATCH|WATCHLIST|CONNECT"}[5m])))'
  static_configs:
  - targets: ["azure-metrics:8080"]
  metric_relabel_configs:
  - source_labels: [__name__]
    regex: azure_promql_query_result
    target_label: __name__
    replacement: azure_aks_apiserver_request_duration_seconds_p99
```

| job | 用途 | 本地导出指标 |
|-----|------|--------------|
| `azure-aks-control-plane-selected` | 选取 API Server 并发请求、etcd leader 状态、待调度 Pod、控制器队列深度及不可调度 Pod 数 | `azure_aks_control_plane`，原始指标名保存在 `metric` 标签 |
| `azure-aks-apiserver-request-rate` | 按集群和 HTTP 状态码统计最近 5 分钟平均 QPS | `azure_aks_apiserver_requests_per_second` |
| `azure-aks-apiserver-latency-p99` | 排除长连接请求后，统计集群级最近 5 分钟请求延迟 P99（秒） | `azure_aks_apiserver_request_duration_seconds_p99` |

注意：

* 多指标 job 先将原始 `__name__` 保存到 `metric` 标签，再统一重命名；不要颠倒这两条 relabel 规则。Grafana 中先选择 `azure_aks_control_plane`，再通过 `metric` 标签下拉框选择原始指标，例如 `azure_aks_control_plane{metric="apiserver_current_inflight_requests"}`。这由 Prometheus 抓取后的 relabel 实现，不需要添加不生效的 `params.name`。规则会覆盖已有的 `metric` 标签，使用前请确认上游没有需要保留的同名标签。不同原始指标的含义和单位可能不同，应按 `metric` 筛选后展示或计算，不能直接求和。
* 第一项只是所需指标的白名单示例，不是采集全部控制平面指标；请先确认这些指标已经入库。多指标 job 使用固定指标名列表，由新版导出器展开为显式指标名及 `or on (__name__)`；请先重新编译并重启导出器，再使用此写法，直接提交给 Azure 仍可能报错。`on (__name__)` 按指标名匹配，避免不同指标具有相同其他标签时，被普通 `or` 当作匹配项而丢失右侧指标。一个 job 使用一个 `query` 值，不能通过重复 `query` 参数执行多条独立查询；不同聚合应拆分 job。
* `workqueue_depth` 可能来自多个组件，诊断时结合返回的上游 `job` 等标签区分。Prometheus 默认遇到标签冲突时会将上游 `job`、`instance` 改为 `exported_job`、`exported_instance`，本地 `job`、`instance` 表示导出器抓取目标。
* 所有输出都是 gauge。计数器的 `rate()`、直方图的 `histogram_quantile()` 建议在上游查询中计算，不要将本地转发后的数据误当成原生 counter/histogram 再处理。尤其不要对已经导出的 QPS 再调用 `rate()`。
* 聚合表达式一般没有 `__name__`，因此上例用 `metric_relabel_configs` 将默认名称改成有业务含义的指标名。不要为不同聚合复用同名且同标签的输出。
* 尽量先在上游按所需维度聚合，避免一次导出全部 API 请求标签或所有 histogram bucket。导出器单次上游响应上限为 16 MiB，过大的查询会失败。
* 抓取周期不是 AKS 侧采集周期。提高本地抓取频率不会提高上游数据的刷新速度；最近 5 分钟的速率查询还需要该窗口内有足够样本。

<a id="etcd-metrics"></a>
### 5. etcd 专项指标与 job

以下示例覆盖 etcd 的 leader 状态、提案积压、数据库容量、leader 切换、提案失败和磁盘延迟。指标名遵循 etcd 的 Prometheus 指标定义，**不表示每个 AKS 版本都会暴露全部指标**。请先在目标工作区或导出器 `/query` 页面执行以下即时查询，确认实际可用的指标及标签：

```promql
count by (cluster, __name__) (
  etcd_server_has_leader
  or on (__name__) etcd_server_proposals_pending
  or on (__name__) etcd_mvcc_db_total_size_in_bytes
  or on (__name__) etcd_mvcc_db_total_size_in_use_in_bytes
  or on (__name__) etcd_server_quota_backend_bytes
)
```

`start`、`end`、`step` 留空。此查询按集群列出上述五个指标中当前可查询到的指标及其时序数量，并非枚举全部 etcd 指标。验证下方速率和延迟 job 时，可分别查询对应的原始指标名。若某项缺失，先检查 `etcd = true`、keep list 和摄取延迟。AKS 未暴露的指标不能通过添加 job 或 keep list 强行开启。

如需发现工作区中的全部指标名，可直接调用 Azure Prometheus API 的 `/api/v1/label/__name__/values`，再在客户端筛选 `etcd_` 前缀；本导出器的 `/probe/metrics/promql` 不代理该元数据接口。不要使用指标名正则来替代元数据查询。

#### 采集白名单

若保持 minimal ingestion 开启，将下列 `etcd` 配置合并到前述 `controlplane-metrics.default-targets-metrics-keep-list` 中，与已有的 `apiserver` 等条目并列：

```yaml
    default-targets-metrics-keep-list: |-
      etcd = "etcd_server_has_leader|etcd_server_proposals_pending|etcd_mvcc_db_total_size_in_bytes|etcd_mvcc_db_total_size_in_use_in_bytes|etcd_server_quota_backend_bytes|etcd_server_leader_changes_seen_total|etcd_server_proposals_failed_total|etcd_disk_wal_fsync_duration_seconds_bucket|etcd_disk_wal_fsync_duration_seconds_sum|etcd_disk_wal_fsync_duration_seconds_count|etcd_disk_backend_commit_duration_seconds_bucket|etcd_disk_backend_commit_duration_seconds_sum|etcd_disk_backend_commit_duration_seconds_count"
```

不要为这段示例重复创建 `default-targets-metrics-keep-list` 键，也不要覆盖已有白名单。已关闭 minimal ingestion 且未设置 etcd keep list 时，无需再添加此白名单。

#### Prometheus job

将以下列表项追加到 `scrape_configs`，导出器地址、工作区权限和超时设置与前述示例一致：

```yaml
- job_name: azure-aks-etcd-health-capacity
  scrape_interval: 1m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query:
    - '{__name__=~"etcd_server_has_leader|etcd_server_proposals_pending|etcd_mvcc_db_total_size_in_bytes|etcd_mvcc_db_total_size_in_use_in_bytes|etcd_server_quota_backend_bytes"}'
  static_configs:
  - targets: ["azure-metrics:8080"]
  metric_relabel_configs:
  - source_labels: [__name__]
    target_label: metric
  - target_label: __name__
    replacement: azure_aks_etcd_health_capacity

- job_name: azure-aks-etcd-leader-changes
  scrape_interval: 1m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query:
    - 'max by (cluster) (increase(etcd_server_leader_changes_seen_total[15m]))'
  static_configs:
  - targets: ["azure-metrics:8080"]
  metric_relabel_configs:
  - source_labels: [__name__]
    regex: azure_promql_query_result
    target_label: __name__
    replacement: azure_aks_etcd_leader_changes_15m

- job_name: azure-aks-etcd-proposals-failed
  scrape_interval: 1m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query:
    - 'max by (cluster) (rate(etcd_server_proposals_failed_total[5m]))'
  static_configs:
  - targets: ["azure-metrics:8080"]
  metric_relabel_configs:
  - source_labels: [__name__]
    regex: azure_promql_query_result
    target_label: __name__
    replacement: azure_aks_etcd_proposals_failed_per_second

- job_name: azure-aks-etcd-wal-fsync-p99
  scrape_interval: 1m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query:
    - 'max by (cluster) (histogram_quantile(0.99, sum by (cluster, instance, le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket[5m]))))'
  static_configs:
  - targets: ["azure-metrics:8080"]
  metric_relabel_configs:
  - source_labels: [__name__]
    regex: azure_promql_query_result
    target_label: __name__
    replacement: azure_aks_etcd_wal_fsync_duration_seconds_p99_max

- job_name: azure-aks-etcd-backend-commit-p99
  scrape_interval: 1m
  scrape_timeout: 30s
  metrics_path: /probe/metrics/promql
  params:
    workspace: ["workspace-a"]
    query:
    - 'max by (cluster) (histogram_quantile(0.99, sum by (cluster, instance, le) (rate(etcd_disk_backend_commit_duration_seconds_bucket[5m]))))'
  static_configs:
  - targets: ["azure-metrics:8080"]
  metric_relabel_configs:
  - source_labels: [__name__]
    regex: azure_promql_query_result
    target_label: __name__
    replacement: azure_aks_etcd_backend_commit_duration_seconds_p99_max
```

#### 结果解读

`azure-aks-etcd-health-capacity` 同样使用统一名称加原始指标标签：Grafana 的 Metric 选择 `azure_aks_etcd_health_capacity`，Label filters 选择 `metric`，即可在下拉框中选择已采集到的原始指标。例如：

```promql
azure_aks_etcd_health_capacity{metric="etcd_server_has_leader"}
```

下表前五项是 `metric` 标签中的原始指标名，不再是该 job 在本地 Prometheus 中的指标名称。`/query` 页面直接访问导出器时仍显示原始名称，因为 relabel 只在 Prometheus 抓取入库时执行；修改配置后需重新加载 Prometheus，并等待下一次成功抓取。

| 指标或 job | 含义与关注点 |
|-----------|--------------|
| `etcd_server_has_leader` | 每个成员是否能识别 leader：`1` 表示存在，`0` 表示没有；不是该成员是否为 leader，也不能仅据此判断整个集群健康 |
| `etcd_server_proposals_pending` | 当前等待提交的提案数，持续升高时关注负载和提交延迟 |
| `etcd_mvcc_db_total_size_in_bytes` | 数据库物理大小（字节），包含可回收空间 |
| `etcd_mvcc_db_total_size_in_use_in_bytes` | 数据库实际使用大小（字节），可与物理大小对比观察空间利用情况 |
| `etcd_server_quota_backend_bytes` | 后端数据库配额（字节）；与同一成员的数据库大小对比，不要将副本大小相加作为集群容量 |
| `azure-aks-etcd-leader-changes` | 各成员最近 15 分钟所见 leader 切换次数的最大值；频繁增长时关注稳定性 |
| `azure-aks-etcd-proposals-failed` | 各成员最近 5 分钟提案失败速率的最大值，单位为次/秒 |
| 两个磁盘 P99 job | 先计算各成员最近 5 分钟 WAL fsync / backend commit 延迟 P99，再取集群内最大值，单位为秒，避免合并所有成员后掩盖慢成员 |

这里假设上游 `instance` 能区分 etcd 成员。请先检查实际标签；若使用其他成员标识，替换两个 P99 查询的分组字段。若 AKS 未提供成员维度，查询只能计算暴露时序的 P99，不能将其解读为最慢成员 P99。排查具体成员时，可以移除外层 `max by (cluster)`，保留成员维度。

leader 切换可能被多个成员同时观察到，因此示例取最大值而不是将各成员计数相加；它是观测指标，不是严格的集群级事件审计。`increase()` 有窗口边界外推，可能返回小数。没有磁盘操作样本时，P99 可能为 `NaN`，不应改成 `0` 来表示健康。

第一项保留了与通用示例重叠的 `etcd_server_has_leader`；若同时启用两个 job，可从通用示例的选择器中移除该指标，避免重复存储。所有速率、增量和分位数均已在上游计算，本地输出仍为 gauge。

<a id="local-startup"></a>
## Windows PowerShell 本地启动

前置条件：安装 Go（版本满足 [go.mod](go.mod) 的要求）和 Azure CLI。以下命令在项目根目录执行，订阅 ID 和查询终结点需要替换为自己的值。

```powershell
# 登录 Azure；如果已登录，可以跳过
az login

# 选择订阅
az account set --subscription "<subscription-id>"

# 使用本机 Azure CLI 登录凭据
$env:AZURE_AUTH = "azcli"

# 先替换配置文件中的占位符；单个 workspace 只保留 workspace-a 条目
$env:AZURE_PROMETHEUS_WORKSPACES_FILE = (Resolve-Path ".\config\prometheus-workspaces.example.yaml").Path

# 编译到当前目录
go build -o azure-metrics-exporter.exe .

# 前台启动，仅监听本机
.\azure-metrics-exporter.exe --server.bind=127.0.0.1:8080 --server.timeout.write=120s
```

登录身份需要目标工作区上的 **Monitoring Data Reader** 角色。也可以在设置好上述环境变量后，直接从源码启动，替代编译和执行两个步骤：

```powershell
go run . --server.bind=127.0.0.1:8080 --server.timeout.write=120s
```

按 **Ctrl+C** 停止前台进程。如果 8080 端口被占用，可改为 8081，并同步修改验证地址。环境变量仅对当前 PowerShell 会话及其启动的子进程生效，程序不会自动读取 `.env` 文件。修改源码后，使用可执行文件启动时需要重新编译并重启。

<a id="verification"></a>
## 验证与排查

启动后，在另一个 PowerShell 窗口中验证：

```powershell
# 健康检查，预期返回 Ok
Invoke-RestMethod "http://127.0.0.1:8080/healthz"

# PromQL 即时查询，返回 up 指标的时间序列数量
Invoke-RestMethod "http://127.0.0.1:8080/probe/metrics/promql?workspace=workspace-a&query=count%28up%29"

# 验证当前工作区所有集群的控制平面指标
$query = [Uri]::EscapeDataString('apiserver_request_total')
Invoke-RestMethod "http://127.0.0.1:8080/probe/metrics/promql?workspace=workspace-a&query=$query"

# 最近 10 分钟的范围查询，步长为 60 秒，每条序列只导出最后一个数据点
$end = [DateTimeOffset]::UtcNow
$start = $end.AddMinutes(-10)
$uri = "http://127.0.0.1:8080/probe/metrics/promql?workspace=workspace-a&query=count%28up%29&start=$($start.ToUnixTimeSeconds())&end=$($end.ToUnixTimeSeconds())&step=60"
Invoke-RestMethod $uri
```

* **返回空结果**：先在工作区直接执行相同查询，并对齐求值时间。如果历史有数据而当前为空，检查最新源采样时间，区分摄取延迟与采集中断；可配置 `query_delay` 动态延迟求值，但它不保证历史补采。检查 AKS 是否关联了正确工作区、控制平面开关、目标组件、minimal ingestion/keep list、实际集群标签及采集延迟。`up` 存在不代表控制平面指标已经启用；不要用 `or vector(0)` 掩盖缺失数据。
* **上游 HTTP 501，提示 `Metric name only support equality(=) filter`**：当前 Azure 查询接口不支持所用的指标名正则筛选。新版导出器会展开独立选择器中的固定指标名列表，更新程序需要重新编译并重启；也可以不更新程序，直接使用上述显式指标名加 `or on (__name__)` 的查询。先用单一显式指标名验证，若组合查询也被上游限制，则拆成每个 job 一个显式指标名。这不是未开启控制平面采集的证据。
* **401/403 或令牌获取错误**：检查导出器实际使用的身份、工作区级角色分配、登录租户以及角色生效时间。导出器可能将上游认证失败作为 HTTP 502 返回，具体原因查看响应正文和日志。
* **超时**：检查导出器到查询终结点的网络连通性，并协调 `scrape_timeout` 与 `--server.timeout.write`。不要只提高超时，应先缩小查询范围、减少标签维度。
* **Prometheus 抓取成功但业务指标缺失**：检查返回正文以及本地 `metric_relabel_configs`；空查询结果也可能是成功响应，不能只依赖本地 `up{job="..."}` 判断上游采集正常。

<a id="references"></a>
## 官方参考

* [监控 AKS 控制平面指标：启用方式、限制和采集配置](https://learn.microsoft.com/azure/aks/control-plane-metrics-monitor)
* [为 AKS 启用 Prometheus 和 Grafana](https://learn.microsoft.com/azure/azure-monitor/containers/kubernetes-monitoring-enable)
* [Azure Monitor 默认 Prometheus 指标与最小摄取配置](https://learn.microsoft.com/azure/azure-monitor/containers/prometheus-metrics-scrape-default)
* [通过 ConfigMap 自定义 Prometheus 采集](https://learn.microsoft.com/azure/azure-monitor/containers/prometheus-metrics-scrape-configuration)
* [Azure Monitor 工作区 PromQL API](https://learn.microsoft.com/azure/azure-monitor/metrics/prometheus-api-promql)
* [etcd 官方指标说明：Server 与 Disk](https://etcd.io/docs/v3.6/metrics/)
