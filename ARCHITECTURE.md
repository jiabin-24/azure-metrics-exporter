# Azure Monitor metrics exporter 架构说明

将 Azure Monitor 平台指标和托管 Prometheus 指标转换为 Prometheus 文本或 JSON。启动方式见 [README](README.md)，PromQL、定时采集和部署分别见对应文档。

## 数据流

```text
Prometheus / HTTP 客户端
        |
        v
exporter：身份认证 -> 资源发现 / workspace 解析 -> Azure 查询 -> 指标转换与缓存
        |                                      |
        v                                      v
Azure Monitor Metrics API              workspace Prometheus API
        |
        v
响应：Prometheus 文本（每序列最后一点）/ JSON（完整查询窗口）
```

- **平台指标**：按订阅和区域批量查询，或通过资源 ID、资源列表、资源标记、Resource Graph 发现资源后查询；支持聚合、维度和命名模板。
- **PromQL**：通过配置的 workspace 终结点执行即时或范围查询，详见 [PromQL](PROMQL.md)。
- **缓存**：分别缓存服务发现结果和指标响应，减少 Azure API 调用；命中缓存不会刷新源时间戳。
- **独立 scraper**：定时调用 exporter，采集 JSON 并通过 SAS 认证发送到 Event Hubs；使用 memberlist 和 Rendezvous Hashing 分配任务，不持久化数据，成员收敛时可能短暂漏采或重复。详见 [scraper](scraper/README.md)。

<a id="configuration"></a>
## 配置与认证

平台查询参数主要配置在 Prometheus job 中；Azure 身份、workspace 映射和服务运行参数由 exporter 配置。

| 启动参数 | 环境变量 | 默认值 / 用途 |
|---|---|---|
| `--azure-environment` | `AZURE_ENVIRONMENT` | `AZUREPUBLICCLOUD`，也支持政府云、中国云 |
| `--azure-ad-resource-url` | `AZURE_AD_RESOURCE` | 默认使用 Resource Manager 终结点 |
| `--azure.prometheus.workspaces-file` | `AZURE_PROMETHEUS_WORKSPACES_FILE` | `/etc/azure-metrics-exporter/workspaces.yaml`；空值禁用 |
| `--azure.servicediscovery.cache` | `AZURE_SERVICEDISCOVERY_CACHE` | `30m`；`0` 禁用服务发现缓存 |
| `--azure.resource-tag` | `AZURE_RESOURCE_TAG` | `owner`，空格分隔资源标记 |
| `--metrics.template` | `METRIC_TEMPLATE` | `{name}` |
| `--metrics.help` | `METRIC_HELP` | `Azure monitor insight metric` |
| `--metrics.dimensions.lowercase` | `METRIC_DIMENSIONS_LOWERCASE` | 将维度值转为小写 |
| `--concurrency.subscription` | `CONCURRENCY_SUBSCRIPTION` | `5`，订阅查询并发 |
| `--concurrency.subscription.resource` | `CONCURRENCY_SUBSCRIPTION_RESOURCE` | `10`，订阅内资源请求并发 |
| `--enable-caching` | `ENABLE_CACHING` | 启用内部指标缓存 |
| `--server.bind` | `SERVER_BIND` | `:8080` |
| `--server.timeout.read` | `SERVER_TIMEOUT_READ` | `5s` |
| `--server.timeout.write` | `SERVER_TIMEOUT_WRITE` | `10s`；大窗口查询需适当增大 |

日志参数及完整选项运行 `go run . --help` 查看。

- 本地可使用 Azure CLI 登录；其他认证方式见 [Azure SDK 认证](https://learn.microsoft.com/azure/developer/go/azure-sdk-authentication)及 [go-common 配置](https://github.com/webdevops/go-common/blob/main/azuresdk/README.md)。
- 读取资源信息需相应资源读取权限；PromQL 查询身份需目标 workspace 的 **Monitoring Data Reader** 权限。
- workspace 文件格式及加载规则见 [快速启动](README.md#快速启动)。

<a id="http-endpoints"></a>
## HTTP 接口

| 端点 | 用途 / 选择资源的参数 |
|---|---|
| `/metrics` | exporter、Go 运行时及 Azure API 请求统计 |
| `/probe/metrics` | 按订阅和区域查询；必填 `subscription`、`resourceType`，可选多值 `region`，省略区域时通过 Resource Graph 发现 |
| `/probe/metrics/resource` | 按资源查询；必填多值 `subscription`、`target`（资源 URI） |
| `/probe/metrics/list` | 资源列表发现；必填 `subscription` 及 `resourceType` 或 `filter` |
| `/probe/metrics/scrape` | 从资源标记读取指标配置；在 list 参数基础上必填 `metricTagName`、`aggregationTagName` |
| `/probe/metrics/resourcegraph` | Resource Graph 发现；必填 `subscription`、`resourceType`，可选 `filter`（附加 Kusto 查询） |
| `/probe/metrics/promql` | workspace PromQL 查询，参数见 [PromQL](PROMQL.md) |
| `/query` | 调试 Web UI，需启用 `--development.webui`；可生成抓取配置 |

订阅模式每个订阅和区域发送一次指标请求；按资源查询时，每批最多请求 20 个指标名称。

### 平台接口公共参数

| 参数 | 默认值 / 说明 |
|---|---|
| `timespan` | `PT1M`，查询时间范围 |
| `interval` | 指标时间粒度 |
| `metricNamespace` | 指标命名空间；订阅模式单值，其余平台接口支持多值 |
| `metric` | 指标名称，支持多值 |
| `aggregation` | `minimum`、`maximum`、`average`、`total`、`count` 等聚合，支持多值 |
| `name` | `azurerm_resource_metric`，输出指标名称 |
| `metricFilter` | 维度筛选；订阅模式最多支持 2 个条件，另一个条件用于按资源 ID 拆分 |
| `metricTop` / `metricOrderBy` | 维度数量限制 / 排序 |
| `validateDimensions` | `true`；设为 `false` 时忽略无效筛选值 |
| `cache` | 内部指标缓存时长，默认与 `timespan` 相同 |
| `template` / `help` | 覆盖全局指标名称 / 帮助信息模板 |
| `format` | `prometheus` 或 `json`；默认 Prometheus 文本 |

多值参数可重复传入或使用逗号分隔。指标可用性、聚合方式和实际时间粒度以 Azure API 为准。

<a id="metrics"></a>
## 输出规则

### 源时间戳

- 平台文本输出保留 Azure `TimeStamp`（Unix 毫秒），每个指标名和标签组合只输出最后一个非空聚合值；缓存命中不改写时间。
- 缺少有效时间戳的数据点会记录错误并跳过，不使用本地时间代替；`/metrics` 自身指标不受影响。
- Prometheus 保持 `honor_timestamps: true`（默认）。时间表示 Azure 聚合桶，不是业务事件发生时间；保留源时间不等于历史补采。
- 旧样本可能超出查询回看窗口；乱序或同一时间戳的修订值可能被拒绝，应配套数据新鲜度告警。
- PromQL 原始选择器使用源采样时间，计算表达式使用求值时间；入库延迟可通过即时查询的 `query_delay` 处理，详见 [PromQL 采样时间](PROMQL.md#采样时间默认启用)。

### 指标与标签

| 指标 | 用途 |
|---|---|
| `azurerm_resource_metric` | 平台资源指标，可通过 `name` 和模板修改 |
| `azurerm_stats_metric_collecttime` | 采集统计 |
| `azurerm_stats_metric_requests` | 资源指标请求及结果计数 |
| `azurerm_api_ratelimit` | Azure API 速率限制，仅 `/metrics` 提供，每次查询后重置 |
| `azurerm_api_request_*` | Azure API 请求数量和延迟 |

资源标记及 AzureTracing 规则见 [go-common 文档](https://github.com/webdevops/go-common/blob/main/azuresdk/README.md)。

<a id="metric-name-and-help-template-system"></a>
### 命名模板

默认模板为 `{name}`，建议使用 `{name}_{metric}_{aggregation}_{unit}`。模板中使用的字段不再作为标签输出。

| 变量 | 含义 |
|---|---|
| `{name}` | 请求指定的指标名称 |
| `{type}` | ResourceType 或 MetricNamespace，部分接口可用 |
| `{metric}` / `{dimension}` | Azure 指标名称 / 维度值 |
| `{unit}` / `{aggregation}` | 单位 / 聚合方式 |
| `{interval}` / `{timespan}` | 请求的时间粒度 / 范围 |

例如 `name=azure_keyvault`、`metric=Availability`、`aggregation=average` 使用建议模板可生成 `azure_keyvault_availability_average_percent`。帮助信息同样支持模板，如 `Azure metrics for {metric} with aggregation {aggregation} as {unit}`。

<a id="platform-window-json"></a>
### 平台指标完整时间窗口 JSON

所有平台 probe 接口支持 `format=json`，使用相同的资源发现、查询、模板和标签配置，但输出窗口内全部数据点，而非每序列最后一点。

```json
{
  "schemaVersion": "1.0",
  "source": "azure-monitor",
  "metrics": [
    {
      "name": "azurerm_resource_metric",
      "labels": {
        "metric": "node_cpu_usage_percentage",
        "aggregation": "average",
        "resourceID": "/subscriptions/.../managedClusters/example"
      },
      "samples": [
        ["2026-10-21T14:00:00Z", 20.5],
        ["2026-10-21T14:01:00Z", 25.0]
      ]
    }
  ]
}
```

示例省略部分标签。关键约定：

- 按完整的“指标名 + 标签组合”分组，序列排序确定，样本按源时间升序；UTC RFC3339 时间支持亚秒精度，值为 JSON number，无数据时 `metrics: []`。
- 保留全部返回点，不补零、不插值、不改写时间。Azure 可自动调整粒度，标签中的 `interval` 仅代表请求值。
- `format` 仅允许一个有效值；空值、重复值或不支持的格式返回 HTTP 400。
- 缺失聚合值不补零；缺失时间戳、非有限值、查询或资源发现失败时返回失败，不返回部分窗口。采集失败通常为 HTTP 502，超时为 HTTP 504。
- 缓存保存完整窗口及源时间，缓存键包含请求 URL 和 `format`；重复请求不保证 Azure 已刷新，失败结果不进入 JSON 指标缓存。
- 不添加平台 API 未提供的业务字段。PromQL JSON 使用同一结构，`source` 为 `azure-prometheus`，详见 [PromQL JSON 窗口](PROMQL.md#json-window)。
- JSON 供 HTTP 客户端或 [scraper](scraper/README.md#平台指标-json-采集) 使用，不能直接作为 Prometheus 文本抓取目标；大窗口需协调客户端与 exporter 写超时。

## 最小抓取示例

按资源类型发现 Key Vault 并查询指标：

```yaml
- job_name: azure-keyvault
  scrape_interval: 1m
  honor_timestamps: true
  metrics_path: /probe/metrics/list
  params:
    subscription: ["<subscription-id>"]
    resourceType: ["Microsoft.KeyVault/vaults"]
    name: ["azure_keyvault"]
    template: ["{name}_{metric}_{aggregation}_{unit}"]
    metric: ["Availability", "ServiceApiLatency"]
    aggregation: ["average"]
    interval: ["PT1M"]
    timespan: ["PT5M"]
  static_configs:
  - targets: ["azure-metrics:8080"]
```

- **指定资源**：改用 `/probe/metrics/resource`，将 `resourceType` 替换为 `target: ["<resource-id>"]`。
- **自定义发现**：用 `filter: ["resourceType eq 'Microsoft.KeyVault/vaults'"]` 替换 `resourceType`。
- **维度**：通过 `metricFilter`、`metricTop` 筛选，如网关指标使用 `metricFilter: ["ConnectionName eq '*'"]`。
- **子资源命名空间**：如 Storage Blob 指标使用 `metricNamespace: ["Microsoft.Storage/storageAccounts/blobServices"]`。

可用指标见 [Azure 支持的指标列表](https://learn.microsoft.com/azure/azure-monitor/reference/supported-metrics/metrics-index)。

## 相关文档

- [快速启动](README.md)
- [PromQL 参数、JSON 窗口与 AKS 控制平面指标](PROMQL.md)
- [独立 scraper：调度、集群及 Event Hubs 输出](scraper/README.md)
- [Kubernetes 部署](deploy/README.md)：distroless 镜像，支持非 root、只读根文件系统和丢弃全部 Linux capability。
