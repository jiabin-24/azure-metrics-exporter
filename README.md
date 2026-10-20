# Azure Monitor metrics exporter

[![license](https://img.shields.io/github/license/webdevops/azure-metrics-exporter.svg)](https://github.com/webdevops/azure-metrics-exporter/blob/master/LICENSE)
[![DockerHub](https://img.shields.io/badge/DockerHub-webdevops%2Fazure--metrics--exporter-blue)](https://hub.docker.com/r/webdevops/azure-metrics-exporter/)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/azure-metrics-exporter)](https://artifacthub.io/packages/search?repo=azure-metrics-exporter)

用于导出 Azure Monitor 指标的 Prometheus 导出器。
支持通过一次抓取获取所有资源的指标（自动服务发现）、使用模板自定义指标名称、完整的维度支持以及缓存。

除 Azure 连接配置外，导出器的所有配置都直接在 Prometheus 中完成，无需单独的配置文件。请参阅下方示例。

目录：
* [功能](#Features)
* [配置](#configuration)
* [指标](#metrics)
    + [Azure 跟踪指标](#azuretracing-metrics)
    + [指标名称和帮助信息模板系统](#metric-name-and-help-template-system)
        - [默认模板](#default-template)
        - [模板 `{name}_{metric}_{unit}`](#template-name_metric_unit)
        - [模板 `{name}_{metric}_{aggregation}_{unit}`](#template-name_metric_aggregation_unit)
* [HTTP 端点](#http-endpoints)
    + [/probe/metrics 参数](#probemetrics-parameters)
    + [/probe/metrics/resource 参数](#probemetricsresource-parameters)
    + [/probe/metrics/list 参数](#probemetricslist-parameters)
    + [/probe/metrics/scrape 参数](#probemetricsscrape-parameters)
    + [/probe/metrics/promql（PromQL）](PROMQL.md)
* [Prometheus 配置示例](#prometheus-configuration-examples)
    * [Redis](#Redis)
    * [VirtualNetworkGateways](#virtualnetworkgateways)
    * [虚拟网络网关连接（支持维度）](#virtualnetworkgateway-connections-dimension-support)
    * [StorageAccount（支持指标命名空间和维度）](#storageaccount-metric-namespace-and-dimension-support)
    * [AKS 高级控制平面指标](PROMQL.md#aks-control-plane)
* [开发和测试查询 Web UI](#development-and-testing-query-webui)
* [PromQL 查询与本地启动](PROMQL.md)

<a id="Features"></a>
## 功能

- 使用官方的 [Go 版 Azure SDK](https://github.com/Azure/azure-sdk-for-go)
- 通过 Azure SDK 配置支持所有 Azure 环境（Azure 公有云、Azure 政府云、Azure 中国云等）
- 缓存 Azure 服务发现结果，以减少 Azure API 调用
- 缓存已获取的指标（无需每分钟都请求 Azure Monitor API；指标抓取间隔可以设为 `30s`）
- 可自定义指标名称（使用[指标信息模板系统](#metric-name-template-system)）
- 可通过 `target` 参数获取一个或多个资源的指标（参见 `/probe/metrics/resource`）
- 可通过基于 `$filter` 的 [Azure 资源 API](https://docs.microsoft.com/en-us/rest/api/resources/resources/list)，获取服务发现发现的资源指标（参见 `/probe/metrics/list`）
- 可通过基于 `$filter` 的 [Azure 资源 API](https://docs.microsoft.com/en-us/rest/api/resources/resources/list) 获取服务发现发现的资源指标，并在 Azure 资源标记中配置相关信息（参见 `/probe/metrics/scrape`）
- 可通过基于 Kusto 查询的 [Azure Resource Graph API](https://docs.microsoft.com/en-us/azure/governance/resource-graph/overview)，获取服务发现发现的资源指标（参见 `/probe/metrics/resourcegraph`）
- 可通过 Azure Monitor 工作区的 Prometheus API 查询 PromQL 指标（参见 [PromQL 查询与 AKS 控制平面指标](PROMQL.md)）
- 基于 Prometheus 抓取配置或 ServiceMonitor 清单（Prometheus Operator）进行配置
- 可在抓取配置中处理指标（添加、删除、更新或筛选标签或指标），例如 [`metric_relabel_configs`](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#metric_relabel_configs)
- 完整支持指标[维度](#virtualnetworkgateway-connections-dimension-support)
- Docker 镜像基于 [Google 的 distroless](https://github.com/GoogleContainerTools/distroless) 静态镜像构建，以缩小攻击面（镜像中不含 shell 或其他二进制文件）
- 可通过 Docker Hub 获取（参见上方徽章）
- 可使用非 root 用户运行，并将根文件系统设为只读；无需任何 Linux capability（可以安全地设置 `drop: ["All"]`）
- 提供 Azure API 速率限制指标（导出器发送 Azure API 请求时，这些指标可通过 `/metrics` 获取）

可与以下导出器配合使用：

- [azure-resourcegraph-exporter](https://github.com/webdevops/azure-resourcegraph-exporter)：使用自定义 Kusto 查询从 Azure Resource Graph API 导出 Azure 资源信息（例如获取资源和资源组的标记）
- [azure-resourcemanager-exporter](https://github.com/webdevops/azure-resourcemanager-exporter)：导出 Azure 订阅信息（例如速率限制、订阅配额、服务主体过期时间、角色分配、资源运行状况等）
- [azure-keyvault-exporter](https://github.com/webdevops/azure-keyvault-exporter)：导出 Azure Key Vault 信息（例如机密、证书和密钥的过期时间）
- [azure-loganalytics-exporter](https://github.com/webdevops/azure-loganalytics-exporter)：使用自定义 Kusto 查询导出 Azure Log Analytics 工作区信息（例如数据引入速率或应用程序错误数量）

<a id="configuration"></a>
## 配置

通常无需额外配置，也可以通过环境变量进行自定义。

```
Usage:
  azure-metrics-exporter [OPTIONS]

Application Options:
      --log.level=[trace|debug|info|warning|error] Log level (default: info) [$LOG_LEVEL]
      --log.format=[logfmt|json]                   Log format (default: logfmt) [$LOG_FORMAT]
      --log.source=[|short|file|full]              Show source for every log message (useful for debugging and bug reports) [$LOG_SOURCE]
      --log.color=[|auto|yes|no]                   Enable color for logs [$LOG_COLOR]
      --log.time                                   Show log time [$LOG_TIME]
      --azure-environment=                         Azure environment name (default: AZUREPUBLICCLOUD) [$AZURE_ENVIRONMENT]
      --azure-ad-resource-url=                     Specifies the AAD resource ID to use. If not set, it defaults to ResourceManagerEndpoint for operations with Azure Resource Manager [$AZURE_AD_RESOURCE]
      --azure.prometheus.query-endpoint=            Azure Monitor workspace Prometheus query endpoint [$AZURE_PROMETHEUS_QUERY_ENDPOINT]
      --azure.servicediscovery.cache=              Duration for caching Azure ServiceDiscovery of workspaces to reduce API calls (time.Duration) (default: 30m) [$AZURE_SERVICEDISCOVERY_CACHE]
      --azure.resource-tag=                        Azure Resource tags (space delimiter) (default: owner) [$AZURE_RESOURCE_TAG]
      --metrics.template=                          Template for metric name (default: {name}) [$METRIC_TEMPLATE]
      --metrics.help=                              Metric help (with template support) (default: Azure monitor insight metric) [$METRIC_HELP]
      --metrics.dimensions.lowercase               Lowercase dimension values [$METRIC_DIMENSIONS_LOWERCASE]
      --concurrency.subscription=                  Concurrent subscription fetches (default: 5) [$CONCURRENCY_SUBSCRIPTION]
      --concurrency.subscription.resource=         Concurrent requests per resource (inside subscription requests) (default: 10) [$CONCURRENCY_SUBSCRIPTION_RESOURCE]
      --enable-caching                             Enable internal caching [$ENABLE_CACHING]
      --server.bind=                               Server address (default: :8080) [$SERVER_BIND]
      --server.timeout.read=                       Server read timeout (default: 5s) [$SERVER_TIMEOUT_READ]
      --server.timeout.write=                      Server write timeout (default: 10s) [$SERVER_TIMEOUT_WRITE]

Help Options:
  -h, --help                                       Show this help message
```

有关 Azure API 身份验证（使用环境变量），请参阅以下文档：
- https://github.com/webdevops/go-common/blob/main/azuresdk/README.md
- https://docs.microsoft.com/en-us/azure/developer/go/azure-sdk-authentication

<a id="how-to-test"></a>
## 如何测试

启用 Web UI（`--development.webui`）后，即可使用基础 Web 前端查询导出器，帮助你找到合适的配置设置。

Web UI 的访问路径为 `/query`。

<a id="metrics"></a>
## 指标

| 指标                                     | 描述                                                                                          |
|------------------------------------------|-------------------------------------------------------------------------------------------------|
| `azurerm_stats_metric_collecttime`       | 导出器的常规统计信息                                                                                |
| `azurerm_stats_metric_requests`          | 资源指标请求计数器，包含请求结果（错误、成功）                                                        |
| `azurerm_resource_metric`（可自定义）     | 探测器导出的资源指标（可通过 `name` 参数和模板系统修改）                                                |
| `azurerm_api_ratelimit`                  | Azure 速率限制指标（仅在 `/metrics` 上提供，每次查询后重置）                                            |
| `azurerm_api_request_*`                  | Azure 请求数量和延迟的直方图                                                                          |

<a id="resourcetags-handling"></a>
### ResourceTags 标记处理

请参阅 [armclient tagmanager 文档](https://github.com/webdevops/go-common/blob/main/azuresdk/README.md#tag-manager)。

<a id="azuretracing-metrics"></a>
### AzureTracing 指标

请参阅 [armclient 跟踪文档](https://github.com/webdevops/go-common/blob/main/azuresdk/README.md#azuretracing-metrics)。

<a id="metric-name-and-help-template-system"></a>
<a id="metric-name-template-system"></a>
### 指标名称和帮助信息模板系统

（适用于 21.5.3 及更高版本）

默认情况下，Azure Monitor 指标使用请求中指定的名称生成（参见 `name` 参数）。
可以通过环境变量 `$METRIC_TEMPLATE` 或请求参数 `template` 修改该名称。

提示：已用于模板的字段不会再作为标签输出！

建议的指标名称模板：`{name}_{metric}_{aggregation}_{unit}`

建议的帮助信息模板：`Azure metrics for {metric} with aggregation {aggregation} as {unit}`

可使用以下模板变量：

| 模板            | 描述                                                                                      |
|-----------------|-------------------------------------------------------------------------------------------|
| `{name}`        | 请求参数 `name` 指定的模板名称                                                            |
| `{type}`        | 请求中指定的 ResourceType 或 MetricNamespace（并非所有 API 都适用）                         |
| `{metric}`      | Azure Monitor 指标名称                                                                    |
| `{dimension}`   | Azure Monitor 指标的维度值（使用维度时）                                                   |
| `{unit}`        | Azure Monitor 指标的单位名称（例如 `count`、`percent` 等）                                |
| `{aggregation}` | Azure Monitor 指标的聚合方式（例如 `total`、`average`）                                    |
| `{interval}`    | 请求的 Azure Monitor 指标间隔                                                             |
| `{timespan}`    | 请求的 Azure Monitor 指标时间范围                                                         |

<a id="default-template"></a>
#### 默认模板

Prometheus 配置：
```yaml
- job_name: azure-metrics-keyvault
  scrape_interval: 1m
  metrics_path: /probe/metrics/list
  params:
    name: ["azure_metric_keyvault"]
    subscription:
    - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    filter: ["resourceType eq 'Microsoft.KeyVault/vaults'"]
    metric:
    - Availability
    - ServiceApiHit
    - ServiceApiLatency
    interval: ["PT15M"]
    timespan: ["PT15M"]
    aggregation:
    - average
    - total
  static_configs:
  - targets: ["azure-metrics:8080"]
```

生成的指标：
```
# HELP azure_metric_keyvault Azure monitor insight metric
# TYPE azure_metric_keyvault gauge
azure_metric_keyvault{aggregation="average",dimension="",interval="PT12H",metric="Availability",resourceID="/subscriptions/...",timespan="PT12H",unit="Percent"} 100
azure_metric_keyvault{aggregation="average",dimension="",interval="PT12H",metric="Availability",resourceID="/subscriptions/...",timespan="PT12H",unit="Percent"} 100
azure_metric_keyvault{aggregation="average",dimension="",interval="PT12H",metric="ServiceApiHit",resourceID="/subscriptions/...",timespan="PT12H",unit="Count"} 0
azure_metric_keyvault{aggregation="average",dimension="",interval="PT12H",metric="ServiceApiHit",resourceID="/subscriptions/...",timespan="PT12H",unit="Count"} 0
azure_metric_keyvault{aggregation="total",dimension="",interval="PT12H",metric="ServiceApiHit",resourceID="/subscriptions/...",timespan="PT12H",unit="Count"} 0
azure_metric_keyvault{aggregation="total",dimension="",interval="PT12H",metric="ServiceApiHit",resourceID="/subscriptions/...",timespan="PT12H",unit="Count"} 0
# HELP azurerm_ratelimit Azure ResourceManager ratelimit
# TYPE azurerm_ratelimit gauge
azurerm_ratelimit{scope="subscription",subscriptionID="...",type="read"} 11997
```


<a id="template-name_metric_unit"></a>
#### 模板 `{name}_{metric}_{unit}`

Prometheus 配置：
```yaml
- job_name: azure-metrics-keyvault
  scrape_interval: 1m
  metrics_path: /probe/metrics/list
  params:
    name: ["azure_metric_keyvault"]
    template: ["{name}_{metric}_{unit}"]
    help: ["Custom help with {metric}"]
    subscription:
    - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    filter: ["resourceType eq 'Microsoft.KeyVault/vaults'"]
    metric:
    - Availability
    - ServiceApiHit
    - ServiceApiLatency
    interval: ["PT15M"]
    timespan: ["PT15M"]
    aggregation:
    - average
    - total
  static_configs:
  - targets: ["azure-metrics:8080"]
```

生成的指标：
```
# HELP azure_metric_keyvault_availability_percent Custom help with availability
# TYPE azure_metric_keyvault_availability_percent gauge
azure_metric_keyvault_availability_percent{aggregation="average",dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 100
azure_metric_keyvault_availability_percent{aggregation="average",dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 100

# HELP azure_metric_keyvault_serviceapihit_count Custom help with serviceapihit
# TYPE azure_metric_keyvault_serviceapihit_count gauge
azure_metric_keyvault_serviceapihit_count{aggregation="average",dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 0
azure_metric_keyvault_serviceapihit_count{aggregation="average",dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 0
azure_metric_keyvault_serviceapihit_count{aggregation="total",dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 0
azure_metric_keyvault_serviceapihit_count{aggregation="total",dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 0

# HELP azurerm_ratelimit Azure ResourceManager ratelimit
# TYPE azurerm_ratelimit gauge
azurerm_ratelimit{scope="subscription",subscriptionID="...",type="read"} 11996
```

<a id="template-name_metric_aggregation_unit"></a>
#### 模板 `{name}_{metric}_{aggregation}_{unit}`

Prometheus 配置：
```yaml
- job_name: azure-metrics-keyvault
  scrape_interval: 1m
  metrics_path: /probe/metrics/list
  params:
    name: ["azure_metric_keyvault"]
    template: ["{name}_{metric}_{aggregation}_{unit}"]
    subscription:
    - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    filter: ["resourceType eq 'Microsoft.KeyVault/vaults'"]
    metric:
    - Availability
    - ServiceApiHit
    - ServiceApiLatency
    interval: ["PT15M"]
    timespan: ["PT15M"]
    aggregation:
    - average
    - total
  static_configs:
  - targets: ["azure-metrics:8080"]
```

生成的指标：
```
# HELP azure_metric_keyvault_availability_average_percent Azure monitor insight metric
# TYPE azure_metric_keyvault_availability_average_percent gauge
azure_metric_keyvault_availability_average_percent{dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 100
azure_metric_keyvault_availability_average_percent{dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 100
# HELP azure_metric_keyvault_availability_total_percent Azure monitor insight metric
# TYPE azure_metric_keyvault_availability_total_percent gauge
azure_metric_keyvault_availability_total_percent{dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 9
# HELP azure_metric_keyvault_serviceapihit_average_count Azure monitor insight metric
# TYPE azure_metric_keyvault_serviceapihit_average_count gauge
azure_metric_keyvault_serviceapihit_average_count{dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 0
azure_metric_keyvault_serviceapihit_average_count{dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 1
# HELP azure_metric_keyvault_serviceapihit_total_count Azure monitor insight metric
# TYPE azure_metric_keyvault_serviceapihit_total_count gauge
azure_metric_keyvault_serviceapihit_total_count{dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 0
azure_metric_keyvault_serviceapihit_total_count{dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 9
# HELP azure_metric_keyvault_serviceapilatency_average_milliseconds Azure monitor insight metric
# TYPE azure_metric_keyvault_serviceapilatency_average_milliseconds gauge
azure_metric_keyvault_serviceapilatency_average_milliseconds{dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 38.666666666666664
# HELP azure_metric_keyvault_serviceapilatency_total_milliseconds Azure monitor insight metric
# TYPE azure_metric_keyvault_serviceapilatency_total_milliseconds gauge
azure_metric_keyvault_serviceapilatency_total_milliseconds{dimension="",interval="PT12H",resourceID="/subscriptions/...",timespan="PT12H"} 348
# HELP azurerm_ratelimit Azure ResourceManager ratelimit
# TYPE azurerm_ratelimit gauge
azurerm_ratelimit{scope="subscription",subscriptionID="...",type="read"} 11999
```

<a id="http-endpoints"></a>
## HTTP 端点

| 端点                           | 描述                                                                                                                             |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `/metrics`                     | 默认的 Prometheus Go 指标                                                                                                           |
| `/probe/metrics`               | 按订阅和区域探测指标，并按资源拆分（每个订阅和区域查询一次；参见 `azurerm_resource_metric`）                                          |
| `/probe/metrics/resource`      | 探测单个资源的指标（每个资源查询一次；参见 `azurerm_resource_metric`）                                                                |
| `/probe/metrics/list`          | 探测资源列表的指标（每个资源查询一次；参见 `azurerm_resource_metric`）                                                                |
| `/probe/metrics/scrape`        | 探测资源列表的指标，并从资源标记读取配置（每个资源查询一次；参见 `azurerm_resource_metric`）                                          |
| `/probe/metrics/resourcegraph` | 基于 Kusto 查询和 Resource Graph API 探测资源列表的指标（每个资源查询一次）                                                          |
| `/probe/metrics/promql`        | 将 PromQL 即时或范围查询转发到 Azure Monitor 工作区，并以 Prometheus 指标格式返回结果                                               |

<a id="promql-query"></a>
### `/probe/metrics/promql`（PromQL）

参数、身份权限、即时/范围查询、本地启动及 AKS 高级控制平面指标的 job 配置已移至独立文档：[PromQL 查询与 AKS 控制平面指标](PROMQL.md)。该端点默认输出显式时间戳：独立原始指标选择器使用源采样时间，计算表达式使用查询求值时间；Prometheus 应保留 `honor_timestamps: true`。原有 Azure 平台指标端点行为不变。

<a id="probemetrics-parameters"></a>
### /probe/metrics 参数

每个订阅和区域发送一次指标请求。

| GET 参数             | 默认值                    | 必填     | 多值     | 描述                                                                                                                                              |
|----------------------|---------------------------|----------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `subscription`       |                           | **是**   | **是**   | Azure 订阅 ID                                                                                                                                       |
| `region`             |                           | 否       | **是**   | Azure 区域（例如 `westeurope`、`northeurope`）。如果省略，将使用 Resource Graph 发现区域                                           |
| `resourceType`       |                           | **是**   | 否       | Azure 资源类型                                                                                                                                      |
| `timespan`           | `PT1M`                    | 否       | 否       | 指标时间范围                                                                                                                                         |
| `interval`           |                           | 否       | 否       | 指标时间间隔                                                                                                                                         |
| `metricNamespace`    |                           | 否       | 否       | 指标命名空间                                                                                                                                         |
| `metric`             |                           | 否       | **是**   | 指标名称                                                                                                                                             |
| `aggregation`        |                           | 否       | **是**   | 指标聚合方式（`minimum`、`maximum`、`average`、`total`、`count`；多个值可用 `,` 分隔）                                           |
| `name`               | `azurerm_resource_metric` | 否       | 否       | Prometheus 指标名称                                                                                                                                  |
| `metricFilter`       |                           | 否       | 否       | Prometheus 指标筛选条件（支持维度；订阅查询模式下最多支持 2 个筛选条件，因为第一个筛选条件用于按资源 ID 拆分） |
| `metricTop`          |                           | 否       | 否       | Prometheus 指标维度数量（支持维度）                                                                                                                  |
| `metricOrderBy`      |                           | 否       | 否       | Prometheus 指标排序方式（支持维度）                                                                                                                  |
| `validateDimensions` | `true`                    | 否       | 否       | 设为 `false` 时，将忽略无效的筛选参数值。                                                                                                            |
| `cache`              |（与 timespan 相同）         | 否       | 否       | 是否使用内部指标缓存                                                                                                                                 |
| `template`           | 设为 `$METRIC_TEMPLATE`    | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                                                               |
| `help`               | 设为 `$METRIC_HELP`        | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                                                               |

*提示：多个值可以通过重复指定参数，或在单个参数值中用逗号分隔来提供。*

<a id="probemetricsresource-parameters"></a>
### /probe/metrics/resource 参数

每个资源的指标请求每批最多包含 20 个指标名称（35 个指标名称会对每个资源发起 2 次请求）。

| GET 参数             | 默认值                    | 必填     | 多值     | 描述                                                                                                       |
|----------------------|---------------------------|----------|----------|--------------------------------------------------------------------------------------------------------------|
| `subscription`       |                           | **是**   | **是**   | Azure 订阅 ID                                                                                                |
| `target`             |                           | **是**   | **是**   | Azure 资源 URI                                                                                               |
| `timespan`           | `PT1M`                    | 否       | 否       | 指标时间范围                                                                                                  |
| `interval`           |                           | 否       | 否       | 指标时间间隔                                                                                                  |
| `metricNamespace`    |                           | 否       | **是**   | 指标命名空间                                                                                                  |
| `metric`             |                           | 否       | **是**   | 指标名称                                                                                                      |
| `aggregation`        |                           | 否       | **是**   | 指标聚合方式（`minimum`、`maximum`、`average`、`total`、`count`；多个值可用 `,` 分隔）                         |
| `name`               | `azurerm_resource_metric` | 否       | 否       | Prometheus 指标名称                                                                                           |
| `metricFilter`       |                           | 否       | 否       | Prometheus 指标筛选条件（支持维度）                                                                            |
| `metricTop`          |                           | 否       | 否       | Prometheus 指标维度数量（支持维度）                                                                            |
| `metricOrderBy`      |                           | 否       | 否       | Prometheus 指标排序方式（支持维度）                                                                            |
| `validateDimensions` | `true`                    | 否       | 否       | 设为 `false` 时，将忽略无效的筛选参数值。                                                                      |
| `cache`              |（与 timespan 相同）         | 否       | 否       | 是否使用内部指标缓存                                                                                            |
| `template`           | 设为 `$METRIC_TEMPLATE`    | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                       |
| `help`               | 设为 `$METRIC_HELP`        | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                       |

*提示：多个值可以通过重复指定参数，或在单个参数值中用逗号分隔来提供。*

<a id="probemetricslist-parameters"></a>
### /probe/metrics/list 参数

每个资源的指标请求每批最多包含 20 个指标名称（35 个指标名称会对每个资源发起 2 次请求）。

提示：服务发现信息的缓存时长由 `$AZURE_SERVICEDISCOVERY_CACHE` 设置（设为 `0` 可禁用缓存）。

| GET 参数                   | 默认值                    | 必填     | 多值     | 描述                                                                                                       |
|----------------------------|---------------------------|----------|----------|--------------------------------------------------------------------------------------------------------------|
| `subscription`             |                           | **是**   | **是**   | Azure 订阅 ID（多个订阅 ID 可用逗号分隔）                                                                     |
| `resourceType` 或 `filter` |                           | **是**   | 否       | Azure 资源类型或筛选查询（https://docs.microsoft.com/en-us/rest/api/resources/resources/list）                 |
| `timespan`                 | `PT1M`                    | 否       | 否       | 指标时间范围                                                                                                  |
| `interval`                 |                           | 否       | 否       | 指标时间间隔                                                                                                  |
| `metricNamespace`          |                           | 否       | **是**   | 指标命名空间                                                                                                  |
| `metric`                   |                           | 否       | **是**   | 指标名称                                                                                                      |
| `aggregation`              |                           | 否       | **是**   | 指标聚合方式（`minimum`、`maximum`、`average`、`total`、`count`；多个值可用 `,` 分隔）                         |
| `name`                     | `azurerm_resource_metric` | 否       | 否       | Prometheus 指标名称                                                                                           |
| `metricFilter`             |                           | 否       | 否       | Prometheus 指标筛选条件（支持维度）                                                                            |
| `metricTop`                |                           | 否       | 否       | Prometheus 指标维度数量（支持维度）                                                                            |
| `metricOrderBy`            |                           | 否       | 否       | Prometheus 指标排序方式（支持维度）                                                                            |
| `validateDimensions`       | `true`                    | 否       | 否       | 设为 `false` 时，将忽略无效的筛选参数值。                                                                      |
| `cache`                    |（与 timespan 相同）         | 否       | 否       | 是否使用内部指标缓存                                                                                            |
| `template`                 | 设为 `$METRIC_TEMPLATE`    | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                       |
| `help`                     | 设为 `$METRIC_HELP`        | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                       |

*提示：多个值可以通过重复指定参数，或在单个参数值中用逗号分隔来提供。*

<a id="probemetricsscrape-parameters"></a>
### /probe/metrics/scrape 参数

提示：服务发现信息的缓存时长由 `$AZURE_SERVICEDISCOVERY_CACHE` 设置（设为 `0` 可禁用缓存）。

| GET 参数                   | 默认值                    | 必填     | 多值     | 描述                                                                                                   |
|----------------------------|---------------------------|----------|----------|----------------------------------------------------------------------------------------------------------|
| `subscription`             |                           | **是**   | **是**   | Azure 订阅 ID（多个订阅 ID 可用逗号分隔）                                                                  |
| `resourceType` 或 `filter` |                           | **是**   | 否       | Azure 资源类型或筛选查询（https://docs.microsoft.com/en-us/rest/api/resources/resources/list）              |
| `metricTagName`            |                           | **是**   | 否       | 用于获取“metrics”列表的资源标记名称                                                                         |
| `aggregationTagName`       |                           | **是**   | 否       | 用于获取“aggregations”列表的资源标记名称                                                                    |
| `timespan`                 | `PT1M`                    | 否       | 否       | 指标时间范围                                                                                                |
| `interval`                 |                           | 否       | 否       | 指标时间间隔                                                                                                |
| `metricNamespace`          |                           | 否       | **是**   | 指标命名空间                                                                                                |
| `metric`                   |                           | 否       | **是**   | 指标名称                                                                                                    |
| `aggregation`              |                           | 否       | **是**   | 指标聚合方式（`minimum`、`maximum`、`average`、`total`；多个值可用 `,` 分隔）                                |
| `name`                     | `azurerm_resource_metric` | 否       | 否       | Prometheus 指标名称                                                                                         |
| `metricFilter`             |                           | 否       | 否       | Prometheus 指标筛选条件（支持维度）                                                                          |
| `metricTop`                |                           | 否       | 否       | Prometheus 指标维度数量（整数，支持维度）                                                                    |
| `metricOrderBy`            |                           | 否       | 否       | Prometheus 指标排序方式（支持维度）                                                                          |
| `validateDimensions`       | `true`                    | 否       | 否       | 设为 `false` 时，将忽略无效的筛选参数值。                                                                    |
| `cache`                    |（与 timespan 相同）         | 否       | 否       | 是否使用内部指标缓存                                                                                         |
| `template`                 | 设为 `$METRIC_TEMPLATE`    | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                     |
| `help`                     | 设为 `$METRIC_HELP`        | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                     |

*提示：多个值可以通过重复指定参数，或在单个参数值中用逗号分隔来提供。*


<a id="probemetricsresourcegraph-parameters"></a>
### /probe/metrics/resourcegraph 参数

此端点使用 Azure Resource Graph API 进行服务发现（适用于 21.9.0 及更高版本）。

每个资源的指标请求每批最多包含 20 个指标名称（35 个指标名称会对每个资源发起 2 次请求）。

提示：服务发现信息的缓存时长由 `$AZURE_SERVICEDISCOVERY_CACHE` 设置（设为 `0` 可禁用缓存）。

| GET 参数             | 默认值                    | 必填     | 多值     | 描述                                                                                                       |
|----------------------|---------------------------|----------|----------|--------------------------------------------------------------------------------------------------------------|
| `subscription`       |                           | **是**   | **是**   | Azure 订阅 ID（多个订阅 ID 可用逗号分隔）                                                                     |
| `resourceType`       |                           | **是**   | 否       | Azure 资源类型                                                                                                |
| `filter`             |                           | 否       | 否       | 附加的 Kusto 查询部分（例如 `where id contains "/xzy/"`）                                                      |
| `timespan`           | `PT1M`                    | 否       | 否       | 指标时间范围                                                                                                  |
| `interval`           |                           | 否       | 否       | 指标时间间隔                                                                                                  |
| `metricNamespace`    |                           | 否       | **是**   | 指标命名空间                                                                                                  |
| `metric`             |                           | 否       | **是**   | 指标名称                                                                                                      |
| `aggregation`        |                           | 否       | **是**   | 指标聚合方式（`minimum`、`maximum`、`average`、`total`、`count`；多个值可用 `,` 分隔）                         |
| `name`               | `azurerm_resource_metric` | 否       | 否       | Prometheus 指标名称                                                                                           |
| `metricFilter`       |                           | 否       | 否       | Prometheus 指标筛选条件（支持维度）                                                                            |
| `metricTop`          |                           | 否       | 否       | Prometheus 指标维度数量（支持维度）                                                                            |
| `metricOrderBy`      |                           | 否       | 否       | Prometheus 指标排序方式（支持维度）                                                                            |
| `validateDimensions` | `true`                    | 否       | 否       | 设为 `false` 时，将忽略无效的筛选参数值。                                                                      |
| `cache`              |（与 timespan 相同）         | 否       | 否       | 是否使用内部指标缓存                                                                                            |
| `template`           | 设为 `$METRIC_TEMPLATE`    | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                       |
| `help`               | 设为 `$METRIC_HELP`        | 否       | 否       | 参见[指标名称和帮助信息模板系统](#metric-name-and-help-template-system)                                       |

*提示：多个值可以通过重复指定参数，或在单个参数值中用逗号分隔来提供。*

<a id="prometheus-configuration-examples"></a>
## Prometheus 配置示例

<a id="Redis"></a>
### Redis

使用 target（单个实例）：

```yaml
- job_name: azure-metrics-redis
  scrape_interval: 1m
  metrics_path: /probe/metrics/resource
  params:
    name: ["my_own_metric_name"]
    subscription:
    - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    target:
    - /subscriptions/.../resourceGroups/.../providers/Microsoft.Cache/Redis/...
    - /subscriptions/.../resourceGroups/.../providers/Microsoft.Cache/Redis/...
    - /subscriptions/.../resourceGroups/.../providers/Microsoft.Cache/Redis/...
    - /subscriptions/.../resourceGroups/.../providers/Microsoft.Cache/Redis/...
    metric:
    - connectedclients
    - totalcommandsprocessed
    - cachehits
    - cachemisses
    - getcommands
    - setcommands
    - operationsPerSecond
    - evictedkeys
    - totalkeys
    - expiredkeys
    - usedmemory
    - usedmemorypercentage
    - usedmemoryRss
    - serverLoad
    - cacheWrite
    - cacheRead
    - percentProcessorTime
    - cacheLatency
    - errors
    interval: ["PT1M"]
    timespan: ["PT1M"]
    aggregation:
    - average
    - total
  static_configs:
  - targets: ["azure-metrics:8080"]
```

使用服务发现：
```yaml
- job_name: azure-metrics-redis
  scrape_interval: 1m
  metrics_path: /probe/metrics/list
  params:
    name: ["my_own_metric_name"]
    subscription:
    - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    resourceType: ["Microsoft.Cache/Redis"]
    metric:
    - connectedclients
    - totalcommandsprocessed
    - cachehits
    - cachemisses
    - getcommands
    - setcommands
    - operationsPerSecond
    - evictedkeys
    - totalkeys
    - expiredkeys
    - usedmemory
    - usedmemorypercentage
    - usedmemoryRss
    - serverLoad
    - cacheWrite
    - cacheRead
    - percentProcessorTime
    - cacheLatency
    - errors
    interval: ["PT1M"]
    timespan: ["PT1M"]
    aggregation:
    - average
    - total
  static_configs:
  - targets: ["azure-metrics:8080"]
```

使用自定义资源筛选查询进行服务发现：
```yaml
- job_name: azure-metrics-redis
  scrape_interval: 1m
  metrics_path: /probe/metrics/list
  params:
    name: ["my_own_metric_name"]
    subscription:
    - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    filter: ["resourceType eq 'Microsoft.Cache/Redis'"]
    metric:
    - connectedclients
    - totalcommandsprocessed
    - cachehits
    - cachemisses
    - getcommands
    - setcommands
    - operationsPerSecond
    - evictedkeys
    - totalkeys
    - expiredkeys
    - usedmemory
    - usedmemorypercentage
    - usedmemoryRss
    - serverLoad
    - cacheWrite
    - cacheRead
    - percentProcessorTime
    - cacheLatency
    - errors
    interval: ["PT1M"]
    timespan: ["PT1M"]
    aggregation:
    - average
    - total
  static_configs:
  - targets: ["azure-metrics:8080"]
```
<a id="virtualnetworkgateways"></a>
### VirtualNetworkGateways

```yaml
- job_name: azure-metrics-virtualNetworkGateways
  scrape_interval: 1m
  metrics_path: /probe/metrics/list
  params:
    name: ["my_own_metric_name"]
    subscription:
    - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    resourceType: ["Microsoft.Network/virtualNetworkGateways"]
    metric:
    - AverageBandwidth
    - P2SBandwidth
    - P2SConnectionCount
    - TunnelAverageBandwidth
    - TunnelEgressBytes
    - TunnelIngressBytes
    - TunnelEgressPackets
    - TunnelIngressPackets
    - TunnelEgressPacketDropTSMismatch
    - TunnelIngressPacketDropTSMismatch
    interval: ["PT5M"]
    timespan: ["PT5M"]
    aggregation:
    - average
    - total
  static_configs:
  - targets: ["azure-metrics:8080"]
```

<a id="virtualnetworkgateway-connections-dimension-support"></a>
### virtualNetworkGateway connections（支持维度）

虚拟网络网关连接指标（支持维度）
```yaml
- job_name: azure-metrics-virtualNetworkGateways-connections
  scrape_interval: 1m
  metrics_path: /probe/metrics/list
  params:
    name: ["my_own_metric_name"]
    subscription:
    - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    resourceType: ["Microsoft.Network/virtualNetworkGateways"]
    metric:
    - TunnelAverageBandwidth
    - TunnelEgressBytes
    - TunnelIngressBytes
    - TunnelEgressPackets
    - TunnelIngressPackets
    - TunnelEgressPacketDropTSMismatch
    - TunnelIngressPacketDropTSMismatch
    interval: ["PT5M"]
    timespan: ["PT5M"]
    aggregation:
    - average
    - total
    # 按连接筛选（支持维度）
    metricFilter: ["ConnectionName eq '*'"]
    metricTop: ["10"]
  static_configs:
  - targets: ["azure-metrics:8080"]
```

<a id="storageaccount-metric-namespace-and-dimension-support"></a>
### StorageAccount（支持指标命名空间和维度）

```yaml
- job_name: azure-metrics-virtualNetworkGateways-connections
  scrape_interval: 1m
  metrics_path: /probe/metrics/list
  params:
    name: ["my_own_metric_name"]
    subscription:
    - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    resourceType: ["Microsoft.Storage/storageAccounts"]
    metricNamespace: ["Microsoft.Storage/storageAccounts/blobServices"]
    metric:
    - BlobCapacity
    interval: ["PT1H"]
    timespan: ["PT1H"]
    aggregation:
    - average
    - count
    # 按 Blob 类型筛选（支持维度）
    metricFilter: ["BlobType eq '*'"]
    metricTop: ["10"]
  static_configs:
  - targets: ["azure-metrics:8080"]
```

在这些示例中，所有指标都以 `my_own_metric_name` 作为指标名称发布。

Microsoft Azure 文档中提供了[支持的指标列表](https://docs.microsoft.com/en-us/azure/azure-monitor/platform/metrics-supported)。

<a id="development-and-testing-query-webui"></a>
### 开发和测试查询 Web UI

azure-metrics-exporter 在 `http://url-to-exporter/query` 提供查询 Web UI，你可以在其中
测试不同的查询设置和端点。查询 Web UI 还会生成一个 Prometheus `scrape_config` 示例。

本地启动及验证命令请参阅 [Windows PowerShell 本地启动](PROMQL.md#local-startup)。
