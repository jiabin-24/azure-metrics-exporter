# Azure Monitor metrics exporter

导出 Azure Monitor 平台指标及托管 Prometheus 指标，支持 Prometheus 文本和 JSON 输出。

如需直接部署到 AKS，请参阅 [Kubernetes 部署指南](deploy/README.md)。

## 快速启动

以下 PowerShell 命令在仓库根目录执行，需要安装 Go（版本见 [go.mod](go.mod)）和 Azure CLI。

本仓库的 Go 模块路径为 `azure-metrics-exporter`；第三方 `github.com/webdevops/go-common` 仍使用上游依赖。

### 1. Azure 登录

```powershell
az login
az account set --subscription "<subscription-id>"
```

采集 PromQL 时，登录身份需具有目标 Azure Monitor workspace 的 **Monitoring Data Reader** 权限；读取 Azure 资源信息还需对应资源的读取权限。其他身份验证方式见[配置说明](ARCHITECTURE.md#configuration)。

### 2. 配置 workspace

采集 PromQL 时，在 [workspace 配置文件](config/prometheus-workspaces.yaml) 中填写 Azure Monitor workspace 的 Prometheus 查询终结点。`workspace-a` 为自定义别名，多个 workspace 可追加定义：

```yaml
workspaces:
  workspace-a:
    query_endpoint: https://<workspace>.<region>.prometheus.monitor.azure.com
```

设置文件路径：

```powershell
$env:AZURE_PROMETHEUS_WORKSPACES_FILE = ".\config\prometheus-workspaces.yaml"
```

### 3. 启动 exporter

```powershell
go run . --server.timeout.write=120s
```

在另一个终端检查服务或测试 PromQL 查询（或直接在浏览器中访问 http://localhost:8080/query 操作）：

```powershell
Invoke-WebRequest "http://127.0.0.1:8080/metrics"
Invoke-RestMethod "http://127.0.0.1:8080/probe/metrics/promql?workspace=workspace-a&query=apiserver_request_total&format=json"
```

服务启动后，可选择以下方式定时采集：

- **Prometheus 拉取**：在自建 Prometheus 中配置标准抓取 job，定时从 exporter 拉取指标。
- **scraper 推送**：按 [scraper 启动指南](scraper/README.md)运行独立采集器，定时从 exporter 采集指标并发送到 Event Hub。

## 更多文档

- [架构说明：数据流、关键配置、接口与输出规则](ARCHITECTURE.md)
- [PromQL 查询参数与 AKS 控制平面指标](PROMQL.md)
- [独立 scraper 启动与部署](scraper/README.md)
- [Kubernetes 部署 exporter 和 scraper，定时推送到 Event Hub](deploy/README.md)
