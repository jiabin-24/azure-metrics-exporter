# Azure Monitor metrics exporter

[![license](https://img.shields.io/github/license/webdevops/azure-metrics-exporter.svg)](https://github.com/webdevops/azure-metrics-exporter/blob/master/LICENSE)
[![DockerHub](https://img.shields.io/badge/DockerHub-webdevops%2Fazure--metrics--exporter-blue)](https://hub.docker.com/r/webdevops/azure-metrics-exporter/)

导出 Azure Monitor 平台指标及托管 Prometheus 指标，支持 Prometheus 文本和 JSON 输出。

## 快速启动

以下 PowerShell 命令在仓库根目录执行，需要安装 Go（版本见 [go.mod](go.mod)）和 Azure CLI。

### 1. Azure 登录

```powershell
az login
az account set --subscription "<subscription-id>"
```

采集 PromQL 时，登录身份需具有目标 Azure Monitor workspace 的 **Monitoring Data Reader** 权限；读取 Azure 资源信息还需对应资源的读取权限。其他身份验证方式见[配置说明](GUIDE.md#configuration)。

### 2. 配置 workspace

采集 PromQL 时，修改 [workspace 配置文件](config/prometheus-workspaces.yaml)，填写 Azure Monitor workspace 的 Prometheus 查询终结点：

```yaml
workspaces:
  workspace-a:
    query_endpoint: https://<workspace>.<region>.prometheus.monitor.azure.com
```

设置文件路径：

```powershell
$env:AZURE_PROMETHEUS_WORKSPACES_FILE = ".\config\prometheus-workspaces.yaml"
```

仅采集 Azure 平台指标时，可跳过此步骤。

默认读取 `/etc/azure-metrics-exporter/workspaces.yaml`，挂载到此路径后无需设置环境变量或启动参数；仍可通过上述环境变量或 `--azure.prometheus.workspaces-file` 覆盖路径。默认文件不存在时会记录警告，并继续提供平台指标服务；显式指定的文件不存在或文件内容无效时启动失败。使用 `--azure.prometheus.workspaces-file=` 可禁用 workspace 文件加载。

### 3. 启动 exporter

```powershell
go run . --server.timeout.write=120s
```

在另一个终端检查服务或测试 PromQL 查询：

```powershell
Invoke-WebRequest "http://127.0.0.1:8080/metrics"
Invoke-RestMethod "http://127.0.0.1:8080/probe/metrics/promql?workspace=workspace-a&query=apiserver_request_total&format=json"
```

需要定时采集并发送到 Event Hub 时，按 [scraper 启动指南](scraper/README.md)启动独立采集器。修改 workspace 配置后需重启 exporter。

## 更多文档

- [详细指南：完整配置、功能说明、指标规则及 Prometheus 示例](GUIDE.md)
- [PromQL 查询参数与 AKS 控制平面指标](PROMQL.md)
- [独立 scraper 启动与部署](scraper/README.md)
- [Kubernetes 部署 exporter 和 scraper，定时推送到 Event Hub](deploy/README.md)
