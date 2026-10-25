# Kubernetes 定时指标推送

[返回项目 README](../README.md)

在同一 namespace 部署以下服务：

```text
Azure Monitor workspace <- exporter (azure-metrics:8080) <- scraper -> Azure Event Hubs
```

- [metrics-exporter.yaml](metrics-exporter.yaml)：exporter Deployment、ClusterIP Service 和 workspace ConfigMap。
- [metrics-scraper.yaml](metrics-scraper.yaml)：scraper Deployment、采集 ConfigMap 和 memberlist headless Service。
- scraper 默认每分钟采集全部已配置 workspace 的 `apiserver_request_total`，查询最近一分钟、步长 15 秒，并将 JSON 发送到 Event Hub。两个副本通过 memberlist 分配采集任务，不是每个副本重复采集全部任务。

以下 PowerShell 命令均在仓库根目录执行。需要可访问的 Kubernetes 集群、已发布的镜像、Azure Monitor workspace 和 Event Hub。

## 1. 修改配置

1. 在 [exporter 清单](metrics-exporter.yaml) 中填写真实的 workspace Prometheus 查询终结点；可增加多个 workspace。
2. 在 [scraper 清单](metrics-scraper.yaml) 中确认 `event_hub.namespace`、`event_hub.name`、PromQL 查询及静态标签。保持目标地址 `azure-metrics:8080` 与 Service 一致。
3. 默认镜像为 `niuai/azure-metrics-exporter:main` 和 `niuai/azure-metrics-scraper:main`，需先由镜像发布工作流推送。生产环境建议将两个镜像改为同一发布版本标签或固定 digest。
4. 如 Azure Monitor 存在入库延迟，调整 `rolling_range.delay`；抓取较慢时同时调整 `scrape_timeout` 和 exporter 的写超时，前者不得超过 `scrape_interval`。

exporter 的 ConfigMap 直接挂载为 `/etc/azure-metrics-exporter/workspaces.yaml`，代码默认读取此路径，无需额外配置 env 或启动参数。需要其他路径时仍可通过 `AZURE_PROMETHEUS_WORKSPACES_FILE` 或 `--azure.prometheus.workspaces-file` 覆盖。

exporter 使用服务主体认证。该身份需要各 workspace 的 **Monitoring Data Reader** 权限；如需补充资源区域，还需对应资源的读取权限。scraper 单独使用具有 **Send** 权限的 Event Hub SAS 连接字符串，其 namespace、hub 名必须与配置一致，且必须包含 `EntityPath=<event_hub.name>`。

## 2. 创建 namespace 和 Secret

将 `$namespace` 改为实际名称。不要将密钥写入清单、提交到仓库或输出到日志。

```powershell
$namespace = "azure-metrics"
kubectl create namespace $namespace --dry-run=client -o yaml | kubectl apply -f -

$clientSecret = Read-Host "Azure client secret" -AsSecureString
@{
    apiVersion = "v1"
    kind = "Secret"
    metadata = @{ name = "azure-metrics-exporter"; namespace = $namespace }
    type = "Opaque"
    stringData = @{
        "tenant-id" = (Read-Host "Azure tenant ID")
        "client-id" = (Read-Host "Azure client ID")
        "client-secret" = [System.Net.NetworkCredential]::new("", $clientSecret).Password
    }
} | ConvertTo-Json -Depth 5 | kubectl apply -f -
Remove-Variable clientSecret

$connectionString = Read-Host "Event Hub SAS connection string" -AsSecureString
@{
    apiVersion = "v1"
    kind = "Secret"
    metadata = @{ name = "azure-metrics-eventhub"; namespace = $namespace }
    type = "Opaque"
    stringData = @{
        "connection-string" = [System.Net.NetworkCredential]::new("", $connectionString).Password
    }
} | ConvertTo-Json -Depth 5 | kubectl apply -f -
Remove-Variable connectionString
```

网络需放行 scraper 到 exporter 的 TCP 8080、scraper 副本间双向 TCP/UDP 7946、DNS，以及到 Event Hubs 的 TCP 5671 和到 Azure 身份、Monitor、Resource Manager 端点的 HTTPS 443。

## 3. 部署和检查

```powershell
kubectl apply -n $namespace -f .\deploy\
kubectl rollout status -n $namespace deployment/azure-metrics-exporter
kubectl rollout status -n $namespace deployment/azure-metrics-scraper
kubectl get pods,services -n $namespace
kubectl logs -n $namespace -l app=azure-metrics-exporter --tail=100 --prefix
kubectl logs -n $namespace -l app=azure-metrics-scraper --tail=100 --prefix
```

Pod 就绪不代表指标已成功发送。至少等待一个采集周期，检查 scraper 的查询/发送日志，并通过 Event Hub 接收端确认消息中的 workspace、时间范围和指标值；同时可在 Azure Monitor 中观察 Event Hub 的传入消息。

scraper 不持久化数据，也不是补采队列；重启或成员收敛期间可能短暂漏采或重复，不保证恰好一次发送。更多采集参数和分片说明见 [scraper 文档](../scraper/README.md)。

## 4. 更新配置

修改 ConfigMap 后重新应用并重启对应服务；增删 workspace 时两个服务都需重启：

```powershell
kubectl apply -n $namespace -f .\deploy\
kubectl rollout restart -n $namespace deployment/azure-metrics-exporter
kubectl rollout status -n $namespace deployment/azure-metrics-exporter
kubectl rollout restart -n $namespace deployment/azure-metrics-scraper
kubectl rollout status -n $namespace deployment/azure-metrics-scraper
```
