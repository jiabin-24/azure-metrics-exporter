# Kubernetes 定时指标推送

[返回项目 README](../README.md)

在同一 namespace 部署以下服务：

```text
Azure Monitor workspace <- exporter (azure-metrics:8080) <- scraper -> Azure Event Hubs
```

- [metrics-exporter.yaml](metrics-exporter.yaml)：exporter Deployment 和 ClusterIP Service。
- [metrics-scraper.yaml](metrics-scraper.yaml)：scraper Deployment 和 memberlist headless Service。
- [kustomization.yaml](kustomization.yaml)：组合两个服务并生成配置 ConfigMap；workspace 来源为 [config/prometheus-workspaces.yaml](../config/prometheus-workspaces.yaml)，采集配置统一来源为 [scraper/scraper.yaml](../scraper/scraper.yaml)。
- scraper 默认每分钟执行共享配置中的 18 个 PromQL 采集任务，查询全部已配置 workspace，使用最近一分钟窗口、15 秒步长，并将 JSON 发送到 Event Hub。两个副本通过 memberlist 分配采集任务，不是每个副本重复采集全部任务。

以下 PowerShell 命令均在仓库根目录执行。需要可访问的 Kubernetes 集群、已发布的镜像、Azure Monitor workspace 和 Event Hub。

## 1. 修改配置

1. 在 [workspace 配置](../config/prometheus-workspaces.yaml) 中填写真实的 workspace Prometheus 查询终结点；可增加多个 workspace。此文件同时用于本地运行和 Kubernetes ConfigMap 生成，无需在部署清单中重复维护。
2. 在唯一的[采集配置](../scraper/scraper.yaml)中确认 `event_hub.namespace`、`event_hub.name`、PromQL 查询及静态标签（例如将 `environment: development` 改为实际环境）。保持目标地址 `azure-metrics:8080` 与 Service 一致，并保留默认的 memberlist 配置。
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
kubectl kustomize .\deploy\
kubectl apply -n $namespace -k .\deploy\
kubectl rollout status -n $namespace deployment/azure-metrics-exporter
kubectl rollout status -n $namespace deployment/azure-metrics-scraper
kubectl get pods,services -n $namespace
kubectl logs -n $namespace -l app=azure-metrics-exporter --tail=100 --prefix
kubectl logs -n $namespace -l app=azure-metrics-scraper --tail=100 --prefix
```

Pod 就绪不代表指标已成功发送。至少等待一个采集周期，检查 scraper 的查询/发送日志，并通过 Event Hub 接收端确认消息中的 workspace、时间范围和指标值；同时可在 Azure Monitor 中观察 Event Hub 的传入消息。

scraper 不持久化数据，也不是补采队列；重启或成员收敛期间可能短暂漏采或重复，不保证恰好一次发送。更多采集参数和分片说明见 [scraper 文档](../scraper/README.md)。

## 4. 更新配置

修改 workspace 或采集配置源文件后，使用 `-k` 重新部署，不要使用 `-f .\deploy\`。生成的 ConfigMap 使用固定名称 `azure-metrics-exporter` 和 `azure-metrics-scraper`，不添加内容哈希后缀。更新 ConfigMap 不会自动触发 Deployment 滚动更新，需要手动重启对应服务。Secret 引用保持原名。

仅修改采集配置时，重新应用后重启 scraper：

```powershell
kubectl apply -n $namespace -k .\deploy\
kubectl rollout restart -n $namespace deployment/azure-metrics-scraper
kubectl rollout status -n $namespace deployment/azure-metrics-scraper
```

修改 workspace 配置时，先重启 exporter 并等待更新完成，再重启 scraper，使其重新发现 workspace：

```powershell
kubectl apply -n $namespace -k .\deploy\
kubectl rollout restart -n $namespace deployment/azure-metrics-exporter
kubectl rollout status -n $namespace deployment/azure-metrics-exporter
kubectl rollout restart -n $namespace deployment/azure-metrics-scraper
kubectl rollout status -n $namespace deployment/azure-metrics-scraper
```

从带哈希后缀的旧部署迁移时，旧 ConfigMap 不会随 `apply` 自动删除；确认旧 Pod 已退出后，可清理不再引用的带哈希后缀 ConfigMap。后续配置更新会复用固定名称的 ConfigMap，无需清理历史版本。
