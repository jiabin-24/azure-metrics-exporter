# Kubernetes 部署

[返回项目 README](../README.md)

## 构建镜像

<details>
<summary>展开 GitHub Actions 构建步骤与所需 Secrets</summary>

通过 GitHub Actions 的 [release/docker](../.github/workflows/release-docker.yaml) 工作流构建 exporter 和 scraper 镜像并推送到 Docker Hub，无需本地构建。

### 1. 配置 GitHub Secrets

在仓库 **Settings → Secrets and variables → Actions → New repository secret** 中添加：

| Secret | 用途 |
|---|---|
| `DOCKERHUB_USERNAME` | 有权向目标 Docker Hub 仓库推送镜像的用户名 |
| `DOCKERHUB_TOKEN` | Docker Hub 访问令牌，需具有目标仓库的读写权限；不要使用账号密码 |

镜像名称定义在 [build-docker.yaml](../.github/workflows/build-docker.yaml) 的构建矩阵中，默认如下；发布到自己的 Docker Hub 时，先修改其中的 `image` 字段，并确保上述账号对目标仓库有推送权限。

| 镜像 | 构建版本 |
|---|---|
| `niuai/azure-metrics-exporter` | 默认静态版本，以及标签带 `-azcli` 后缀的 Azure CLI 版本 |
| `niuai/azure-metrics-scraper` | 独立定时采集器 |

### 2. 触发构建

1. 打开仓库 **Actions → release/docker → Run workflow**。
2. 选择包含待发布代码和镜像配置的分支（比如 main），点击 **Run workflow**。
3. 工作流先执行 lint，再构建并推送 `linux/amd64`、`linux/arm64` 镜像。
4. 构建成功后，在 Docker Hub 查看两个镜像仓库的标签。

每次运行生成统一的 UTC 日期标签 `YYYYMMDD-HHmmss-<run_id>-<run_attempt>`，Azure CLI 版本追加 `-azcli`。

</details>

## 部署架构

在同一 namespace 部署以下服务：

```text
Azure Monitor workspace <- exporter (azure-metrics:8080) <- scraper -> Azure Event Hubs
```

- [metrics-exporter.yaml](metrics-exporter.yaml)：exporter Deployment 和 ClusterIP Service。
- [metrics-scraper.yaml](metrics-scraper.yaml)：scraper Deployment 和 memberlist headless Service。
- [kustomization.yaml](kustomization.yaml)：组合两个服务并生成配置 ConfigMap。
    - workspace 来源为 [config/prometheus-workspaces.yaml](../config/prometheus-workspaces.yaml)
    - 采集配置统一来源为 [scraper/scraper.yaml](../scraper/scraper.yaml)。
- scraper 默认每分钟执行配置中的 PromQL 采集任务，查询全部已配置的 workspace，使用最近一分钟窗口、15 秒步长，并将时序指标 JSON 发送到 Event Hub。两个副本通过 memberlist 分配采集任务，不是每个副本重复采集全部任务。

以下 PowerShell 命令均在仓库根目录执行。需要可访问的 Kubernetes 集群、已发布的镜像、Azure Monitor workspace 和 Event Hub。

## 在 Entra ID 中创建应用并赋予权限

1. 在 Azure Portal 打开 **Microsoft Entra ID → 应用注册 → 新注册**，填写应用名称，选择“仅此组织目录中的帐户”，无需配置重定向 URI。
2. 注册后，在“概述”中记录 **应用程序（客户端）ID** 和 **目录（租户）ID**；在 **证书和密码 → 客户端密码 → 新客户端密码** 中创建密码，立即安全保存密码的**值**，供后续创建 Kubernetes Secret 使用，不要提交到仓库。
3. 打开每个目标 **Azure Monitor workspace → 访问控制（IAM）→ 添加角色分配**，选择 **Monitoring Data Reader**，将访问权限分配给刚创建的应用对应的服务主体。如需补充资源区域，再在对应资源或资源组范围授予 **Reader**。

以上是 Azure 资源的 RBAC 授权，无需在应用注册的“API 权限”中添加 Microsoft Graph 权限。Event Hub 仍使用下文单独配置的 SAS 连接字符串。

## 1. 修改配置

1. 在 [workspace 配置](../config/prometheus-workspaces.yaml) 中填写真实的 workspace Prometheus 查询终结点；可增加多个 workspace。此文件同时用于本地运行和 Kubernetes ConfigMap 生成，无需在部署清单中重复维护。
2. 在唯一的 [采集配置](../scraper/scraper.yaml) 中确认 `event_hub.namespace`、`event_hub.name`、PromQL 查询及静态标签（例如将 `environment: development` 改为实际环境）。保持目标地址 `azure-metrics:8080` 与 exporter Service 一致，并保留默认的 memberlist 配置。
3. 部署前，将 [exporter 清单](metrics-exporter.yaml) 和 [scraper 清单](metrics-scraper.yaml) 中的 `image` 分别更新为 [构建镜像](#构建镜像) 章节发布的镜像地址，包含对应的版本标签。
4. 若 Azure Monitor 存在入库延迟，调整 `rolling_range.delay`；抓取较慢时同时调整 `scrape_timeout` 和 exporter 的写超时，前者不得超过 `scrape_interval`。

- exporter 使用服务主体认证。该身份需要各 workspace 的 **Monitoring Data Reader** 权限；如需补充资源区域，还需对应资源的读取权限。
- scraper 单独使用具有 **Send** 权限的 Event Hub SAS 连接字符串，其 namespace、hub 名必须与实际一致，且必须包含 `;EntityPath=<event_hub.name>`。

## 2. 在 Kubernetes（AKS）中创建 namespace 和 Secret

首次连接 AKS 时，使用 `az aks get-credentials` 获取集群凭据并配置本地 kubectl 上下文；后续操作前，确认当前上下文指向目标集群。

```powershell
az aks get-credentials --subscription "<subscription-id>" --resource-group "<resource-group>" --name "<aks-cluster-name>"
```

将下方 `$namespace` 设置为实际使用的命名空间。密钥通过交互输入，不要写入清单、提交到仓库或输出到日志。

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

使用 AKS 默认网络配置且未额外限制出站流量时，以下通信通常已连通，无需额外放行；如配置了 NetworkPolicy、NSG、防火墙或私有端点，则需确认允许 scraper 到 exporter 的 TCP 8080、scraper 副本间双向 TCP/UDP 7946、DNS，以及到 Event Hubs 的 TCP 5671 和到 Azure 身份、Monitor、Resource Manager 端点的 HTTPS 443。

## 3. 部署和检查

```powershell
kubectl apply -n $namespace -k .\deploy\

# 通过 Portal 或者命令行的方式观察部署是否成功
kubectl rollout status -n $namespace deployment/azure-metrics-exporter
kubectl rollout status -n $namespace deployment/azure-metrics-scraper
```

Pod 就绪不代表指标已成功发送。实际还需通过 Event Hub 接收端确认消息中的 workspace、时间范围和指标值。

scraper 不持久化数据，也不是补采队列；重启或多实例成员收敛期间可能短暂漏采或重复，不保证恰好一次发送。更多采集参数和分片说明见 [scraper 文档](../scraper/README.md)。

## 4. 更新配置

修改 workspace 或采集配置源文件后，使用 `-k` 重新部署（不要使用 `-f .\deploy\`）。生成的 ConfigMap 使用固定名称 `azure-metrics-exporter` 和 `azure-metrics-scraper`。更新 ConfigMap 不会自动触发 Deployment 滚动更新，需要手动重启对应服务。Secret 引用保持原名。

仅修改采集配置时，重新应用后重启 scraper：

```powershell
kubectl apply -n $namespace -k .\deploy\

kubectl rollout restart -n $namespace deployment/azure-metrics-scraper
kubectl rollout status -n $namespace deployment/azure-metrics-scraper
```

修改 workspace 配置时，先重启 exporter 并等待更新完成，再重启 scraper，使其重新发现 workspace

## 5. 开启 AKS 控制平面指标并配置拉取 job

如需采集 API Server、etcd、Scheduler 等控制平面指标，需先在 AKS 侧启用 Azure Monitor 托管 Prometheus 和控制平面指标采集，再配置导出器及 Prometheus 拉取 job；仅添加 job 不会自动开启 AKS 侧采集。

具体开启步骤、指标白名单和 job 示例见 [AKS 高级控制平面指标](../PROMQL.md#aks-control-plane)。
