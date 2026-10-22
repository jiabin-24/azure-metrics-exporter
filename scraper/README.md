# 独立定时指标采集器

[返回项目 README](../README.md)

`azure-metrics-scraper` 定时从 exporter 拉取指标，可发送到 Azure Event Hubs，无需部署 Prometheus。exporter 和 scraper 是两个独立进程。

**以下 PowerShell 命令均在仓库根目录执行。**

## 本地启动

### 1. 启动 exporter

填写 [workspace 配置文件](../config/prometheus-workspaces.example.yaml)，将示例地址替换为真实的 Azure Monitor Prometheus 查询终结点：

```yaml
workspaces:
  workspace-a:
    query_endpoint: https://<workspace-a>.<region>.prometheus.monitor.azure.com
  workspace-b:
    query_endpoint: https://<workspace-b>.<region>.prometheus.monitor.azure.com
```

采集 PromQL 指标时，**必须通过环境变量或命令行参数指定 workspace 文件**，不会自动读取示例文件。未指定时 exporter 虽然可以启动，但 workspace 发现接口返回 HTTP 503，包含 PromQL job 的 scraper 无法启动。

在第一个终端指定文件并启动：

```powershell
$env:AZURE_PROMETHEUS_WORKSPACES_FILE = ".\config\prometheus-workspaces.example.yaml"
go run . --server.timeout.write=120s
```

也可使用参数 `--azure.prometheus.workspaces-file=.\config\prometheus-workspaces.example.yaml`。相对路径以启动目录为基准。Azure 身份按[项目配置](../README.md#configuration)设置，并授予各 workspace 的 `Monitoring Data Reader` 权限。

### 2. 启动 scraper

[scraper.yaml](scraper.yaml) 默认访问 `127.0.0.1:8080`，已启用 Event Hub。先修改其中的 Event Hub 目标，再在第二个终端读取具有 **Send** 权限的 SAS 连接字符串并启动：

```powershell
$secret = Read-Host "Event Hub SAS connection string" -AsSecureString
$env:AZURE_EVENTHUB_CONNECTION_STRING = [System.Net.NetworkCredential]::new("", $secret).Password
go run .\scraper\cmd\metrics-scraper --config.file=.\scraper\scraper.yaml --web.listen-address=:8081
```

连接字符串须包含与配置匹配的 namespace 和 `EntityPath=<hub-name>`。按当前示例配置，若从 Azure 门户复制的连接字符串不含 `EntityPath`，须在末尾追加 `;EntityPath=shein-aks-metrics`；若已包含，则确认其值与 `event_hub.name` 一致，不要重复追加。不要将密钥写入 YAML 或提交到仓库。只调试采集时，可移除 `event_hub` 配置；此时 batch 仅记录日志后丢弃，不发送、不落盘。

检查 workspace 发现和 scraper 就绪状态：

```powershell
Invoke-RestMethod "http://127.0.0.1:8080/probe/metrics/promql/workspaces"
Invoke-WebRequest "http://127.0.0.1:8081/readyz"
```

`/healthz` 表示进程存活；`/readyz` 不代表上游查询或 Event Hub 发送成功，采集和发送错误需查看日志。

## 核心配置

本地修改 [scraper.yaml](scraper.yaml)，Kubernetes 修改[部署清单](deploy/metrics-scraper.yaml)中的 ConfigMap，两者独立维护。以下为启用 Event Hub 的配置结构：

```yaml
global:
  scrape_interval: 1m
  scrape_timeout: 30s

event_hub:
  namespace: example.servicebus.windows.net
  name: metrics

scrape_configs:
  - job_name: azure-control-plane
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

- `scrape_interval`：抓取周期，job 可覆盖全局值；`scrape_timeout` 必须不大于周期。
- `targets`：exporter 的 `host:port`，不带协议或路径；HTTPS 使用 job 的 `scheme: https`。
- `params`：请求参数，值为字符串列表。启用 Event Hub 时必须使用支持的指标 probe 端点和 `format: ["json"]`，PromQL 必须只有一个 `query`。
- 不修改源指标标签和时间戳；`honor_timestamps` 只支持 `true` 或省略。
- 配置不支持热加载，修改后需重启。未知字段、非法配置或 workspace 发现失败会在启动时报错。

### PromQL JSON 采集

PromQL job 固定采集 exporter 配置的**全部 workspace**，无需也不允许填写 `workspaces` 或 `params.workspace`。每个 workspace 独立执行相同查询。别名列表在启动时获取，exporter 增删 workspace 后需重启 scraper。

### PromQL 滚动查询范围

`rolling_range` 每次生成 `[当前时间 - delay - window, 当前时间 - delay]` 的查询范围，默认 `window: 1m`、`step: 15s`、`delay: 0s`。

有入库延迟时调整 `delay`，例如 `20m`。不能同时配置 `params.start/end/step/time/query_delay`。即时查询需移除 `rolling_range`。更多查询参数见 [PromQL 文档](../PROMQL.md)。

### 平台指标 JSON 采集

使用平台指标端点，在 `params` 中填写订阅、资源、指标及 `format: ["json"]`，不涉及 workspace 发现。可与 PromQL job 共用 Event Hub，示例见 [scraper.yaml](scraper.yaml)中的注释及[平台指标文档](../README.md#platform-window-json)。

## 多 workspace 与 memberlist 分片

单实例不配置 `cluster`。多实例使用相同 job 配置、Event Hub 目标、exporter 地址及 workspace 映射，并启用：

```yaml
cluster:
  name: azure-metrics-scraper
  bind_address: 0.0.0.0:7946
  join: ["scraper-seed:7946"]
  retry_interval: 5s
```

- `join` 填写可达的种子或成员发现 DNS；首次加入成功前不采集，`/readyz` 返回 503，并持续重试。
- `join: []` 用于建立新集群，不能让所有独立实例都以空种子启动。
- 成员之间须双向放行 **TCP/UDP 7946**。必要时指定 `advertise_address: <可达IP>:7946`。
- 节点名自动生成，无需设置 total/index。副本增减或故障后自动重新分配任务，允许短暂漏采或重复；不持久化、不补采、不保证 exactly-once。
- 可用 `SCRAPER_CLUSTER_NODE_NAME`、`SCRAPER_CLUSTER_BIND_ADDRESS`、`SCRAPER_CLUSTER_ADVERTISE_ADDRESS`、`SCRAPER_CLUSTER_JOIN` 覆盖字段；须先在 YAML 启用 `cluster`，种子用逗号分隔。
- 可通过 `SCRAPER_CLUSTER_SECRET_KEY` 设置成员共享的 base64 AES 密钥（解码后 16、24 或 32 字节）；未加密时仅用于可信私网。

本地测试两个进程时，共享配置中使用 `cluster: {}`。第一个终端设置绑定地址 `127.0.0.1:7946`；第二个终端设置 `127.0.0.1:7947` 并通过 `SCRAPER_CLUSTER_JOIN=127.0.0.1:7946` 加入，健康监听端口分别使用 8081、8082。

## Docker Compose

构建镜像：

```powershell
docker build -f .\scraper\Dockerfile -t azure-metrics-scraper:local .
```

在共享配置中启用 `cluster: {}`，将 target 改为容器可访问的 exporter 地址，例如 `azure-metrics:8080`。以下 Compose 片段按文件位于仓库根目录编写：

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

exporter 与 scraper 须处于互通的 Docker 网络。使用 `docker compose up -d --scale scraper=2` 启动两个副本，不要设置固定 `container_name`。容器间直接通信，无需向宿主机发布 gossip 端口。

## Kubernetes 部署

[部署清单](deploy/metrics-scraper.yaml)默认包含双副本 Deployment、ConfigMap 和用于成员发现的 Headless Service。

1. 构建并推送镜像，将清单中的 `image` 改为可拉取的镜像地址。
2. 修改 ConfigMap 中的 exporter target、查询和 Event Hub 目标。
3. 在同一 namespace 准备 Secret `azure-metrics-eventhub`，键为 `connection-string`。
4. 放行 scraper Pod 间 TCP/UDP 7946，以及到 exporter、DNS 和 Event Hubs（TCP 5671）的网络访问。

```powershell
docker tag azure-metrics-scraper:local <registry>/azure-metrics-scraper:v1
docker push <registry>/azure-metrics-scraper:v1
kubectl apply -n <namespace> -f .\scraper\deploy\metrics-scraper.yaml
kubectl rollout status -n <namespace> deployment/azure-metrics-scraper
kubectl logs -n <namespace> deployment/azure-metrics-scraper --follow
```

可直接修改 `replicas` 或使用 HPA，无需额外协调 Pod、Kubernetes API 权限或分片编号。修改 ConfigMap 后重新 `apply`，再执行：

```powershell
kubectl rollout restart -n <namespace> deployment/azure-metrics-scraper
```
