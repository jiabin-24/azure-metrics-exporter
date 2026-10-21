# 独立定时指标采集 Pod

[返回项目 README](../README.md)

`azure-metrics-scraper` 是与 exporter 分开运行的 Go 进程，通过 HTTP 定时拉取 exporter 返回的 Prometheus 文本，不需要部署 Prometheus 服务。

采集器的代码、测试、配置文件、Dockerfile、部署清单和说明统一放在本目录，与 exporter 共用仓库根目录的 Go module，但独立构建镜像。以下命令均从 `scraper` 目录执行；若当前在仓库根目录，先执行 `cd .\scraper`。

**当前阶段仅实现采集。Azure Event Hub 发送尚未实现，数据不持久化、不缓存、不重试发送。** 每次成功采集只记录 job、target、指标族数量和响应字节数，并明确记录丢弃 batch 的日志；不会输出完整指标内容，也不会声称消息已经发送。

## 配置

本地调试使用 [scraper.yaml](scraper.yaml)，默认访问 `127.0.0.1:8080`；Kubernetes 使用[部署清单](deploy/metrics-scraper.yaml) 中的 ConfigMap，默认访问 `azure-metrics:8080`。两份配置独立维护，不会自动同步，镜像不内置配置文件。配置使用 Prometheus 风格的静态 job 子集，不是完整的 Prometheus 配置兼容实现，以下为本地配置示例：

```yaml
global:
  scrape_interval: 1m
  scrape_timeout: 30s
scrape_configs:
  - job_name: azure-control-plane
    metrics_path: /probe/metrics/promql
    honor_timestamps: true
    params:
      query: ["apiserver_request_total"]
      # 根据工作区入库延迟按需启用。
      # query_delay: ["20m"]
    static_configs:
      - targets: ["127.0.0.1:8080"]
        labels:
          environment: development
  - job_name: exporter-self
    scrape_interval: 30s
    scrape_timeout: 5s
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:8080"]
```

- 支持 `global.scrape_interval` / `scrape_timeout`；默认分别为 `1m` / `10s`。job 可以覆盖，两者必须为正的 Go duration，显式超时不能大于周期；继承的超时超过 job 周期时自动缩短到周期。省略全局超时时，默认超时也不超过全局周期。
- job 名称必须非空且唯一。支持 `scheme`（`http` / `https`）、`metrics_path`（默认 `/metrics`）、`params`（值为字符串列表，可多值）和 `static_configs.targets` / `labels`。
- target 使用 `host[:port]`，不包含协议、路径或认证信息；IPv6 使用方括号。相同 job 内 target 不可重复。
- `params` 原样 URL 编码后发送到 exporter，适用于其所有 probe 端点；这里只请求 exporter，不直接请求 Azure。PromQL 的 `query_delay` 等规则见 [PROMQL 文档](../PROMQL.md)。
- 立即执行第一次抓取，然后按每个 job 的周期调度。不同 target 独立运行，同一 target 不重叠抓取；失败记录错误日志，下一个周期继续，绝不复用旧结果或补零。
- 限制单次响应为 16 MiB；只接受 HTTP 200 和合法的 Prometheus 文本格式（允许空结果）。不跟随 HTTP 重定向。请求和响应读取均受超时与退出信号约束。
- 原始指标文本及其中的显式时间戳不做修改；只支持 `honor_timestamps: true`（或省略）。job、target、静态 labels、采集时间、Content-Type 和原始文本进入 batch，留给后续发送逻辑使用。**labels 是 batch 元数据，不会注入或改写指标文本，也不实现 Prometheus 的 job/instance 标签合并规则。**
- 不实现服务发现、relabel、认证配置、本地 PromQL 计算、TSDB、历史补采或完整 OpenMetrics/protobuf 协议。未知配置字段、重复 YAML 键、非法周期等会在启动时直接报错退出，不会静默忽略。
- 配置在启动时读取，不支持热加载。本地修改后重启进程；Kubernetes 修改 ConfigMap 后执行 `apply` 和 `rollout restart`，无需重新构建镜像。

对于 `30s` probe 抓取超时，应协调 exporter 的写超时，例如设置 `--server.timeout.write=120s`，避免 exporter 默认 `10s` 提前结束响应。

## 本地运行

先确保本地 exporter 已启动；本目录的 [scraper.yaml](scraper.yaml) 默认 target 为 `127.0.0.1:8080`，若 exporter 在其他地址则修改各 job 的 target，然后运行：

```powershell
go run .\cmd\metrics-scraper --config.file=.\scraper.yaml --web.listen-address=:8081
```

采集器不需要 Azure 凭据；身份和权限仍由现有 exporter 管理。`/healthz` 和 `/readyz` 表示进程及配置已就绪，不代表上游抓取成功或 Event Hub 已连通。抓取失败需查看日志。Ctrl+C 或 SIGTERM 会取消正在执行的请求并退出。

## Kubernetes 部署

[部署清单](deploy/metrics-scraper.yaml) 包含 ConfigMap 和单副本 Deployment，配置只读挂载到 `/etc/azure-metrics-scraper/scraper.yaml`。`Recreate` 策略用于避免常规滚动更新时两个版本同时采集；不要横向扩容，否则会重复采集。

先修改部署清单 ConfigMap 中的 target 为现有 exporter 的 Kubernetes Service 地址（跨 namespace 使用完整 DNS），再使用本目录独立的 [Dockerfile](Dockerfile) 构建并推送镜像。虽然命令在 `scraper` 目录执行，构建上下文仍需指定为上一级仓库根目录，以读取共享的 Go module：

```powershell
docker build -f .\Dockerfile -t <registry>/azure-metrics-scraper:v1 ..
docker push <registry>/azure-metrics-scraper:v1
```

将清单中的 `image` 替换为该镜像。不是新建 exporter，也不会创建 Event Hub 资源。采集器镜像与 exporter 镜像独立，根目录 Dockerfile 仍只构建 exporter。

```powershell
kubectl apply -n <namespace> -f .\deploy\metrics-scraper.yaml
kubectl rollout status -n <namespace> deployment/azure-metrics-scraper
kubectl logs -n <namespace> deployment/azure-metrics-scraper --follow
```

后续修改部署清单中的 ConfigMap 后，应用配置并重启采集器以重新读取：

```powershell
kubectl apply -n <namespace> -f .\deploy\metrics-scraper.yaml
kubectl rollout restart -n <namespace> deployment/azure-metrics-scraper
kubectl rollout status -n <namespace> deployment/azure-metrics-scraper
```

无需 Service、Azure 身份或额外 RBAC；健康探针直接访问 Pod。集群网络策略需要允许采集 Pod 访问 exporter。

## Event Hub TODO

后续在 [采集逻辑](internal/scraper/scraper.go) 的 `PendingEventHubSink` 接入真实发送，沿用 batch 中的原始源时间戳及 job / target / labels 元数据。届时再确定消息 schema、身份权限、批大小、重试、背压和持久化策略；当前未引入 Event Hub SDK 或凭据配置。

## 验证

```powershell
go test ./...
```

此命令覆盖采集器代码；若需验证整个仓库，在仓库根目录运行 `go test ./...`。
