<div align="center">
  <img src="docs/image_readme.png" alt="Aqueduct Banner">
</div>

# Aqueduct

[ [🇬🇧 English](README.md) | [🇷🇺 Русский](README.ru.md) | 🇨🇳 中文 ]

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kshishtovsky/aqueduct.svg)](https://pkg.go.dev/github.com/kshishtovsky/aqueduct)

Aqueduct 是一个基于 **QUIC**（通过 `quic-go`，ALPN = `aqueduct-v1`）构建的超高性能、零内存分配消息代理。采用 Go 编写，面向极低延迟（< 1.5 微秒）、零拷贝二进制解析和面向数据设计（DoD）。

> [!IMPORTANT]
> **生产就绪 (v1.16.0)**
> Aqueduct 提供 **Consumer Groups & Lock-Free Atomic Round-Robin Routing**、**gRPC Control Plane (Admin API)** 实现 Quotas 与 ACL 的 **Lock-Free RCU 热重载**、**硬实时延迟优先级队列 (QoS)**、**Per-Priority TTL**、**严格优先级调度**、**mTLS 1.3 传输身份验证**、**零分配 ACL 授权**、**AES-256-GCM 加密日志 (AAL)** 与 **启动状态恢复 (Replay)**、**异步 Fan-Out 与慢消费者隔离**、**零分配 ZSTD 数据压缩**、**MQTT 通配符主题路由**、**直连网状集群 (P2P Federation)**、**零拷贝协议批处理**、**订阅者写合并**、**NACK 重投**、**死信队列 (DLQ)** 以及 **WebTransport (HTTP/3) 浏览器网关**。

---

## 核心特性

- **Consumer Groups & Atomic Round-Robin Routing**: 竞争消费者 (Competing Consumers) 可加入指定消费组 (如 `topic:orders:group:payment-workers`)。消息在组内成员间通过 **Lock-Free Atomic Round-Robin** (`0 allocs/op`, `< 10 ns/op`) 实现无锁负载均衡。Group Durable Offset 在 Worker 故障转移时维持组级别的断点续传。
- **Dynamic Control Plane (gRPC Admin API)**: 独立 gRPC Admin 服务器 (`admin_addr`, 默认 `:9091`)，支持 mTLS 角色验证 (客户端证书 CN 必须以 `admin-` 开头)，实现无锁 RCU 动态热重载限额与 ACL 规则。
- **QUIC 传输层 (ALPN `aqueduct-v1`)**: 具备 0-RTT 连接建立、流隔离 (`MaxIncomingStreams: 100`)、放大攻击保护与强制 TLS 1.3。
- **WebTransport Gateway (HTTP/3)**: 可选的 `internal/webtransport/` 监听器接受来自浏览器的 W3C WebTransport API (ALPN `h3`)。复用 Broker 的 mTLS 证书，因此单一份根证书同时保护原生客户端与浏览器客户端。浏览器客户端写入与原生 QUIC 客户端**完全相同的二进制帧格式** — 零协议转换开销。
- **零拷贝二进制协议**: 扁平 10 字节二进制首部 (`[Magic:1] [Cmd:1] [StreamID:4] [DataLen:4]`)，支持 TLV 扩展块的零拷贝解析 (`unsafe.Slice`)。
- **延迟优先级队列 (QoS)**: 支持 4 个消息优先级 (`0` 最高, `1` 高, `2` 普通, `3` 低)，通过 TLV `ExtPriority` (`0x03`) 传输。队列从 `sync.Pool` 延迟按需分配 (`0 allocs/op`)，单优先级订阅者仅消耗 1 个队列内存。
- **严格优先级调度与防饥饿**: 专用 Writer 协程按严格顺序 (`0 -> 1 -> 2 -> 3`) 轮询优先级队列，确保高优先级紧急消息超越低优先级流量。
- **按优先级 TTL (Per-Priority TTL)**: 可配置 `priority_ttls` (`["500ms", "5s", "0", "0"]`)，强制重写过期时间戳。出队时自动延迟丢弃过期消息 (`aqueduct_messages_expired_total{topic, priority}`)。
- **内存自动回收与垃圾清理**: 队列变空 (`len(q) == 0`) 时自动归还给 `sync.Pool` 并清空，由订阅者锁保护。
- **零分配数据压缩**: ZSTD 批处理压缩 (`internal/compress`)，搭配 `ExtCompression` (`0x02`) TLV 扩展 (`[Algo:1][UncompressedSize:4]`)，仅对 ≥ `min_batch_size` (默认 1024 字节) 的批次应用。接收端自动解压后剥除 TLV，本地订阅者始终收到未压缩数据。
- **Slab 分配器 (v1.8.0)**: 预分配 64 MB Arena (大小类 128B/256B/512B/2KB/8KB/32KB)，Treiber 栈原子 CAS，零 GC 扫描。
- **每租户速率限制 (Token Bucket, v1.8.0)**: 原子操作补充令牌；每 100 ms 后台补充 goroutine。
- **Structure of Arrays (SoA) 路由器**: 内存 Pub/Sub 路由，使用扁平并行切片 + FNV-1a `topicIndex` 优化 CPU L1/L2 缓存命中率。
- **TopicHash 一致性 (v1.16.0)**: 通过 `parsePublishTopic` + `topicHashKey` 单一真相源规范化发布载荷 (剥离 `ttl:<ms>:` 与可选 `topic:` 前缀)，消除路由键碰撞。
- **异步 Fan-Out 与环形队列**: 每个订阅者拥有非阻塞队列与独立的 Writer 协程。
- **慢消费者隔离 (Backpressure)**: 每个优先级独立的队列溢出策略 (`drop_oldest`, `drop_newest`, `disconnect`)。
- **原子引用计数 (`MessageRef`)**: 安全的零分配 `sync.Pool` 缓冲区回收 (`0 allocs/op`)；批处理使用父子嵌套引用计数。
- **零拷贝协议批处理**: `CmdPublishBatch` (`0x05`) 命令通过 `unsafe.Slice` 实现零拷贝批量发布 (< 4 ns/frame, `0 allocs/op`)。
- **订阅者写合并**: 可配置的 64 KB 阈值和 50 µs 微定时器微批处理，实现 6.67M msg/s 吞吐量。
- **MQTT 通配符主题路由**: 支持单级 (`+`) 和多级 (`#`) 通配符匹配 (< 51 ns/op, `0 allocs/op`)。
- **加密追加日志 (AAL)**: AES-256-GCM 加密与 12 字节 Nonce (4 字节随机会话前缀 + 8 字节单调计数器) 和长度前缀记录。启动时按顺序 Replay 重建状态后绑定 UDP 监听端口。
- **mTLS 与零分配 ACL**: 双向 TLS 1.3 认证 (`MinVersion = tls.VersionTLS13`)，非交换 FNV-1a 组合哈希权限矩阵 (`CombineHashes("clientID", topic)`)。
- **NACK 重投与死信队列 (DLQ)**: `CmdNack` (`0x06`) 操作码 (`[offset:8]`，小端序 uint64)，支持自动重投 (达 `max_retries`)、订阅者端绑定帧缓存 (FIFO 256) 和毒丸消息路由到 `__dlq__<topic>`。
- **Prometheus 监控**: `/metrics` 端点 + Ready-to-use Grafana 仪表盘；19 个原生指标 (Counter/Gauge/Histogram)。注意：`aqueduct_frame_parse_duration_ns` 与 `aqueduct_aal_rotations_total` 当前**已注册但未更新**（详见 [配置参考](docs/zh/configuration.md) 与 [指标参考](docs/zh/metrics.md)）。

---

## 2 分钟快速开始 (Docker Compose)

```bash
docker compose up -d
```

- **健康检查**: `http://localhost:9090/healthz`
- **Prometheus**: `http://localhost:9091`
- **Grafana**: `http://localhost:3000` (User: `admin` / Password: `admin`)

```bash
docker compose down
```

## 本地安装和使用

### 通过二进制或 Go 运行

```bash
# 使用 YAML 配置运行
go run ./cmd/broker/main.go -config config.yaml

# 使用 CLI 参数覆盖运行
go run ./cmd/broker/main.go \
  -config config.yaml \
  -addr :4242 \
  -metrics-addr :9090 \
  -cert /etc/certs/cert.pem \
  -key /etc/certs/key.pem \
  -aal /var/log/aqueduct/aal.log
```

### 配置 (`config.yaml`)

```yaml
listen_addr: ":4242"
metrics_addr: ":9090"

tls:
  generate: true
  cert_file: ""
  key_file: ""
  require_client_cert: false
  client_ca_file: ""

aal:
  enabled: false
  file_path: ""
  key: "" # Base64 编码的 32 字节 AES-256-GCM 密钥
  max_aal_size: 104857600     # 100 MB 触发轮转
  retention_period: "24h"      # 声明但当前未强制（详见下方警告）
  retention_size: 1073741824   # 1 GB — 声明但当前未强制（详见下方警告）

acl:
  enabled: false
  default: "none"
  rules:
    - client: "service-a"
      topic: "orders"
      permission: "publish"

admin:
  enabled: false
  addr: ":9091"

broker:
  queue_size: 1024
  backpressure_policy: "drop_oldest"
  batch_size: 65536
  flush_interval: "50us"
  max_retries: 3
  priority_ttls:
    - "500ms"  # 优先级 0 (最高)
    - "5s"     # 优先级 1 (高)
    - "0"      # 优先级 2 (普通 — 不覆盖)
    - "0"      # 优先级 3 (低 — 不覆盖)
  quotas:
    default_publish_rate: 0
    default_burst_size: 1000

transport:
  max_buf_size: 65536
  read_buf_size: 1024

tracing:
  enabled: false
  service_name: "aqueduct-broker"
  endpoint: "localhost:4317"

compression:
  enabled: false
  min_batch_size: 1024
  level: 0

webtransport:
  enabled: false
  listen_addr: ":4433"
  path_prefix: "/aqueduct/wt"

cluster:
  peers: []
  discovery:
    enabled: false
    type: "dns"
    host: ""
    port: "4242"
    interval: "10s"
  mesh:
    insecure_skip_verify: false  # 生产保持 false；自签开发用 true
    ca_file: ""
```

### 环境变量覆盖

| 环境变量 | 覆盖项 | 示例 |
| :--- | :--- | :--- |
| `AQUEDUCT_LISTEN_ADDR` | `listen_addr` | `:4242` |
| `AQUEDUCT_METRICS_ADDR` | `metrics_addr` | `:9090` |
| `AQUEDUCT_TLS_GENERATE` | `tls.generate` | `false` |
| `AQUEDUCT_TLS_CERT_FILE` | `tls.cert_file` | `/etc/certs/cert.pem` |
| `AQUEDUCT_TLS_KEY_FILE` | `tls.key_file` | `/etc/certs/key.pem` |
| `AQUEDUCT_TLS_REQUIRE_CLIENT_CERT` | `tls.require_client_cert` | `true` |
| `AQUEDUCT_TLS_CLIENT_CA_FILE` | `tls.client_ca_file` | `/etc/certs/ca.pem` |
| `AQUEDUCT_AAL_ENABLED` | `aal.enabled` | `true` |
| `AQUEDUCT_AAL_FILE_PATH` | `aal.file_path` | `/var/log/aal.log` |
| `AQUEDUCT_AAL_KEY` | `aal.key` | `base64_encoded_key` |
| `AQUEDUCT_AAL_MAX_SIZE` | `aal.max_aal_size` | `104857600` |
| `AQUEDUCT_ACL_ENABLED` | `acl.enabled` | `true` |
| `AQUEDUCT_ACL_DEFAULT` | `acl.default` | `all` |
| `AQUEDUCT_ADMIN_ENABLED` | `admin.enabled` | `true` |
| `AQUEDUCT_ADMIN_ADDR` | `admin.addr` | `:9091` |
| `AQUEDUCT_BROKER_QUEUE_SIZE` | `broker.queue_size` | `2048` |
| `AQUEDUCT_BROKER_BACKPRESSURE_POLICY` | `broker.backpressure_policy` | `drop_oldest` |
| `AQUEDUCT_BROKER_BATCH_SIZE` | `broker.batch_size` | `65536` |
| `AQUEDUCT_BROKER_FLUSH_INTERVAL` | `broker.flush_interval` | `50us` |
| `AQUEDUCT_BROKER_MAX_RETRIES` | `broker.max_retries` | `3` |
| `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` | `broker.quotas.default_publish_rate` | `100` |
| `AQUEDUCT_BROKER_DEFAULT_BURST_SIZE` | `broker.quotas.default_burst_size` | `1000` |
| `AQUEDUCT_TRANSPORT_MAX_BUF_SIZE` | `transport.max_buf_size` | `131072` |
| `AQUEDUCT_TRANSPORT_READ_BUF_SIZE` | `transport.read_buf_size` | `4096` |
| `AQUEDUCT_TRACING_ENABLED` | `tracing.enabled` | `false` |
| `AQUEDUCT_TRACING_SERVICE_NAME` | `tracing.service_name` | `aqueduct-broker` |
| `AQUEDUCT_TRACING_ENDPOINT` | `tracing.endpoint` | `localhost:4317` |
| `AQUEDUCT_COMPRESSION_ENABLED` | `compression.enabled` | `true` |
| `AQUEDUCT_WEBTRANSPORT_ENABLED` | `webtransport.enabled` | `true` |
| `AQUEDUCT_WEBTRANSPORT_LISTEN_ADDR` | `webtransport.listen_addr` | `:4433` |
| `AQUEDUCT_WEBTRANSPORT_PATH_PREFIX` | `webtransport.path_prefix` | `/aqueduct/wt` |
| `AQUEDUCT_CLUSTER_DISCOVERY_ENABLED` | `cluster.discovery.enabled` | `true` |
| `AQUEDUCT_CLUSTER_DISCOVERY_HOST` | `cluster.discovery.host` | `aqueduct-headless.default.svc.cluster.local` |
| `AQUEDUCT_CLUSTER_DISCOVERY_PORT` | `cluster.discovery.port` | `4242` |
| `AQUEDUCT_CLUSTER_DISCOVERY_INTERVAL` | `cluster.discovery.interval` | `10s` |
| `AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY` | `cluster.mesh.insecure_skip_verify` | `false` |
| `AQUEDUCT_CLUSTER_MESH_CA_FILE` | `cluster.mesh.ca_file` | `/etc/certs/mesh-ca.pem` |

> [!WARNING]
> **安全警告**：`AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY=true` 会禁用对等节点证书的验证，使网格容易受到 MITM 攻击。生产环境必须保持 `false` 并通过 `AQUEDUCT_CLUSTER_MESH_CA_FILE` 或系统 CA 池提供受信 CA。

---

## 基准测试 (`aqueduct-bench`)

运行内置的高并发负载测试 CLI（实际 flag 名以 `cmd/aqueduct-bench/main.go` 为准）：

```bash
go run ./cmd/aqueduct-bench/main.go \
  -addr 127.0.0.1:4242 \
  -c 10 \
  -n 100000 \
  -size 128 \
  -topic bench \
  -batch 1
```

支持标志 (默认): `-addr` (`:4242`), `-c` (10 并发 worker), `-n` (100000 请求), `-size` (128 字节载荷), `-topic` (`bench`), `-timeout` (`5s`), `-batch` (1 = 单帧), `-tls-verify` (默认 `false`；生产环境请启用), `-ca-file` (PEM CA bundle)。

---

## 文档 (Diátaxis 框架)

- **教程 (Tutorial)**: [入门指南](docs/zh/getting-started.md)
- **参考 (Reference)**: [二进制协议规范](docs/zh/protocol-spec.md)
- **解释 (Explanation)**: [架构与内存模型](docs/zh/architecture.md)
- **指南 (How-to)**: [生产部署与安全](docs/zh/production-deployment.md), [配置参考](docs/zh/configuration.md), [指标参考](docs/zh/metrics.md), [Admin API 参考](docs/zh/admin-api.md), [网格 TLS](docs/zh/cluster-mesh-tls.md), [故障排查](docs/zh/troubleshooting.md)

---

## Kubernetes 部署 (Helm)

一条命令部署 3 节点 Aqueduct 集群，启用 DNS 对等发现：

```bash
helm install aqueduct ./deploy/helm/aqueduct \
  --namespace aqueduct --create-namespace
```

### 对等发现工作原理

在 Kubernetes 上部署时，Aqueduct 通过 Headless Service 使用 **DNS 对等发现**：

1. 每个 Pod 获得稳定的 DNS 名称：`aqueduct-0.aqueduct-headless.aqueduct.svc.cluster.local`
2. Headless Service 返回所有就绪 Pod 的 **A 记录**
3. 后台协程每 10 秒轮询 DNS（可配置）
4. 新 Pod（扩容）自动连接，终止的 Pod（缩容）自动移除
5. 使用 RCU（Read-Copy-Update）原子 `atomic.Pointer[peerSlice]` 交换 — 消息转发热路径零锁

### 为什么选择 DNS 而非 K8s API（client-go）

| 方面 | DNS 解析 | client-go |
| :--- | :--- | :--- |
| 二进制大小影响 | 0 MB（标准库） | ~40 MB |
| 外部依赖 | 无 | REST 客户端、protobuf、informers |
| 动态更新 | 自动（Headless Service） | Watch + label selector |
| 静态二进制哲学 | 是 | 否 |

### 配置

```yaml
cluster:
  discovery:
    enabled: true
    type: "dns"
    host: "aqueduct-headless.aqueduct.svc.cluster.local"
    port: "4242"
    interval: "10s"
```

### 扩缩容

```bash
# 扩容到 5 个副本
helm upgrade aqueduct ./deploy/helm/aqueduct --set replicaCount=5

# 缩容到 2 个副本
helm upgrade aqueduct ./deploy/helm/aqueduct --set replicaCount=2
```

DNS 发现自动协调 P2P mesh — 无需手动配置。

### 原始 K8s 清单

对于不使用 Helm 的部署，原始清单位于 `deploy/k8s/`：

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

---

## 许可证

MIT License. 详见 [LICENSE](LICENSE)。