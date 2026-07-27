<div align="center">
  <img src="docs/image_readme.png" alt="Aqueduct Banner">
</div>

# Aqueduct

[ [🇬🇧 English](README.md) | [🇷🇺 Русский](README.ru.md) | 🇨🇳 中文 ]

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kshishtovsky/aqueduct.svg)](https://pkg.go.dev/github.com/kshishtovsky/aqueduct)

Aqueduct 是一个基于 **QUIC**（通过 `quic-go`）使用 Go 语言构建的超高性能、零内存分配消息代理。专为极低延迟（< 1.5 微秒）、零拷贝二进制解析和面向数据设计（DoD）而设计。

> [!IMPORTANT]
> **生产就绪 (v1.13.0)**
> Aqueduct 支持 **Consumer Groups & Lock-Free Atomic Round-Robin Routing**、**gRPC Control Plane (Admin API)** 实现 Quotas 与 ACL 的 **Lock-Free RCU 热重载**、**硬实时延迟优先级队列 (QoS)**、**Per-Priority TTL**、**严格优先级调度**、**mTLS 1.3 传输身份验证**、**零分配 ACL 授权**、**AES-256-GCM 加密日志 (AAL)** 与 **启动状态恢复 (Replay)**、**异步 Fan-Out 与慢消费者隔离**、**零分配 ZSTD 数据压缩**、**MQTT 通配符主题路由**、**直连网状集群 (P2P Federation)**、**零拷贝协议批处理**、**订阅者写合并**、**NACK 重投** 和 **死信队列 (DLQ)**。

---

## 核心特性

- **Idempotent Producers (Exactly-Once)**: 基于 2048 位环形缓冲区 (32 × `uint64` = 256 B) 的生产者级滑动窗口去重，使用 lock-free 位检查/设置 (`3.7 ns/op`, `0 allocs/op`)。生产者附加 `(ProducerID, SeqNum)` TLV 对；重复消息被静默丢弃并发送合成的 `dedup_ack`，严重过期的 SeqNum 则作为协议错误关闭流。
- **Consumer Groups & Atomic Round-Robin Routing**: 竞争消费者 (Competing Consumers) 可加入指定消费组 (如 `topic:orders:group:payment-workers`)。消息在组内成员间通过 **Lock-Free Atomic Round-Robin** (`0 allocs/op`, `< 10 ns/op`) 实现无锁负载均衡。Group Durable Offset 在 Worker 故障转移时维持组级别的断点续传。
- **Dynamic Control Plane (gRPC Admin API)**: 独立 gRPC Admin 服务器 (`:9091`)，支持 mTLS 角色验证 (`admin-*` CN)，实现无锁 RCU 动态热重载限额与 ACL 规则。
- **QUIC 传输层**: 具备 0-RTT 连接建立、流隔离和放大攻击保护。
- **零拷贝二进制协议**: 扁平 10 字节二进制首部 (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`)，支持 TLV 扩展块的零拷贝解析。
- **延迟优先级队列 (QoS)**: 支持 4 个消息优先级 (`0` 最高, `1` 高, `2` 普通, `3` 低)，通过 TLV `ExtPriority` (`0x03`) 传输。队列从 `sync.Pool` 延迟按需分配 (`0 allocs/op`)，单优先级订阅者仅消耗 1 个队列内存。
- **严格优先级调度与防饥饿**: 专用 Writer 协程按严格顺序 (`0 -> 1 -> 2 -> 3`) 轮询优先级队列，确保高优先级紧急消息超越低优先级流量。
- **按优先级 TTL (Per-Priority TTL)**: 可配置 `priority_ttls` (`["500ms", "5s", "0", "0"]`)，强制重写过期时间戳。出队时自动延迟丢弃过期消息 (`aqueduct_messages_expired_total{topic, priority}`)。
- **内存自动回收与垃圾清理**: 队列变空 (`len(q) == 0`) 时自动归还给 `sync.Pool` 并清空，由订阅者锁保护。
- **零分配数据压缩**: ZSTD 批处理压缩 (`internal/compress`)，搭配 `ExtCompression` (`0x02`) TLV 扩展，实现跨节点转发前高效压缩。
- **Structure of Arrays (SoA) 路由器**: 内存 Pub/Sub 路由，使用连续数组优化 CPU L1/L2 缓存命中率。
- **异步 Fan-Out 与环形队列**: 每个订阅者拥有非阻塞队列与独立的 Writer 协程。
- **慢消费者隔离 (Backpressure)**: 每个优先级独立的队列溢出策略 (`drop_oldest`, `drop_newest`, `disconnect`)。
- **原子引用计数 (`MessageRef`)**: 安全的零分配 `sync.Pool` 缓冲区回收 (`0 allocs/op`)。
- **零拷贝协议批处理**: `CmdPublishBatch` (0x04) 命令通过 `unsafe.Slice` 实现零拷贝批量发布 (< 4 ns/frame, `0 allocs/op`)。
- **订阅者写合并**: 可配置的 64 KB 阈值和 50 µs 微定时器微批处理，实现 6.67M msg/s 吞吐量。
- **MQTT 通配符主题路由**: 支持单级 (`+`) 和多级 (`#`) 通配符匹配 (< 51 ns/op, `0 allocs/op`)。
- **加密追加日志 (AAL)**: AES-256-GCM 加密与 12 字节 Nonce 和长度前缀记录。
- **mTLS 与零分配 ACL**: 双向 TLS 1.3 认证与非交换 FNV-1a 组合哈希权限矩阵。
- **NACK 重投与死信队列 (DLQ)**: `CmdNack` (0x05) 操作码，支持自动重投 (达 `max_retries`)、订阅者端绑定帧缓存 (FIFO 256) 和毒丸消息路由到 `__dlq__<topic>`。
- **Prometheus 监控**: 提供 `/metrics` 端点和 Ready-to-use Grafana 仪表盘。

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
  -metrics-addr :9090
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
  max_aal_size: 104857600 # 100 MB

acl:
  enabled: false
  default: "none"
  rules:
    - client: "service-a"
      topic: "orders"
      permission: "publish"

broker:
  queue_size: 1024
  backpressure_policy: "drop_oldest"
  batch_size: 65536
  flush_interval: "50us"
  max_retries: 3
  quotas:
    default_publish_rate: 0
    default_burst_size: 1000

transport:
  max_buf_size: 65536
  read_buf_size: 1024
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
| `AQUEDUCT_BROKER_QUEUE_SIZE` | `broker.queue_size` | `2048` |
| `AQUEDUCT_BROKER_BACKPRESSURE_POLICY` | `broker.backpressure_policy` | `drop_oldest` |
| `AQUEDUCT_BROKER_BATCH_SIZE` | `broker.batch_size` | `65536` |
| `AQUEDUCT_BROKER_FLUSH_INTERVAL` | `broker.flush_interval` | `50us` |
| `AQUEDUCT_BROKER_MAX_RETRIES` | `broker.max_retries` | `3` |
| `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` | `broker.quotas.default_publish_rate` | `100` |
| `AQUEDUCT_BROKER_DEFAULT_BURST_SIZE` | `broker.quotas.default_burst_size` | `1000` |
| `AQUEDUCT_TRANSPORT_MAX_BUF_SIZE` | `transport.max_buf_size` | `131072` |
| `AQUEDUCT_CLUSTER_DISCOVERY_ENABLED` | `cluster.discovery.enabled` | `true` |
| `AQUEDUCT_CLUSTER_DISCOVERY_HOST` | `cluster.discovery.host` | `aqueduct-headless.default.svc.cluster.local` |
| `AQUEDUCT_CLUSTER_DISCOVERY_PORT` | `cluster.discovery.port` | `4242` |
| `AQUEDUCT_CLUSTER_DISCOVERY_INTERVAL` | `cluster.discovery.interval` | `10s` |

---

## 文档 (Diátaxis 框架)

- **教程**: [入门指南](docs/zh/getting-started.md)
- **参考**: [二进制协议规范](docs/zh/protocol-spec.md)
- **解释**: [架构与内存模型](docs/zh/architecture.md)
- **指南**: [生产部署与安全](docs/zh/production-deployment.md)

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
5. 使用 RCU（Read-Copy-Update）原子交换 — 消息转发热路径零锁

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
