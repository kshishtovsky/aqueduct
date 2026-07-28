<div align="center">
  <img src="docs/image_readme.png" alt="Aqueduct Banner">
</div>

<div align="center">

[ 🇬🇧 English | [🇷🇺 Русский](README.ru.md) | [🇨🇳 中文](README.zh.md) ]

# 🌊 Aqueduct

**Ultra-fast, zero-allocation message broker built on Go & QUIC**

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions)
![Go Version](https://img.shields.io/badge/go-1.22%2B-blue)
![Latency](https://img.shields.io/badge/latency-%3C1.5%C2%B0s-orange)
![Allocations](https://img.shields.io/badge/allocs-0_B%2Fop-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

</div>

Aqueduct is an ultra-high performance, zero-allocation message broker built in Go on top of **QUIC** (via `quic-go`). Engineered for extreme low latency (< 1.5 µs), zero-copy binary framing, and Data-Oriented Design (DoD), Aqueduct delivers predictable performance with zero heap allocations on the hot path.

> [!IMPORTANT]
> **Production Ready (v1.16.0)**
> Aqueduct features **Consumer Groups & Lock-Free Atomic Round-Robin Routing**, **gRPC Control Plane (Admin API)** for **lock-free RCU Hot-Reload** of Quotas and ACL rules, **Hard Real-Time Lazy Priority Queues (QoS)**, **Per-Priority TTL**, **Strict Prioritization**, **mTLS 1.3 transport authentication**, **zero-allocation ACL authorization**, **encrypted AES-256-GCM append-only logging (AAL)** with **startup state replay**, **async fan-out with backpressure isolation**, **zero-allocation ZSTD payload compression**, **MQTT wildcard topic routing**, **Direct Mesh Clustering**, **zero-copy protocol batching**, **coalesced subscriber writes**, **NACK-based redelivery** and **Dead Letter Queues**.

---

## Features

- **Consumer Groups & Atomic Round-Robin Routing**: Competing consumers join named groups (e.g. `topic:orders:group:payment-workers`). Messages published to a topic are load-balanced across active group workers via **Lock-Free Atomic Round-Robin** (`0 allocs/op`, `< 10 ns/op`). Group Durable Offsets persist and recover at the group level across worker failovers.
- **Dynamic Control Plane (gRPC Admin API)**: Dedicated gRPC Admin server (`:9091`) with mTLS role validation (`admin-*` CN) for lock-free RCU Hot-Reload of client rate quotas and ACL authorization rules without restarting or lock contention.
- **QUIC Transport Layer**: Multiplexed QUIC connection handling with 0-RTT connection establishment, stream isolation, and amplification protection.
- **WebTransport Gateway (HTTP/3)**: Optional `internal/webtransport/` listener accepts the W3C WebTransport API from browsers. Reuses the broker's mTLS certificate, so a single TLS trust root secures both native and browser clients. Browser clients write the **same binary frame format** as native QUIC clients — zero protocol-translation overhead.
- **Zero-Copy Binary Protocol**: Flat 10-byte binary header parser (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`) with optional TLV extension region using zero-allocation pointer arithmetic.
- **Lazy Priority Queues (QoS)**: 4 message priority levels (`0` Highest, `1` High, `2` Normal, `3` Low) carried in TLV `ExtPriority` (`0x03`). Subscriber priority queues are lazily acquired from `sync.Pool` on first use (`0 allocs/op`). Single-priority subscribers consume memory for 1 queue only.
- **Strict Prioritization & Starvation Prevention**: Dedicated Writer goroutines poll priority queues in strict priority order (`0 -> 1 -> 2 -> 3`), ensuring critical alerts bypass low-priority traffic.
- **Per-Priority TTL**: Configurable `priority_ttls` (`["500ms", "5s", "0", "0"]`) forcing per-priority expiration timestamps. Stale critical messages are lazily dropped on dequeue (`aqueduct_messages_expired_total{topic, priority}`).
- **Memory Cleanup & Recycling**: Empty priority queues (`len(q) == 0`) are automatically returned to `sync.Pool` and reset to `nil` under per-subscriber mutex protection.
- **Zero-Allocation Payload Compression**: ZSTD batch compression (`internal/compress`) with `ExtCompression` (`0x02`) TLV extension — compresses batch payloads before peer forwarding.
- **Structure of Arrays (SoA) Router**: In-memory direct mesh pub/sub routing using flat arrays for CPU L1/L2 cache locality.
- **Async Fan-Out & Ring Queues**: Per-subscriber non-blocking bounded channels and Writer goroutines eliminating Head-of-Line blocking.
- **Slow Consumer Isolation (Backpressure)**: Per-priority queue overflow handling (`drop_oldest`, `drop_newest`, `disconnect`).
- **Atomic Reference Counting (`MessageRef`)**: Safe zero-allocation buffer recycling into `sync.Pool` when count drops to zero (`0 allocs/op`).
- **Zero-Copy Protocol Batching**: `CmdPublishBatch` (0x04) command with zero-copy bulk publish — sub-frames unpack via `unsafe.Slice` directly into the batch buffer (< 4 ns/frame, `0 allocs/op`).
- **Coalesced Subscriber Writes**: Per-subscriber micro-batching with configurable 64 KB threshold and 50 µs micro-timer flush. Achieves 6.67M msg/s throughput.
- **MQTT Wildcard Topic Routing**: Zero-allocation single-level (`+`) and multi-level (`#`) pattern matching (< 51 ns/op, `0 allocs/op`).
- **Encrypted Append-Only Logging (AAL)**: AES-256-GCM encrypted persistence with cryptographically unique 12-byte nonces and streaming length-prefixed records.
- **mTLS & Zero-Allocation ACL**: Dual-side TLS 1.3 authentication and non-commutative FNV-1a composite hash matrix permission engine.
- **NACK-Based Redelivery & Dead Letter Queues**: `CmdNack` (0x05) opcode with automatic redelivery (up to `max_retries`), bounded per-subscriber frame cache (256 entries FIFO), and poison pill routing to `__dlq__<topic>`.
- **Prometheus Observability**: Comprehensive metrics (`/metrics`) and ready-to-run Docker Compose stack with Grafana dashboard.

---

## 2-Minute Quick Start (Docker Compose)

Launch Aqueduct broker, Prometheus, and Grafana in seconds:

```bash
docker compose up -d
```

Verify status and metrics:
- **Broker Health**: `http://localhost:9090/healthz`
- **Prometheus Metrics**: `http://localhost:9091`
- **Grafana Dashboard**: `http://localhost:3000` (User: `admin` / Password: `admin`)

Stop the stack:
```bash
docker compose down
```

---

## Local Installation & Usage

### Running via Binary or Go

```bash
# Run using YAML config
go run ./cmd/broker/main.go -config config.yaml

# Run using CLI flags override
go run ./cmd/broker/main.go \
  -config config.yaml \
  -addr :4242 \
  -metrics-addr :9090
```

### Configuration (`config.yaml`)

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
  key: "" # Base64 encoded 32-byte key for AES-256-GCM
  max_aal_size: 104857600 # 100 MB max size before rotation

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

### Environment Variable Overrides

| Environment Variable | Overrides | Example |
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

## Benchmarking (`aqueduct-bench`)

Run the included high-concurrency load testing CLI:

```bash
go run ./cmd/aqueduct-bench/main.go \
  -addr 127.0.0.1:4242 \
  -streams 10 \
  -messages 100000 \
  -payload-size 128
```

---

## Documentation (Diátaxis Framework)

- **Tutorial**: [Getting Started Guide](docs/en/getting-started.md)
- **Reference**: [Binary Protocol Specification](docs/en/protocol-spec.md)
- **Explanation**: [Architecture & Memory Model](docs/en/architecture.md)
- **How-to Guide**: [Production Deployment & Security](docs/en/production-deployment.md)

---

## Kubernetes Deployment (Helm)

Deploy a 3-node Aqueduct cluster with DNS-based peer discovery in one command:

```bash
helm install aqueduct ./deploy/helm/aqueduct \
  --namespace aqueduct --create-namespace
```

### How Peer Discovery Works

When deployed on Kubernetes, Aqueduct uses **DNS-based peer discovery** via the Headless Service:

1. Each pod gets a stable DNS name: `aqueduct-0.aqueduct-headless.aqueduct.svc.cluster.local`
2. The Headless Service returns **A records** for all ready pods
3. A background goroutine polls DNS every 10 seconds (configurable)
4. New pods (scale-up) are automatically connected; terminated pods (scale-down) are removed
5. Uses RCU (Read-Copy-Update) atomic swap — zero locks on the message forwarding hot path

```
aqueduct-0 ←→ aqueduct-1 ←→ aqueduct-2
     ↕              ↕              ↕
   clients       clients       clients
```

### Why DNS over K8s API (client-go)

| Aspect | DNS Resolution | client-go |
| :--- | :--- | :--- |
| Binary size impact | 0 MB (stdlib) | ~40 MB |
| External dependencies | None | REST client, protobuf, informers |
| Dynamic updates | Automatic (Headless Service) | Watch + label selector |
| Static binary philosophy | Yes | No |

### Configuration

```yaml
cluster:
  discovery:
    enabled: true
    type: "dns"
    host: "aqueduct-headless.aqueduct.svc.cluster.local"
    port: "4242"
    interval: "10s"
```

### Scaling

```bash
# Scale to 5 replicas
helm upgrade aqueduct ./deploy/helm/aqueduct --set replicaCount=5

# Scale down to 2 replicas
helm upgrade aqueduct ./deploy/helm/aqueduct --set replicaCount=2
```

DNS discovery automatically reconciles the peer mesh — no manual configuration needed.

### Raw K8s Manifests

For non-Helm deployments, raw manifests are in `deploy/k8s/`:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

---

## License

MIT License. See [LICENSE](LICENSE) for details.
