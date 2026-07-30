<div align="center">
  <img src="docs/image_readme.png" alt="Aqueduct Banner">
</div>

<div align="center">

[ 🇬🇧 English | [🇷🇺 Русский](README.ru.md) | [🇨🇳 中文](README.zh.md) ]

# 🌊 Aqueduct

**Ultra-fast, zero-allocation message broker built on Go & QUIC**

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions)
![Go Version](https://img.shields.io/badge/go-1.23%2B-blue)
![Latency](https://img.shields.io/badge/latency-%3C1.5%C2%B5s-orange)
![Allocations](https://img.shields.io/badge/allocs-0_B%2Fop-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

</div>

Aqueduct is an ultra-high performance, zero-allocation message broker built in Go on top of **QUIC** (via `quic-go`). Engineered for extreme low latency (< 1.5 µs), zero-copy binary framing, and Data-Oriented Design (DoD), Aqueduct delivers predictable performance with zero heap allocations on the hot path.

> [!IMPORTANT]
> **Production Ready (v1.16.0)**
> Aqueduct features **Consumer Groups & Lock-Free Atomic Round-Robin Routing**, **gRPC Control Plane (Admin API)** for **lock-free RCU Hot-Reload** of Quotas and ACL rules, **Hard Real-Time Lazy Priority Queues (QoS)**, **Per-Priority TTL**, **Strict Prioritization**, **mTLS 1.3 transport authentication**, **zero-allocation ACL authorization**, **encrypted AES-256-GCM append-only logging (AAL)** with **startup state replay**, **async fan-out with backpressure isolation**, **zero-allocation ZSTD payload compression**, **MQTT wildcard topic routing**, **Direct Mesh Clustering with TLS-secured peer links**, **WebTransport (HTTP/3) gateway for browsers**, **zero-copy protocol batching**, **coalesced subscriber writes**, **NACK-based redelivery** and **Dead Letter Queues**.

---

## Features

- **Consumer Groups & Atomic Round-Robin Routing** — competing consumers join named groups (e.g. `topic:orders:group:payment-workers`). Messages published to a topic are load-balanced across active group workers via **Lock-Free Atomic Round-Robin** (`0 allocs/op`, `< 10 ns/op`). Group Durable Offsets persist and recover at the group level across worker failovers.
- **Dynamic Control Plane (gRPC Admin API)** — dedicated gRPC Admin server (`:9091`) with mTLS role validation (`admin-*` CN prefix) for **lock-free RCU Hot-Reload** of client rate quotas and ACL authorization rules without restarting or lock contention.
- **QUIC Transport Layer** — multiplexed QUIC connection handling with 0-RTT connection establishment, stream isolation, and amplification protection. Wire ALPN: `aqueduct-v1`. Mesh ALPN: `aqueduct-mesh`.
- **WebTransport Gateway (HTTP/3)** — optional `internal/webtransport/` listener accepts the W3C WebTransport API from browsers on a separate UDP port (default `:4433`, ALPN `h3`). Reuses the broker's mTLS certificate, so a single TLS trust root secures both native and browser clients. Browser clients write the **same binary frame format** as native QUIC clients — zero protocol-translation overhead.
- **Zero-Copy Binary Protocol** — flat 10-byte binary header `[Magic:1][Cmd:1][StreamID:4][DataLen:4]` followed by an optional TLV extension block and the payload. Zero-allocation pointer arithmetic; `unsafe.Slice` views validated by bounds-checks before every operation.
- **Lazy Priority Queues (QoS)** — 4 message priority levels (`0` Highest, `1` High, `2` Normal, `3` Low) carried in TLV `ExtPriority` (`0x03`). Subscriber priority queues are lazily acquired from `sync.Pool` on first use (`0 allocs/op`). Single-priority subscribers consume memory for 1 queue only.
- **Strict Prioritization & Starvation Prevention** — dedicated Writer goroutines poll priority queues in strict priority order (`0 -> 1 -> 2 -> 3`), ensuring critical alerts bypass low-priority traffic.
- **Per-Priority TTL** — optional `priority_ttls` array of 4 durations (`["500ms", "5s", "0", "0"]`); when set, every message of priority `P` inherits `expiresAt = now + priority_ttls[P]`. **Built-in default is `nil` (no per-priority TTL, messages never expire)** — there is no `AQUEDUCT_BROKER_PRIORITY_TTLS` env override; configure via YAML only. Stale critical messages are lazily dropped on dequeue (`aqueduct_messages_expired_total{topic, priority}`).
- **Memory Cleanup & Recycling** — empty priority queues (`len(q) == 0`) are automatically returned to `sync.Pool` and reset to `nil` under per-subscriber mutex protection.
- **Zero-Allocation Payload Compression** — ZSTD compression (`internal/compress`) gated by `compression.enabled`. Compresses the peer-forwarded copy of a `CmdPublishBatch` whose payload exceeds `compression.min_batch_size` (default `1024` bytes). Local subscribers always receive the uncompressed payload.
- **Structure of Arrays (SoA) Router** — in-memory direct mesh pub/sub routing using flat arrays for CPU L1/L2 cache locality. Topic identity uses the single-source-of-truth `topicHashKey()` helper (`authz.CombineHashStrings("topic", topicName)`) so publishers and subscribers always agree on the routing key.
- **Async Fan-Out & Ring Queues** — per-subscriber non-blocking bounded channels and Writer goroutines eliminating Head-of-Line blocking.
- **Slow Consumer Isolation (Backpressure)** — per-priority queue overflow handling (`drop_oldest`, `drop_newest`, `disconnect`).
- **Atomic Reference Counting (`MessageRef`)** — safe zero-allocation buffer recycling into `sync.Pool` when count drops to zero (`0 allocs/op`).
- **Zero-Copy Protocol Batching** — `CmdPublishBatch` (`0x05`) command with zero-copy bulk publish — sub-frames unpack via `unsafe.Slice` directly into the batch buffer (< 4 ns/frame, `0 allocs/op`).
- **Coalesced Subscriber Writes** — per-subscriber micro-batching with configurable `broker.batch_size` (default `64 KB`) threshold and `broker.flush_interval` (default `50 µs`) micro-timer flush.
- **MQTT Wildcard Topic Routing** — zero-allocation single-level (`+`) and multi-level (`#`) pattern matching (`0 allocs/op`).
- **Encrypted Append-Only Logging (AAL)** — AES-256-GCM encrypted persistence with cryptographically unique 12-byte nonces and streaming length-prefixed records. **`aal.max_aal_size` is declared but no rotation scheduler calls `aal.Log.Rotate(...)` in v1.16.0 — the AAL file grows unbounded**. The `aal.retention_period` and `aal.retention_size` defaults (`24h` / `1 GB`) are declared in the config but **not enforced** by any in-process code path. Use OS-level rotation (`logrotate`, cron, k8s job) to enforce retention — see [Production Deployment §2](docs/en/production-deployment.md) and [Troubleshooting §6](docs/en/troubleshooting.md).
- **mTLS & Zero-Allocation ACL** — dual-side TLS 1.3 authentication (CN extracted from peer cert for ACL identity) and non-commutative FNV-1a composite hash permission engine (`Allowed` ~14.5 ns/op, lock-free).
- **NACK-Based Redelivery & Dead Letter Queues** — `CmdNack` (`0x06`) opcode with automatic redelivery (up to `broker.max_retries`), bounded per-subscriber frame cache (256 entries FIFO), and poison pill routing to `__dlq__<topic>`.
- **OpenTelemetry Distributed Tracing** — config-gated nil-safe wrapper. When `tracing.enabled: false` all operations are inlined no-op (~3.4 ns). When enabled, W3C Trace Context TLV (`ExtTraceContext`, `0x01`) bridges OTLP spans across publishers and subscribers.
- **Prometheus Observability** — comprehensive metrics (`/metrics`) and ready-to-run Docker Compose stack with Grafana dashboard. `/healthz` returns `200 OK`.

---

## 2-Minute Quick Start (Docker Compose)

Launch Aqueduct broker, Prometheus, and Grafana in seconds:

```bash
docker compose up -d
```

Verify status and metrics:

- **Broker health**: <http://localhost:9090/healthz>
- **Prometheus metrics**: <http://localhost:9090/metrics>
- **Grafana dashboard**: <http://localhost:3000> (User: `admin` / Password: `admin`)

> The broker exposes both `/healthz` and `/metrics` on the same HTTP listener (`metrics_addr`, default `:9090`). The Prometheus port `9091` in the compose stack is Prometheus's own web UI.

Stop the stack:

```bash
docker compose down
```

---

## Local Installation & Usage

### Running via Binary or Go

```bash
# Run using YAML config
go run ./cmd/broker -config config.yaml

# Run using CLI flags override
go run ./cmd/broker \
  -config config.yaml \
  -addr :4242 \
  -metrics-addr :9090 \
  -cert /etc/aqueduct/cert.pem \
  -key /etc/aqueduct/key.pem
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
  key: ""                         # Base64 encoded 32-byte key for AES-256-GCM
  max_aal_size: 104857600         # 100 MB threshold — NOT enforced by the broker in v1.16.0 (no rotation scheduler); use OS-level rotation
  retention_period: "24h"          # declared but NOT enforced; configure external rotation
  retention_size: 1073741824       # 1 GB — declared but NOT enforced; configure external rotation

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
  batch_size: 65536               # 64 KB coalesced write threshold
  flush_interval: "50us"          # micro-timer flush
  max_retries: 3
  priority_ttls: ["500ms", "5s", "0", "0"]
  quotas:
    default_publish_rate: 0
    default_burst_size: 1000

transport:
  max_buf_size: 65536
  read_buf_size: 1024

admin:
  enabled: false
  addr: ":9091"

tracing:
  enabled: false
  service_name: "aqueduct-broker"
  endpoint: "localhost:4317"

compression:
  enabled: false
  min_batch_size: 1024
  level: 0                        # 0 = default, 1 = fastest, 3 = default

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
    insecure_skip_verify: false   # G402 default false; never enable in production
    ca_file: ""
```

### Environment Variable Overrides

Every YAML key has a matching `AQUEDUCT_*` env override (read by `internal/config/applyEnvOverrides`). Precedence: CLI flag → env var → YAML → defaults.

| Environment Variable | Overrides YAML Key | Example |
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
| `AQUEDUCT_TRACING_ENABLED` | `tracing.enabled` | `true` |
| `AQUEDUCT_TRACING_SERVICE_NAME` | `tracing.service_name` | `aqueduct-broker` |
| `AQUEDUCT_TRACING_ENDPOINT` | `tracing.endpoint` | `otel-collector:4317` |
| `AQUEDUCT_TRANSPORT_MAX_BUF_SIZE` | `transport.max_buf_size` | `131072` |
| `AQUEDUCT_TRANSPORT_READ_BUF_SIZE` | `transport.read_buf_size` | `1024` |
| `AQUEDUCT_CLUSTER_DISCOVERY_ENABLED` | `cluster.discovery.enabled` | `true` |
| `AQUEDUCT_CLUSTER_DISCOVERY_HOST` | `cluster.discovery.host` | `aqueduct-headless.default.svc.cluster.local` |
| `AQUEDUCT_CLUSTER_DISCOVERY_PORT` | `cluster.discovery.port` | `4242` |
| `AQUEDUCT_CLUSTER_DISCOVERY_INTERVAL` | `cluster.discovery.interval` | `10s` |
| `AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY` | `cluster.mesh.insecure_skip_verify` | `false` |
| `AQUEDUCT_CLUSTER_MESH_CA_FILE` | `cluster.mesh.ca_file` | `/etc/aqueduct/mesh-ca.pem` |
| `AQUEDUCT_COMPRESSION_ENABLED` | `compression.enabled` | `true` |
| `AQUEDUCT_WEBTRANSPORT_ENABLED` | `webtransport.enabled` | `true` |
| `AQUEDUCT_WEBTRANSPORT_LISTEN_ADDR` | `webtransport.listen_addr` | `:4433` |
| `AQUEDUCT_WEBTRANSPORT_PATH_PREFIX` | `webtransport.path_prefix` | `/aqueduct/wt` |

---

## Benchmarking (`aqueduct-bench`)

Run the included high-concurrency load testing CLI:

```bash
go run ./cmd/aqueduct-bench \
  -addr 127.0.0.1:4242 \
  -c 10 \
  -n 100000 \
  -size 128 \
  -topic bench \
  -batch 1 \
  -tls-verify=false
```

The real flag names (verified against `cmd/aqueduct-bench/main.go`) are `-c` (concurrency), `-n` (total requests), `-size` (payload bytes), `-topic`, `-timeout`, `-batch`, `-tls-verify`, and `-ca-file`. Use `-tls-verify` together with `-ca-file <pem>` when benchmarking a production broker with a real CA.

---

## Documentation (Diátaxis Framework)

### Tutorials

- [Getting Started](docs/en/getting-started.md) — install, configure, run, and interact with the broker in 10 minutes.

### How-to Guides

- [Production Deployment & Security](docs/en/production-deployment.md) — mTLS, AAL, sysctl tuning, Kubernetes, NACK/DLQ, rate limiting.
- [Cluster Mesh TLS](docs/en/cluster-mesh-tls.md) — secure peer-to-peer mesh federation, certificate strategy, hardening checklist.
- [Troubleshooting](docs/en/troubleshooting.md) — diagnose auth failures, mesh storms, AAL decryption errors, performance regressions.

### Reference

- [Binary Protocol Specification](docs/en/protocol-spec.md) — frame layout, opcodes, TLV extensions, WebTransport binding.
- [Configuration Reference](docs/en/configuration.md) — every YAML key, default, env override, CLI flag, and security implications.
- [Metrics Reference](docs/en/metrics.md) — Prometheus metric inventory, types, and scrape guidance.
- [Admin API Reference](docs/en/admin-api.md) — gRPC service definition, RPC contracts, role enforcement.

### Explanation

- [Architecture & Memory Model](docs/en/architecture.md) — SoA router, lazy queues, `MessageRef`, batch protocol, NACK/DLQ, AAL, WebTransport, mesh design, slab allocator, rate limiting.

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
3. A background goroutine polls DNS every `interval` (default `10s`)
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

> Security: pair discovery with a strict mesh TLS profile — see [Cluster Mesh TLS](docs/en/cluster-mesh-tls.md).

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