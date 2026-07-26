# Aqueduct

[ English | [Русский](README.ru.md) | [中文](README.zh.md) ]

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kshishtovsky/aqueduct.svg)](https://pkg.go.dev/github.com/kshishtovsky/aqueduct)

Aqueduct is an ultra-high performance, zero-allocation message broker built in Go on top of **QUIC** (via `quic-go`). Engineered for extreme low latency (< 1.5 µs), zero-copy binary framing, and Data-Oriented Design (DoD), Aqueduct delivers predictable performance with zero heap allocations on the hot path.

> [!IMPORTANT]
> **Production Ready (v1.8.0)**
> Aqueduct features **mTLS 1.3 transport authentication**, **zero-allocation ACL authorization**, **encrypted AES-256-GCM append-only logging (AAL)** with **startup state replay**, **async fan-out with backpressure isolation**, **message TTL**, **MQTT wildcard topic routing**, **Direct Mesh Clustering**, **zero-copy protocol batching**, **coalesced subscriber writes**, **NACK-based redelivery** and **Dead Letter Queues**.

---

## Features

- **QUIC Transport Layer**: Multiplexed QUIC connection handling with 0-RTT connection establishment, stream isolation, and amplification protection.
- **Zero-Copy Binary Protocol**: Flat 10-byte binary header parser (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`) using zero-allocation pointer arithmetic and cross-platform Little-Endian safety.
- **Structure of Arrays (SoA) Router**: In-memory direct mesh pub/sub routing using flat arrays for CPU L1/L2 cache locality.
- **Async Fan-Out & Ring Queues**: Per-subscriber non-blocking bounded channels and Writer goroutines eliminating Head-of-Line blocking.
- **Slow Consumer Isolation (Backpressure)**: Configurable queue overflow handling (`drop_oldest`, `drop_newest`, `disconnect`).
- **Atomic Reference Counting (`MessageRef`)**: Safe zero-allocation buffer recycling into `sync.Pool` when count drops to zero (`0 allocs/op`).
- **Zero-Copy Protocol Batching**: `CmdPublishBatch` (0x04) command with zero-copy bulk publish — sub-frames unpack via `unsafe.Slice` directly into the batch buffer (< 4 ns/frame, `0 allocs/op`).
- **Coalesced Subscriber Writes**: Per-subscriber micro-batching with configurable 64 KB threshold and 50 µs micro-timer flush. Achieves 6.67M msg/s throughput.
- **Nested Reference Counting**: Parent-child `MessageRef` hierarchy for batch buffer lifetime management — all `atomic.Int32`, zero locks on hot path.
- **MQTT Wildcard Topic Routing**: Zero-allocation single-level (`+`) and multi-level (`#`) pattern matching (< 51 ns/op, `0 allocs/op`).
- **Message Time-To-Live (TTL)**: Lazy message expiration on dequeue (`ttl:<ms>:<payload>` format).
- **Encrypted Append-Only Logging (AAL)**: AES-256-GCM encrypted persistence with cryptographically unique 12-byte nonces (4-byte random session prefix) and streaming length-prefixed records.
- **AAL Replay on Startup**: Restores state before opening the QUIC UDP listener socket, preventing message loss on restart.
- **AAL File Rotation**: Automatic log compaction when file size exceeds `max_aal_size`.
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

## License

MIT License. See [LICENSE](LICENSE) for details.
