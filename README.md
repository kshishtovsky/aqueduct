# Aqueduct

[ English | [Русский](README.ru.md) | [中文](README.zh.md) ]

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kshishtovsky/aqueduct.svg)](https://pkg.go.dev/github.com/kshishtovsky/aqueduct)

Aqueduct is an ultra-high performance, zero-allocation message broker built in Go on top of **QUIC** (via `quic-go`). Engineered for extreme low latency (< 1.5 µs), zero-copy binary framing, and Data-Oriented Design (DoD), Aqueduct delivers predictable performance with zero heap allocations on the hot path.

> [!IMPORTANT]
> **Production Ready (v1.0.0)**
> Aqueduct strictly requires **TLS 1.3** and provides built-in memory hardening against oversized payload DoS attacks, alongside disk-backed Append-Only Logging (AAL) and YAML/ENV configuration loading.

---

## Features

- **QUIC Transport Layer**: Built on QUIC multiplexing with 0-RTT connection establishment, stream isolation, and amplification protection.
- **Zero-Copy Binary Protocol**: Flat 10-byte binary header parser (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`) using zero-allocation pointer operations.
- **Structure of Arrays (SoA) Router**: In-memory direct mesh pub/sub routing using flat arrays for L1/L2 CPU cache locality.
- **Append-Only Logging (AAL)**: Synchronous zero-allocation disk logging (`0 allocs/op`) directly from network buffers into OS page cache.
- **Memory Hardening**: Strict stream-level OOM protection enforcing `maxBufSize` limits.
- **Flexible Configuration**: YAML configuration file support with `AQUEDUCT_*` environment variable overrides.
- **Prometheus & Grafana**: Built-in HTTP server (`:9090`) serving `/metrics` and `/healthz` endpoints, with ready-to-run Docker Compose stack.

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

aal:
  enabled: false
  file_path: ""

transport:
  max_buf_size: 65536
  read_buf_size: 1024
```

### Environment Variable Overrides

All values in `config.yaml` can be overridden via environment variables:

| Environment Variable | Overrides | Example |
| :--- | :--- | :--- |
| `AQUEDUCT_LISTEN_ADDR` | `listen_addr` | `:4242` |
| `AQUEDUCT_METRICS_ADDR` | `metrics_addr` | `:9090` |
| `AQUEDUCT_TLS_GENERATE` | `tls.generate` | `false` |
| `AQUEDUCT_TLS_CERT_FILE` | `tls.cert_file` | `/etc/certs/cert.pem` |
| `AQUEDUCT_TLS_KEY_FILE` | `tls.key_file` | `/etc/certs/key.pem` |
| `AQUEDUCT_AAL_ENABLED` | `aal.enabled` | `true` |
| `AQUEDUCT_AAL_FILE_PATH` | `aal.file_path` | `/var/log/aal.log` |
| `AQUEDUCT_TRANSPORT_MAX_BUF_SIZE` | `transport.max_buf_size` | `131072` |

---

## Client Code Examples

Minimal client examples demonstrating binary frame assembly over QUIC:

- [Go Client Example](examples/go/main.go) — Native `quic-go` implementation.
- [Python Client Example](examples/python/client.py) — `aioquic` asynchronous client.
- [Node.js Buffer Example](examples/nodejs/client.js) — Binary frame packing example.

---

## Performance & Benchmarks

Benchmarked on AMD Ryzen 5 5500U (Linux amd64):

| Benchmark | Latency / Throughput | Memory / Op | Allocations |
| :--- | :--- | :--- | :--- |
| `BenchmarkRouterPublishWithAAL` | **1403 ns/op** (10.69 MB/s) | **0 B/op** | **0 allocs/op** |
| `BenchmarkQUICRoundTrip` | **2448 ns/op** (56.37 MB/s) | **53 B/op** | **1 alloc/op** |

---

## Documentation

Explore our comprehensive documentation following the Diátaxis framework:

- [Getting Started Tutorial](docs/en/getting-started.md) — Step-by-step guide for first-time users.
- [Production Deployment Guide](docs/en/production-deployment.md) — Configuring TLS 1.3, AAL, and Prometheus monitoring.
- [Binary Protocol Specification](docs/en/protocol-spec.md) — Complete frame header layout and command codes.
- [Architecture & Memory Model](docs/en/architecture.md) — In-depth explanation of SoA routing and zero-copy strategy.
