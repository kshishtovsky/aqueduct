# Aqueduct

[ English | [Русский](README.ru.md) | [中文](README.zh.md) ]

Aqueduct is an ultra-high performance, zero-allocation message broker built in Go on top of **QUIC** (via `quic-go`). Engineered for extreme low latency (< 1.5 µs), zero-copy binary framing, and Data-Oriented Design (DoD), Aqueduct delivers predictable performance with zero heap allocations on the hot path.

> [!IMPORTANT]
> **Production Ready (v1.0.0)**
> Aqueduct strictly requires **TLS 1.3** and provides built-in memory hardening against oversized payload DoS attacks, alongside disk-backed Append-Only Logging (AAL).

---

## Features

- **QUIC Transport Layer**: Built on QUIC multiplexing with 0-RTT connection establishment, stream isolation, and amplification protection.
- **Zero-Copy Binary Protocol**: Flat 10-byte binary header parser (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`) using zero-allocation pointer operations.
- **Structure of Arrays (SoA) Router**: In-memory direct mesh pub/sub routing using flat arrays for L1/L2 CPU cache locality.
- **Append-Only Logging (AAL)**: Synchronous zero-allocation disk logging (`0 allocs/op`) directly from network buffers into OS page cache.
- **Memory Hardening**: Strict stream-level OOM protection enforcing `maxBufSize` limits.
- **Prometheus Metrics**: Built-in HTTP server (`:9090`) serving `/metrics` and `/healthz` endpoints.

---

## Quick Start

### Prerequisites

- **Go**: 1.22+ installed
- **OS**: Linux / macOS

### Running the Broker

Build and run the broker using standard flags:

```bash
# Run in development mode (ephemeral self-signed TLS cert)
go run ./cmd/broker/main.go -addr :4242

# Run in production mode with TLS certificates and Append-Only Logging
go run ./cmd/broker/main.go \
  -cert /path/to/cert.pem \
  -key /path/to/key.pem \
  -aal /path/to/aqueduct.log \
  -addr :4242 \
  -metrics-addr :9090
```

### CLI Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-addr` | `:4242` | UDP address for QUIC broker listener |
| `-metrics-addr` | `:9090` | HTTP address for Prometheus metrics and health check |
| `-cert` | `""` | Path to TLS 1.3 certificate file |
| `-key` | `""` | Path to TLS 1.3 private key file |
| `-aal` | `""` | Optional path to Append-Only Log file |

> [!WARNING]
> If `-cert` and `-key` flags are omitted, Aqueduct generates an ephemeral self-signed TLS certificate for development and logs a warning. Do not use ephemeral certificates in production.

---

## Architecture & Design Highlights

Aqueduct is designed around hardware-conscious engineering principles:

1. **Zero-Allocation Hot Path**: Network reads use pooled buffers (`sync.Pool`). Binary frames are parsed directly from buffers without intermediate allocations.
2. **Cache-Friendly Mesh Routing**: Subscribers are stored in parallel flat slices (`streamIDs`, `streams`, `topics`, `active`), maximizing L1/L2 cache hit rate during batch message distribution.
3. **Synchronous AAL Write**: Published messages are appended to disk using direct kernel syscalls (`os.File.Write`) from network slices before buffer recycling, ensuring `0 allocs/op`.

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
