# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-07-26

### Added
- **QUIC Core Transport**: Multiplexed QUIC connection handling via `quic-go` with 0-RTT support, stream isolation, and amplification protection.
- **Zero-Copy Binary Protocol**: Minimal 10-byte binary frame parser (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`) using zero-allocation pointer arithmetic (`0 allocs/op`).
- **Structure of Arrays (SoA) Router**: High-throughput in-memory Pub/Sub mesh router using flat parallel memory slices for L1/L2 CPU cache locality.
- **Append-Only Logging (AAL)**: Synchronous zero-allocation disk logging (`0 allocs/op`) flushing raw frames directly to OS page cache.
- **Memory Hardening & OOM Protection**: Strict `maxBufSize` buffer limit enforcement and stream cancellation (`CancelRead`/`CancelWrite`) on oversized payloads.
- **Production TLS 1.3**: Mandatory TLS 1.3 configuration with CLI certificate flags (`-cert`, `-key`) and fallback warning for ephemeral dev certificates.
- **Configuration Engine**: YAML configuration loader (`config.yaml`) with environment variable overrides (`AQUEDUCT_*`) and `-config` CLI flag.
- **Observability & Prometheus Metrics**: Prometheus exporter endpoints (`/metrics`, `/healthz`) on port `:9090` and Grafana dashboard (`aqueduct-overview.json`).
- **Containerization**: Multi-stage `Dockerfile` (`gcr.io/distroless/static-debian12:nonroot`) and 2-minute `docker-compose` environment.
- **CI/CD Workflows**: GitHub Actions for automated linting, testing, race detection, coverage gates (>= 75%), Docker build verification, and cross-platform releases (`linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`).
- **Load Benchmarking Tool (`aqueduct-bench`)**: CLI benchmarking tool supporting concurrent stream load generation and detailed latency percentiles (p50, p90, p99, p99.9).
- **Multi-Language Documentation**: Complete Diátaxis framework documentation across English, Russian, and Chinese.
