# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.6.0] - 2026-07-26

### Added
- **Zero-Copy Protocol Batching (`CmdPublishBatch`)**: New `0x04` command opcode for batch-publishing multiple frames in a single QUIC stream write. Zero-copy batch unpacking via `unsafe.Slice` with pointer arithmetic — sub-frames point directly into the parent batch buffer with no copying.
- **Nested Reference Counting**: `MessageRef` now supports a parent-child hierarchy. Child refs share the parent batch buffer's lifetime via cascading `Release()` (child → parent → pool return). All ref operations are `atomic.Int32`, zero locks on hot path.
- **Coalesced Subscriber Writer**: `runSubscriberWriter` now accumulates outgoing frames and flushes them as a batch (configurable 64 KB threshold or 50 µs micro-timer, whichever comes first). Zero allocations after initial construction.
- **Configurable Batch Settings**: `batch_size` and `flush_interval` in config (`config.yaml`) with env overrides `AQUEDUCT_BROKER_BATCH_SIZE` and `AQUEDUCT_BROKER_FLUSH_INTERVAL`.
- **Router Batch Dispatch**: `Router.PublishBatch()` iterates batch sub-frames, resolves topic for each, creates child `MessageRef`s, and dispatches to local subscribers.
- **Transport Batch Dispatch**: `dispatchFrames` handles `CmdPublishBatch` with full authz and AAL logging support.

### Performance
- `BenchmarkBatchUnpack` (1000 frames): **0 allocs/op**, ~19.9 GB/s throughput, ~3700 ns/op (3.7 ns/frame)
- `BenchmarkBatchPublish` (100 msg): **0 allocs/op**, ~921 MB/s, **6.67M msg/s** (22x above 300k RPS target)
- `BenchmarkPublishSingleVsBatch`: batch coalescing achieves ~150 ns/msg vs ~920 ns/msg for individual writes

### Security
- Fuzz test `FuzzParseBatch` (2.3M execs in 10s): 0 crashes, 0 panics
- Strict OOB validation in `ParseBatchFrame` before any unsafe slice operation

## [1.5.0] - 2026-07-26

### Added
- **Direct Mesh Clustering (P2P Federation)**: New `internal/cluster` package with `PeerManager` managing QUIC-based peer connections, auto-reconnect with exponential backoff, and zero-copy frame forwarding between cluster nodes.
- **Mesh Storm Prevention**: `MeshForwardedBit` in protocol frame headers prevents re-forwarding of already-forwarded frames, eliminating broadcast storms in multi-hop topologies.
- **Router Peer Integration**: `PeerForwarder` interface, `PublishFromPeer()` for local-only dispatch of peer-originated frames, and `Publish() -> Forward()` path that forwards even without local subscribers.
- **Zero-Copy In-Place Forwarding**: In-place mutation of the MeshForwarded bit in the shared buffer (`0 allocs/op` on the Forward hot path) eliminates heap allocation formerly caused by stack-to-heap escape through `quic.Stream.Write`.
- **Cluster Configuration**: Static peer address list via `cluster.peers` in YAML config with `AQUEDUCT_CLUSTER_PEERS` env override.
- **Cluster Observability**: Prometheus `aqueduct_cluster_peers_active` gauge and `aqueduct_cluster_frames_forwarded_total` counter.
- **Integration Test Suite**: 2-node and 3-node full mesh forwarding tests, storm protection bit-level unit test, `ForwardRaw` and `ForwardNoBit` edge case tests, and race-condition-free PeerManager lifecycle test.
- **Data Race Fix**: Peer slice fully built before goroutine spawn in `New()` to prevent concurrent slice append vs iteration.

### Performance
- 0 allocs/op on `Forward()` hot path (in-place bit mutation), 373 B/op quic-go internal overhead
- Coverage: 84.9% on `internal/cluster` package (exceeds 75% gate)
- 0 data races verified via `go test -race ./...`

## [1.3.0] - 2026-07-26

### Added
- **AAL Replay Startup Restoration**: Replays encrypted AAL log records into the router before opening the QUIC UDP socket listener, ensuring zero state loss on broker restarts.
- **Zero-Allocation MQTT Wildcards**: Implemented segment-based wildcard topic matching (`+` single-level, `#` multi-level) operating directly on byte slices (`50.41 ns/op`, `0 allocs/op`).
- **Message TTL & Expiration**: Lazy message expiry in subscriber Writer queues (`ttl:<ms>:<payload>` payload format) with `aqueduct_messages_expired_total` Prometheus counter.
- **AAL Log Rotation**: Automatic rotation when file size exceeds `max_aal_size`, filtering out expired records into a new log file.
- **Observability Metrics**: Added `aqueduct_aal_replay_duration_seconds`, `aqueduct_messages_expired_total`, and `aqueduct_aal_rotations_total` metrics.

## [1.2.0] - 2026-07-26

### Added
- **Async Fan-Out & Ring Queues**: Dedicated non-blocking bounded channels per subscriber with dedicated Writer goroutines writing to QUIC streams.
- **Slow Consumer Isolation (Backpressure Policies)**: Configurable overflow handling (`drop_oldest`, `drop_newest`, `disconnect`) preventing Head-of-Line blocking.
- **Atomic Reference Counting (`MessageRef`)**: Thread-safe atomic reference counting (`ref.Add(-1) == 0`) ensuring buffers recycled into `sync.Pool` are never reused prematurely.
- **Backpressure Observability**: Added `aqueduct_messages_dropped_total` and `aqueduct_slow_consumers_disconnected_total` metrics.

## [1.1.1] - 2026-07-26

### Security
- **ACL Key Collision Fix**: Replaced XOR key combination with non-commutative sequential FNV-1a composite hashing (`CombineHashes`, `CombineHashStrings`) with delimiter bytes (`0 allocs/op`, `< 5 ns/op`).
- **AES-GCM Nonce Safety**: Added 4-byte random session prefix (`crypto/rand`) to 12-byte Nonce composition to prevent IV reuse across broker restarts.
- **mTLS Client CA Verification**: Added `client_ca_file` configuration and `AQUEDUCT_TLS_CLIENT_CA_FILE` env override for custom Client CA certificate pools.
- **Unaligned Pointer Parsing Fix**: Replaced direct pointer casting with `binary.LittleEndian` to prevent `SIGBUS` alignment faults on ARM/RISC-V architectures.

## [1.1.0] - 2026-07-26

### Added
- **Transport Authentication (mTLS)**: Mutual TLS support (`require_client_cert: true`) extracting Client Common Name (CN) from TLS 1.3 peer certificates.
- **Encrypted Append-Only Log (AAL)**: AES-256-GCM log encryption for disk persistence with zero heap allocations on hot path.
- **Zero-Allocation ACL Engine**: Structure of Arrays bitmask authorization matrix for fast client-topic permission validation.

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
