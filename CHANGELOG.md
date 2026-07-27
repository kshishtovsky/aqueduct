# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.15.0] - 2026-07-27

### Added
- **Idempotent Producer (Exactly-Once Semantics)**: New `internal/dedup` package implementing sliding-window deduplication for Idempotent Producers. Producers attach a `ProducerID` + `SeqNum` TLV pair to every publish; the broker keeps a per-producer 2048-bit ring buffer (32 × `uint64` = 256 B) of seen sequence numbers and silently drops duplicates while emitting a synthetic `dedup_ack` so the producer's retry loop terminates. Out-of-window sequences are treated as a protocol error (stream closed).
- **Lock-Free Bit Check-and-Set**: Per-producer dedup state uses `atomic.LoadUint64` + `atomic.CompareAndSwapUint64` for the hot path. Only the high-watermark advance takes a short mutex. In-window / duplicate path: **3.7 ns/op, 0 allocs/op**.
- **ProducerID Lifecycle Management**: New `dedup.Store` is an LRU + TTL cache mapping `ProducerID → *Window`. Capacity (default 65536) bounds total memory to ~16 MB. Background goroutine reaps windows idle for `idle_ttl` (default 5 min).
- **New TLV Extensions**: `ExtProducerID (0x04)` and `ExtSeqNum (0x05)` (20 bytes total per frame) — zero-copy parse via `FindExtension`/`ExtractProducerID`/`ExtractSeqNum`. Builders `BuildIdempotentExtension` and `BuildIdempotentExtensionFromExisting` merge into existing TLV blocks.
- **Dispatch Integration**: `dispatchFrames` runs the dedup check immediately after decompression, before authorization / AAL / Router. Duplicates get a synthetic `CmdAck` frame containing `dedup_ack:<id>:<seq>` so producers stop retrying. `TooOld` errors cancel the stream.
- **Configuration**: New `broker.idempotent_producers.{enabled,window_capacity,idle_ttl}` block with env overrides `AQUEDUCT_BROKER_IDEMPOTENT_ENABLED`, `AQUEDUCT_BROKER_IDEMPOTENT_CAPACITY`, `AQUEDUCT_BROKER_IDEMPOTENT_IDLE_TTL`.
- **Metrics**: New `aqueduct_messages_deduplicated_total` counter.
- **Documentation**: Protocol spec, architecture doc updated in EN / RU / ZH.

### Performance & Security
- `BenchmarkDedupCheck_InWindow` (in-window duplicate): **3.7 ns/op, 0 allocs/op** (well under 5 ns target).
- `BenchmarkDedupCheck` (advancing highSeqNum): 12.4 ns/op, 0 allocs/op.
- `BenchmarkDedupCheck_Parallel` (32 cores, lock-free): 72.7 ns/op.
- `BenchmarkStoreCheck_HotProducer`: 9.4 ns/op, 0 allocs/op.
- `BenchmarkPublishWithDedup` vs `BenchmarkPublishBaseline`: ~17% overhead on a 27 ns baseline, dominated by the TLV scan in `Router.Publish` (priority extraction).
- `go test -race ./internal/...`: **0 data races**.
- Fuzz `FuzzExtractProducerID` (10s): ~9.8M execs, 0 crashes, 0 panics.
- Statement coverage: **98.2%** on `internal/dedup` (target ≥ 75%).

## [1.14.0] - 2026-07-26

### Added
- **DNS-Based Peer Discovery**: New `internal/cluster/discovery.go` module polling Headless Service DNS records via `net.LookupHost` (stdlib, 0 additional binary dependencies). Automatic peer addition/removal on DNS record changes with normalized IP diffing. Supports Kubernetes StatefulSet pod discovery patterns.
- **Dynamic Peer Management (RCU)**: `PeerManager` refactored to use `atomic.Pointer[peerSlice]` for lock-free reads on the hot path (`Forward()`, `PeerCount()`). New `AddPeer(ctx, addr)`, `RemovePeer(addr)`, and `swapPeers(fn)` methods for runtime peer topology changes. Write-path mutex only protects `addrSet` map.
- **Kubernetes Helm Chart**: New `deploy/helm/aqueduct/` chart with configurable `replicaCount`, `resources`, `persistence`, and `cluster.discovery.*` values. Supports StatefulSet deployment with Headless Service for DNS-based peer discovery.
- **Kubernetes Raw Manifests**: New `deploy/k8s/` directory with `namespace.yaml`, `configmap.yaml`, `services.yaml` (Headless + Client), and `statefulset.yaml` (with volumeClaimTemplates and health probes).
- **Injectable Resolver Interface**: `Resolver` interface with `DefaultResolver` wrapper enables DNS mocking in tests without external dependencies.

### Performance & Security
- `go test -race ./...`: **0 data races** across all packages
- `TestDiscoveryInitialResolution`: 3 IPs → 3 peers with correct ports, 0 allocs
- `TestDiscoveryNewPeerAdded` / `TestDiscoveryPeerRemoved`: Dynamic add/remove verified
- `TestDiscoveryDNSError`: Graceful error handling, peer set preserved
- `TestDiscoveryNormalizeIPv6`: IPv6 normalization + link-local filtering
- `TestDiscoveryPollingLoop`: Background ticker triggers re-resolution
- `TestDiscoveryNoOpWhenSameIPs`: Stable DNS = zero churn
- Statement coverage: **86.8%** (`internal/cluster`), **100%** on discovery key functions

## [1.13.0] - 2026-07-26

### Added
- **Consumer Groups & Competing Consumers**: Support for competing consumer groups registered via `:group:<group_id>` in `CmdSubscribe` payload (e.g. `topic:orders:group:payment-workers`).
- **Lock-Free Atomic Round-Robin Routing**: Implemented lock-free O(1) worker selection using `atomic.Pointer[[]int]` for active member snapshots and `atomic.Uint64` for round-robin balancing (`0 allocs/op`, `< 10 ns/op`).
- **Group-Level Durable Offsets**: Track acknowledged offsets on the group level (`CombineHashStrings(groupID, topic)`). Allows seamless failover across group workers without message duplication or offset drift.
- **Dual Delivery Support**: Preserves topic fan-out for individual subscribers while delivering exactly one copy per published message to each Consumer Group registered on the topic.

### Performance & Security
- `BenchmarkGroupRouting`: **0 allocs/op**, **0 B/op** on hot path worker selection.
- `TestCompetingConsumers`: Verified even round-robin distribution (~1000 msgs across 3 workers out of 3000 published, 0 duplicates).
- `TestGroupRebalancingAndDurable`: Verified automatic failover and group offset recovery upon worker disconnect/reconnect.
- `go test -race ./...`: **0 data races**.
- Statement coverage: **76.7%** (`internal/broker`), **84.9%** (`internal/cluster`).

## [1.12.0] - 2026-07-26

### Added
- **Dynamic Control Plane (gRPC Admin API)**: Dedicated `internal/admin` package running gRPC Admin server (`:9091`) for dynamic configuration updates without broker restart or message processing interruption.
- **mTLS Operator Authentication**: Enforces client TLS certificate Common Name (CN) starting with `admin-` (`adminAuthInterceptor`). Unauthorized requests receive `PermissionDenied`.
- **Lock-Free RCU Quota Management**: `quotas.Manager` uses `atomic.Pointer` for client bucket map updates. `Bucket.rate` uses `atomic.Int64` for zero-lock refill rate adjustments on hot path (`0 allocs/op`, 0 mutexes).
- **Lock-Free RCU ACL Management**: `authz.Engine` uses `atomic.Pointer` for atomic rule map replacement (`Reload`). `Allowed` checks execute lock-free in ~14.5 ns/op.
- **Admin Observability**: Counter metric `aqueduct_admin_requests_total{method}` and structured `slog.Info` audit logging for all configuration mutations.

### Performance
- `BenchmarkACLHotReload` (Rebuilding 10,000 rules): **876 µs/op** (< 1 ms target achieved)
- `BenchmarkACLCheck` (Lock-Free RCU Hot Path): **14.51 ns/op, 0 allocs/op**

### Security
- `go test -race ./...`: 0 data races
- Test coverage on `internal/admin`: **79.2%** (target >= 75%)
- Strict mTLS client certificate CN validation (`admin-*`)

## [1.11.0] - 2026-07-26

### Added
- **Priority Flag TLV Extension (`Type 0x03`)**: Added `ExtPriority` TLV extension type carrying a 1-byte priority level (`0` Highest, `1` High, `2` Normal, `3` Low, default `2`). Supported zero-alloc parsing via `ExtractPriority` and `BuildPriorityExtension`.
- **Lazy Priority Queues (DoD)**: Refactored subscriber queues in `Router` to `[]*[4]chan *MessageRef`. Queue instances are lazily acquired from a global `r.queuePool` (`0 allocs/op`) only when messages of priority `P` arrive. Subscribers using single priority consume memory for 1 queue only.
- **Per-Priority TTL Override**: Added `priority_ttls` configuration (`["500ms", "5s", "0", "0"]`). Enforced per-priority expiration timestamp `expiresAt` on enqueueing, overwriting publisher TTL. Lazy expiration check drops stale messages upon dequeue (`aqueduct_messages_expired_total{topic, priority}`).
- **Strict Priority Sending**: Writer goroutines process subscriber priority queues in strict order (`0 -> 1 -> 2 -> 3`). Higher priority messages bypass lower priority traffic without starvation.
- **Empty Queue Memory Cleanup**: Empty priority channels (`len(q) == 0`) are automatically returned to `r.queuePool` and reset to `nil` under per-subscriber `subMu`.

### Performance & Security
- `BenchmarkPriorityPublish`: **1882 ns/op, 0 B/op, 0 allocs/op** on hot path.
- `go test -race ./...`: **0 data races** across all packages.
- Statement coverage: **82.0%** (`internal/broker`), **82.2%** (`internal/protocol`).

## [1.10.0] - 2026-07-26

### Added
- **Zero-Allocation ZSTD Compression for Batches**: New `internal/compress` package with `ZstdEngine` — slab-backed zero-copy compression using `klauspost/compress/zstd`. Encoder/Decoder pools eliminate allocations on the hot path.
- **Compression TLV Extension**: `ExtCompression` type (`0x02`) carrying `[Algo:1][UncompressedSize:4]` metadata alongside compressed payload. Merged into existing TLV block during serialization, stripped after decompression.
- **Cluster Peer Compression**: `Router.PublishBatch` transparently compresses batch payload before peer forwarding (skips batches < 1 KB). Local subscribers receive uncompressed data — zero protocol breakage.
- **Decompression in Transport**: `dispatchFrames` detects Compression TLV, decompresses via `ZstdEngine`, strips the TLV, and routes the decompressed frame through standard dispatch. Corrupted payloads close the stream with `ProtocolError`.
- **Compression Configuration**: `CompressionConfig` block in YAML/config with `enabled` (default: `false`), `min_batch_size` (default: `1024`), and `level` (default: `0`) fields + `AQUEDUCT_COMPRESSION_*` env overrides.

### Performance
- `BenchmarkZSTDEncode` (9 KB random): **2361 ns/op, 0 allocs/op**, 1734 MB/s
- `BenchmarkZSTDDecode`: **6272 ns/op, 1 allocs/op** (1 alloc from `make` required for async lifecycle safety)
- `BenchmarkBatchPublishWithCompression` (100 msg × 128 B, with peers): **0 allocs/op**, ~467 MB/s (CPU cost of ZSTD adds ~14 µs per batch)
- Compression ratio (compressible data): **67:1** (4300 → 64 bytes)

### Security
- `go test -race ./internal/...`: 0 data races
- Corrupted compressed payloads return `ErrCorruptedPayload` — no panic, no OOB read, stream closed gracefully
- Coverage: 82.1% on `internal/compress`, 0/0 lint errors (`go vet`)

## [1.9.0] - 2026-07-26

### Added
- **TLV Protocol Extensions**: New bit 6 (HasExtensions) in Command byte enables variable-length metadata block between header and payload. Zero-copy TLV parser via `unsafe.Slice` with strict bounds checking. Unknown TLV types silently skipped (forward compatibility guaranteed).
- **W3C Trace Context Propagation**: `ExtTraceContext` TLV type (`0x01`) carries [`TraceID:16`][`SpanID:8`][`TraceFlags:1`] for OpenTelemetry distributed tracing. Extracted via zero-copy `unsafe.Pointer` cast — 4 ns/op, 0 allocs.
- **OpenTelemetry Tracer (`internal/tracing/`)**: Config-gated OTLP gRPC exporter. When `tracing.enabled: false` (default), the tracer is nil — all span operations are inlined no-ops at ~3.4 ns/op, 0 allocs. When enabled: batched OTLP export, lazy init via `sync.Once`.
- **Tracing in Dispatch**: `dispatchFrames` extracts trace context from published frames (single + batch), creates child spans `aqueduct.process` for local publish and `aqueduct.forward` for mesh-forwarded frames. Context propagated through `Router.Publish`.
- **TLV Block Preservation**: Router preserves the original TLV extension block when serializing frames for subscriber delivery and peer forwarding via `SerializeFrameWithExtensions`.
- **Tracing Metrics**: `aqueduct_tracing_spans_total` counter tracks total spans created by the broker.

### Performance
- `BenchmarkParseFrameWithExtensions`: **5.2 ns/op, 0 allocs/op**
- `BenchmarkExtractTraceContext`: **4.1 ns/op, 0 allocs/op**
- `BenchmarkFindExtension`: **3.1 ns/op, 0 allocs/op**
- `BenchmarkTracerDisabled`: **3.4 ns/op, 0 allocs/op** (zero overhead when disabled)
- `BenchmarkParseFrameBackwardCompat`: **4.6 ns/op, 0 allocs/op** (regression-free)

### Security
- Strict OOB validation in `ParseTLVEntry`, `FindExtension`, `ExtractTraceContext` before any unsafe pointer operation
- `MaxExtTotalLen` (1024 bytes) prevents TLV-based DoS amplification
- Fuzz test `FuzzParseFrame` (2.3M execs in 10s): 0 crashes, 0 panics

## [1.8.0] - 2026-07-26

### Added
- **Slab Allocator (`internal/mem/`)**: Replaces `sync.Pool` for `*[]byte` frame buffers on the hot path. Uses pre-allocated 64 MB arenas per size class (128B, 256B, 512B, 2KB, 8KB, 32KB) with lock-free free-list (atomic CAS). Zero GC pressure — arena memory is never scanned.
- **Per-Tenant Rate Limiting (`internal/quotas/`)**: Lock-free Token Bucket with background refill goroutine. Uncontended check: 2.1 ns/op, 0 allocs/op. Configurable via `broker.quotas.default_publish_rate` and per-client overrides.
- **Quota Integration**: `Router.Publish` checks quota before message dispatch. Rate-limited messages are silently dropped (with `aqueduct_messages_rate_limited_total` counter). Config via `config.yaml` and `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` env var.

### Performance
- `BenchmarkSlabAcquireRelease`: **15.1 ns/op, 0 allocs/op** (lock-free, uncontended)
- `BenchmarkSlabAcquireReleaseContended`: ~55 ns/op, 0 allocs/op
- `BenchmarkTokenBucketCheck`: **2.1 ns/op, 0 allocs/op** (< 5 ns target, 1000× headroom)
- `BenchmarkBatchPublish`: **0 allocs/op** (regression-free, same as v1.6.0)
- GC isolation: arena memory bypasses Go GC scan phase, reducing STW pause risk at 6M msg/s

## [1.7.0] - 2026-07-26

### Added
- **Negative Acknowledgment (`CmdNack`)**: New `0x05` command opcode allowing subscribers to NACK a message by offset. NACK payload: 8-byte little-endian `MessageOffset`.
- **Automatic Redelivery**: NACK'd messages are redelivered to the same subscriber up to `max_retries` (default: 3, configurable via `AQUEDUCT_BROKER_MAX_RETRIES`).
- **Dead Letter Queues (DLQ)**: After `max_retries` exhausted, the message is published to `__dlq__<original_topic>` for offline inspection.
- **NACK Routing**: `NackByStream(streamID, offset)` routes NACK to the correct subscriber writer goroutine via a buffered channel — zero locks on hot path.
- **Bounded Frame Cache**: Per-subscriber FIFO cache (256 entries) stores `offset→topic` mappings for NACK → redelivery/DLQ resolution. Evicted oldest-first.
- **NACK/DLQ Metrics**: `aqueduct_messages_nacked_total` and `aqueduct_messages_dead_lettered_total` counter vecs.

### Performance
- `BenchmarkNackHandling`: **0 allocs/op** on `NackByStream` hot path (non-blocking channel send).
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
