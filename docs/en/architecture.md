# Explanation: Architecture & Memory Model (v1.16.0)

This document explains the architectural principles, Data-Oriented Design (DoD) choices, security primitives, and zero-allocation memory strategies underlying Aqueduct's performance. It is **understanding-oriented**: read it to understand *why* the broker is built the way it is. For exact byte layouts, opcodes, and configuration see the [Reference](.) docs.

---

## 1. Why QUIC over TCP?

Traditional TCP-based message brokers suffer from Head-of-Line (HoL) blocking when multiplexing multiple topics or streams over a single connection. Packet loss on one topic halts delivery for all independent topics.

Aqueduct uses **QUIC** (`quic-go`), offering:

- **Stream-level isolation** — loss on one stream does not block unrelated topics on other streams.
- **0-RTT resumption** — eliminates TLS round-trips for re-connecting clients (`quic.Config.Allow0RTT = true`).
- **UDP transport** — lowers kernel latency and avoids TCP connection-setup bottlenecks.
- **Built-in TLS 1.3** — every connection is encrypted and authenticated with no extra handshake round-trip.

The wire protocol itself is intentionally transport-agnostic: a browser reaching the broker over [WebTransport](https://www.w3.org/TR/webtransport/) writes the **same** 10-byte binary header (`[Magic:1][Cmd:1][StreamID:4][DataLen:4]`) into a `WebTransportBidirectionalStream` as a native QUIC client. See the [WebTransport Gateway](#7-webtransport-gateway-http3-v1160) below.

---

## 2. Structure of Arrays (SoA) Router & Lazy Priority Queues

Standard Go implementations store subscribers using maps of pointers: `map[string][]*Subscriber`. This pattern degrades CPU L1/L2 cache performance due to pointer chasing across scattered heap allocations.

Aqueduct implements **Structure of Arrays (SoA)** paired with lazy per-priority ring queues and dedicated Writer goroutines (`internal/broker/router.go`):

```go
type Router struct {
    mu sync.RWMutex

    // SoA flat parallel slices (same index = same subscriber)
    streamIDs []uint32               // Stream IDs
    streams   []*quic.Stream         // QUIC stream pointers
    topics    []string               // Topic names
    active    []bool                 // Active subscriber flags
    queues    []*[4]chan *MessageRef // Lazy priority ring queues pointer (0=Highest .. 3=Low)
    notifyChs []chan struct{}        // Per-subscriber notification handle
    subMus    []*sync.RWMutex        // Per-subscriber RWMutex for lazy queue init/cleanup
    cancels   []context.CancelFunc   // Writer goroutine cancellation handles
    nackChs   []chan uint64          // NACK delivery channels

    queuePool sync.Pool              // Pool of chan *MessageRef (queueSize)

    topicIndex      map[uint64][]int // topicHash → flat-array indices
    subGroups       []string         // Per-subscriber group ID ("" if individual)
    groups          map[uint64][]*ConsumerGroup
    wildcardSubs    []WildcardSub    // MQTT wildcard patterns (+ and #)
    topicOffsets    map[uint64]*atomic.Uint64
    durableOffsets  map[uint64]uint64
    nackCounters    map[nackKey]int8

    quotaManager     *quotas.Manager
    peerForwarder    PeerForwarder
    compression      CompressionEngine
    compressionMinSize int
    batchSize        int
    flushInterval    time.Duration
    priorityTTLs     [4]time.Duration
    aalPath          string
    aalKey           []byte

    wg sync.WaitGroup
}
```

### Lazy Queue Allocation & Strict Prioritization

1. **Lazy initialization.** Upon subscription, `queues[idx]` is a pointer `*[4]chan *MessageRef` with all four entries `nil`. The first time a message of priority `P` is published, `enqueueToSubscriber` acquires a channel from `r.queuePool` (`0 allocs/op`) under `subMus[idx]`. Subscribers that only ever see one priority level consume memory for one channel only.
2. **Strict priority sending.** Dedicated Writer goroutines call `fetchNextMessage`, which polls queues in strict priority order `0 → 1 → 2 → 3`. Critical alerts bypass lower-priority traffic without starvation.
3. **Per-priority TTL.** `expiresAt` is calculated on enqueue using `priorityTTLs[pLevel]` (and any inline `ttl:<ms>:` prefix). On dequeue, `msgRef.IsExpired(nowNano)` lazily drops expired messages before writing them to QUIC streams (`aqueduct_messages_expired_total{topic, priority}`).
4. **Memory recycling.** When `len(q) == 0` on dequeue, `cleanupEmptyQueue` returns the channel to `r.queuePool` and resets `queues[idx][P] = nil`.

### `topicHashKey`: Single Source of Truth (v1.16.0)

Before v1.16.0, publishers and subscribers could disagree on topic identity when the publish payload contained an embedded `topic:` or `ttl:<ms>:` prefix, silently dropping messages and producing inconsistent `topic` labels on the `OnPublish` / `OnDeliver` callbacks (which in turn feed the `aqueduct_messages_published_total{topic}` and `aqueduct_messages_delivered_total{topic}` Prometheus counters). The router now funnels every routing-key derivation through one helper:

```go
// internal/broker/router.go
func topicHashKey(topic string) uint64 {
    return authz.CombineHashStrings("topic", topic)
}
```

`parsePublishTopic(payload)` strips both the optional `ttl:<ms>:` prefix and the optional `topic:` routing prefix before computing the routing key. `parseSubscriptionPayload` produces a `SubscriptionSpec.Topic` already stripped of the `topic:` envelope. Both ends hash the same bytes. Lock the test in `internal/broker/topic_extraction_test.go`.

---

## 3. Atomic Reference Counting (`MessageRef`)

To recycle message frame buffers safely into `sync.Pool` without races:

```go
type MessageRef struct {
    buf        *[]byte     // pooled buffer (parent only)
    frame      []byte      // zero-copy sub-slice (child only)
    parent     *MessageRef // parent ref (nil for parents)
    ref        atomic.Int32
    expiresAt  int64       // unix-nano, 0 = no expiry
    offset     uint64      // monotonic topic offset
    batchChild bool
}
```

- On `Publish`, a single buffer is pulled from `sync.Pool` (via the slab allocator) and wrapped in `MessageRef` with `ref = 1`.
- For each target subscriber queue, `Retain()` increments `ref`.
- Publisher and Writer goroutines call `Release()` after dispatch or write.
- When `ref.Add(-1) == 0`, the buffer is returned to `protocol.ReleaseBuffer` and the ref to `msgRefPool` (**`0 allocs/op`**).
- **Nested reference counting** — `CmdPublishBatch` frames use a parent-child hierarchy. The parent wraps the batch buffer (`ref = 1 + frameCount`). Each child is created via `AcquireChildMessageRef(parent, frame []byte, ...)` with `frame` pointing into the parent buffer. On `Release()`, a child whose ref reaches zero calls `parent.Release()`. The parent's `buf` lifecycle is managed via `protocol.ReleaseBuffer`.

---

## 4. Zero-Allocation Wildcard Topic Matcher

MQTT wildcard matching operates directly on byte slices without string conversions:

- `+` matches one topic segment between `/`.
- `#` matches zero or more trailing segments.
- `MatchWildcard(pattern, topic []byte)` executes in **< 51 ns/op** with **`0 allocs/op`** on commodity hardware.

---

## 5. Security Architecture (mTLS, Non-Commutative ACL, Encrypted AAL)

1. **mTLS 1.3.** Requires valid client certificates verified against trusted CA cert pools (`client_ca_file`). The first peer cert's Common Name (CN) is extracted as the `clientID` and used for ACL keying, durable subscription offsets, and admin authorization. The TLS config forces `MinVersion = tls.VersionTLS13`.
2. **Non-commutative FNV-1a ACL.** `authz.CombineHashes(clientID, topicBytes)` and `CombineHashStrings(clientID, topic)` hash `clientID` and `topic` sequentially (with a `:` separator in between), preventing XOR commutativity bypasses — `Combine("A","B") != Combine("B","A")`. Permission checks (`authz.Engine.Allowed`) execute lock-free via `atomic.Pointer[map[uint64]Permission]` in ~14.5 ns/op.
3. **AES-256-GCM AAL & replay.** Records are encrypted with 12-byte nonces (4-byte random session prefix + 8-byte strictly monotonic counter) and length-prefixed headers. Startup replay (`aal.Replay`) restores state sequentially before opening the QUIC UDP listener socket. See [Protocol §5](protocol-spec.md) for the exact byte layout.
4. **Admin role enforcement.** The gRPC Admin server requires every client TLS cert's CN to start with `admin-` (`internal/admin/adminAuthInterceptor`). Requests without the prefix are rejected with `codes.PermissionDenied`.
5. **Mesh TLS.** Cluster peer links use `aqueduct-mesh` ALPN, never `aqueduct-v1`. `cluster.mesh.insecure_skip_verify` defaults to `false`; setting it to `true` logs a startup warning and is **not safe** in production. See [Cluster Mesh TLS](cluster-mesh-tls.md).

---

## 6. Direct Mesh Clustering & DNS Discovery

Aqueduct supports forming a cluster of broker instances connected via a direct peer-to-peer QUIC mesh. There is no central coordinator or consensus protocol — forwarding is fire-and-forget. The [MeshForwarded bit](#meshforwarded-bit) (Command bit 7) prevents broadcast storms.

### PeerManager (RCU Pattern)

```go
type PeerManager struct {
    peers    atomic.Pointer[peerSlice]   // lock-free atomic snapshot
    tlsConf  *tls.Config
    quicConf *quic.Config
    mu       sync.Mutex                  // write path only (addrSet)
    addrSet  map[string]context.CancelFunc
    closed   atomic.Bool
    wg       sync.WaitGroup
}
```

- **Reads** (`Forward()`, `PeerCount()`, `ActivePeers()`): grab the atomic pointer — zero locks, zero contention.
- **Writes** (`AddPeer()`, `RemovePeer()`): create a new slice, atomic swap. The write-path mutex only protects `addrSet`.
- **Reconnect loop**: each peer has a dedicated goroutine with exponential backoff (`initialBackoff = 250ms`, `maxBackoff = 30s`). Per-peer ctx cancellation tears the loop down cleanly.

### DNS Discovery (Kubernetes Headless Service)

```go
type Discovery struct {
    resolver  Resolver            // injectable for tests
    hostname  string
    port      string
    interval  time.Duration
    pm        *PeerManager
    knownIPs  map[string]struct{} // fast diff tracking
}
```

- Polls `net.LookupHost(hostname)` every `interval` (default `10s`).
- Diffs against `knownIPs` and calls `AddPeer()` / `RemovePeer()` only on change.
- `normalize()` deduplicates IPs and re-stringifies through `net.ParseIP` so IPv6 representations collapse.

### MeshForwarded Bit

A single bit in the protocol's Command byte (`0x80`) marks a frame as already forwarded. Receivers check the bit with `protocol.IsForwarded(cmd)` and skip re-forwarding.

### Zero-Copy Forwarding

`PeerManager.Forward(rawBuf, addForwardedBit)` reads the atomic peer snapshot, then mutates `rawBuf[1]` in place:

```go
orig := rawBuf[1]
rawBuf[1] = orig | byte(protocol.MeshForwardedBit)
_, werr := s.Write(rawBuf)
rawBuf[1] = orig
```

No heap allocation, no stack-escape into `quic.Stream.Write`, no `var combined [256]byte` temporary. The buffer is owned by the caller (returned to `sync.Pool` after `Release()`), so temporary mutation is safe.

### Router Integration

`Router.publishWithClientID` checks `peerForwarder.ActivePeers() > 0` **before** the early-return when there are no local subscribers, so messages are still forwarded to the cluster even when nobody is subscribed locally. Receivers dispatch via `Router.PublishFromPeer(...)` which never re-forwards.

---

## 7. WebTransport Gateway (HTTP/3, v1.16.0+)

Browsers cannot speak the broker's `aqueduct-v1` ALPN — they only support HTTP/3 + WebTransport. The gateway translates at the transport layer **without touching the protocol**:

```text
   ┌─────────────┐     HTTP/3     ┌──────────────────────┐     QUIC bidi     ┌───────────────────┐
   │ Browser     │ ─ WebTransport► │ internal/webtransport│ ────────────────► │ internal/transport│
   │ (W3C API)   │     streams    │ (this package)       │  raw *quic.Stream │ (broker)          │
   └─────────────┘                └──────────────────────┘                   └───────────────────┘
                                            │                                        │
                                            └─── reused via broker.HandleStream ────┘
```

Constraints honoured by the gateway (`internal/webtransport/server.go`):

- **One TLS config, two listeners.** The gateway calls `http3.ConfigureTLSConfig` on the broker's `*tls.Config` so the same mTLS cert secures both ports. It adds `h3` to `NextProtos` without mutating the broker's config.
- **Handshake hijack.** `responseWriter.HTTPStream()` returns the underlying `*http3.Stream`. We send `200 OK` to complete the WebTransport Extended CONNECT, then loop on `conn.AcceptStream()` and feed every bidi stream into `broker.HandleStream(...)` — the same call a native QUIC session makes.
- **Zero protocol translation.** The browser writes `[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]` into a `WebTransportBidirectionalStream`; the broker's existing frame parser handles it unchanged. Cross-transport routing (browser → native, native → browser) is automatic.
- **Synchronous handshake timeout.** A bounded `WithHandshakeTimeout(...)` (default `10s`) prevents Slowloris-style attacks. An over-budget handshake gets `stream.CancelRead(1)` + `conn.CloseWithError(ErrCodeRequestRejected, "wt handshake timeout")`.
- **0-RTT by default.** `quic.Config{Allow0RTT: true, MaxIdleTimeout: 30s, MaxIncomingStreams: 100}`. Browsers with a stored session ticket re-use it transparently.
- **TLS 1.3 enforced.** `cloneTLSForH3` overrides `MinVersion = tls.VersionTLS13` so misconfigured production certs cannot silently downgrade to 1.2.

Statement coverage on `internal/webtransport/` is 79.7%, 100% on the routing-key path.

---

## 8. Batch Protocol & Coalesced Writes

### The Problem: OS PPS Limits

QUIC streams provide excellent stream-level isolation, but `quic.Stream.Write()` has significant per-call overhead (syscall boundary, packetization, crypto). Sending one frame per write caps throughput at ~300k RPS regardless of CPU speed.

### Smart Batching

Aqueduct uses two complementary batching strategies:

#### 8.1 Protocol-Level Batching (`CmdPublishBatch`, `0x05`)

A `CmdPublishBatch` frame carries multiple standard frames as a flat byte array inside a single QUIC stream write:

```text
+----------------------------+
| CmdPublishBatch Frame      |
| +------------------------+ |
| | Sub-frame 1            | |
| | [Magic|Cmd|StreamID    | |
| |  |DataLen|Payload]     | |
| +------------------------+ |
| | Sub-frame 2            | |
| | ...                    | |
| +------------------------+ |
| | Sub-frame N            | |
| +------------------------+ |
+----------------------------+
```

Sub-frames are parsed via `unsafe.Slice` with pointer arithmetic — each sub-slice points directly into the parent batch buffer (zero-copy). All OOB checks are performed before any unsafe operation.

#### 8.2 Nested Reference Counting

When a batch buffer arrives at the router:

1. **Parent `MessageRef`** wraps the batch buffer (`ref = 1 + frameCount`).
2. **Child `MessageRef`s** are created per sub-frame via `AcquireChildMessageRef(...)` — each stores a `frame []byte` sub-slice pointing into the parent and bumps the parent's ref counter.
3. On `Release()`, when a child's ref reaches zero it calls `parent.Release()`. When the parent reaches zero the batch buffer returns to `sync.Pool`.
4. All ref operations use `atomic.Int32` — zero locks on the hot path.

#### 8.3 Coalesced Subscriber Writer

`runSubscriberWriter` (one goroutine per subscriber) accumulates outgoing frames and flushes them when either:

1. **Size threshold reached** — accumulated payload exceeds `broker.batch_size` (default `64 KB`).
2. **Micro-timer fires** — a single reusable `time.Timer` is `Reset()` on the first accumulated frame and fires after `broker.flush_interval` (default `50 µs`). Latency is bounded even under low load.

Both parameters are configurable in `config.yaml`.

---

## 9. NACK-Based Redelivery & Dead Letter Queues

Negative acknowledgement (NACK) enables reliable delivery without polling.

### NACK Protocol (`CmdNack`, `0x06`)

- **Opcode**: `CmdNack`.
- **Payload**: 8-byte little-endian `uint64` message offset.
- On receipt, the broker looks up the original message by offset and schedules redelivery.

### Automatic Redelivery

- Retry counter tracked per `(topicHash, offset)`.
- Default `max_retries: 3` (override via `broker.max_retries` or `AQUEDUCT_BROKER_MAX_RETRIES`).
- After `max_retries` is exceeded, the message is routed to the DLQ topic `__dlq__<original_topic>`.

### Per-Subscriber Frame Cache

- Bounded FIFO cache (`defaultNackCacheSize = 256`) per subscriber.
- Stores `offset → topic` mappings for O(1) redelivery lookup.
- Prevents memory exhaustion from malicious rapid NACKs.

### Lock-Free Path

- `Router.NackByStream(streamID, offset)` routes NACKs via a buffered per-subscriber channel — zero locks on the hot path.
- The per-subscriber channel decouples NACK receipt from redelivery processing.

Metrics: `aqueduct_messages_nacked_total{topic}`, `aqueduct_messages_dead_lettered_total{topic}`.

---

## 10. Slab Allocator

Aqueduct uses a lock-free slab allocator (`internal/mem/slab.go`) for `*[]byte` frame buffers on the hot path:

- **Pre-allocated arenas** — 64 MB contiguous memory regions per size class.
- **Size classes** — `128, 256, 512, 2048, 8192, 32768` bytes.
- **Lock-free free-list** — Treiber stack (atomic CAS) for alloc/free.
- **Zero GC pressure** — arena memory is invisible to the Go garbage collector (`runtime.KeepAlive` is used to prevent premature collection when buffers escape via `unsafe.Slice`).

Performance (`BenchmarkSlabAcquireRelease`):

| Metric | Value |
| :--- | :--- |
| Allocation latency (uncontended) | ~15 ns/op |
| Allocations per op | 0 (pre-allocated) |
| GC impact | None |

Buffers exceeding the largest size class (32 KB) fall back to heap allocation. `protocol.ReleaseBuffer` returns the buffer to the slab when its capacity matches a class; otherwise it goes back to the generic `bufPool`.

---

## 11. Per-Tenant Rate Limiting (Token Bucket)

Lock-free per-tenant rate limiting using a Token Bucket algorithm (`internal/quotas/bucket.go`):

- **Lock-free token bucket** — `atomic.Int64` for token consumption, `atomic.Int64` for refill rate.
- **Background refill** — dedicated goroutine with a 100 ms ticker refills all buckets.
- **Per-tenant isolation** — each client has an independent bucket; `Manager.TryAcquire(clientID)` executes lock-free via `atomic.Pointer[map[string]*Bucket]`.

Performance:

| Metric | Value |
| :--- | :--- |
| Uncontended check | ~2 ns/op |
| Allocations per op | 0 |

Configuration:

```yaml
broker:
  quotas:
    default_publish_rate: 1000  # msg/s, 0 = unlimited
    default_burst_size: 100     # burst capacity
    per_client:
      "service-a":
        rate: 100
        burst: 200
```

Set up or override rates dynamically via the gRPC Admin API (`SetClientQuota`) — see [Admin API Reference](admin-api.md).

---

## 12. OpenTelemetry Distributed Tracing (config-gated, v1.8.0+)

`internal/tracing/tracer.go` is a **nil-safe, config-gated wrapper** around OTel:

- When `tracing.enabled: false`, `tr.tracer == nil` and every `StartSpan(...)` call returns the original context plus a **shared no-op finish callback**. The empty function body is intentional — the compiler inlines the nil check and the cost on the hot path is ~3.4 ns.
- When enabled, the broker creates an OTLP gRPC exporter with a batched span processor. W3C Trace Context TLV (`ExtTraceContext = 0x01`) is propagated automatically across publishers, subscribers, and cluster peers.

Configuration:

```yaml
tracing:
  enabled: true
  service_name: "aqueduct-broker"
  endpoint: "otel-collector:4317"
```