# Explanation: Architecture & Memory Model (v1.11.0)

This document explains the architectural principles, Data-Oriented Design (DoD) choices, security primitives, and zero-allocation memory strategies underlying Aqueduct's performance.

---

## 1. Why QUIC over TCP?

Traditional TCP-based message brokers suffer from Head-of-Line (HoL) blocking when multiplexing multiple topics or streams over a single connection. Packet loss on one topic halts delivery for all independent topics.

Aqueduct uses **QUIC** (`quic-go`), offering:
- **Stream-Level Isolation**: Loss on one stream does not block unrelated topics on other streams.
- **0-RTT Resumption**: Eliminates TLS round-trips for re-connecting clients.
- **UDP Transport**: Lowers kernel latency and avoids TCP connection setup bottlenecks.

---

## 2. Structure of Arrays (SoA) Router & Lazy Priority Queues (QoS)

Standard Go implementations store subscribers using maps of pointers: `map[string][]*Subscriber`. This pattern degrades CPU L1/L2 cache performance due to pointer chasing across scattered heap allocations.

Aqueduct implements **Structure of Arrays (SoA)** paired with lazy per-priority ring queues and dedicated Writer goroutines:

```go
type Router struct {
    mu sync.RWMutex

    // SoA flat parallel slices
    streamIDs []uint32               // Stream IDs
    streams   []*quic.Stream         // QUIC stream pointers
    topics    []string               // Topic names
    active    []bool                 // Active subscriber flags
    queues    []*[4]chan *MessageRef // Lazy priority ring queues pointer (0=Highest .. 3=Low)
    subMus    []*sync.RWMutex        // Per-subscriber RWMutex for lazy queue pool init/cleanup
    cancels   []context.CancelFunc   // Writer goroutine cancellation handles

    topicIndex   map[uint64][]int    // FNV-1a topic hash -> parallel slice indices
    wildcardSubs []WildcardSub       // MQTT wildcard patterns (+ and #)
    queuePool    sync.Pool           // Global pool of chan *MessageRef (queueSize)
}
```

### Lazy Queue Allocation & Strict Prioritization

1. **Lazy Initialization:** Upon subscription, `queues[idx]` contains a pointer `*[4]chan *MessageRef` with all 4 entries `nil`. When a message of priority `P` is published, `enqueueToSubscriber` lazily acquires a channel from `r.queuePool` (`0 allocs/op`) under `subMus[idx]`.
2. **Strict Priority Sending:** Dedicated Writer goroutines call `fetchNextMessage`, which polls queues in strict priority order `0 -> 1 -> 2 -> 3`. High priority critical alerts bypass lower priority traffic.
3. **Per-Priority TTL & Expiration:** Expiration timestamp `expiresAt` is calculated upon enqueueing (using `priority_ttls[P]` if set). On dequeueing, `msgRef.IsExpired(nowNano)` lazily drops expired messages before writing to QUIC streams.
4. **Memory Recycling:** When `len(q) == 0` upon dequeueing, `cleanupEmptyQueue` returns the channel to `r.queuePool` and resets `queues[idx][P] = nil`. Single-priority subscribers consume memory for 1 queue only.

---

## 3. Atomic Reference Counting (`MessageRef`)

To recycle message frame buffers safely into `sync.Pool` without data races or memory corruption:

```go
type MessageRef struct {
    buf       *[]byte    // pooled buffer (parent only)
    frame     []byte     // zero-copy sub-slice of parent buffer (for batch children)
    ref       atomic.Int32
    expiresAt int64      // unix nano timestamp, 0 = no expiry
    offset    uint64     // topic offset
    parent    *MessageRef // parent ref (nil for parents)
}
```

- When `Publish` is called, a single buffer is pulled from `sync.Pool` and wrapped in `MessageRef` (`ref = 1`).
- For each target subscriber queue, `Retain()` increments `ref`.
- Publisher and Writer goroutines call `Release()` after dispatching or writing to network sockets.
- When `ref.Add(-1) == 0`, the buffer is returned to `protocol.ReleaseBuffer` and `MessageRef` to `msgRefPool` (**`0 allocs/op`**).
- **Nested Reference Counting (v1.6.0)**: Batch messages use a parent-child `MessageRef` hierarchy. A parent wraps the batch buffer with `ref = 1 + frameCount`. Each child is created via `AcquireChildMessageRef()` with a `frame []byte` sub-slice pointing into the parent buffer. On `Release()`, when a child's ref reaches 0 it calls `parent.Release()`. The parent's `buf` lifecycle is managed via `protocol.ReleaseBuffer`.

---

## 4. Zero-Allocation Wildcard Topic Matcher

MQTT wildcard matching operates directly on byte slices without string conversions:

- `+` matches one topic segment between `/`.
- `#` matches zero or more topic segments at the end.
- `MatchWildcard(pattern, topic []byte)` executes in **50.41 ns/op** with **`0 allocs/op`**.

---

## 5. Security Architecture (mTLS, Non-Commutative ACL, Encrypted AAL)

1. **mTLS 1.3**: Requires valid client certificates verified against trusted CA cert pools (`client_ca_file`). Client Common Name (CN) is extracted for authorization.
2. **Non-Commutative FNV-1a ACL**: Combines `clientID` and `topic` sequentially (`FNV1a(clientID + ":" + topic)`), preventing XOR commutativity bypasses.
3. **AES-256-GCM Encrypted AAL & Replay**: Log records are encrypted with 12-byte Nonces (4-byte random session prefix) and length-prefixed headers. Startup replay restores state sequentially before opening the QUIC UDP listener socket.

---

## 6. Direct Mesh Clustering (P2P Federation)

Aqueduct supports forming a cluster of broker instances connected via a direct peer-to-peer QUIC mesh. There is no central coordinator or consensus protocol (no Raft/Paxos). Forwarding is fire-and-forget.

### PeerManager

Each broker maintains outgoing QUIC connections to a static peer list. The PeerManager:
- Dials each peer address on startup with mTLS 1.3
- Runs a background reconnect loop with exponential backoff on disconnect
- Exposes `Forward()` for zero-copy frame forwarding to all connected peers

### MeshForwarded Bit

A single bit in the protocol's Command byte (bit 7, mask `0x80`) marks a frame as already forwarded. Receiving peers check this bit and skip re-forwarding, preventing broadcast storms in multi-hop topologies.

### Zero-Copy Forwarding

The `Forward()` method sets the MeshForwarded bit in-place on the shared buffer (0 heap allocations, 0 allocs/op) and writes the modified frame directly to each peer's QUIC stream.

### Router Integration

When `Router.Publish()` processes a local message:
1. Delivers to local subscribers via SoA fan-out
2. Calls `PeerManager.Forward()` to broadcast to all peers
3. The receiving peer calls `Router.PublishFromPeer()` which dispatches locally only (no re-forwarding)

---

## 7. Batch Protocol & Coalesced Writes

### The Problem: OS PPS Limits

QUIC streams provide excellent stream-level isolation, but `quic.Stream.Write()` has significant per-call overhead (syscall boundary, packetization, crypto). Sending one frame per write caps throughput at ∼300k RPS regardless of CPU speed.

### Solution: Smart Batching

Aqueduct v1.6.0 uses two complementary batching strategies:

#### 7.1 Protocol-Level Batching (`CmdPublishBatch`)

A new command `0x04` encodes multiple standard frames as a flat byte array within a single QUIC stream write payload:

```text
+--------------------------+
| CmdPublishBatch Frame    |
| +----------------------+ |
| | Sub-frame 1          | |
| | [Magic|Cmd|StreamID  | |
| |  |Len|Payload]       | |
| +----------------------+ |
| | Sub-frame 2          | |
| | ...                  | |
| +----------------------+ |
| | Sub-frame N          | |
| +----------------------+ |
+--------------------------+
```

Sub-frames are parsed via `unsafe.Slice` with pointer arithmetic — each sub-slice points directly into the parent batch buffer, achieving **zero-copy unpacking**. All OOB checks are performed before any unsafe operation.

#### 7.2 Nested Reference Counting

When a batch buffer arrives at the router:

1. **Parent `MessageRef`** is created wrapping the batch buffer (ref = 1 + frameCount)
2. **Child `MessageRef`s** are created for each sub-frame via `AcquireChildMessageRef()` — each child stores a `frame []byte` sub-slice pointing into the parent buffer and increments the parent's ref counter
3. On `Release()`: when a child's ref reaches 0, it calls `parent.Release()`. When parent's ref reaches 0, the batch buffer is returned to `sync.Pool`
4. All ref operations are `atomic.Int32` — zero locks on the hot path

```go
type MessageRef struct {
    buf       *[]byte     // pooled buffer (parent only)
    frame     []byte      // zero-copy sub-slice (child only)
    parent    *MessageRef // parent ref (nil for parents)
    ref       atomic.Int32
    expiresAt int64
}
```

#### 7.3 Coalesced Subscriber Writer

The `runSubscriberWriter` goroutine (one per subscriber) accumulates outgoing frames and flushes them as a batch when:

1. **Size threshold reached**: Accumulated payload exceeds `batch_size` (default 64 KB)
2. **Micro-timer fires**: A single reusable `time.Timer` is `Reset()` after the first accumulated frame and fires after `flush_interval` (default 50 µs) — ensuring latency is bounded even under low load

Both parameters are configurable via `config.yaml`:

```yaml
broker:
  batch_size: 65536
  flush_interval: 50us
```

#### 7.4 Benchmarks

| Scenario | Throughput | allocs/op |
|----------|-----------|-----------|
| **BatchUnpack** (1000 frames) | 19.9 GB/s | **0** |
| **BatchPublish** (100 msgs) | 6.67M msg/s, 921 MB/s | **0** |
| Single vs Batch (per message) | ~150 ns/msg (batch) vs ~920 ns/msg (single) | **0** |

---

## 8. NACK-Based Redelivery & Dead Letter Queues

Aqueduct v1.7.0 introduces negative acknowledgment (NACK) for reliable message delivery:

### NACK Protocol (`CmdNack`)

- **Opcode**: `0x05` (`CmdNack`)
- **Payload**: 8-byte uint64 message offset (little-endian)
- On receipt, the broker looks up the original message by offset and schedules redelivery

### Automatic Redelivery

- Each message has a retry counter tracked internally
- Default `max_retries`: 3
- After each NACK, the message is re-queued to the subscriber's channel
- After `max_retries` exceeded: the message is routed to the Dead Letter Queue

### Per-Subscriber Frame Cache

- Bounded FIFO cache (256 entries) per subscriber
- Stores offset → topic mappings for O(1) redelivery lookup
- Prevents memory exhaustion from malicious rapid NACKs

### Dead Letter Queue

- After `max_retries` exhausted, a poison pill is routed to `__dlq__<original_topic>`
- DLQ subscribers use standard subscribe semantics on the `__dlq__` topic pattern

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `aqueduct_messages_nacked_total` | Counter | Total NACKed messages |
| `aqueduct_messages_dead_lettered_total` | Counter | Total messages routed to DLQ |

### Lock-Free Path

- `NackByStream` routes NACKs via a buffered channel — zero locks on the hot path
- The per-subscriber channel decouples NACK receipt from redelivery processing

---

## 9. Slab Allocator

Aqueduct v1.8.0 replaces `sync.Pool` for `*[]byte` frame buffers on the hot path with a high-performance slab allocator:

### Design

- **Pre-allocated arenas**: 64 MB contiguous memory regions per size class
- **Size Classes**: 128B, 256B, 512B, 2KB, 8KB, 32KB
- **Lock-Free Free-List**: Treiber stack (atomic CAS) for allocation and deallocation
- **Zero GC Pressure**: Arena memory is never scanned by the Go garbage collector

### Performance

| Metric | Value |
|--------|-------|
| Allocation latency | ~15 ns/op (uncontended) |
| Allocations per op | 0 (pre-allocated) |
| GC impact | None (arena memory invisible to GC) |

### Integration

- `slab.Allocate(size) → *[]byte` replaces `pool.Get().(*[]byte)`
- `slab.Deallocate(buf)` replaces `pool.Put(buf)`
- Fallback to heap allocation for sizes exceeding 32 KB

---

## 10. Per-Tenant Rate Limiting (Token Bucket)

Aqueduct v1.8.0 adds lock-free per-tenant rate limiting using a Token Bucket algorithm:

### Design

- **Lock-Free Token Bucket**: Atomic operations for token consumption and refill
- **Background Refill**: Dedicated goroutine with a 100ms ticker refills all buckets
- **Per-Tenant Isolation**: Each client has an independent token bucket

### Performance

| Metric | Value |
|--------|-------|
| Uncontended check | 2.1 ns/op |
| Allocations per op | 0 |

### Configuration

```yaml
broker:
  quotas:
    default_publish_rate: 1000  # messages per second
    default_burst_size: 100     # burst capacity
```

Environment variable overrides:

| Variable | Default |
|----------|---------|
| `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` | 1000 |
| `AQUEDUCT_BROKER_DEFAULT_BURST_SIZE` | 100 |

### Integration

- Checked in `Router.Publish()` before message dispatch
- If rate limited, the message is dropped and the counter is incremented
- Metrics: `aqueduct_messages_rate_limited_total` (counter)
