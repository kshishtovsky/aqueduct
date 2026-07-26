# Explanation: Architecture & Memory Model (v1.6.0)

This document explains the architectural principles, Data-Oriented Design (DoD) choices, security primitives, and zero-allocation memory strategies underlying Aqueduct's performance.

---

## 1. Why QUIC over TCP?

Traditional TCP-based message brokers suffer from Head-of-Line (HoL) blocking when multiplexing multiple topics or streams over a single connection. Packet loss on one topic halts delivery for all independent topics.

Aqueduct uses **QUIC** (`quic-go`), offering:
- **Stream-Level Isolation**: Loss on one stream does not block unrelated topics on other streams.
- **0-RTT Resumption**: Eliminates TLS round-trips for re-connecting clients.
- **UDP Transport**: Lowers kernel latency and avoids TCP connection setup bottlenecks.

---

## 2. Structure of Arrays (SoA) Router & Async Fan-Out

Standard Go implementations store subscribers using maps of pointers: `map[string][]*Subscriber`. This pattern degrades CPU L1/L2 cache performance due to pointer chasing across scattered heap allocations.

Aqueduct implements **Structure of Arrays (SoA)** paired with per-subscriber non-blocking channels and dedicated Writer goroutines:

```go
type Router struct {
    mu sync.RWMutex

    // SoA flat parallel slices
    streamIDs []uint32               // Stream IDs
    streams   []*quic.Stream         // QUIC stream pointers
    topics    []string               // Topic names
    active    []bool                 // Active subscriber flags
    queues    []chan *MessageRef     // Bounded ring queues per subscriber
    cancels   []context.CancelFunc   // Writer goroutine cancellation handles

    topicIndex   map[uint64][]int    // FNV-1a topic hash -> parallel slice indices
    wildcardSubs []WildcardSub       // MQTT wildcard patterns (+ and #)
}
```

### Cache Locality & Async Fan-Out Benefit

During publish operations, the broker iterates sequentially over contiguous memory slices (`queues[idx]`). It pushes pointers non-blockingly into bounded subscriber ring queues in nanoseconds. Dedicated Writer goroutines drain each queue independently, completely isolating slow consumers from publishers.

---

## 3. Atomic Reference Counting (`MessageRef`)

To recycle message frame buffers safely into `sync.Pool` without data races or memory corruption:

```go
type MessageRef struct {
    buf       *[]byte
    ref       atomic.Int32
    expiresAt int64 // unix nano timestamp, 0 = no expiry
}
```

- When `Publish` is called, a single buffer is pulled from `sync.Pool` and wrapped in `MessageRef` (`ref = 1`).
- For each target subscriber queue, `Retain()` increments `ref`.
- Publisher and Writer goroutines call `Release()` after dispatching or writing to network sockets.
- When `ref.Add(-1) == 0`, the buffer is returned to `protocol.ReleaseBuffer` and `MessageRef` to `msgRefPool` (**`0 allocs/op`**).

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
