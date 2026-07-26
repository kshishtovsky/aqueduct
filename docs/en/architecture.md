# Explanation: Architecture & Memory Model (v1.3.0)

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
