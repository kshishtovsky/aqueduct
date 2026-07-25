# Explanation: Architecture & Memory Model

This document explains the architectural principles, Data-Oriented Design (DoD) choices, and zero-allocation memory strategies underlying Aqueduct's performance.

## 1. Why QUIC over TCP?

Traditional TCP-based message brokers suffer from Head-of-Line (HoL) blocking when multiplexing multiple topics or streams over a single connection. Packet loss on one topic halts delivery for all independent topics.

Aqueduct uses **QUIC** (`quic-go`), offering:
- **Stream-Level Isolation**: Loss on one stream does not block unrelated topics on other streams.
- **0-RTT Resumption**: Eliminates TLS round-trips for re-connecting clients.
- **UDP Transport**: Lowers kernel latency and avoids TCP connection setup bottlenecks.

## 2. Structure of Arrays (SoA) Router Design

Standard Go implementations store subscribers using maps of pointers: `map[string][]*Subscriber`. This pattern degrades CPU L1/L2 cache performance due to pointer chasing across scattered heap allocations.

Aqueduct implements **Structure of Arrays (SoA)**:

```go
type Router struct {
    mu sync.RWMutex

    // SoA flat parallel slices
    streamIDs []uint32       // Stream IDs
    streams   []*quic.Stream // QUIC stream pointers
    topics    []string       // Topic names
    active    []bool         // Active subscriber flags

    topicIndex map[string][]int
}
```

### Cache Locality Benefit

During batch publish operations, the broker iterates sequentially over contiguous memory slices (`streams[idx]`). Sequential memory access pattern matches CPU prefetchers, maximizing L1/L2 cache hit rate and avoiding GC traversal penalties.

## 3. Zero-Allocation Hot Path Strategy

To maintain predictable sub-microsecond latency, Aqueduct avoids allocations on the hot path:

1. **Pooled Read Buffers**: Every stream reader retrieves a fixed-capacity byte slice from `sync.Pool`.
2. **Pointer Arithmetic Header Parsing**: Frame headers are parsed directly from byte slices without creating intermediate struct pointers or string copies.
3. **Synchronous AAL Disk Logging**: When AAL is enabled, published frames are written directly from the network buffer slice (`buf[consumed : consumed+totalLen]`) into the OS page cache via `os.File.Write`. This bypasses background channels and keeps `allocs/op == 0`.
