# 解释: 架构与内存模型 (v1.15.0)

本文档说明 Aqueduct 的架构设计、面向数据设计 (DoD)、安全机制以及零内存分配策略。

---

## 1. 为什么选择 QUIC？

传统 TCP 消息代理在单个连接上复用多个主题时存在队头阻塞 (Head-of-Line blocking) 问题。

Aqueduct 使用 **QUIC** (`quic-go`):
- **流级隔离**: 单个流上的丢包不会影响其他独立主题。
- **0-RTT 重连**: 消除重复连接时的 TLS 握手开销。
- **UDP 传输**: 降低内核延迟。

---

## 2. Structure of Arrays (SoA) 与延迟优先级队列 (QoS)

Aqueduct 使用 **SoA 数组布局** 配合延迟绑定的多优先级环形队列：

```go
type Router struct {
    mu sync.RWMutex

    // SoA 扁平并行数组
    streamIDs []uint32               // 流 ID
    streams   []*quic.Stream         // QUIC 流指针
    topics    []string               // 主题名称
    active    []bool                 // 激活标志
    queues    []*[4]chan *MessageRef // 延迟绑定的 4 级优先级队列指针 (0=最高 .. 3=最低)
    subMus    []*sync.RWMutex        // 每个订阅者的 RWMutex
    cancels   []context.CancelFunc   // 协程取消 Handle

    topicIndex   map[uint64][]int    // FNV-1a 主题哈希
    wildcardSubs []WildcardSub       // 通配符模式 (+ 和 #)
    queuePool    sync.Pool           // 全局队列对象池 (queue_size)
}
```

### 延迟队列分配与严格优先级调度

1. **延迟初始化**: 订阅时 `queues[idx]` 仅包含全 `nil` 的指针。只有当对应优先级 `P` 的消息到达时，`enqueueToSubscriber` 才从 `r.queuePool` 动态获取 Channel (`0 allocs/op`)。
2. **严格优先级调度**: Writer 协程按 `0 -> 1 -> 2 -> 3` 严格顺序轮询，高优先级紧急消息超越低优先级流量。
3. **按优先级 TTL**: 根据 `priority_ttls` 强制重写过期时间戳，出队时延迟丢弃过期消息。
4. **内存自动回收**: 队列清空 (`len(q) == 0`) 时自动归还至 `r.queuePool` 并复位指针。单优先级订阅者仅占用 1 个队列内存。

---

## 3. 原子引用计数 (`MessageRef`)

安全地将帧缓冲区回收至 `sync.Pool`:

```go
type MessageRef struct {
    buf       *[]byte    // 来自池的缓冲区（仅父级）
    frame     []byte     // 父缓冲区的零拷贝子切片（批量子节点使用）
    ref       atomic.Int32
    expiresAt int64      // unix 纳秒时间戳，0 = 永不过期
    offset    uint64     // 主题偏移量
    parent    *MessageRef // 父级引用（父级为 nil）
}
```

- `AcquireMessageRef` 初始化 `ref = 1`。
- 每个目标订阅者通过 `Retain()` 增加引用。
- 发送完成后调用 `Release()`。
- 当 `ref.Add(-1) == 0` 时，缓冲区安全归还至 `protocol.ReleaseBuffer` (**`0 allocs/op`**)。
- **嵌套引用计数 (Nested RC, v1.6.0)**: 批量消息使用父子 `MessageRef` 层次结构。父级包装批处理缓冲区，`ref = 1 + 帧数`。每个子级通过 `AcquireChildMessageRef()` 创建，带有指向父缓冲区的 `frame []byte` 子切片。当子级的引用计数归零时，`Release()` 调用 `parent.Release()`。父级的 `buf` 生命周期通过 `protocol.ReleaseBuffer` 管理。

---

## 4. 零分配通配符主题匹配

- `+` 匹配单个主题层级。
- `#` 匹配后续所有主题层级。
- `MatchWildcard(pattern, topic []byte)` 运行时间为 **50.41 ns/op**，**`0 allocs/op`**。

---

## 5. 安全架构 (mTLS, 非交换 ACL, 加密 AAL)

1. **mTLS 1.3**: 通过 `client_ca_file` 验证客户端证书。
2. **非交换 ACL**: 采用 `FNV1a(clientID + ":" + topic)` 组合哈希，规避 XOR 碰撞。
3. **AES-256-GCM AAL 与重放**: 带长度前缀与 12 字节 Nonce (4 字节随机会话前缀) 的加密日志。在开启 UDP 监听前完成重放状态恢复。

---

## 6. 直接网格集群 (P2P 联邦) 与 DNS 发现

Aqueduct 支持通过直接 P2P QUIC 网格连接多个代理实例形成集群。没有中央协调器或共识协议（无 Raft/Paxos）。消息转发采用 fire-and-forget 模式。

### PeerManager (RCU 模式, v1.14.0)

PeerManager 使用 **Read-Copy-Update (RCU)** 模式实现热路径无锁读取：

```go
type PeerManager struct {
    peers   atomic.Pointer[peerSlice]   // 无锁原子快照
    mu      sync.Mutex                  // 仅用于写路径
    addrSet map[string]int              // 快速地址查找
}
```

- **读取** (`Forward()`, `PeerCount()`): 获取原子指针 — 零锁，零竞争
- **写入** (`AddPeer()`, `RemovePeer()`): 创建新切片，原子替换。写路径互斥锁仅保护 `addrSet` map
- **动态对等管理** (v1.14.0):
  - `AddPeer(ctx, addr)` — 拨号新对等节点，添加到原子快照，启动重连循环
  - `RemovePeer(addr)` — 取消上下文，关闭流，从原子快照移除
  - `PeerCount()` — 当前对等节点数（原子读取）

### DNS 发现 (v1.14.0)

对于 Kubernetes StatefulSet 部署，`Discovery` 模块轮询 Headless Service DNS 记录：

```go
type Discovery struct {
    manager     *PeerManager
    resolver    Resolver            // 可注入接口用于测试
    hostname    string
    port        int
    interval    time.Duration
    knownIPs    map[string]struct{} // 快速变更跟踪
}
```

- 每 `interval`（默认 10s）轮询 `net.LookupHost(hostname)`
- 与 `knownIPs` 计算差集 → 仅在变化时调用 `AddPeer()`/`RemovePeer()`
- `normalize()` 去重 IP 并过滤链路本地 (`169.254.x.x`) 地址
- `Resolver` 接口支持测试中模拟 DNS

### MeshForwarded Bit

协议 Command 字节中的一个位（第 7 位，掩码 `0x80`）标记帧已被转发。接收对等节点检查此位并跳过重复转发，防止多跳拓扑中的广播风暴。

### 零拷贝转发

`Forward()` 方法读取原子对等快照，然后在原地修改共享缓冲区的 MeshForwarded 位（0 堆分配，0 allocs/op），并将修改后的帧直接写入每个对等节点的 QUIC 流。写入后恢复位以保留缓冲区用于本地投递。

### 路由器集成

当 `Router.Publish()` 处理本地消息时：
1. 通过 SoA fan-out 投递到本地订阅者
2. 调用 `PeerManager.Forward()` 广播到所有对等节点
3. 接收节点调用 `Router.PublishFromPeer()`，仅本地分发（不重新转发）

---

## 7. 批处理协议与写合并

### 问题: OS PPS 限制

`quic.Stream.Write()` 单次调用开销大（syscall、打包、加密）。逐帧发送将吞吐量限制在 ∼300k RPS。

### 解决方案: 智能批处理

#### 7.1 协议级批处理 (`CmdPublishBatch`)

命令 `0x04` 将多个标准帧编码为单个 QUIC 流写入中的扁平字节数组：

```text
+--------------------------+
| CmdPublishBatch Frame    |
| +----------------------+ |
| | 子帧 1..N            | |
| +----------------------+ |
+--------------------------+
```

子帧通过 `unsafe.Slice` 和指针运算解析 — 每个子切片直接指向父批处理缓冲区（**零拷贝解包**，`0 allocs/op`）。

#### 7.2 嵌套引用计数 (Nested RC)

1. **父 `MessageRef`** 包装批处理缓冲区 (ref = 1 + 帧数)
2. 通过 `AcquireChildMessageRef()` 为每个子帧创建 **子 `MessageRef`** — 每个子节点存储 `frame []byte` 子切片（指向父缓冲区）并递增父节点的引用计数
3. `Release()` 时：子 ref → 0 触发 `parent.Release()`。父 ref → 0 时缓冲区返回 `sync.Pool`
4. 全部操作 `atomic.Int32`，热路径零锁

#### 7.3 写合并 (Coalesced Writer)

订阅者 Writer 协程累积出站帧，在以下条件触发时批量刷新：

1. **大小阈值**: 累积超过 `batch_size`（默认 64 KB）
2. **微定时器**: 单个可复用 `time.Timer` 在第一个帧累积后重置，在 `flush_interval`（默认 50 µs）后触发

```yaml
broker:
  batch_size: 65536
  flush_interval: "50us"
```

#### 7.4 基准测试

| 场景 | 吞吐量 | allocs/op |
|----------|-----------|-----------|
| **BatchUnpack** (1000 帧) | 19.9 GB/s | **0** |
| **BatchPublish** (100 消息) | 6.67M msg/s, 921 MB/s | **0** |
| 单帧 vs 批量 (每消息) | ~150 ns/msg (batch) vs ~920 ns/msg (single) | **0** |

---

## 8. 基于 NACK 的重投递与死信队列

Aqueduct v1.7.0 引入否定确认 (NACK) 机制实现可靠投递：

### NACK 协议 (`CmdNack`)

- **操作码**: `0x05` (`CmdNack`)
- **负载**: 8 字节 uint64 消息偏移量 (小端序)
- 收到后，代理根据偏移量查找原始消息并调度重投递

### 自动重投递

- 每条消息有内部重试计数器
- 默认 `max_retries`: 3
- 每次 NACK 后，消息重新入队到订阅者通道
- 超过 `max_retries` 后：消息路由到死信队列

### 每订阅者帧缓存

- 每订阅者有界 FIFO 缓存 (256 条目)
- 存储 offset → topic 映射，实现 O(1) 重投递查找
- 防止恶意快速 NACK 导致内存耗尽

### 死信队列

- 超过 `max_retries` 后，毒丸消息路由到 `__dlq__<原始主题>`
- DLQ 订阅者对 `__dlq__` 主题模式使用标准订阅语义

### 指标

| 指标 | 类型 | 描述 |
|------|------|------|
| `aqueduct_messages_nacked_total` | Counter | 总计 NACK 消息数 |
| `aqueduct_messages_dead_lettered_total` | Counter | 总计路由到 DLQ 的消息数 |

### 无锁路径

- `NackByStream` 通过缓冲通道路由 NACK — 热路径零锁
- 每订阅者通道解耦 NACK 接收与重投递处理

---

## 9. Slab 分配器

Aqueduct v1.8.0 在热路径上使用高性能 slab 分配器替代 `sync.Pool` 管理 `*[]byte` 帧缓冲区：

### 设计

- **预分配 Arena**: 每个大小类连续 64 MB 内存区域
- **大小类**: 128B、256B、512B、2KB、8KB、32KB
- **无锁空闲链表**: Treiber 栈（原子 CAS）实现分配与释放
- **零 GC 压力**: Arena 内存不被 Go GC 扫描

### 性能

| 指标 | 值 |
|------|-----|
| 分配延迟 | ~15 ns/op（无竞争） |
| 每次操作分配数 | 0（预分配） |
| GC 影响 | 无 |

### 集成

- `slab.Allocate(size) → *[]byte` 替代 `pool.Get().(*[]byte)`
- `slab.Deallocate(buf)` 替代 `pool.Put(buf)`
- 超过 32 KB 的大小回退到堆分配

---

## 10. 每租户速率限制 (Token Bucket)

Aqueduct v1.8.0 使用令牌桶算法实现无锁每租户速率限制：

### 设计

- **无锁令牌桶**: 使用原子操作进行令牌消费和补充
- **后台补充**: 专用 goroutine 以 100ms 为周期补充所有桶
- **每租户隔离**: 每个客户端有独立的令牌桶

### 性能

| 指标 | 值 |
|------|-----|
| 无竞争检查 | 2.1 ns/op |
| 每次操作分配数 | 0 |

### 配置

```yaml
broker:
  quotas:
    default_publish_rate: 1000  # 每秒消息数
    default_burst_size: 100     # 突发容量
```

环境变量覆盖：

| 变量 | 默认值 |
|------|--------|
| `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` | 1000 |
| `AQUEDUCT_BROKER_DEFAULT_BURST_SIZE` | 100 |

### 集成

- 在 `Router.Publish()` 消息分发前检查
- 如果被限流，消息被丢弃，计数器递增
- 指标: `aqueduct_messages_rate_limited_total`（计数器）

---

## 11. 幂等生产者滑动窗口去重

Aqueduct v1.15.0 引入幂等生产者 (Idempotent Producer) 支持，在现有的 at-least-once 发布订阅之上提供 broker 端去重，从而保证 exactly-once 投递语义。幂等生产者在每次发布时附加 `ProducerID` + `SeqNum` TLV 对；broker 维护每个生产者最近 2048 个序列号的滑动窗口，并静默丢弃重复消息。

### 为什么需要 Exactly-Once？

当生产者也能收到 broker 的 ACK 时，At-Least-Once（通过 Durable Subscriptions + CmdAck）已经足够。但在有损网络上，生产者可能因 ACK 丢失而重发消息，broker 无法区分新消息和重试。例如支付账本等消费者数据库若处理了两份副本，就会出现重复扣款。Kafka 的解决方案是 `enable.idempotence=true`；Aqueduct 在协议层提供同样的保证。

### 线缆格式

TLV 块携带两个条目（zero-copy，通过现有的 `FindExtension` 解析）：

```text
[ ExtProducerID (0x04) | Len: 8 | ID: 8B ] [ ExtSeqNum (0x05) | Len: 8 | Seq: 8B ]
```

总共 20 字节。两个条目必须同时存在，去重才会生效；否则帧按普通发布处理。

### 滑动窗口环形缓冲区

每个生产者由一个 256 字节的 `Window`（32 × `uint64`）跟踪：

```go
type Window struct {
    bitmap       [32]uint64     // 2048 位，每个 seq 占 1 位
    advanceMu    sync.Mutex     // 保护 highSeqNum 和清理
    highSeqNum   uint64         // 迄今为止看到的最大 seq
    lastUsedNs   atomic.Int64   // unix-nano，用于 LRU 淘汰
    producerID   uint64
}
```

`SeqNum` 对应的槽位是 `SeqNum % 2048`。位检查和设置使用 `atomic.LoadUint64` + `atomic.CompareAndSwapUint64` —— 热路径完全无锁。只有 high-watermark 推进需要短互斥锁。

### Lock-Free 位检查与设置

```go
// 索引计算
idx   := seqNum % 2048         // 0..2047
word  := idx / 64               // 0..31
bit   := uint64(1) << (idx%64)  // 0..63

// CAS 循环（无互斥锁）
for {
    old := atomic.LoadUint64(&w.bitmap[word])
    if old & bit != 0 { return Duplicate }
    if atomic.CompareAndSwapUint64(&w.bitmap[word], old, old|bit) {
        return New
    }
    // CAS 失败；重试。
}
```

当新的 `SeqNum` 推进窗口超出其范围时，回收槽位范围内的陈旧位 `(highSeqNum, seqNum - 2048]` 在 CAS **之前**被清空，因此同一槽位上的回绕冲突不会被误读为重复。

### 生产者生命周期（LRU + TTL）

```go
type Store struct {
    entries   map[uint64]*storeEntry  // ProducerID → entry
    lru       *list.List              // LRU 淘汰顺序
    capacity  int                     // 最大窗口数（默认 65536）
    idleTTL   time.Duration           // 淘汰空闲时间超过此值的窗口（默认 5m）
}
```

- **LRU**：当 `len(entries) >= capacity` 时，最老的窗口被淘汰。
- **TTL**：后台 goroutine 每 `evictInterval`（默认 30s）回收 `lastUsedNs` 早于 `now - idleTTL` 的窗口。
- **总内存**：`capacity × 256 B` 加上 map 开销 —— 默认容量下 16 MB。

### 与分发器集成

```text
QUIC stream → dispatchFrames()
                 ↓
            1. ParseFrame
            2. Decompress (Compression TLV)
            3. Dedup check  ← v1.15.0 新增
                 ├─ New          → 继续 router.Publish
                 ├─ Duplicate    → 发送合成的 dedup_ack，丢弃帧
                 └─ Too Old      → 取消流（ProtocolError）
            4. Authorization
            5. AAL
            6. Router.Publish
```

重复消息以合成的 `dedup_ack:<id>:<seq>` ACK 丢弃，使生产者的重试循环终止。`TooOld` 触发 `stream.CancelRead(1)/CancelWrite(1)`，因为客户端存在严重 bug（时钟漂移、重启重置、卡死重试）。

### 性能

| 基准 | ns/op | allocs/op |
|------|-------|-----------|
| `BenchmarkDedupCheck_InWindow`（窗口内，重复） | 3.7 | 0 |
| `BenchmarkDedupCheck_DuplicatePath`（重发同一 seq） | 3.7 | 0 |
| `BenchmarkDedupCheck`（推进 highSeqNum） | 12.4 | 0 |
| `BenchmarkDedupCheck_Parallel`（32 核，无锁） | 72.7 | 0 |
| `BenchmarkStoreCheck_HotProducer`（1 个热生产者） | 9.4 | 0 |
| `BenchmarkStoreCheck`（256 个生产者，混合） | 375 | 0 |

热路径是窗口内 / 重复情况：3.7 ns/op，零分配。完整 dedup check（推进 + 清理 + CAS）为 12.4 ns/op，原因是 `highSeqNum` 互斥锁 —— 仍在端到端发布（baseline ~30 ns）的 5–10% 预算内。

### 配置

```yaml
broker:
  idempotent_producers:
    enabled: true
    window_capacity: 65536
    idle_ttl: 5m
```

### 指标

| 指标 | 类型 | 描述 |
|------|------|------|
| `aqueduct_messages_deduplicated_total` | Counter | 被去重窗口丢弃的消息总数。 |
