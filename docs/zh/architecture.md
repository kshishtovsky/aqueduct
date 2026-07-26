# 解释: 架构与内存模型 (v1.8.0)

本文档说明 Aqueduct 的架构设计、面向数据设计 (DoD)、安全机制以及零内存分配策略。

---

## 1. 为什么选择 QUIC？

传统 TCP 消息代理在单个连接上复用多个主题时存在队头阻塞 (Head-of-Line blocking) 问题。

Aqueduct 使用 **QUIC** (`quic-go`):
- **流级隔离**: 单个流上的丢包不会影响其他独立主题。
- **0-RTT 重连**: 消除重复连接时的 TLS 握手开销。
- **UDP 传输**: 降低内核延迟。

---

## 2. Structure of Arrays (SoA) 与异步 Fan-Out

Aqueduct 使用 **SoA 数组布局** 与每个订阅者独立非阻塞队列：

```go
type Router struct {
    mu sync.RWMutex

    // SoA 扁平并行数组
    streamIDs []uint32               // 流 ID
    streams   []*quic.Stream         // QUIC 流指针
    topics    []string               // 主题名称
    active    []bool                 // 激活标志
    queues    []chan *MessageRef     // 每个订阅者的环形队列
    cancels   []context.CancelFunc   // 协程取消 Handle

    topicIndex   map[uint64][]int    // FNV-1a 主题哈希
    wildcardSubs []WildcardSub       // 通配符模式 (+ 和 #)
}
```

发布消息时，代理以纳秒级速度将 `MessageRef` 入队，独立的 Writer 协程异步发送，彻底隔离慢消费者。

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

## 6. 直接网格集群 (P2P 联邦)

Aqueduct 支持通过直接 P2P QUIC 网格连接多个代理实例形成集群。没有中央协调器或共识协议（无 Raft/Paxos）。消息转发采用 fire-and-forget 模式。

### PeerManager

每个代理维护到静态对等列表的出站 QUIC 连接。PeerManager：
- 启动时使用 mTLS 1.3 拨号每个对等地址
- 运行后台重连循环，断连时使用指数退避
- 暴露 `Forward()` 方法用于零拷贝帧转发到所有已连接对等节点

### MeshForwarded Bit

协议 Command 字节中的一个位（第 7 位，掩码 `0x80`）标记帧已被转发。接收对等节点检查此位并跳过重复转发，防止多跳拓扑中的广播风暴。

### 零拷贝转发

`Forward()` 方法在原地修改共享缓冲区的 MeshForwarded 位（0 堆分配，0 allocs/op），并将修改后的帧直接写入每个对等节点的 QUIC 流。

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
