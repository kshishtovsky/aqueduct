# 解释: 架构与内存模型 (v1.16.0)

本文档说明 Aqueduct 的架构设计原则、面向数据设计 (DoD)、安全原语和零内存分配策略。

> [!NOTE]
> Diátaxis 分类：**Explanation** — 解释 *为什么* 与 *如何*，不替代操作步骤。

---

## 1. 为什么选择 QUIC？

传统基于 TCP 的消息代理在单连接上复用多个主题时存在**队头阻塞 (HoL Blocking)** 问题：一个主题的丢包会阻塞同一连接上所有独立主题的投递。

Aqueduct 使用 **QUIC**（基于 `quic-go`，ALPN 标识符为 `aqueduct-v1`），提供：

- **流级隔离**: 单个流上的丢包不会阻塞其他流上的独立主题。
- **0-RTT 重连**: 消除重连客户端的 TLS 握手（依赖已缓存的 session ticket）。
- **UDP 传输**: 降低内核延迟，规避 TCP 连接建立的瓶颈。
- **0-RTT 防护**: 原生 QUIC 监听器设置 `Allow0RTT: true`，`MaxIncomingStreams: 100`，`MaxIdleTimeout: 30s`。
- **强制 TLS 1.3**: 监听器 `*tls.Config` 设置 `MinVersion = tls.VersionTLS13`。

---

## 2. Structure of Arrays (SoA) 路由器与延迟优先级队列 (QoS)

标准 Go 实现使用 `map[string][]*Subscriber` 存储订阅者，通过跨堆指针追逐降低 CPU L1/L2 缓存性能。

Aqueduct 实现 **Structure of Arrays (SoA)**，搭配延迟绑定的多优先级环形队列和专用 Writer 协程：

```go
type Router struct {
    mu sync.RWMutex

    // SoA 扁平并行数组 — 索引在数组间对齐
    streamIDs []uint32
    streams   []*quic.Stream
    topics    []string
    active    []bool
    queues    []*[4]chan *MessageRef // 4 级优先级环形队列指针 (0=最高 .. 3=最低)
    notifyChs []chan struct{}
    subMus    []*sync.RWMutex
    cancels   []context.CancelFunc

    topicIndex   map[uint64][]int       // FNV-1a topic 哈希 -> 切片索引
    subGroups    []string               // GroupID per slot
    groups       map[uint64][]*ConsumerGroup // FNV-1a topic 哈希 -> ConsumerGroup 列表
    wildcardSubs []WildcardSub          // MQTT 通配符 (+ and #)
    queuePool    sync.Pool              // chan *MessageRef (queueSize)
    topicOffsets map[uint64]*atomic.Uint64
    durableOffsets map[uint64]uint64
    nackChs      []chan uint64
    nackCounters map[nackKey]int8
    // ...
    priorityTTLs [4]time.Duration
}
```

### 延迟队列分配与严格优先级调度

1. **延迟初始化**: 订阅时 `queues[idx]` 是包含 4 个 `nil` 的指针 `*[4]chan *MessageRef`。仅当优先级 `P` 的消息到达时，`enqueueToSubscriber` 才在 `subMus[idx]` 保护下从 `r.queuePool` 获取 Channel（`0 allocs/op`）。
2. **严格优先级发送**: 专用 Writer goroutine 按 `0 -> 1 -> 2 -> 3` 严格顺序调用 `fetchNextMessage` 轮询队列。
3. **按优先级 TTL**: 入队时计算 `expiresAt = nowNano + priorityTTLs[P]`（如已配置）。出队时 `msgRef.IsExpired(nowNano)` 延迟丢弃过期消息。
4. **内存回收**: 出队时若 `len(q) == 0`，`cleanupEmptyQueue` 将 channel 归还到 `r.queuePool` 并复位为 `nil`。单优先级订阅者仅占用 1 个 channel。

---

## 3. TopicHash 单一真相源 (v1.16.0 修复)

发布者通过 `CmdPublish` (`[ttl:<ms>:]topic:<name>` 或 `[ttl:<ms>:]<name>`) 发送主题名；订阅者通过 `CmdSubscribe` (`topic:<name>[:group:<gid>][:durable:<id>:<offset>]`) 注册。之前发布与订阅路径独立计算主题名，导致：

- 发布 `topic:orders` 与订阅 `orders` 进入不同的 `topicIndex` 槽位。
- `OnPublish`/`OnDeliver` 指标标签在两种形式间漂移。
- 同一逻辑主题的 ACL 哈希在路径之间碰撞。

v1.16.0 引入两个规范化助手作为单一真相源：

```go
// parsePublishTopic 同时剥离 "ttl:<ms>:" 和可选的 "topic:" 前缀
func parsePublishTopic(payload []byte) (expiresAt int64, cleanTopic []byte)

// topicHashKey 是路由 SoA 表 (topicIndex, groups, durableOffsets) 唯一的 FNV-1a 入口
func topicHashKey(topic string) uint64 {
    return authz.CombineHashStrings("topic", topic)
}
```

`Subscribe`、`publishWithClientID`、`publishLocal`、`PublishBatch`、`runBackfillWorker` 全部通过这两个函数，订阅 `topic:<name>` 与发布 `topic:<name>` 现路由到同一槽位。回归测试在 `internal/broker/topic_extraction_test.go` 锁死。

---

## 4. 原子引用计数 (`MessageRef`)

为安全地将帧缓冲区回收至 `sync.Pool`，无数据竞争与内存损坏：

```go
type MessageRef struct {
    buf       *[]byte     // pooled buffer (parent only)
    frame     []byte      // 父缓冲区零拷贝子切片 (仅 child)
    parent    *MessageRef // 父级 (父级为 nil)
    ref       atomic.Int32
    expiresAt int64
    offset    uint64
}
```

- `Publish` 时从 `sync.Pool` 取一个缓冲区并包装为 `MessageRef` (`ref = 1`)。
- 每个目标订阅者队列调用 `Retain()` 递增 `ref`。
- Publisher 与 Writer 协程在派发或写网络后调用 `Release()`。
- 当 `ref.Add(-1) == 0` 时，缓冲区归还到 `protocol.ReleaseBuffer`，`MessageRef` 归还到 `msgRefPool`（**`0 allocs/op`**）。
- **嵌套引用计数 (v1.6.0)**: 批处理消息使用父子 `MessageRef` 层级。父级包装批处理缓冲区，`ref = 1 + 帧数`。每个子级通过 `AcquireChildMessageRef()` 创建，持有指向父缓冲区的 `frame []byte` 子切片。子级 `Release()` 到 0 时调用 `parent.Release()`。父级 `buf` 生命周期由 `protocol.ReleaseBuffer` 管理。

---

## 5. 零分配通配符主题匹配

MQTT 通配符匹配直接运行在字节切片上，无字符串转换：

- `+` 匹配 `/` 之间的单个主题层级。
- `#` 在末尾匹配零个或多个主题层级。
- `MatchWildcard(pattern, topic []byte)` 在 **50.41 ns/op**、**`0 allocs/op`** 完成匹配。

---

## 6. 安全架构 (mTLS, 非交换 ACL, 加密 AAL)

1. **mTLS 1.3**: 客户端证书通过受信 CA 池 (`client_ca_file`) 验证。客户端证书 Common Name (CN) 用于授权。
2. **非交换 FNV-1a ACL**: 按顺序组合 `clientID` 与 `topic` (`FNV1a(clientID + ":" + topic)`)，规避 XOR 交换绕过。
3. **AES-256-GCM AAL & Replay**: 日志记录使用 12 字节 Nonce (4 字节随机会话前缀 + 8 字节单调计数器) 和长度前缀。启动 Replay 在绑定 UDP 监听端口之前按顺序恢复状态。

---

## 7. 直接网格集群 (P2P Federation) 与 DNS 发现

Aqueduct 支持通过直接 P2P QUIC 网格 (ALPN `aqueduct-mesh`) 连接多个代理实例形成集群。无中央协调器或共识协议（无 Raft/Paxos）。转发采用 fire-and-forget 模式。

### PeerManager (RCU 模式, v1.14.0+)

`PeerManager` 使用 **Read-Copy-Update (RCU)** 模式实现热路径无锁读取：

```go
type PeerManager struct {
    peers   atomic.Pointer[peerSlice]   // 锁无关原子快照
    mu      sync.Mutex                  // 仅用于写路径
    addrSet map[string]context.CancelFunc
}
```

- **读取** (`Forward()`, `PeerCount()`, `ActivePeers()`): 加载原子指针 — 零锁、零竞争。
- **写入** (`AddPeer()`, `RemovePeer()`): 创建新切片并原子替换。写路径互斥锁仅保护 `addrSet` map。
- **构造器**: `NewWithLogger()` 初始化静态对等节点，预先构建完整 `peerSlice` 后再启动 reconnect goroutine (修复 v1.5.0 引入的 `append`/`range` 数据竞争)。

### 动态对等管理 (v1.14.0+)

- `AddPeer(ctx, addr)` — 拨号新对等节点，加入原子快照，启动 reconnect loop。
- `RemovePeer(addr)` — 取消 context，关闭流，从原子快照移除。
- `PeerCount()` — 当前对等节点数 (原子读取)。
- `ActivePeers()` — 当前已连接 (有活动 stream) 的对等节点数。

### MeshForwarded Bit

协议 Command 字节的**第 7 位 (掩码 `0x80`)** 标记帧已被转发。接收对等节点检查该位并跳过重新转发，避免多跳拓扑中的广播风暴。

### 零拷贝就地转发

`Forward()` 读取原子对等快照，然后**就地修改共享缓冲区的 MeshForwarded 位**（0 堆分配，0 allocs/op），并将修改后的帧直接写入每个对等节点的 QUIC 流：

```go
if addForwardedBit {
    orig := rawBuf[1]
    rawBuf[1] = orig | byte(protocol.MeshForwardedBit)
    _, werr := s.Write(rawBuf)
    rawBuf[1] = orig   // 立即恢复 — 调用者最终释放缓冲区到 sync.Pool
}
```

不使用临时堆分配副本 — 这避免了 `var combined [256]byte` 通过 `quic.Stream.Write` 逃逸到堆（v1.5.0 关键修复）。

### 路由器集成

当 `Router.Publish()` 处理本地消息时：

1. 通过 SoA fan-out 投递到本地订阅者。
2. 调用 `PeerManager.Forward()` 广播到所有对等节点（即便没有本地订阅者，`hasPeers` 检查也保证消息不会在集群中"丢失"）。
3. 接收节点调用 `Router.PublishFromPeer()`，仅本地分发（**绝不**重新转发 — `OpcodeOf(frame.Command)` 剥离 MeshForwarded 位后调用 `publishLocal`）。

### Mesh TLS 配置 (v1.16.0)

`cluster.mesh` 块控制对等 TLS 验证：

```go
type MeshConfig struct {
    InsecureSkipVerify bool   `yaml:"insecure_skip_verify"` // 默认 false
    CAFile             string `yaml:"ca_file"`              // PEM CA bundle
}
```

> [!WARNING]
> 生产部署必须保持 `insecure_skip_verify: false` 并提供 `ca_file`（或依赖系统 CA 池）。默认 `false` 修复了 v1.16.0 之前隐式 `InsecureSkipVerify: true` 的 MITM 漏洞。

### DNS 发现 (v1.14.0+)

Kubernetes StatefulSet 部署使用 `internal/cluster/discovery.go` 通过 `net.LookupHost` (标准库，0 二进制依赖) 轮询 Headless Service DNS 记录：

```go
type Discovery struct {
    manager  *PeerManager
    resolver Resolver   // 可注入接口用于测试
    hostname string
    port     int
    interval time.Duration
    knownIPs map[string]struct{}
    cancel   context.CancelFunc
}
```

- 每 `interval` (默认 10s) 调用 `net.LookupHost(hostname)`。
- 与 `knownIPs` map 计算差集 → 仅在变更时调用 `AddPeer()`/`RemovePeer()`。
- `normalize()` 去重 IP，过滤链路本地地址 (`169.254.x.x`)。

---

## 8. WebTransport Gateway (HTTP/3, v1.16.0)

浏览器无法打开带原生 `aqueduct-v1` ALPN 的原始 QUIC 流 — 它们只支持 HTTP/3 + W3C WebTransport API。网关在传输层转换，不触碰协议：

```
   ┌─────────────┐     HTTP/3      ┌──────────────────────┐   QUIC bidi    ┌───────────────────┐
   │ Browser     │ ─ WebTransport► │ internal/webtransport│ ─────────────► │ internal/transport│
   │ (W3C API)   │     streams     │ (this package)       │  raw *quic.Stream│ (broker)        │
   └─────────────┘                 └──────────────────────┘                └───────────────────┘
                                              │                                       │
                                              └─── 复用 broker.HandleStream ─────────┘
```

### 设计约束

- **一份 TLS 配置，两个监听器。** 网关在 broker 的 `*tls.Config` 上调用 `http3.ConfigureTLSConfig`，因此同一 mTLS 证书同时保护两个端口。`cloneTLSForH3` 仅向 `NextProtos` 添加 `h3`，不修改 broker 配置。
- **劫持握手流。** `responseWriter.HTTPStream()` 返回底层 `*http3.Stream`。我们发送 `200 OK` 完成 WebTransport Extended CONNECT 握手，然后循环 `conn.AcceptStream()` 将每个后续双向流送入 `broker.HandleStream(ctx, conn, s, clientID)` — 与原生 QUIC 会话调用同一函数。
- **零协议转换。** 浏览器将 `[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]` 写入 `WebTransportBidirectionalStream`；broker 现有帧解析器原样处理。跨传输路由（浏览器发布 → 原生 QUIC 订阅者，反之亦然）自动启用，因为两种传输都接入同一 `*broker.Router`。
- **同步握手超时。** `WithHandshakeTimeout(...)` (默认 10s) 防御 Slowloris 攻击；超时的握手会被 `stream.CancelRead(1)` + `conn.CloseWithError(ErrCodeRequestRejected, "wt handshake timeout")` 关闭。
- **0-RTT 默认启用。** 网关设置 `quic.Config{Allow0RTT: true, MaxIncomingStreams: 100, MaxIdleTimeout: 30s}`。拥有缓存 session ticket 的浏览器透明复用。

网关位于 `internal/webtransport/`。三个测试文件 (`server_test.go`, `stream_dispatch_test.go`, `server_bench_test.go`) 覆盖握手、多流会话、跨传输路由与基准测试延迟。**语句覆盖率 79.7%**。

**WebTransport 基准测试 (v1.16.0)**：

| 场景 | 结果 |
|------|------|
| `BenchmarkWTGateway_Handshake` | 10 次握手 3.3 ms/次 (含完整 mTLS + HTTP/3 SETTINGS + Extended CONNECT + 帧派发) |
| `BenchmarkWTGateway_PublishLatency` | 端到端 Publish → WT 订阅者 Read ≈ **1.25 ms/op** (loopback) |

---

## 9. 批处理协议与写合并

### 问题: OS PPS 限制

QUIC 流提供出色的流级隔离，但 `quic.Stream.Write()` 单次调用开销显著（系统调用边界、打包、加密）。逐帧发送将吞吐量限制在 ~300k RPS，与 CPU 速度无关。

### 解决方案: 智能批处理

Aqueduct v1.6.0 采用两种互补的批处理策略：

#### 9.1 协议级批处理 (`CmdPublishBatch`)

新命令 `0x04` 将多个标准帧编码为单个 QUIC 流写入中的扁平字节数组：

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

子帧通过 `unsafe.Slice` 与指针运算解析 — 每个子切片直接指向父批处理缓冲区（**零拷贝解包**）。所有 OOB 检查在任何 unsafe 操作之前完成。

#### 9.2 嵌套引用计数

批处理缓冲区到达路由器时：

1. 创建父 `MessageRef`，包装批处理缓冲区 (`ref = 1 + frameCount`)。
2. 通过 `AcquireChildMessageRef()` 为每个子帧创建子 `MessageRef` — 每个子级存储 `frame []byte` 子切片并递增父级 `ref`。
3. `Release()` 时：子 `ref` 到 0 调用 `parent.Release()`。父 `ref` 到 0 时，批处理缓冲区归还到 `sync.Pool`。
4. 全部为 `atomic.Int32` — 热路径零锁。

#### 9.3 写合并订阅者 Writer

`runSubscriberWriter` 协程 (每个订阅者一个) 累积出站帧，满足以下任一条件即批量刷新：

1. **大小阈值**: 累积 payload 超过 `batch_size` (默认 64 KB)。
2. **微定时器**: 首个累积帧后通过单个可复用 `time.Timer` 的 `Reset()` 在 `flush_interval` (默认 50 µs) 后触发 — 确保低负载下延迟有界。

可由 YAML 配置：

```yaml
broker:
  batch_size: 65536
  flush_interval: 50us
```

#### 9.4 基准测试

| 场景 | 吞吐量 | allocs/op |
|----------|-----------|-----------|
| **BatchUnpack** (1000 帧) | 19.9 GB/s | **0** |
| **BatchPublish** (100 消息) | 6.67M msg/s, 921 MB/s | **0** |
| 单帧 vs 批量 (每消息) | ~150 ns/msg (batch) vs ~920 ns/msg (single) | **0** |

---

## 10. NACK 重投与死信队列

Aqueduct v1.7.0 引入 NACK (Negative Acknowledgment) 以实现可靠投递：

### NACK 协议 (`CmdNack`)

- **操作码**: `0x06` (`CmdNack`)
- **Payload**: 8 字节 uint64 消息偏移量 (小端序)
- 收到后，broker 根据偏移量查找原始消息并调度重投。

### 自动重投

- 每条消息有内部重试计数器（存储于 `nackCounters[nackKey{topicHash, offset}]`）。
- 默认 `max_retries`: 3 (`AQUEDUCT_BROKER_MAX_RETRIES` 可覆盖)。
- 每次 NACK 后，消息重新入队到订阅者 channel。
- 超过 `max_retries` 后：消息路由到死信队列。

### 每订阅者帧缓存

- 每订阅者有界 FIFO 缓存 (256 条目)。
- 存储 offset → topic 映射，实现 O(1) 重投查找。
- 防止恶意快速 NACK 耗尽内存。

### 死信队列 (DLQ)

- 超过 `max_retries` 后，毒丸消息路由到 `__dlq__<原始主题>`。
- DLQ 订阅者对 `__dlq__` 主题模式使用标准订阅语义。

### 指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `aqueduct_messages_nacked_total` | Counter | NACK 消息总数 (按 topic) |
| `aqueduct_messages_dead_lettered_total` | Counter | 路由到 DLQ 的消息总数 (按 topic) |

### 无锁路径

- `NackByStream` 通过每订阅者缓冲通道路由 NACK — 热路径零锁。
- 每订阅者 channel 解耦 NACK 接收与重投处理。

---

## 11. Slab 分配器

Aqueduct v1.8.0 在热路径上使用高性能 slab 分配器替代 `sync.Pool` 管理 `*[]byte` 帧缓冲区：

### 设计

- **预分配 Arena**: 每个大小类连续 64 MB 内存区域。
- **大小类**: 128B、256B、512B、2KB、8KB、32KB。
- **无锁空闲链表**: Treiber 栈 (原子 CAS) 实现分配与释放。
- **零 GC 压力**: Arena 内存永不被 Go GC 扫描。

### 性能

| 指标 | 值 |
|------|-----|
| 分配延迟 | ~15 ns/op (无竞争) |
| 每次操作分配数 | 0 (预分配) |
| GC 影响 | 无 (arena 内存对 GC 不可见) |
| 有竞争分配 | ~55 ns/op, 0 allocs/op |

### 集成

- `slab.Acquire(size) → *[]byte` 替代 `pool.Get().(*[]byte)`。
- `slab.Deallocate(buf)` 替代 `pool.Put(buf)`。
- 超过 32 KB 的大小回退到堆分配。

---

## 12. 每租户速率限制 (Token Bucket)

Aqueduct v1.8.0 通过令牌桶算法实现无锁每租户速率限制：

### 设计

- **无锁令牌桶**: 原子操作完成令牌消费与补充。
- **后台补充**: 专用 goroutine 以 100 ms ticker 间隔补充所有桶。
- **每租户隔离**: 每个客户端拥有独立令牌桶。

### 性能

| 指标 | 值 |
|------|-----|
| 无竞争检查 | 2.1 ns/op |
| 每次操作分配数 | 0 |

### 配置

```yaml
broker:
  quotas:
    default_publish_rate: 1000  # 每秒消息数 (0 = 无限)
    default_burst_size: 100     # 突发容量
```

环境变量覆盖：

| 变量 | 默认 |
|------|------|
| `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` | 0 |
| `AQUEDUCT_BROKER_DEFAULT_BURST_SIZE` | 1000 |

### 集成

- 在 `Router.Publish()` 消息派发之前检查。
- 若被限速，消息静默丢弃并递增计数器。
- 指标：`aqueduct_messages_rate_limited_total` (counter, 按 `client` 标签)。

### 热重载 (v1.12.0+)

`AdminServer.SetClientQuota` 通过 `quotas.Manager.SetRate(clientID, rate, burst)` 调用更新每客户端桶。`Bucket.rate` 是 `atomic.Int64` — 零锁热路径重载。基准：`BenchmarkACLHotReload` (重建 10,000 条规则) **876 µs/op**。

---

## 13. 加密追加日志 (AAL)

Aqueduct v1.6.0+ 提供可选的 AES-256-GCM 加密持久化，所有发布帧按长度前缀写入磁盘：

```text
+-------------------+-------------------+-------------------+
| 4-Byte Record Len | 12-Byte Nonce     | Encrypted Payload |
| (Little-Endian)   | (Prefix + Counter)| (AEAD Seal)       |
+-------------------+-------------------+-------------------+
```

- Nonce：`[4 字节随机会话前缀 | 8 字节单调计数器]` — 12 字节 GCM Nonce，密码学唯一。
- 密钥必须恰好为 32 字节（Base64 编码或裸字节）。
- 启动时按顺序 Replay 重建状态 **早于** 绑定 UDP 监听端口。
- 当文件大小 ≥ `max_aal_size` 时调用 `Rotate(maxSize, key)`，原地压缩（重放 + 替换）。

---

## 14. OpenTelemetry Tracer (配置门控, v1.9.0+)

`internal/tracing/tracer.go` 是 OpenTelemetry 的 nil 安全包装器。当 `tracing.enabled: false` (默认)，所有操作都是内联 no-op：

- `StartSpan(...)` 返回原始 context 与零开销 `func() {}` 结束回调。
- 调用者可无条件 `defer endSpan()` 而无热路径分支。
- 空函数体由编译器优化掉。

启用时 (`tracing.enabled: true`)，通过 `otlptracegrpc` 导出器初始化批量 OTLP gRPC provider，使用 `endpoint` (默认 `localhost:4317`) 与 `service_name` (默认 `aqueduct-broker`)。

| 指标 | 值 |
|------|------|
| `BenchmarkTracerDisabled` | 3.4 ns/op, 0 allocs/op |
| Span 名 | `aqueduct.process` (本地发布), `aqueduct.forward` (mesh 转发) |
| 指标 | `aqueduct_tracing_spans_total` (counter) |

> [!NOTE]
> v1.16.0 修复：在 `cmd/broker/main.go` 中实际接入 tracer (`transport.WithTracer(...)`)。当前 tracer 可选启用，但**默认禁用**以保持零开销。