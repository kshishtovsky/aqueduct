# 解释: 架构与内存模型 (v1.6.0)

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
    buf       *[]byte
    ref       atomic.Int32
    expiresAt int64 // unix nano
}
```

- `AcquireMessageRef` 初始化 `ref = 1`。
- 每个目标订阅者通过 `Retain()` 增加引用。
- 发送完成后调用 `Release()`。
- 当 `ref.Add(-1) == 0` 时，缓冲区安全归还至 `sync.Pool` (**`0 allocs/op`**)。

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
