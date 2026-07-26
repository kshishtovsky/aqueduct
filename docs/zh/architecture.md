# 解释: 架构与内存模型 (v1.5.0)

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
