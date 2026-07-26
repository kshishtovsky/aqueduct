# 解释: 架构与内存模型 (v1.3.0)

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
