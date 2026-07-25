# 解析: 架构与内存模型

本文档说明 Aqueduct 的架构设计原则、面向数据设计（Data-Oriented Design, DoD）以及零内存分配策略。

## 1. 为什么选择 QUIC 而不是 TCP？

传统 TCP 消息代理在单个连接上复用多个流时容易面临队头阻塞（Head-of-Line Blocking, HoL）问题。单个主题的丢包会导致所有独立主题的投递停滞。

Aqueduct 采用 **QUIC** (`quic-go`) 架构，提供：
- **流级别隔离**: 单个流丢包不会影响其他流上的主题。
- **0-RTT 快速恢复**: 重连客户端免去 TLS 握手延迟。
- **UDP 传输**: 降低内核延迟，避免 TCP 连接建立瓶颈。

## 2. 数组结构 (SoA) 路由器设计

普通 Go 实现通常使用指针 Map 存储订阅者: `map[string][]*Subscriber`。这种模式因指针追踪及堆分散导致 CPU L1/L2 缓存命中率显著下降。

Aqueduct 采用 **数组结构 (Structure of Arrays, SoA)** 设计：

```go
type Router struct {
    mu sync.RWMutex

    // SoA 扁平并行切片
    streamIDs []uint32       // 流 ID
    streams   []*quic.Stream // QUIC 流指针
    topics    []string       // 主题名称
    active    []bool         // 订阅者状态标识

    topicIndex map[string][]int
}
```

### 缓存局部性优势

在批量发布消息时，代理连续遍历平铺的内存切片 (`streams[idx]`)，契合 CPU 预取器机制，大幅提升 L1/L2 缓存命中率并规避 GC 遍历开销。

## 3. 热路径零分配策略

1. **池化缓冲区**: 每个流读取器从 `sync.Pool` 获取固定容量字节切片。
2. **指针运算解析**: 帧头直接从字节切片解析，无需创建结构体指针或字符串拷贝。
3. **同步 AAL 落盘**: 开启 AAL 时，发布的帧直接从网络缓冲区切片通过 `os.File.Write` 系统调用落盘至 OS 页缓存，保持 `0 allocs/op`。
