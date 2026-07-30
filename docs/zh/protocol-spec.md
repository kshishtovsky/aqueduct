# 参考: 二进制协议规范 (v1.16.0)

本文档为 Aqueduct 零拷贝二进制线缆协议提供正式技术规范。所有数值字段使用 **Little-Endian** 字节序。

> [!NOTE]
> Diátaxis 分类：**Reference** — 描述 *是什么*，含完整字段表与操作码。

---

## 1. 帧线缆格式 (Frame Wire Format)

Aqueduct 使用 10 字节扁平二进制首部，后接可选 TLV 扩展块与有效载荷字节数组。

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic (0x1F)  | Command (1B)  |         Stream ID (4B)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| StreamID cntd |        DataLen (Payload + ExtBlock, 4B)       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| [ExtTotalLen (2B)] [Type] [Len] [Value..]   (only if Cmd & 0x40)|
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Payload (N Bytes) ...                                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 首部字段

| 偏移 | 字段 | 类型 | 说明 |
| :--- | :--- | :--- | :--- |
| `0` | `Magic` | `uint8` | 协议标识符。必须为 `0x1F` (Unit Separator)。 |
| `1` | `Command` | `uint8` | 操作码 + 控制位 (Bit 7: MeshForwarded = `0x80`, Bit 6: HasExtensions = `0x40`)。 |
| `2-5` | `StreamID` | `uint32` | QUIC 流标识符 (Little-Endian)。 |
| `6-9` | `DataLen` | `uint32` | 数据长度，覆盖 ExtBlock (若 Bit 6 置位) + Payload (Little-Endian)。 |
| `10..` | `ExtBlock` | `[]byte` | 可选 TLV 扩展块 (当 `Command & 0x40 != 0` 时存在)。格式 `[ExtTotalLen: 2B][TLV entries...]`。 |
| `10+Ext..` | `Payload` | `[]byte` | 原始有效载荷内容。 |

> [!NOTE]
> `DataLen` 字段包含 TLV 扩展块与 Payload 的总字节数。仅含 Payload 帧时 `DataLen == PayloadLen`；带扩展的帧时 `DataLen == ExtBlockSize + PayloadLen`。`protocol.Frame.DataLen()` 返回该值。

---

## 2. 命令操作码与控制位

| 操作码 | 名称 | 说明 | Payload 格式 |
| :--- | :--- | :--- | :--- |
| `0x01` | `CmdPublish` | 向主题发布消息 | `[ttl:<ms>:][topic:]<topic_name>` 或裸消息；可选 TLV `ExtPriority` (QoS 级别) |
| `0x02` | `CmdSubscribe` | 订阅主题或 Consumer Group | `topic:<name>[:group:<group_id>][:durable:<consumer_id>:<offset>]` |
| `0x03` | `CmdUnsubscribe` | 取消主题订阅 | `topic:<topic_name>` |
| `0x04` | `CmdAck` | 确认消息偏移量 | `[offset: 8]` — 8 字节 Little-Endian uint64 消息偏移量 |
| `0x05` | `CmdPublishBatch` | 原子批量发布子帧 | 标准帧扁平数组 `[Magic][Cmd][StreamID][Len][Payload]...` |
| `0x06` | `CmdNack` | 按消息偏移量 NACK | `[offset: 8]` — 8 字节 Little-Endian uint64 消息偏移量 |

### Consumer Groups 订阅载荷格式 (v1.13.0+)

竞争消费者通过 `CmdSubscribe` Payload 中包含 `:group:<group_id>` 加入消费组：

- **标准消费组订阅**: `topic:orders:group:payment-workers`
- **持久化消费组订阅**: `topic:orders:group:payment-workers:durable:worker1:0`

每个 Consumer Group 内的消息通过 **Lock-Free Atomic Round-Robin** (`0 allocs/op`, `< 10 ns/op`) 在活跃 Worker 之间负载均衡。Group Durable Offset 持久化并在 Worker 故障转移时恢复。

### 控制位 (Bit 6 与 Bit 7)

- **MeshForwarded 标志 (Bit 7, `0x80`)**: 当帧由另一个集群节点转发时设置。接收方 `OpcodeOf(cmd)` 必须同时剥离 `0x80` 与 `0x40`，再判断操作码。**接收方不得重新转发**带该位的帧。
- **HasExtensions 标志 (Bit 6, `0x40`)**: 当帧在 10 字节首部后携带 TLV 扩展块时设置。

`OpcodeOf` 同时掩码两个控制位：

```go
func OpcodeOf(cmd Command) Command {
    return cmd & ^MeshForwardedBit & ^HasExtensionsBit
}
```

### `CmdPublish` 主题名解析 (v1.16.0)

发布 Payload 可携带两个可选前缀，由 `parsePublishTopic` 单一函数剥离：

1. `ttl:<milliseconds>:` — 内联 TTL (毫秒)。解析为 unix 纳秒过期时间戳。
2. `topic:` — 与 `CmdSubscribe` 载荷兼容性保留的可选路由前缀。

发布 `topic:orders` 与发布 `orders` **路由到同一订阅者槽位**。两者最终都通过 `topicHashKey("orders")` = FNV-1a 哈希进入同一 `topicIndex` 槽。

---

## 3. TLV 扩展块格式

当 `Command & 0x40 != 0` 时，Payload 区域首先包含 2 字节 `ExtTotalLen`，后接打包的 TLV 条目 `[Type: 1B][Length: 1B][Value: N Bytes]`：

```text
+--------+--------+--------+--------+
| ExtTL  |ExtTL+1 |ExtTL+2 |ExtTL+3 |  ...
| Type   | Length |  Value ...     |
+--------+--------+--------+--------+
```

| 扩展类型 | Type ID | Value 长度 | 说明 |
| :--- | :--- | :--- | :--- |
| `ExtTraceContext` | `0x01` | 25 字节 | OpenTelemetry W3C Trace Context `[TraceID: 16B][SpanID: 8B][TraceFlags: 1B]` |
| `ExtCompression` | `0x02` | 5 字节 | ZSTD 压缩元数据 `[Algo: 1B][UncompressedSize: 4B Little-Endian]`；`Algo=1` 为 ZSTD |
| `ExtPriority` | `0x03` | 1 字节 | QoS 消息优先级 (`0` 最高, `1` 高, `2` 普通, `3` 低) |
| `ExtRetryOffset` | `0xF0` | 8 字节 | NACK 重试偏移量 (内部使用)，`[OriginalOffset: 8B]` |

> [!WARNING]
> `MaxExtTotalLen = 1024` 字节硬上限防止 TLV DoS 放大。超过该上限的帧立即拒绝。

未知 TLV 类型**静默跳过**（不返回错误），保证向前兼容。

---

## 4. 消息 TTL 与 QoS 优先级

消息可携带 TTL 与 Priority：

- **优先级 TLV (`ExtPriority = 0x03`)**: 携带 1 字节优先级 (`0` 最高 到 `3` 低)。Writer 协程按严格顺序 (`0 -> 1 -> 2 -> 3`) 出队。
- **Per-Priority TTL**: 可配置 `priority_ttls` (`["500ms", "5s", "0", "0"]`)，强制覆盖发布者 TTL。出队时延迟丢弃过期消息 (`aqueduct_messages_expired_total{topic, priority}`)。
- **内联 TTL Payload 前缀**: `ttl:<milliseconds>:<payload_data>` (TLV 不可用时的回退格式)。

---

## 5. AAL 日志记录二进制格式

当持久化到追加日志文件 (加密或裸格式) 时，记录格式为：

```text
+-------------------+-------------------+-------------------+
| 4-Byte Record Len | 12-Byte Nonce     | Encrypted Payload |
| (Little-Endian)   | (Session + Cntr)  | (Header + Payload)|
+-------------------+-------------------+-------------------+
```

- **加密模式**: 12 字节 Nonce = `[4 字节随机会话前缀 | 8 字节单调计数器]`；密文 = `AES-256-GCM-Seal(nonce, plaintext)`。
- **裸模式**: 仅 4 字节长度前缀 + 原始 frameBytes。
- 记录长度字段上限 = 10 MB (`decodeReplayChunk` 验证)；损坏记录按 1 字节步进 best-effort resync。

---

## 6. WebTransport 传输绑定 (v1.16.0+)

浏览器客户端通过 W3C [WebTransport API](https://www.w3.org/TR/webtransport/) 连接，它在 QUIC 之上封装 HTTP/3 (ALPN `h3`)。上述帧格式与原生 QUIC 传输**完全相同** — WebTransport 网关 (`internal/webtransport/`) 仅翻译 HTTP/3 连接层。

### 6.1 连接建立

浏览器 JS:
```js
const transport = new WebTransport("https://broker.example.com:4433/aqueduct/wt");
await transport.ready;
```

服务端: `webtransport.Gateway.handleConn` 调用 `http3.NewRawServerConn` 完成 HTTP/3 SETTINGS 交换，然后解析入站 Extended CONNECT 请求并校验：

- `:method = CONNECT`
- `:scheme = https`
- `:authority = broker.example.com:4433`
- `:path = /aqueduct/wt` (通过 `webtransport.path_prefix` 配置，默认 `/aqueduct/wt`)
- `:protocol = webtransport`

200 OK 完成握手。同一 QUIC 连接上的后续双向流承载 Broker 的二进制帧。

### 6.2 流映射

| WebTransport 流类型 | Broker 对应物 |
| :------------------------ | :------------------ |
| Server-initiated uni      | (HTTP/3 control 预留；网关忽略) |
| Client-initiated uni      | (丢弃 — 按 RFC 9298 未知类型) |
| Client-initiated bidi     | `*quic.Stream` 来自 `conn.AcceptStream()` |
| Server-initiated bidi     | (handshake 请求流本身，为 capsule 协议保持打开) |

握手之后，每个客户端打开的 bidi 流都送入 `transport.Broker.HandleStream(ctx, conn, stream, clientID)` — 与原生 QUIC 传输使用同一方法。

### 6.3 0-RTT 与 TLS

HTTP/3 之下的 QUIC 在双方均设置 `quic.Config.Allow0RTT = true` 时协商 0-RTT。网关默认启用 (broker QUIC 监听器同样启用)。浏览器在首次连接时保存 session ticket 并在后续连接中复用，节省一个完整 RTT。

Broker 在 WebTransport 监听器上强制 TLS 1.3 (`MinVersion = tls.VersionTLS13`，由 `cloneTLSForH3` 应用)，即便运维证书禁用了它 — 老版本 TLS 被静默拒绝。

### 6.4 浏览器帧格式

写入 WebTransport 双向流的浏览器产生与原生客户端**完全相同的二进制布局**：

```
[Magic: 1 字节 = 0x1F][Cmd: 1 字节][StreamID: 4 字节 (little-endian)][DataLen: 4 字节 (little-endian)][Payload: N 字节]
```

常量相同（见 §1, §2）。TLV 扩展编码相同（见 §3）。`examples/web/app.js` 参考客户端实现了匹配此布局的 `buildFrame()` / `parseFrame()`。

---

## 7. 压缩格式 (v1.10.0+)

批量 (`CmdPublishBatch`) Payload 可通过 ZSTD (`internal/compress`) 压缩，仅当 `compression.enabled: true` 且批次 ≥ `compression.min_batch_size` (默认 1024 字节) 时启用。压缩元数据通过 `ExtCompression` TLV (Type `0x02`) 编码：

```text
[Algo: 1 字节 = 0x01 (ZSTD)][UncompressedSize: 4 字节 Little-Endian]
```

接收端 (`internal/transport`) 在解压后**剥除** `ExtCompression` TLV。订阅者始终接收**未压缩** Payload — 零协议破坏。

---

## 8. ALPN 标识符

| 端点 | ALPN |
|------|------|
| 原生 QUIC 客户端 (`internal/transport`) | `aqueduct-v1` |
| 集群网格对等 (`cluster.PeerManager`) | `aqueduct-mesh` |
| 浏览器 WebTransport (`internal/webtransport`) | `h3` |

`cmd/broker/main.go` 配置：

```go
tlsConf = &tls.Config{
    Certificates: []tls.Certificate{cert},
    NextProtos:   []string{"aqueduct-v1"},
    MinVersion:   tls.VersionTLS13,
}
```

WebTransport 网关克隆该 `*tls.Config` 并附加 `"h3"` (不改写 broker 的配置) — 同一证书同时保护原生与浏览器客户端。