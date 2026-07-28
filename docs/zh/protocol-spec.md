# 参考: 二进制协议规范 (v1.13.0)

本规范为 Aqueduct Zero-Copy 二进制网络协议的正式技术文档。

---

## 1. 帧网络格式 (Frame Wire Format)

Aqueduct 使用 10 字节扁平二进制标头，后跟可选 TLV 扩展块与有效载荷字节数组。所有整数均使用 **Little-Endian (小端)** 编码。

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic (0x1F)  | Command (1B)  |         Stream ID (4B)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Stream ID cntd|        Payload/Data Length (4B)               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Payload Length| [Ext Total Len (2B)] | [Type] [Len] [Value..] |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Payload Data (N Bytes) ...                                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 标头字段

| 偏移 | 字段 | 类型 | 说明 |
| :--- | :--- | :--- | :--- |
| `0` | `Magic` | `uint8` | 协议标识符。必须为 `0x1F` (Unit Separator)。 |
| `1` | `Command` | `uint8` | 命令操作码 + 控制位 (Bit 7: MeshForwarded, Bit 6: HasExtensions)。 |
| `2-5` | `StreamID` | `uint32` | QUIC 流标识符 (Little-Endian)。 |
| `6-9` | `PayloadLen`| `uint32` | 覆盖 ExtBlock (若 Bit 6 置位) 与 Payload 的数据长度 (Little-Endian)。 |
| `10..` | `ExtBlock` | `[]byte` | 可选 TLV 扩展块 (当 Command & `0x40` != 0 时存在)。 |
| `10+Ext..`| `Payload` | `[]byte` | 原始有效载荷内容。 |

---

## 2. 命令操作码与控制位

| 操作码 | 名称 | 说明 | 有效载荷格式 |
| :--- | :--- | :--- | :--- |
| `0x01` | `CmdPublish` | 向主题发布消息 | `[ttl:<ms>:]<topic_name>` 或消息体 |
| `0x02` | `CmdSubscribe` | 订阅主题或 Consumer Group | `topic:<name>[:group:<group_id>][:durable:<client_id>:<offset>]` |
| `0x03` | `CmdUnsubscribe`| 取消主题订阅 | `topic:<topic_name>` |
| `0x04` | `CmdPublishBatch` | 原子批量发布子帧 | 标准帧扁平数组 `[Magic][Cmd][StreamID][Len][Payload]...` |
| `0x05` | `CmdNack` | 按消息偏移量否认确认 | `[offset: 8]` — 8 字节 uint64 Little-Endian 消息偏移量 |

### Consumer Groups 订阅载荷格式 (`v1.13.0`)
竞争消费者通过在 `CmdSubscribe` 有效载荷中附带 `:group:<group_id>` 加入消费组：
- **普通消费组订阅**: `topic:orders:group:payment-workers`
- **持久化消费组订阅**: `topic:orders:group:payment-workers:durable:worker1:0`

每个 Consumer Group 内的消息通过 **Lock-Free Atomic Round-Robin** (`0 allocs/op`, `< 10 ns/op`) 在活跃 Worker 之间实现负载均衡。Group Durable Offset 会在收到 `CmdAck` 时由 Broker 自动同步更新。

### 控制位（第 6 位与第 7 位）

- **MeshForwarded 标志（第 7 位, `0x80`）**: 当帧从集群中的其他节点转发时设置，防止循环转发。
- **HasExtensions 标志（第 6 位, `0x40`）**: 当帧紧跟 10 字节首部后包含 TLV 扩展块时设置。

---

## 3. TLV 扩展块格式

当 `Command & 0x40 != 0` 时，载荷区域首先包含 2 字节 `ExtTotalLen`，后跟打包的 TLV 条目 `[Type: 1B][Length: 1B][Value: N Bytes]`：

| 扩展类型 | Type ID | Value 长度 | 说明 |
| :--- | :--- | :--- | :--- |
| `ExtTraceContext` | `0x01` | 25 字节 | OpenTelemetry 上下文 `[TraceID: 16B][SpanID: 8B][TraceFlags: 1B]` |
| `ExtCompression` | `0x02` | 5 字节 | ZSTD 压缩元数据 `[Algo: 1B][UncompressedSize: 4B]` |
| `ExtPriority` | `0x03` | 1 字节 | QoS 消息优先级 (`0` 最高, `1` 高, `2` 普通, `3` 低) |

---

## 4. 消息 TTL 与 QoS 优先级

- **优先级 TLV 扩展 (`ExtPriority = 0x03`)**: 传输 1 字节优先级。Writer 协程按严格顺序 (`0 -> 1 -> 2 -> 3`) 轮询。
- **按优先级 TTL (Per-Priority TTL)**: 可配置 `priority_ttls` (`["500ms", "5s", "0", "0"]`)，强制重写过期时间，出队时自动延迟丢弃过期消息。
- **内联 TTL 格式**: `ttl:<毫秒>:<消息数据>` (回退格式)。

---

## 5. AAL 日志记录线缆格式

```text
+-------------------+-------------------+-------------------+
| 4 字节记录长度    | 12 字节 Nonce     | 加密帧载荷        |
| (Little-Endian)   | (会话前缀+计数器) | (AEAD Seal)       |
+-------------------+-------------------+-------------------+
```

---

## 6. WebTransport 传输绑定 (v1.16.0+)

浏览器客户端通过 W3C [WebTransport API](https://www.w3.org/TR/webtransport/) 连接，它在 QUIC 之上封装 HTTP/3。上面的帧格式与原生 QUIC 传输**完全相同**——WebTransport 网关 (`internal/webtransport/`) 仅翻译 HTTP/3 连接层。

### 6.1 建立连接

浏览器 JS:
```js
const transport = new WebTransport("https://broker.example.com:4433/aqueduct/wt");
await transport.ready;
```

服务端: `webtransport.Gateway.handleConn` 调用 `http3.NewRawServerConn` 完成 HTTP/3 SETTINGS 交换，然后解析入站 Extended CONNECT 请求并校验：

- `:method = CONNECT`
- `:scheme = https`
- `:authority = broker.example.com:4433`
- `:path = /aqueduct/wt`（通过 `webtransport.path_prefix` 配置）
- `:protocol = webtransport`

200 OK 完成握手。同一 QUIC 连接上的后续双向流承载 Broker 的二进制帧。

### 6.2 流映射

| WebTransport 流类型 | Broker 对应物 |
| :------------------------ | :------------------ |
| Server-initiated uni      | (HTTP/3 control 预留，网关忽略) |
| Client-initiated uni      | (丢弃——按 RFC 9298 未知类型) |
| Client-initiated bidi     | `*quic.Stream` 来自 `conn.AcceptStream()` |
| Server-initiated bidi     | (handshake 请求流本身，为 capsule 协议保持打开) |

握手之后，每个客户端打开的 bidi 流都送入 `transport.Broker.HandleStream(ctx, conn, stream, clientID)`——与原生 QUIC 传输使用同一方法。

### 6.3 0-RTT 与 TLS

HTTP/3 之下的 QUIC 在双方均设置 `quic.Config.Allow0RTT = true` 时协商 0-RTT。网关默认启用。浏览器在首次连接时保存 session ticket 并在后续连接中复用，节省一个完整的 RTT。

Broker 在 WebTransport 监听器上强制 TLS 1.3（`MinVersion = tls.VersionTLS13`），即便运维证书禁用了它——老版本 TLS 被静默拒绝。

### 6.4 浏览器帧格式

写入 WebTransport 双向流的浏览器产生与原生客户端**完全相同的二进制布局**：

```
[Magic: 1 字节 = 0x1F][Cmd: 1 字节][StreamID: 4 字节 (little-endian)][DataLen: 4 字节 (little-endian)][Payload: N 字节]
```

常量相同（见 §1, §2）。TLV 扩展编码相同（见 §3）。`examples/web/app.js` 参考客户端实现了匹配此布局的 `buildFrame()` / `parseFrame()`。
