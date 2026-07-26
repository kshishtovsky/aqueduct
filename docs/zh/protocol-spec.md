# 参考: 二进制协议规范 (v1.11.0)

本文档提供 Aqueduct 零拷贝二进制线缆协议的技术规范。

---

## 1. 帧线缆格式

协议使用 10 字节二进制首部，后跟可选 TLV 扩展块和载荷，采用 **Little-Endian** 字节序:

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

### 首部字段

| 偏移 | 字段 | 类型 | 说明 |
| :--- | :--- | :--- | :--- |
| `0` | `Magic` | `uint8` | 协议魔数 `0x1F` (unit separator)。 |
| `1` | `Command` | `uint8` | 命令操作码 + 控制位 (第 7 位: MeshForwarded, 第 6 位: HasExtensions)。 |
| `2-5` | `StreamID` | `uint32` | QUIC 流 ID (Little-Endian)。 |
| `6-9` | `PayloadLen`| `uint32` | 数据长度 (覆盖 ExtBlock 和 Payload)。 |
| `10..` | `ExtBlock` | `[]byte` | 可选 TLV 扩展块 (当 Command & `0x40` != 0 时存在)。 |
| `10+Ext..`| `Payload` | `[]byte` | 载荷数据。 |

---

## 2. 命令操作码与控制位

| 操作码 | 名称 | 说明 | Payload 格式 |
| :--- | :--- | :--- | :--- |
| `0x01` | `CmdPublish` | 发布消息到主题 | `[ttl:<ms>:]<topic_name>` 或数据 |
| `0x02` | `CmdSubscribe` | 订阅主题 | `topic:<topic_name>` (支持 `+` 与 `#`) |
| `0x03` | `CmdUnsubscribe`| 取消订阅 | `topic:<topic_name>` |
| `0x04` | `CmdPublishBatch` | 批量发布子帧 | 标准帧的扁平数组 `[Magic][Cmd][StreamID][Len][Payload]...` |
| `0x05` | `CmdNack` | 按消息偏移进行否定确认 | `[offset: 8]` — 8 字节小端 uint64 消息偏移 |

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
