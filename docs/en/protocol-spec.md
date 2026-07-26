# Reference: Binary Protocol Specification (v1.13.0)

This document provides the formal technical specification for Aqueduct's zero-copy binary wire protocol.

---

## 1. Frame Wire Format

Aqueduct uses a flat 10-byte binary header followed by an optional TLV Extension block and the payload byte array. All integer fields are encoded in **Little-Endian** byte order.

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

### Header Fields

| Offset | Field | Type | Description |
| :--- | :--- | :--- | :--- |
| `0` | `Magic` | `uint8` | Protocol identifier. Must be `0x1F` (unit separator). |
| `1` | `Command` | `uint8` | Command opcode + control bits (Bit 7: MeshForwarded, Bit 6: HasExtensions). |
| `2-5` | `StreamID` | `uint32` | QUIC stream identifier (Little-Endian). |
| `6-9` | `PayloadLen`| `uint32` | Data length covering ExtBlock (if Bit 6 set) + Payload (Little-Endian). |
| `10..` | `ExtBlock` | `[]byte` | Optional TLV Extension block (present if Command & `0x40` != 0). |
| `10+Ext..`| `Payload` | `[]byte` | Raw payload content. |

---

## 2. Command Opcodes & Control Bits

| Opcode | Name | Description | Payload Format |
| :--- | :--- | :--- | :--- |
| `0x01` | `CmdPublish` | Publish message to topic | `[ttl:<ms>:]<topic_name>` or raw message |
| `0x02` | `CmdSubscribe` | Subscribe QUIC stream to topic or Consumer Group | `topic:<name>[:group:<group_id>][:durable:<client_id>:<offset>]` |
| `0x03` | `CmdUnsubscribe`| Unsubscribe stream from topic | `topic:<topic_name>` |
| `0x04` | `CmdPublishBatch` | Publish batch of sub-frames atomically | Flat array of standard frames `[Magic][Cmd][StreamID][Len][Payload]...` |
| `0x05` | `CmdNack` | Negative acknowledgment by message offset | `[offset: 8]` — 8-byte little-endian uint64 message offset |

### Consumer Groups Payload Framing (`v1.13.0`)
Competing consumers join a named Consumer Group by supplying `:group:<group_id>` in the `CmdSubscribe` payload:
- **Standard Group Subscription**: `topic:orders:group:payment-workers`
- **Durable Group Subscription**: `topic:orders:group:payment-workers:durable:worker1:0`

Within each Consumer Group registered on a topic, messages are load-balanced across active group members via **Lock-Free Atomic Round-Robin** (`0 allocs/op`, `< 10 ns/op`). Group Durable Offsets are updated automatically on consumer ACKs.

### Control Bits (Bits 6 & 7)

- **MeshForwarded Flag (Bit 7, `0x80`)**: Set when a frame is forwarded from another cluster broker node. Prevents re-forwarding loops in mesh topology.
- **HasExtensions Flag (Bit 6, `0x40`)**: Set when the frame contains a TLV Extension block immediately following the 10-byte header.

---

## 3. TLV Extension Block Format

When `Command & 0x40 != 0`, the payload region begins with a 2-byte `ExtTotalLen` followed by packed TLV entries `[Type: 1B][Length: 1B][Value: N Bytes]`:

| Extension Type | Type ID | Value Length | Description |
| :--- | :--- | :--- | :--- |
| `ExtTraceContext` | `0x01` | 25 Bytes | OpenTelemetry Trace Context `[TraceID: 16B][SpanID: 8B][TraceFlags: 1B]` |
| `ExtCompression` | `0x02` | 5 Bytes | Payload Compression Metadata `[Algo: 1B][UncompressedSize: 4B]` |
| `ExtPriority` | `0x03` | 1 Byte | QoS Message Priority Level (`0` Highest, `1` High, `2` Normal, `3` Low) |

---

## 4. Message TTL & Priority QoS

Messages can specify TTL and Priority:
- **Priority Flag TLV (`ExtPriority = 0x03`)**: Carries 1-byte priority (`0` Highest to `3` Low). Writer goroutines dequeue queues in strict priority order `0 -> 1 -> 2 -> 3`.
- **Per-Priority TTL**: Configurable `priority_ttls` (`["500ms", "5s", "0", "0"]`) automatically enforces expiration timestamps, lazily dropping expired messages upon dequeueing.
- **Inline TTL Payload Header**: `ttl:<milliseconds>:<payload_data>` (Fallback format if TLV is absent).

---

## 5. AAL Log Record Binary Framing

When stored in Append-Only Log files (encrypted or raw), records are framed as:

```text
+-------------------+-------------------+-------------------+
| 4-Byte Record Len | 12-Byte Nonce     | Encrypted Payload |
| (Little-Endian)   | (Session+Counter) | (Header+Payload)  |
+-------------------+-------------------+-------------------+
```
