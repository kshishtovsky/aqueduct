# Reference: Binary Protocol Specification (v1.7.0)

This document provides the formal technical specification for Aqueduct's zero-copy binary wire protocol.

---

## 1. Frame Wire Format

Aqueduct uses a flat 10-byte binary header followed by an arbitrary payload byte array. All integer fields are encoded in **Little-Endian** byte order.

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic (0x1F)  | Command (1B)  |         Stream ID (4B)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Stream ID cntd|        Payload Length (4B)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Payload Length| Payload Data (N Bytes) ...                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Header Fields

| Offset | Field | Type | Description |
| :--- | :--- | :--- | :--- |
| `0` | `Magic` | `uint8` | Protocol identifier. Must be `0x1F` (unit separator). |
| `1` | `Command` | `uint8` | Command opcode (see Command Table below). |
| `2-5` | `StreamID` | `uint32` | QUIC stream identifier (Little-Endian). |
| `6-9` | `PayloadLen`| `uint32` | Length $N$ of `Payload Data` in bytes (Little-Endian). |
| `10..` | `Payload` | `[]byte` | Raw payload payload content. |

---

## 2. Command Opcodes

| Opcode | Name | Description | Payload Format |
| :--- | :--- | :--- | :--- |
| `0x01` | `CmdPublish` | Publish message to topic | `[ttl:<ms>:]<topic_name>` or raw message |
| `0x02` | `CmdSubscribe` | Subscribe QUIC stream to topic | `topic:<topic_name>` (supports `+` and `#` wildcards) |
| `0x03` | `CmdUnsubscribe`| Unsubscribe stream from topic | `topic:<topic_name>` |
| `0x04` | `CmdPublishBatch` | Publish batch of sub-frames atomically | Flat array of standard frames `[Magic][Cmd][StreamID][Len][Payload]...` |
| `0x05` | `CmdNack` | Negative acknowledgment by message offset | `[offset: 8]` — 8-byte little-endian uint64 message offset |

### MeshForwarded Bit (Bit 7)

The Command byte's most significant bit (bit 7, value `0x80`) is reserved as the **MeshForwarded** flag for cluster forwarding:

```
Bit 7 set (Command & 0x80)  == Frame was forwarded from another broker node
Bit 7 clear                 == Frame originated locally or is being forwarded for the first time
```

When a broker receives a frame with the MeshForwarded bit set, it dispatches the frame only to local subscribers and does **not** re-forward it to its own peers. This prevents infinite forwarding loops in mesh topologies.

---

## 3. Message TTL Payload Format

Messages can specify an optional Time-To-Live (TTL) header in the payload byte array:

```text
ttl:<milliseconds>:<payload_data>
```

Example:
- `ttl:500:sensor/room1/temp` (Expires 500ms after publish if queued).

---

## 4. AAL Log Record Binary Framing

When stored in Append-Only Log files (encrypted or raw), records are framed as:

```text
+-------------------+-------------------+-------------------+
| 4-Byte Record Len | 12-Byte Nonce     | Encrypted Payload |
| (Little-Endian)   | (Session+Counter) | (Header+Payload)  |
+-------------------+-------------------+-------------------+
```
