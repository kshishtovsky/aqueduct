# Reference: Binary Protocol Specification (v1.0.0)

This document provides a technical specification of the binary frame layout, command codes, and byte ordering used by Aqueduct.

## Binary Frame Structure

Every frame sent over a QUIC stream consists of a fixed 10-byte binary header followed by a variable-length payload.

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic (0xAQ)  | Command (0x..) |          Stream ID            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          Stream ID (cont.)     |        Payload Length        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Payload Length (cont.)     | Payload Bytes (N bytes) ...  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

## Header Fields

| Field | Size (Bytes) | Offset | Type | Description |
| :--- | :--- | :--- | :--- | :--- |
| `Magic` | 1 | 0 | `uint8` | Fixed magic identifier `0xAQ` (`'A'` ASCII byte = 0x41) |
| `Command` | 1 | 1 | `uint8` | Command code identifier |
| `StreamID` | 4 | 2 | `uint32` (Big Endian) | QUIC stream identifier |
| `PayloadLen` | 4 | 6 | `uint32` (Big Endian) | Length of following payload bytes ($N$) |

Total Header Size: **10 Bytes** (`protocol.HeaderSize`).

---

## Command Codes

| Code | Constant | Description | Payload Format |
| :--- | :--- | :--- | :--- |
| `0x01` | `CmdPublish` | Publish message to topic | `topic` name bytes |
| `0x02` | `CmdSubscribe` | Subscribe stream to topic | `"topic:<name>"` string bytes |
| `0x03` | `CmdAck` | Acknowledgment response | Optional response payload |

---

## Limits & Validation Rules

- **Magic Byte Validation**: If `header[0] != 0x41`, the parser returns `ErrInvalidMagicByte`.
- **Maximum Payload Limit**: Default `maxPayloadSize` per message is 1 MB (`1 << 20` bytes).
- **Buffer Hardening**: If `PayloadLen > maxBufSize` (default 64 KB), the stream is immediately canceled by the broker with a `WARN` log.
