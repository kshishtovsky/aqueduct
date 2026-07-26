# Reference: Binary Protocol Specification (v1.7.0)
...
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
