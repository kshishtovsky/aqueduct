# Reference: Binary Protocol Specification (v1.16.0)

This document is the formal technical specification of Aqueduct's zero-copy binary wire protocol. It is **information-oriented**: it describes the exact byte layout, opcodes, TLV extensions, and transport bindings. For background and design rationale see [Architecture & Memory Model](architecture.md); for end-to-end usage see [Getting Started](getting-started.md).

---

## 1. Frame Wire Format

Aqueduct uses a flat 10-byte binary header followed by an optional TLV extension block and the payload byte array. All integer fields are encoded in **Little-Endian** byte order.

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic (0x1F)  | Command (1B)  |         Stream ID (4B)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Stream ID cntd|       DataLen (4B)                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| [ExtTotalLen: 2B | [Type:1][Len:1][Value..] ... | Payload:N ] |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

`DataLen` covers everything from byte offset 10 to the end of the frame. When the Command byte has the `HasExtensions` bit (`0x40`) set, `DataLen = ExtBlockSize + PayloadLen`. When the bit is clear, `DataLen = PayloadLen`. Old parsers that mask only `MeshForwardedBit` (`0x80`) still skip the frame as an unknown opcode and advance exactly `DataLen` bytes, preserving wire alignment.

### Header Fields

| Offset | Field | Type | Description |
| :--- | :--- | :--- | :--- |
| `0` | `Magic` | `uint8` | Protocol identifier. Must be `0x1F` (unit separator). |
| `1` | `Command` | `uint8` | Opcode (`bits 0..5`) + control bits: `bit 7` MeshForwarded (`0x80`), `bit 6` HasExtensions (`0x40`). |
| `2..5` | `StreamID` | `uint32` | QUIC stream identifier, little-endian. |
| `6..9` | `DataLen` | `uint32` | Total byte count of the data segment (ExtBlock + Payload if extensions present, else Payload), little-endian. |
| `10..10+ExtBlockSize-1` | `ExtBlock` | `[]byte` | Optional TLV block. Present when `(Command & 0x40) != 0`. |
| `10+ExtBlockSize..10+DataLen-1` | `Payload` | `[]byte` | Raw payload bytes. |

Constants (from `internal/protocol/frame.go` and `internal/protocol/extensions.go`):

| Name | Value | Source |
| :--- | :--- | :--- |
| `MagicByte` | `0x1F` | `MagicByte = 0x1F` |
| `HeaderSize` | `10` | `HeaderSize = 10` |
| `MeshForwardedBit` | `0x80` | `MeshForwardedBit Command = 0x80` |
| `HasExtensionsBit` | `0x40` | `HasExtensionsBit Command = 0x40` |
| `MaxExtTotalLen` | `1024` | `MaxExtTotalLen = 1024` (`extensions.go`) |

> **Note.** There is **no** `MaxFrameSize` constant in the codebase. The effective per-frame payload cap is the **transport-layer** `transport.max_buf_size` (default `65536` bytes / `64 KB`, configurable via `AQUEDUCT_TRANSPORT_MAX_BUF_SIZE` or `transport.max_buf_size` in YAML). Payloads exceeding this are rejected by `prepareFrame` with `errOversizedPayload`. The TLV region is bounded by `MaxExtTotalLen = 1024` and the *whole* data segment (`Header + ExtBlock + Payload`) by `transport.max_buf_size`. `internal/protocol/frame.go` and `internal/protocol/extensions.go` reference `MaxFrameSize` only in code comments — treat those as the transport buffer limit, not as a defined constant. A separate, hard-coded `maxPayloadSize = 1 << 20` (1 MB) cap lives in `internal/broker/router.go` and rejects oversized publishes at the router.

---

## 2. Command Opcodes & Control Bits

| Opcode | Constant | Description | Payload Format |
| :--- | :--- | :--- | :--- |
| `0x01` | `CmdPublish` | Publish a message to a topic | Bare topic name (e.g. `orders`), or `topic:<name>`, or `ttl:<ms>:<name>` |
| `0x02` | `CmdSubscribe` | Subscribe a QUIC stream to a topic or Consumer Group | `topic:<name>[:group:<gid>][:durable:<consumer_id>:<offset>]` |
| `0x03` | `CmdUnsubscribe` | Unsubscribe a stream from a topic | `topic:<topic_name>` |
| `0x04` | `CmdAck` | Acknowledge a delivered message (consumer groups) | `topic:<topic>:consumer:<id>:offset:<uint64>` |
| `0x05` | `CmdPublishBatch` | Publish a batch of sub-frames atomically | Flat array of standard frames: `[Magic|Cmd|StreamID|DataLen|Payload]...` |
| `0x06` | `CmdNack` | Negative acknowledgement by message offset | 8-byte little-endian `uint64` offset |

### Consumer Groups (`v1.13.0+`)

Competing consumers join a named Consumer Group by adding `:group:<group_id>` to the `CmdSubscribe` payload:

- **Standard group subscription**: `topic:orders:group:payment-workers`
- **Durable group subscription**: `topic:orders:group:payment-workers:durable:worker1:0`

Within each Consumer Group registered on a topic, messages are load-balanced across active members via **Lock-Free Atomic Round-Robin** (`0 allocs/op`, `< 10 ns/op`). Group-level Durable Offsets are updated automatically on consumer ACKs and persist across worker failovers. The single routing key for a topic is computed by `topicHashKey(topic)` = `authz.CombineHashStrings("topic", topic)` — see [Architecture §2](../en/architecture.md) for the rationale behind the `topic:` salt.

### Control Bits (Command Byte Bits 6 & 7)

- **MeshForwarded (bit 7, mask `0x80`)** — set when a frame is forwarded from another cluster broker node. Receivers must not re-forward such frames. Use `protocol.IsForwarded(cmd)`, `protocol.SetForwarded(cmd)`, or `protocol.OpcodeOf(cmd)` (which masks **both** control bits) to manipulate and inspect this flag.
- **HasExtensions (bit 6, mask `0x40`)** — set when the frame carries a TLV extension block immediately after the 10-byte header. The `DataLen` field then covers `ExtBlock + Payload`.

When `Command & 0x40 != 0`, the data segment begins with a 2-byte `ExtTotalLen` (`uint16` LE) followed by packed TLV entries:

```text
+--------+--------+--------+----+
|Type:1B | Len:1B | Value  |... |   (each entry)
+--------+--------+--------+----+
```

---

## 3. TLV Extension Block Format

`ExtTotalLen` is the byte count of the packed entries (excluding the 2-byte prefix itself). Each entry is `[Type:1B][Length:1B][Value:N B]`. The block is bounded by `MaxExtTotalLen = 1024` and the whole data segment (`ExtBlock + Payload`) by `transport.max_buf_size` (default `65536` bytes / `64 KB`); parsers silently skip unknown `Type` values.

| Extension Type | ID | Value Length | Description |
| :--- | :--- | :--- | :--- |
| `ExtTraceContext` | `0x01` | 25 bytes | W3C Trace Context `[TraceID:16B][SpanID:8B][TraceFlags:1B]` |
| `ExtCompression`  | `0x02` | 5 bytes  | Compression metadata `[Algo:1B][UncompressedSize:4B]`. Algo `1` = ZSTD. |
| `ExtPriority`     | `0x03` | 1 byte   | QoS priority level (`0` Highest, `1` High, `2` Normal, `3` Low). |
| `ExtRetryOffset`  | `0xF0` | 8 bytes  | NACK retry: original monotonic `uint64` offset, LE. Internal use; receivers fall back to the topic counter if absent. |

Helpers (from `internal/protocol/extensions.go`):

| Function | Purpose |
| :--- | :--- |
| `BuildExtensions(traceID, spanID, traceFlags)` | Allocate a slab TLV block with a single `ExtTraceContext` entry. Caller must `ReleaseExtensions(...)`. |
| `BuildPriorityExtension(priority)` | Slab-allocated TLV block with a single `ExtPriority` entry. |
| `BuildCompressionExtension(algo, uncompressedSize)` | Slab-allocated TLV block with a single `ExtCompression` entry. |
| `BuildMergedExtensionsWithCompression(existing, uncompressedSize)` | Append an `ExtCompression` entry to an existing block. |
| `StripExtension(block, typ)` | Return a new slab block with all entries of the given type removed. |
| `FindExtension(block, typ)` | Zero-alloc lookup; returns `(value []byte, found bool)`. |
| `ExtractPriority(block)` | Returns `(priority uint8, ok bool)`; defaults to `DefaultPriority = 2` if absent or invalid. |
| `ExtractTraceContext(block)` | Returns `(traceID, spanID, traceFlags, ok)`; zero-copy views into `block`. |

---

## 4. Message TTL & Priority QoS

Three mechanisms cooperate on the publish path:

1. **Per-priority TTL (`broker.priority_ttls`)** — `[<dur>, <dur>, <dur>, <dur>]` for priorities `0..3`. When non-zero, every message of that priority inherits an `expiresAt = now + TTL` (independent of any inline `ttl:` prefix). Stale messages are dropped lazily on dequeue by the subscriber Writer goroutine (`aqueduct_messages_expired_total{topic,priority}`).
2. **Inline TTL prefix** — publishers may prepend `ttl:<milliseconds>:<topic>` to the payload. Parsed by `parseTTL()` and returned as `expiresAt`; the broker strips it before hashing the topic.
3. **Priority Flag TLV (`ExtPriority = 0x03`)** — 1-byte priority. Writer goroutines dequeue in strict order `0 → 1 → 2 → 3`; empty priority channels are recycled to `sync.Pool` automatically.

---

## 5. AAL Log Record Binary Framing

When stored in Append-Only Log files (encrypted or raw), records are length-prefixed:

### Encrypted (AES-256-GCM)

```text
+-------------------+-------------------+--------------------------+
| 4-Byte Record Len | 12-Byte Nonce     | GCM Sealed Ciphertext    |
| (Little-Endian)   | (session:4 + ctr:8) | (Header+Payload+Tag)   |
+-------------------+-------------------+--------------------------+
```

- The 4-byte `Record Len` field is the byte count of `12-Byte Nonce + Ciphertext` (little-endian `uint32`).
- The 12-byte nonce is composed of a 4-byte random **session prefix** (generated once on `OpenEncrypted`) and an 8-byte **strictly monotonic counter** (`atomic.Uint64`).
- Ciphertext = `Seal(plaintext, associatedData=nil)`, which appends a 16-byte GCM tag.

### Unencrypted (key nil)

```text
+-------------------+--------------------------+
| 4-Byte Record Len | Raw Frame Bytes          |
| (Little-Endian)   | (Header+Payload)         |
+-------------------+--------------------------+
```

Replay (`aal.Replay(path, key, handler)`) walks the file sequentially, decrypts records, parses each with `protocol.ParseFrame`, and feeds `CmdPublish` frames to `Router.Publish` so durable subscribers and consumer-group offsets are restored before the QUIC UDP listener binds.

---

## 6. ALPN Identifiers

| Listener | ALPN | Source |
| :--- | :--- | :--- |
| Native broker QUIC (`:4242`) | `aqueduct-v1` | `cmd/broker/main.go` `tlsConf.NextProtos` |
| Cluster peer mesh (`cluster.peers`) | `aqueduct-mesh` | `cmd/broker/main.go` `peerTLS.NextProtos` |
| WebTransport gateway (`:4433`) | `h3` | `internal/webtransport/server.go` `cloneTLSForH3` |

`tls.MinVersion = tls.VersionTLS13` is enforced on every listener; the gateway explicitly overrides this even if the operator's certificate relaxes it.

---

## 7. WebTransport Transport Binding (v1.16.0+)

Browser clients reach the broker via the W3C [WebTransport API](https://www.w3.org/TR/webtransport/), which encapsulates HTTP/3 over QUIC. The framing above is **identical** to the native QUIC transport — the WebTransport gateway (`internal/webtransport/`) only translates the HTTP/3 connection layer.

### 7.1 Connection Establishment

Browser JS:

```js
const transport = new WebTransport("https://broker.example.com:4433/aqueduct/wt");
await transport.ready;
```

Server-side, `webtransport.Gateway.handleConn` calls `http3.NewRawServerConn` to complete the HTTP/3 SETTINGS exchange, then validates the inbound Extended CONNECT request:

- `:method = CONNECT`
- `:scheme = https`
- `:authority = broker.example.com:4433`
- `:path = /aqueduct/wt` (configurable via `webtransport.path_prefix`)
- `:protocol = webtransport`

A `200 OK` response completes the handshake. Subsequent bi-directional streams on the same QUIC connection carry the broker's binary frames.

### 7.2 Stream Mapping

| WebTransport stream type | Broker counterpart |
| :--- | :--- |
| Client-initiated bidirectional | `*quic.Stream` from `conn.AcceptStream()` → `transport.Broker.HandleStream(...)` |
| Server-initiated bidi (handshake request stream) | Kept open for the capsule protocol; hijacked via `HTTPStream()` |
| Client-initiated unidirectional | Currently dropped (reserved for HTTP/3 control; WT uni data streams are a v1.17+ roadmap item) |
| Server-initiated unidirectional | Reserved for HTTP/3 control |

After handshake, every client-opened bidi stream is fed into the same `transport.Broker.HandleStream(...)` pipeline used by native QUIC sessions. The frame parser, ACL engine, AAL logger, and router are all unchanged.

### 7.3 0-RTT and TLS

The QUIC layer underneath HTTP/3 negotiates 0-RTT as long as `quic.Config.Allow0RTT = true` is set on both sides. The gateway sets this by default with `MaxIdleTimeout: 30s` and `MaxIncomingStreams: 100`. Browsers store the session ticket on the first connection and reuse it on subsequent connections, eliminating a full RTT.

The broker enforces TLS 1.3 (`MinVersion = tls.VersionTLS13`) on the WebTransport listener even if the operator's certificate disables it — older TLS versions are silently rejected.

### 7.4 Browser Frame Format

Browsers writing to a WebTransport bidirectional stream produce the **same binary layout** as native clients:

```text
[Magic: 1B = 0x1F][Cmd: 1B][StreamID: 4B LE][DataLen: 4B LE][Payload: NB]
```

The `examples/web/app.js` reference client implements `buildFrame()` / `parseFrame()` matching this layout.

---

## 8. Operational Limits & Bounds

| Limit | Default | Source | Override |
| :--- | :--- | :--- | :--- |
| Max frame payload | `64 KB` | `transport.max_buf_size` default | `transport.max_buf_size` |
| Max extension block | `1024 bytes` | `MaxExtTotalLen` constant | hard-coded |
| Max QUIC streams / conn | `100` | `MaxIncomingStreams` | hard-coded in broker and WT gateway |
| Max payload (`Router.publishWithClientID`) | `1 MB` | `maxPayloadSize = 1 << 20` | hard-coded |
| Topic-offset monotonic counter | unbounded `uint64` | `atomic.Uint64` | n/a |
| NACK frame cache per subscriber | `256` entries FIFO | `defaultNackCacheSize` | hard-coded |
| Decoded decompressed size | `16 × maxBufSize` | `decompressFrame` cap | `transport.max_buf_size` |

All bounds are validated **before** any `unsafe.Slice` is constructed; parser errors return `errors.New("...")` rather than panicking, satisfying the protocol's "no panic on malformed input" guarantee.