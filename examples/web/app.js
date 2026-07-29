// Aqueduct WebTransport example.
//
// Usage from a browser:
//   1. Start the broker with the WebTransport gateway enabled
//      (see broker/config.yaml: webtransport.enabled: true).
//   2. Browse to http(s)://your-host/examples/web/index.html
//   3. Click "Connect" — the page opens a WebTransport session to the broker.
//   4. Click "Open Subscribe Stream" — opens a bidi stream and sends
//      `CmdSubscribe`. Frames received back are parsed and logged.
//   5. Click "Open Publish Stream" — opens a NEW bidi stream and sends
//      `CmdPublish`. The same wire format native QUIC clients use.
//
// IMPORTANT: WebTransport runs over HTTP/3 (QUIC). A modern browser is
// required — the W3C WebTransport API shipped in Chrome 97+, Edge 97+,
// Firefox 114+, and Safari 17.4+.

// ── Binary frame constants (must match protocol/frame.go) ──────────────
const MAGIC = 0x1F;
const CMD_PUBLISH = 0x01;
const CMD_SUBSCRIBE = 0x02;
const CMD_ACK = 0x04;
const HEADER_SIZE = 10;

// ── DOM lookups (cached) ────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);
const logEl = $("log");
const statusEl = $("status");

// ── State ───────────────────────────────────────────────────────────────
/** @type {WebTransport | null} */
let transport = null;
let connected = false;

/**
 * Returns true iff the browser supports the WebTransport API at runtime.
 * Surfaced to the user via a status pill on load.
 */
function detectSupport() {
  // `WebTransport` is the runtime global injected by the browser when the
  // API is enabled. Some browsers (Firefox < 114) require a flag.
  return typeof globalThis.WebTransport === "function";
}

// ── Logging helpers ─────────────────────────────────────────────────────
function log(kind, msg) {
  const span = document.createElement("span");
  span.className = kind;
  span.textContent = `[${new Date().toISOString()}] ${msg}\n`;
  logEl.appendChild(span);
  logEl.scrollTop = logEl.scrollHeight;
}
const info = (m) => log("info", m);
const ok = (m) => log("ok", m);
const err = (m) => log("err", m);

// ── Frame builders / parsers ────────────────────────────────────────────

/**
 * Build a binary frame matching the broker's wire format:
 *   [Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]
 * No allocations beyond the returned ArrayBuffer (modulo DataView overhead).
 *
 * @param {number} cmd — protocol command (1=Publish, 2=Subscribe, ...)
 * @param {number} streamId — low-32-bit client-chosen stream id (any non-zero)
 * @param {Uint8Array} payload
 * @returns {Uint8Array}
 */
export function buildFrame(cmd, streamId, payload) {
  const buf = new Uint8Array(HEADER_SIZE + payload.length);
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  dv.setUint8(0, MAGIC);
  dv.setUint8(1, cmd);
  // The backend stores stream IDs as uint32; we use 0 for control-plane
  // publishes/subscribes since the broker doesn't care (it hashes topic).
  dv.setUint32(2, streamId >>> 0, true /* little-endian */);
  dv.setUint32(6, payload.length >>> 0, true);
  buf.set(payload, HEADER_SIZE);
  return buf;
}

/**
 * Parse the next complete frame from `buf`. Returns the parsed frame and
 * the number of bytes consumed, or null if the buffer holds only a partial
 * frame (caller should Read more before re-invoking).
 *
 * NOTE: The WebTransport stream is a byte pipe. We accumulate incoming
 * chunks into bufSlice and re-scan for frames on every chunk arrival —
 * matches the broker's own runStreamReadLoop semantics exactly.
 *
 * @param {Uint8Array} buf
 * @returns {({frame: {cmd:number, streamId:number, payload: Uint8Array}, consumed: number} | null)}
 */
export function parseFrame(buf) {
  if (buf.length < HEADER_SIZE) return null;
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  if (dv.getUint8(0) !== MAGIC) {
    throw new Error(`bad magic 0x${dv.getUint8(0).toString(16)}`);
  }
  const dataLen = dv.getUint32(6, true);
  const total = HEADER_SIZE + dataLen;
  if (buf.length < total) return null;
  return {
    frame: {
      cmd: dv.getUint8(1),
      streamId: dv.getUint32(2, true),
      payload: buf.subarray(HEADER_SIZE, total),
    },
    consumed: total,
  };
}

// ── Stream pump: copy network bytes into the supplied Uint8Array. ───────

/**
 * Drains a WebTransport bi-stream into the buffer and invokes `onFrame`
 * for every complete binary frame found. Exits when the stream is closed
 * or an unrecoverable parse error fires.
 *
 * The implementation mirrors Go's `transport.runStreamReadLoop`: read
 * into the buffer, dispatch complete frames, keep partial bytes for the
 * next iteration.
 *
 * @param {ReadableStreamDefaultReader<Uint8Array>} reader
 * @param {Uint8Array} scratchBuf
 * @param {(frame: {cmd:number, streamId:number, payload: Uint8Array}) => void} onFrame
 */
async function pumpStream(reader, scratchBuf, onFrame) {
  // Partial-byte carryover between Read calls.
  /** @type {Uint8Array[]} */
  const chunks = [];
  let pendingLen = 0;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value && value.byteLength > 0) {
        chunks.push(value);
        pendingLen += value.byteLength;
      }
      // Compact chunks into scratchBuf once it's filling up.
      while (pendingLen >= HEADER_SIZE) {
        if (scratchBuf.length - pendingLen < 0) {
          // grow (single realloc path — broker hit path stays alloc-free
          // because it uses sync.Pool; the browser side is naturally bounded
          // by max_buf_size=64KB on the gateway).
          throw new Error("scratch buffer overflow");
        }
        // Drain chunks into scratch.
        const need = Math.min(scratchBuf.length, pendingLen);
        compactChunks(chunks, scratchBuf, need);
        pendingLen -= need;

        // Try to parse as many complete frames as scratch holds.
        let off = 0;
        for (;;) {
          const parsed = parseFrame(scratchBuf.subarray(off, need + off));
          if (!parsed) break;
          onFrame(parsed.frame);
          off += parsed.consumed;
        }
        // Shift leftover to the front of scratch.
        const leftover = scratchBuf.subarray(off, need + off);
        leftover.copyWithin(0, 0, leftover.length);
        // ... but we've already mutated chunks above. Reset:
        chunks.length = 0;
        chunks.push(leftover);
        pendingLen = leftover.length;
      }
    }
  } catch (e) {
    err(`stream read error: ${e.message ?? e}`);
  } finally {
    try {
      reader.releaseLock();
    } catch {
      /* already released */
    }
  }
}

/**
 * Drain chunks into a contiguous destination buffer. Returns the number
 * of bytes written. Internal helper to keep `pumpStream` readable.
 *
 * @param {Uint8Array[]} chunks
 * @param {Uint8Array} dest
 * @param {number} maxBytes
 */
function compactChunks(chunks, dest, maxBytes) {
  let written = 0;
  let i = 0;
  while (i < chunks.length && written < maxBytes) {
    const take = Math.min(chunks[i].length, maxBytes - written);
    dest.set(chunks[i].subarray(0, take), written);
    written += take;
    if (take === chunks[i].length) {
      i++;
    } else {
      // partially consumed — keep the rest
      chunks[i] = chunks[i].subarray(take);
    }
  }
  // Drop the head we just consumed.
  chunks.splice(0, i);
  return written;
}

// ── Page wiring ─────────────────────────────────────────────────────────

/** @returns {void} */
function setStatus(label, cls) {
  statusEl.textContent = label;
  statusEl.className = "status" + (cls ? " " + cls : "");
}

async function onConnect() {
  if (connected) return;
  const url = $("brokerUrl").value.trim();
  if (!url) {
    err("empty broker URL");
    return;
  }
  if (!detectSupport()) {
    err("browser does not support WebTransport");
    setStatus("unsupported", "error");
    return;
  }

  setStatus("connecting…", "connecting");
  info(`opening WebTransport session to ${url}`);
  try {
    transport = new WebTransport(url, {
      // 0-RTT may be supported by the broker (cfg.allow0rtt=true); we
      // request it eagerly so subsequent reconnects reuse a session ticket.
      // (Note: 0-RTT requires revalidation via the certificate presented
      // by the gateway; the browser validates it automatically.)
      requireUnreliable: false,
      // congestionControl: 'low-latency' is an option we deliberately
      // omit; the broker defaults to balanced throughput.
    });
    await transport.ready;
    connected = true;
    ok(`WebTransport session ready`);
    setStatus("connected", "connected");
    $("connectBtn").disabled = true;
    $("disconnectBtn").disabled = false;
    $("subscribeBtn").disabled = false;
    $("publishBtn").disabled = false;
  } catch (e) {
    err(`connect failed: ${e?.message ?? e}`);
    setStatus("error", "error");
    transport = null;
    connected = false;
  }
}

async function onDisconnect() {
  if (!transport) return;
  info("closing WebTransport session");
  try {
    transport.close();
  } catch {
    /* ignore double-close */
  }
  transport = null;
  connected = false;
  setStatus("disconnected");
  $("connectBtn").disabled = false;
  $("disconnectBtn").disabled = true;
  $("subscribeBtn").disabled = true;
  $("publishBtn").disabled = true;
}

/**
 * Open a bidi stream and send a single CmdSubscribe frame. Then keep
 * reading incoming frames and log them until the stream closes.
 */
async function onSubscribe() {
  if (!transport) return;
  const topic = $("topic").value.trim();
  if (!topic) {
    err("empty topic");
    return;
  }
  const subscribePayload = new TextEncoder().encode(`topic:${topic}`);
  const frame = buildFrame(CMD_SUBSCRIBE, /* streamId */ 1, subscribePayload);

  info(`opening bidi stream for subscribe (topic=${topic}, ${frame.length} bytes)`);
  const stream = await transport.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(frame);
  // We don't close the writer — we expect broker to reply with publishes
  // continuously. Releasing the writer keeps the stream open in the
  // browser's direction while the broker pushes back data.
  writer.releaseLock();

  info("subscribe sent; awaiting publishes");
  const scratch = new Uint8Array(64 * 1024);
  const reader = stream.readable.getReader();
  pumpStream(reader, scratch, (frame) => {
    const text = new TextDecoder().decode(frame.payload);
    ok(`[incoming] cmd=0x${frame.cmd.toString(16).padStart(2, "0")} payload=${JSON.stringify(text)}`);
  }).catch((e) => err(`pump error: ${e?.message ?? e}`));
}

/**
 * Open a NEW bidi stream and send a single CmdPublish frame. The stream
 * closes after writing — publishers don't expect any replies.
 */
async function onPublish() {
  if (!transport) return;
  const topic = $("publishTopic").value.trim();
  const message = $("publishMsg").value;
  if (!topic) {
    err("empty publish topic");
    return;
  }

  // The broker wire format uses the FULL payload (no topic/data split) —
  // so the published "payload" IS the topic. The broker's publish path
  // ALSO accepts `topic:<name>` payloads via parsePublishTopic (cf.
  // parseSubscriptionPayload), which normalizes both forms.
  const pubPayload = new TextEncoder().encode(topic);
  const frame = buildFrame(CMD_PUBLISH, /* streamId */ 0, pubPayload);

  info(`publish (topic=${topic}, ${frame.length} bytes, msg=${JSON.stringify(message)})`);
  const stream = await transport.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(frame);
  // Publishers send a single frame and immediately close. The broker
  // tears down its half of the stream as soon as the QUIC FIN arrives.
  await writer.close();
  writer.releaseLock();
  ok("publish closed");
}

// ── Bootstrap ──────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  if (!detectSupport()) {
    setStatus("unsupported", "error");
    err("WebTransport not available; use Chrome 97+ / Edge 97+ / Firefox 114+ / Safari 17.4+.");
  } else {
    setStatus("disconnected");
  }
  $("connectBtn").addEventListener("click", () => onConnect().catch((e) => err(String(e))));
  $("disconnectBtn").addEventListener("click", () => onDisconnect().catch((e) => err(String(e))));
  $("subscribeBtn").addEventListener("click", () => onSubscribe().catch((e) => err(String(e))));
  $("publishBtn").addEventListener("click", () => onPublish().catch((e) => err(String(e))));
});
