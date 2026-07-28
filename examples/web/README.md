# Aqueduct — Browser WebTransport Example

A static HTML page that connects to the Aqueduct broker via the browser's
native **WebTransport API** (HTTP/3 + QUIC) and exchanges the same binary
frames that native QUIC clients use. No JavaScript framework, no bundler,
no dependencies — just open `index.html` in a modern browser.

## What this demonstrates

* **Protocol transparency** — the wire format is identical across native QUIC and WebTransport clients. No translations, no JSON wrapping.
* **Cross-transport routing** — messages from any client (browser, native Go, Node.js, Python) reach subscribers across all transports transparently.
* **mTLS reuse** — the gateway serves from the broker's existing certificate, so security posture is preserved end-to-end.

## Prerequisites

* A modern browser with WebTransport support:
  - Chrome / Edge ≥ 97
  - Firefox ≥ 114
* An Aqueduct broker with the WebTransport gateway enabled (see below).
* A TLS certificate the browser trusts. Self-signed certificates will be rejected by the browser — use [`mkcert`](https://github.com/FiloSottile/mkcert), a Let's Encrypt cert, or a manual cert.

## Quick start

### 1. Enable the gateway in the broker config

Edit `config.yaml`:

```yaml
tls:
  generate: false
  cert_file: "/path/to/fullchain.pem"
  key_file:  "/path/to/privkey.pem"

webtransport:
  enabled: true
  listen_addr: ":4433"           # any free UDP port
  path_prefix: "/aqueduct/wt"
```

For development, generate a trusted local cert with [mkcert](https://github.com/FiloSottile/mkcert):

```bash
mkcert -install
mkcert localhost 127.0.0.1 ::1
# writes cert and key to ./localhost+2.pem / ./localhost+2-key.pem
```

### 2. Start the broker

```bash
go run ./cmd/broker -config config.yaml
```

You should see:

```
INFO  webtransport gateway started addr=:4433 path_prefix=/aqueduct/wt
```

### 3. Serve the browser example

The page is a static asset bundle; any HTTP server with HTTPS works. The
simplest is the Go binary already on your PATH:

```bash
# From the repo root
cd examples/web
go run -mod=mod - <<'EOF'
package main

import (
    "log"
    "net/http"
)

func main() {
    log.Println("listening on https://localhost:8443")
    log.Fatal(http.ListenAndServeTLS(":8443", "/path/to/localhost+2.pem", "/path/to/localhost+2-key.pem", http.FileServer(http.Dir("."))))
}
EOF
```

Then open `https://localhost:8443/index.html`.

### 4. Use the page

1. Click **Connect** to open a WebTransport session.
2. Click **Open Subscribe Stream** to start receiving messages on the chosen topic.
3. From a *separate* QUIC publisher (`examples/go/main.go`) or another browser tab, publish to the same topic — the messages appear in the log.

## Wire format

Every browser → broker and broker → browser stream carries the same
binary frame the broker uses for native QUIC clients:

```
[Magic: 1 byte][Cmd: 1 byte][StreamID: 4 bytes][DataLen: 4 bytes][Payload: N bytes]
```

Constants:

| Field    | Value                                  |
| :------- | :------------------------------------- |
| Magic    | `0x1F`                                 |
| CmdPublish   | `0x01` |
| CmdSubscribe | `0x02` |

Inspect `app.js`'s `buildFrame` / `parseFrame` for the exact byte layout.

## 0-RTT and session resumption

Browsers negotiate 0-RTT when (a) the broker advertises QUIC 0-RTT support
and (b) the browser has a stored session ticket from a previous connection.
The Aqueduct broker enables 0-RTT by default. On 0-RTT success the very
first request lands inside the same RTT as the QUIC handshake; the
browser will retry transparently if 0-RTT is rejected (e.g. server
chooses not to honor it).

## Limitations of this MVP

* **Bidirectional streams only.** The gateway accepts uni streams from the
  client only silently (HTTP/3 control streams are passed to quic-go's
  HTTP/3 layer; raw WebTransport uni data streams would require extra
  handling that is not yet implemented).
* **No WebTransport Datagrams.** Reliable delivery over bidi streams is
  sufficient for the protocol. Datagrams are a v1.17+ roadmap item.

## Security notes

* Self-signed certificates are rejected by the browser. Always use a
  trusted cert in development (`mkcert` is the simplest path) and a
  public CA (Let's Encrypt) in production.
* The certificate's SAN must include the host name in the URL bar, or
  the browser will reject the WebTransport handshake.
* `mTLS` (mutual auth) is enforced by the broker's main listener; the
  WebTransport gateway reuses the same `*tls.Config`. Operators who
  need to disable mTLS for browser clients must do so explicitly on the
  broker's `tls.require_client_cert` flag — that affects both listeners.
