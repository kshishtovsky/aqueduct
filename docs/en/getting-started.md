# Tutorial: Getting Started with Aqueduct (v1.16.0)

This tutorial walks you through installing, configuring, running, and interacting with the Aqueduct message broker in roughly ten minutes. It is **learning-oriented**: each step ends in a tangible success so you can see the broker work before you read the [Reference](.) docs.

---

## Prerequisites

- **Go 1.23+**
- **Docker & Docker Compose** (optional, for the local stack)
- **Kubernetes 1.28+** with `kubectl` and `helm` (optional, for cluster deployment)
- **A modern browser** with WebTransport support for `examples/web/`: Chrome/Edge ≥ 97, Firefox ≥ 114, or Safari ≥ 17.4

---

## 1. Quick Start with Docker Compose

Run Aqueduct broker alongside Prometheus and Grafana:

```bash
docker compose up -d
```

- **Broker health**: <http://localhost:9090/healthz>
- **Broker metrics**: <http://localhost:9090/metrics>
- **Prometheus UI**: <http://localhost:9091> (Prometheus's own web port — not the broker)
- **Grafana dashboard**: <http://localhost:3000> (User: `admin`, Password: `admin`)

The Docker Compose stack starts one broker with the WebTransport gateway enabled on UDP `:4433`, Prometheus scraping `:9090/metrics`, and Grafana pre-loaded with the Aqueduct dashboard.

Stop the stack:

```bash
docker compose down
```

---

## 2. Local Binary Run

```bash
git clone https://github.com/kshishtovsky/aqueduct.git
cd aqueduct

# Run with the bundled default config
go run ./cmd/broker -config config.yaml

# Or override listen/metrics addresses
go run ./cmd/broker \
  -config config.yaml \
  -addr :4242 \
  -metrics-addr :9090
```

You should see log output like:

```
INFO  metrics server started addr=:9090
INFO  broker listening addr=:4242
INFO  webtransport gateway started addr=:4433 path_prefix=/aqueduct/wt
```

(The `webtransport` line appears only if you enable it in `config.yaml` — see [Step 6](#6-webtransport-browser-connectivity-v1160).)

Hit the health endpoint in another terminal:

```bash
curl -s http://localhost:9090/healthz
# OK
```

---

## 3. Send and Receive with the Go Example

In one terminal, start the example subscriber/publisher:

```bash
go run ./examples/go/main.go
```

This opens a QUIC connection, subscribes to `topic:orders`, publishes one message, and reads the delivery back. Output:

```
[Go Client] Subscribed to topic 'orders'.
[Go Client] Published message to topic 'orders'.
[Go Client] Received frame: Cmd=1, StreamID=2, Topic/Payload="orders"
```

The example uses a self-signed dev certificate. The broker is configured with `tls.generate: true` by default, which creates an ephemeral RSA-2048 cert at startup. For real workloads always set `tls.generate: false` and point `tls.cert_file` / `tls.key_file` at a trusted certificate — see [Configuration Reference](configuration.md).

---

## 4. Configuration (`config.yaml`)

Create or modify `config.yaml` in the repo root:

```yaml
listen_addr: ":4242"
metrics_addr: ":9090"

tls:
  generate: true
  cert_file: ""
  key_file: ""
  require_client_cert: false
  client_ca_file: ""

aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "dGhpcyBpcyBhIDMyIGJ5dGUgYWVzLTI1NiBrZXkh"  # Base64 32-byte key
  max_aal_size: 104857600          # 100 MB threshold — NOT enforced by the broker in v1.16.0 (no rotation scheduler); use OS-level rotation
  retention_period: "24h"           # declared but NOT enforced; use external rotation
  retention_size: 1073741824        # 1 GB — declared but NOT enforced; use external rotation

acl:
  enabled: true
  default: "none"
  rules:
    - client: "sensor-service"
      topic: "sensor/#"
      permission: "publish"
    - client: "analytics-service"
      topic: "sensor/+/temp"
      permission: "subscribe"

broker:
  queue_size: 1024
  backpressure_policy: "drop_oldest"   # "drop_oldest", "drop_newest", or "disconnect"
  batch_size: 65536
  flush_interval: 50us
  max_retries: 3
  priority_ttls:
    - "500ms"                          # Priority 0 (Highest)
    - "5s"                             # Priority 1 (High)
    - "0"                              # Priority 2 (Normal — no override)
    - "0"                              # Priority 3 (Low — no override)
  quotas:
    default_publish_rate: 0
    default_burst_size: 1000

transport:
  max_buf_size: 65536
  read_buf_size: 1024

cluster:
  peers:
    - "192.168.1.10:4242"
    - "192.168.1.11:4242"

admin:
  enabled: false
  addr: ":9091"

tracing:
  enabled: false
  service_name: "aqueduct-broker"
  endpoint: "localhost:4317"

compression:
  enabled: false
  min_batch_size: 1024
  level: 0

webtransport:
  enabled: false
  listen_addr: ":4433"
  path_prefix: "/aqueduct/wt"
```

Each YAML key has a matching `AQUEDUCT_*` environment variable. CLI flags `-config`, `-addr`, `-metrics-addr`, `-cert`, `-key`, and `-aal` take precedence over both env vars and YAML. See [Configuration Reference](configuration.md) for the full table.

---

## 5. QoS Priority, Per-Priority TTL, and MQTT Wildcards

### Priority TLV Extension (`ExtPriority = 0x03`)

- Priority levels are `0` (Highest/Critical), `1` (High), `2` (Normal), `3` (Low). Default is `2`.
- Critical messages (`Priority 0`) bypass lower-priority traffic in subscriber Writer queues — strict priority order `0 → 1 → 2 → 3`.
- Build the TLV block at publish time with `protocol.BuildPriorityExtension(0)` (slab-allocated; release via `protocol.ReleaseExtensions(...)`).

### Per-Priority TTL

- Configured via `broker.priority_ttls` in YAML only. **The built-in default is `nil` (no per-priority TTL, messages never expire).** There is **no** `AQUEDUCT_BROKER_PRIORITY_TTLS` env override — the field is an array and is only editable in `config.yaml`.
- Messages published at Priority `P` automatically inherit `priority_ttls[P]` if non-zero. Stale messages are lazily dropped on dequeue — observable via `aqueduct_messages_expired_total{topic, priority}`.

### Wildcard Subscriptions

- `sensor/+/temp` matches `sensor/room1/temp` and `sensor/room2/temp` (one segment).
- `sensor/#` matches every subtopic under `sensor/`, including `sensor` itself.
- `MatchWildcard` runs in < 51 ns/op with zero allocations.

---

## 6. WebTransport (Browser) Connectivity (v1.16.0+)

Aqueduct ships with an optional HTTP/3 + WebTransport gateway that lets browsers connect using the W3C [WebTransport API](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport). The gateway reuses the broker's mTLS certificate, so a single trust root secures both native and browser clients.

### 6.1 Enable the gateway in `config.yaml`

```yaml
tls:
  generate: false                    # REQUIRED for WebTransport — browsers reject raw self-signed certs
  cert_file: "/etc/aqueduct/fullchain.pem"
  key_file:  "/etc/aqueduct/privkey.pem"

webtransport:
  enabled: true
  listen_addr: ":4433"               # distinct UDP port (use :443 in production)
  path_prefix: "/aqueduct/wt"        # clients send Extended CONNECT here
```

For local development, the simplest path is [`mkcert`](https://github.com/FiloSottile/mkcert):

```bash
mkcert -install
mkcert localhost 127.0.0.1 ::1
# produces localhost+2.pem / localhost+2-key.pem
```

Point `tls.cert_file` / `tls.key_file` at those files.

### 6.2 Run the broker

```bash
go run ./cmd/broker -config config.yaml
```

Look for the line:

```
INFO  webtransport gateway started addr=:4433 path_prefix=/aqueduct/wt
```

### 6.3 Try the browser example

Serve `examples/web/` over HTTPS:

```bash
cd examples/web
go run -mod=mod - <<'EOF'
package main
import ("log"; "net/http")
func main() {
    log.Fatal(http.ListenAndServeTLS(":8443",
        "/path/to/localhost+2.pem",
        "/path/to/localhost+2-key.pem",
        http.FileServer(http.Dir("."))))
}
EOF
```

Open <https://localhost:8443/index.html>. Click **Connect** to open a WebTransport session, then **Open Subscribe Stream** and watch messages published from any client (browser, native Go, Node.js) appear in the event log.

### 6.4 Write your own browser client

The wire format is identical to native QUIC clients: `[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]`. Magic is `0x1F`. `CmdSubscribe = 0x02`, `CmdPublish = 0x01`. See `examples/web/app.js` for a self-contained implementation (`buildFrame` / `parseFrame`).

### 6.5 0-RTT and TLS

WebTransport rides on HTTP/3 which rides on QUIC, which supports 0-RTT. The broker enables 0-RTT by default (`Allow0RTT: true`, `MaxIncomingStreams: 100`, `MaxIdleTimeout: 30s` in the gateway's `quic.Config`). Browsers negotiate 0-RTT when they have a session ticket from a previous connection; on success the very first request lands inside the same RTT as the QUIC handshake.

### 6.6 Production checklist for browser clients

- Use a publicly trusted certificate (Let's Encrypt or your CA).
- Ensure the certificate SAN list includes the hostname clients will use (e.g., `broker.example.com`).
- Open UDP/443 (or the configured port) in your firewall — browsers refuse to negotiate WebTransport on TCP, only UDP.
- Leave `tls.require_client_cert: false` unless browsers can present a client certificate. The gateway honors the broker's TLS config unchanged.