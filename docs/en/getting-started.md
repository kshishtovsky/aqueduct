# Tutorial: Getting Started with Aqueduct (v1.16.0)

This tutorial guides you through installing, configuring, running, and interacting with the Aqueduct message broker.

---

## Prerequisites

- **Go 1.23+**
- **Docker & Docker Compose** (optional)
- **Kubernetes 1.28+** with `kubectl` and `helm` (optional, for K8s deployment)
- **Modern browser** with WebTransport support (for the browser client example): Chrome/Edge ≥ 97, Firefox ≥ 114, or Safari ≥ 17.4

---

## 1. Quick Start with Docker Compose

Run Aqueduct broker along with Prometheus and Grafana:

```bash
docker compose up -d
```

- **Broker Health**: `http://localhost:9090/healthz`
- **Prometheus UI**: `http://localhost:9091`
- **Grafana Dashboard**: `http://localhost:3000` (User: `admin`, Password: `admin`)

---

## 2. Kubernetes Quick Start (v1.14.0)

Deploy a 3-replica cluster with DNS-based peer discovery:

```bash
helm install aqueduct deploy/helm/aqueduct \
  --set replicaCount=3 \
  --set config.cluster.discovery.enabled=true \
  --set config.cluster.discovery.host="aqueduct-headless.default.svc.cluster.local" \
  --set config.cluster.discovery.port=4242
```

Verify the cluster is running:

```bash
kubectl get pods -l app.kubernetes.io/name=aqueduct
kubectl logs statefulset/aqueduct --tail=10 -f
```

Scale up dynamically:

```bash
kubectl scale statefulset aqueduct --replicas=5
```

For raw manifests (without Helm):

```bash
kubectl apply -f deploy/k8s/
```

---

## 3. Configuration (`config.yaml`)

Create or modify `config.yaml`:

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
  key: "dGhpcyBpcyBhIDMyIGJ5dGUgYWVzLTI1NiBrZXkh" # Base64 32-byte key
  max_aal_size: 104857600

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
  backpressure_policy: "drop_oldest" # "drop_oldest", "drop_newest", or "disconnect"
  batch_size: 65536
  flush_interval: 50us
  max_retries: 3
  priority_ttls:
    - "500ms"  # Priority 0 (Highest)
    - "5s"     # Priority 1 (High)
    - "0"      # Priority 2 (Normal - no TTL override)
    - "0"      # Priority 3 (Low - no TTL override)
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
```

---

## 4. Using QoS Priority Queues, Per-Priority TTL & Wildcards

### Priority TLV Extension (`ExtPriority = 0x03`)
- Priority levels range from `0` (Highest/Critical) to `3` (Low/Bulk).
- Critical messages (Priority 0) bypass lower priority messages in subscriber Writer queues.

### Per-Priority TTL
- Configured via `priority_ttls` in `config.yaml`.
- Messages published at Priority `P` automatically inherit `priority_ttls[P]`. If subscriber queues are delayed past the TTL, the stale message is lazily dropped on dequeue.

### Wildcard Subscription Examples
- `sensor/+/temp`: Matches `sensor/room1/temp` and `sensor/room2/temp`.
- `sensor/#`: Matches all subtopics under `sensor/`.

---

## 5. NACK & Dead Letter Queue (DLQ)

Subscribers can NACK (Negative Acknowledgement) a message by offset using the `CmdNack` (0x05) opcode. The broker automatically redelivers the message (up to `max_retries`), after which the message is routed to the dead letter queue topic `__dlq__<topic>`.

---

## 6. WebTransport (Browser) Connectivity (v1.16.0+)

Aqueduct ships with an optional HTTP/3 + WebTransport gateway that lets browsers connect using the W3C [WebTransport API](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport). The gateway reuses the broker's mTLS certificate so a single trust root secures both native and browser clients.

### 6.1 Enable the gateway in `config.yaml`

```yaml
tls:
  generate: false                  # REQUIRED for WebTransport — browsers reject raw self-signed certs.
  cert_file: "/etc/aqueduct/fullchain.pem"
  key_file:  "/etc/aqueduct/privkey.pem"

webtransport:
  enabled: true
  listen_addr: ":4433"            # distinct UDP port (browsers expect :443 in production)
  path_prefix: "/aqueduct/wt"     # clients send Extended CONNECT here
```

For local development, the simplest path is [`mkcert`](https://github.com/FiloSottile/mkcert):

```bash
mkcert -install
mkcert localhost 127.0.0.1 ::1
# produces localhost+2.pem / localhost+2-key.pem
```

Then point `tls.cert_file` / `tls.key_file` at those files.

### 6.2 Run the broker

```bash
go run ./cmd/broker -config config.yaml
# INFO  webtransport gateway started addr=:4433 path_prefix=/aqueduct/wt
```

### 6.3 Try the browser example

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

The wire format is identical to native QUIC clients: a 10-byte header `[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]`. Magic is `0x1F`, `CmdSubscribe = 0x02`, `CmdPublish = 0x01`. See `examples/web/app.js` for a self-contained implementation (`buildFrame`/`parseFrame`).

### 6.5 0-RTT and TLS

WebTransport rides on HTTP/3 which rides on QUIC, which supports 0-RTT. The broker enables 0-RTT by default (`MaxIncomingStreams` + `Allow0RTT` in the QUIC config). Browsers negotiate 0-RTT when they have a session ticket from a previous connection; on success the very first request lands inside the same RTT as the QUIC handshake. The browser validates the gateway's certificate transparently — no manual step required.

### 6.6 Production checklist for browser clients

- Use a publicly trusted certificate (Let's Encrypt or your CA).
- Ensure the certificate SAN list includes the hostname clients will use (e.g., `broker.example.com`).
- Open UDP/443 (or the configured port) in your firewall — browsers refuse to negotiate WebTransport on TCP, only UDP.
- Enable mTLS only if browsers have a way to present a client certificate; otherwise set `tls.require_client_cert: false` (the gateway honors the broker's TLS config unchanged).
