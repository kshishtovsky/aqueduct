# How-to: Production Deployment & Security (v1.16.0)

This guide details best practices for deploying Aqueduct securely in enterprise production environments. It is **problem-oriented**: each section starts from a specific operational need and walks to a verified configuration. For the conceptual background behind any of these choices see [Architecture & Memory Model](architecture.md).

---

## 1. Transport Security & mTLS Setup

In production, strictly enforce **mTLS 1.3** and provide corporate CA certificates:

```yaml
tls:
  generate: false
  cert_file: "/etc/certs/server.crt"
  key_file: "/etc/certs/server.key"
  require_client_cert: true
  client_ca_file: "/etc/certs/client_ca.pem"
```

- `generate: false` — operators must supply a real cert. The dev cert generated at startup is signed by an ephemeral CA that browsers and other clients will reject.
- `require_client_cert: true` — every client connection presents a certificate during the QUIC/TLS handshake. Anonymous clients are rejected.
- `client_ca_file` — PEM bundle containing every CA that signs legitimate client certs. The broker builds an `x509.CertPool` from this file and enforces chain verification.
- The broker's `tls.Config` always sets `MinVersion = tls.VersionTLS13`. There is no fallback to TLS 1.2.
- The cert's CN is extracted and used as the `clientID` for ACL keys (`authz.CombineHashes`), durable subscription offsets, and admin authorization.

---

## 2. Encrypted Persistence (AAL) & Retention

Generate a 32-byte cryptographically secure key:

```bash
openssl rand -base64 32
```

Configure `config.yaml`:

```yaml
aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "<BASE64_32_BYTE_KEY>"
  max_aal_size: 104857600      # 100 MB threshold — NOT enforced in v1.16.0 (no rotation scheduler)
  retention_period: "24h"      # declared only — NOT enforced; use external rotation
  retention_size: 1073741824   # 1 GB — declared only — NOT enforced; use external rotation
```

On broker startup, Aqueduct streams and replays AAL records into the router **before** the QUIC UDP listener binds, so durable subscribers and consumer-group offsets are restored before traffic is accepted. Records are encrypted with AES-256-GCM using a 12-byte nonce (`4-byte` random session prefix + `8-byte` strictly monotonic counter). The exact byte layout is in [Protocol §5](protocol-spec.md).

> **Operator warning — AAL retention is not enforced in v1.16.0.** `aal.Log.Rotate(maxSize, key)` is implemented but **never called** from any production code path; the `aal.max_aal_size`, `aal.retention_period`, and `aal.retention_size` keys are **declared and parsed** but **not enforced**. The AAL file grows unbounded until the disk is full. Operators **must** configure external rotation (see [Troubleshooting §6](troubleshooting.md) for a `logrotate` example).

> **Backups.** AAL files are append-only and ciphertext-only. Back up the AAL file **and** the encryption key separately — losing the key makes the backup unreadable. Rotated files are named with their original path plus a timestamp suffix.

---

## 3. Slow Consumer Backpressure Tuning

Choose backpressure isolation based on application requirements:

- `drop_oldest` — best for real-time telemetry (telemetry drops old data, keeps latest).
- `drop_newest` — best for ordered event streams where gaps are worse than overruns.
- `disconnect` — best for security-critical environments where slow subscribers must be purged.

```yaml
broker:
  queue_size: 2048
  backpressure_policy: "drop_oldest"
  batch_size: 65536
  flush_interval: 50us
  max_retries: 3
  quotas:
    default_publish_rate: 100
    default_burst_size: 1000
```

Tune `broker.batch_size` upward (256 KB, 1 MB) when individual frames are small and you measure high `quic.Stream.Write` overhead; tune downward if end-to-end latency must stay bounded.

---

## 4. System Limits (`sysctl` & `ulimit`)

Increase OS UDP buffer limits for high-throughput QUIC traffic:

```bash
# Linux
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
sysctl -w net.core.rmem_default=2097152
sysctl -w net.core.wmem_default=2097152
sysctl -w net.ipv4.udp_mem="262144 327680 393216"

# File descriptor limit
ulimit -n 65536
```

These are *defaults*; the right values depend on your expected peak connection count and message rate. On Kubernetes, configure the same values via a privileged `initContainer` or the node-level `sysctl` controller.

---

## 5. Cluster Deployment

Deploy multiple Aqueduct brokers in a direct mesh for horizontal scaling. Each broker connects to all others via QUIC with the `aqueduct-mesh` ALPN.

| Node | Address | Role |
| :--- | :--- | :--- |
| Broker A | `192.168.1.10:4242` | Peer |
| Broker B | `192.168.1.11:4242` | Peer |
| Broker C | `192.168.1.12:4242` | Peer |

### Static Peer Configuration

Each node lists the **other** nodes (not itself):

```yaml
cluster:
  peers:
    - "192.168.1.10:4242"
    - "192.168.1.11:4242"
    - "192.168.1.12:4242"
```

### Mesh TLS (Required in Production)

By default the mesh rejects insecure peers. To enable secure federation:

```yaml
cluster:
  peers:
    - "broker-b.example.internal:4242"
    - "broker-c.example.internal:4242"
  mesh:
    insecure_skip_verify: false              # never true in production
    ca_file: "/etc/aqueduct/mesh-ca.pem"     # PEM bundle signing every peer cert
```

See [Cluster Mesh TLS](cluster-mesh-tls.md) for the full hardening checklist, certificate rotation strategy, and gotchas with SAN / SubjectAltName.

### Forwarding Behavior

- A message published on any node is forwarded to all peers.
- The `MeshForwarded` bit (Command byte `0x80`) prevents re-forwarding loops — see [Protocol §2](protocol-spec.md).
- No consensus or leader election. The mesh is fully decentralized.
- Peer connections use the mesh TLS configuration (above), not the client mTLS config.
- Reconnect loops use exponential backoff (`250ms` initial, doubles up to `30s`).

### Network Requirements

- All nodes must be reachable via **UDP** on the configured port.
- Each node must hold its own TLS certificate / key.
- For 3+ node topologies, each node must list all other peers.
- No ordering guarantees across nodes (fire-and-forget).

---

## 6. Kubernetes Deployment

### Why Kubernetes?

Static peer lists (`cluster.peers`) require manual coordination — every node must know all others in advance. Kubernetes StatefulSets with Headless Services provide **dynamic DNS-based peer discovery** with zero external dependencies (Consul, etcd).

### Helm Chart (Recommended)

```bash
helm install aqueduct deploy/helm/aqueduct \
  --set replicaCount=3 \
  --set config.cluster.peers[0]="aqueduct-0.aqueduct-headless.default.svc.cluster.local:4242" \
  --set config.cluster.peers[1]="aqueduct-1.aqueduct-headless.default.svc.cluster.local:4242" \
  --set config.cluster.peers[2]="aqueduct-2.aqueduct-headless.default.svc.cluster.local:4242" \
  --set config.cluster.discovery.enabled=true \
  --set config.cluster.discovery.host="aqueduct-headless.default.svc.cluster.local" \
  --set config.cluster.discovery.port=4242 \
  --set config.cluster.discovery.interval="10s"
```

### Headless Service (Required for DNS Discovery)

The Headless Service (`clusterIP: None`) returns A records for individual StatefulSet pods:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: aqueduct-headless
spec:
  clusterIP: None
  ports:
    - name: quic
      port: 4242
  selector:
    app.kubernetes.io/name: aqueduct
```

Pod DNS patterns:

- `aqueduct-0.aqueduct-headless.<namespace>.svc.cluster.local`
- `aqueduct-1.aqueduct-headless.<namespace>.svc.cluster.local`

### DNS Discovery

With discovery enabled, the broker polls the Headless Service DNS record every `interval` and diffs the result against the peer set:

- **Scale up** — new pod IPs are connected via `PeerManager.AddPeer(...)`.
- **Scale down** — removed IPs are disconnected via `PeerManager.RemovePeer(...)`.
- **Zero downtime** — connections use exponential backoff reconnect.

```yaml
cluster:
  peers: []                   # empty — discovery populates this automatically
  discovery:
    enabled: true
    type: "dns"
    host: "aqueduct-headless.default.svc.cluster.local"
    port: 4242
    interval: "10s"
```

### Scaling

```bash
# Scale to 5 replicas
kubectl scale statefulset aqueduct --replicas=5

# Rollout restart (zero-downtime upgrade)
kubectl rollout restart statefulset aqueduct
```

DNS discovery reconciles the peer mesh automatically within `interval` seconds.

### Raw Kubernetes Manifests

For non-Helm users, raw manifests are in `deploy/k8s/`:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

---

## 7. NACK / DLQ in Production

Configure NACK redelivery and Dead Letter Queue:

- Set `broker.max_retries` (default `3`) in `config.yaml` or via `AQUEDUCT_BROKER_MAX_RETRIES`.
- DLQ topics follow the pattern `__dlq__<original_topic>`. A subscriber on `__dlq__*` will receive the poison pill after `max_retries` is exceeded.
- Monitor `aqueduct_messages_nacked_total{topic}` and `aqueduct_messages_dead_lettered_total{topic}`.
- Connect an offline-inspection consumer to `__dlq__*` topics.
- The per-subscriber NACK frame cache is bounded at `defaultNackCacheSize = 256` entries FIFO; rapid NACK storms cannot blow up memory.

---

## 8. Rate Limiting Quotas

Configure per-tenant rate limiting:

- Set `broker.quotas.default_publish_rate` and `broker.quotas.default_burst_size` in `config.yaml`.
- Override per-client under `broker.quotas.per_client.<client_id>`:

```yaml
broker:
  quotas:
    default_publish_rate: 100
    default_burst_size: 1000
    per_client:
      "service-a":
        rate: 500
        burst: 1000
```

- Monitor `aqueduct_messages_rate_limited_total{client}` to spot misbehaving publishers.
- Dynamic rate updates via the gRPC Admin API (`SetClientQuota`) — see [Admin API Reference](admin-api.md). The `Manager` swaps bucket maps with RCU (`atomic.Pointer[map[string]*Bucket]`) so updates never block the publish hot path.

---

## 9. Observability

### Prometheus Scrape

`metrics_addr` (default `:9090`) exposes two endpoints:

- `GET /metrics` — Prometheus text format. Every metric listed in [Metrics Reference](metrics.md).
- `GET /healthz` — returns `200 OK`. Use it as your readiness probe target.

### Grafana

The Compose stack includes a pre-provisioned dashboard under `deploy/grafana/dashboards/`. Point Prometheus at the broker with the bundled `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'aqueduct'
    static_configs:
      - targets: ['aqueduct:9090']
    metrics_path: /metrics
```

### Tracing (Optional)

Enable OTLP tracing only when you need end-to-end visibility across publishers and subscribers. The cost on the hot path when disabled is a single `nil` check (~3.4 ns).

```yaml
tracing:
  enabled: true
  service_name: "aqueduct-broker"
  endpoint: "otel-collector.observability.svc.cluster.local:4317"
```

W3C Trace Context is propagated transparently through the `ExtTraceContext = 0x01` TLV extension.

---

## 10. Compression

`compression.enabled: true` enables ZSTD compression on the **peer-forwarded copy** of `CmdPublishBatch` frames whose payload exceeds `compression.min_batch_size` (default `1024` bytes). Local subscribers always receive the uncompressed payload — compression is only applied to bytes going over the wire to other brokers in the mesh.

```yaml
compression:
  enabled: true
  min_batch_size: 1024        # only compress batches >= 1 KB
  level: 0                    # 0 = ZSTD default, 1 = fastest, 3 = default
```

The decompressed size cap on the receiver is `16 × transport.max_buf_size`. Payloads above this are rejected.

---

## 11. Operational Runbook

A few invariants worth checking during incident response:

- **Hot-reload rules.** The Admin API hot-reloads quotas and ACL via RCU — there is no broker restart required and no lock contention on the message hot path.
- **DNS discovery churn.** Each new IP triggers an outbound QUIC dial; a flapping DNS record will trigger repeated connect/disconnect cycles. Set `interval` ≥ `2 × expected_dns_ttl` to dampen.
- **Self-signed dev certs.** Browsers reject the WebTransport handshake on self-signed certs. Production must use a publicly trusted certificate with the broker's hostname in the SAN list.
- **Backpressure + disconnect.** If you choose `disconnect`, slow consumers are torn down with stream error code `1`. The subscriber must reconnect; offsets survive because they are tracked on the group level.