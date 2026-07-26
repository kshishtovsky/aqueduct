# How-to: Production Deployment & Security (v1.14.0)

This guide details best practices for deploying Aqueduct securely in enterprise production environments.

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

---

## 2. Encrypted Persistence (AAL) & Rotation

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
  max_aal_size: 104857600 # 100 MB max size before rotation
```

On broker startup, Aqueduct automatically streams and replays AAL records to restore in-memory state before binding to the UDP listener port.

---

## 3. Slow Consumer Backpressure Tuning

Choose backpressure isolation based on application requirements:

- `drop_oldest`: Best for real-time telemetry (telemetry drops old data, keeps latest).
- `drop_newest`: Best for ordered event streams.
- `disconnect`: Best for security-critical environments where slow subscribers must be purged.

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

---

## 4. System Limits (`sysctl` & `ulimit`)

Increase OS UDP buffer limits:

```bash
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
ulimit -n 65536
```

---

## 5. Cluster Deployment (v1.8.0+)

Deploy multiple Aqueduct brokers in a direct mesh for horizontal scaling. Each broker connects to all others:

| Node | Address | Role |
|------|---------|------|
| Broker A | `192.168.1.10:4242` | Peer |
| Broker B | `192.168.1.11:4242` | Peer |
| Broker C | `192.168.1.12:4242` | Peer |

### Configuration

Each node lists the **other** nodes (not itself):

```yaml
cluster:
  peers:
    - "192.168.1.10:4242"
    - "192.168.1.11:4242"
    - "192.168.1.12:4242"
```

### Forwarding Behavior

- A message published on any node is forwarded to all peers
- The MeshForwarded bit prevents re-forwarding loops
- No consensus or leader election -- the mesh is fully decentralized
- Peer connections use the same mTLS configuration as client connections

### Network Requirements

- All nodes must be reachable via UDP on the configured port
- Each node must be configured with its own TLS certificate/key
- For 3+ node topologies, each node must list all other peers
- No ordering guarantees across nodes (fire-and-forget)

---

## 6. Kubernetes Deployment (v1.14.0+)

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
- etc.

### DNS Discovery

With discovery enabled, the broker polls the Headless Service DNS record every `interval` and diffs the result against known peers:

```go
// ResolveHead resolves Headless Service A records via net.LookupHost
ips, err := net.LookupHost("aqueduct-headless.default.svc.cluster.local")
```

- **Scale up**: New pod IPs are automatically connected via `AddPeer()`
- **Scale down**: Removed IPs are disconnected via `RemovePeer()`
- **Zero downtime**: Connections use exponential backoff reconnect

### Configuration

```yaml
cluster:
  peers: []  # empty — discovery populates automatically
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

### Raw Kubernetes Manifests

For non-Helm users, raw manifests are in `deploy/k8s/`:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

---

## 7. NACK/DLQ in Production

Configure NACK redelivery and Dead Letter Queue:

- Set `max_retries` (default 3) in `config.yaml` or via `AQUEDUCT_BROKER_MAX_RETRIES`
- DLQ topics follow the pattern `__dlq__<original_topic>`
- Monitor `aqueduct_messages_nacked_total` and `aqueduct_messages_dead_lettered_total`
- Connect a subscriber to `__dlq__*` topics for offline inspection

---

## 8. Rate Limiting Quotas

Configure per-tenant rate limiting:

- Set `broker.quotas.default_publish_rate` and `broker.quotas.default_burst_size` in `config.yaml`
- Per-client overrides: `broker.quotas.per_client.<client_id>`
- Monitor `aqueduct_messages_rate_limited_total` metric
- The YAML configuration shown in Section 3 includes the full quota block
