# Tutorial: Getting Started with Aqueduct (v1.14.0)

This tutorial guides you through installing, configuring, running, and interacting with the Aqueduct message broker.

---

## Prerequisites

- **Go 1.23+**
- **Docker & Docker Compose** (optional)
- **Kubernetes 1.28+** with `kubectl` and `helm` (optional, for K8s deployment)

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
