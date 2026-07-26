# Tutorial: Getting Started with Aqueduct (v1.3.0)

This tutorial guides you through installing, configuring, running, and interacting with the Aqueduct message broker.

---

## Prerequisites

- **Go 1.22+**
- **Docker & Docker Compose** (optional)

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

## 2. Configuration (`config.yaml`)

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

transport:
  max_buf_size: 65536
  read_buf_size: 1024
```

---

## 3. Using Message TTL & Wildcards

### Wildcard Subscription Examples
- `sensor/+/temp`: Matches `sensor/room1/temp` and `sensor/room2/temp`.
- `sensor/#`: Matches all subtopics under `sensor/`.

### Message TTL Payload Format
To publish a message with a 500ms expiration time:
- Set payload to: `"ttl:500:sensor/room1/temp"`
- If the subscriber queue is delayed past 500ms, the message is automatically dropped before network transmission.
