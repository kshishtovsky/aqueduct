# How-to: Production Deployment & Security (v1.3.0)

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
```

---

## 4. System Limits (`sysctl` & `ulimit`)

Increase OS UDP buffer limits:

```bash
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
ulimit -n 65536
```
