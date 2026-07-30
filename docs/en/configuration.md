# Reference: Configuration (v1.16.0)

This document is the canonical reference for every Aqueduct configuration key, its default, its environment-variable override, and its security implications. It is **information-oriented**: lookup-shaped. For the architecture behind any of these knobs see [Architecture & Memory Model](architecture.md); for production recipes see [Production Deployment & Security](production-deployment.md).

The configuration is loaded by `internal/config/Load(path)`:

1. `Default()` produces a `*Config` with safe defaults.
2. If `path != ""`, the YAML file is read and unmarshalled on top of the defaults.
3. `applyEnvOverrides(...)` walks the struct and applies `AQUEDUCT_*` env vars.
4. CLI flags (`-config`, `-addr`, `-metrics-addr`, `-cert`, `-key`, `-aal`) take precedence over both YAML and env vars.

Precedence (highest first): **CLI flag → env var → YAML file → built-in default**.

---

## 1. Top-Level Keys

| YAML Key | Type | Default | Env Override | CLI Flag | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `listen_addr` | `string` | `":4242"` | `AQUEDUCT_LISTEN_ADDR` | `-addr` | UDP address for the QUIC broker listener. |
| `metrics_addr` | `string` | `":9090"` | `AQUEDUCT_METRICS_ADDR` | `-metrics-addr` | TCP address for `/metrics` and `/healthz`. |

---

## 2. `tls` — TLS & mTLS

```yaml
tls:
  generate: bool          # AQUEDUCT_TLS_GENERATE  (no CLI flag)
  cert_file: string       # AQUEDUCT_TLS_CERT_FILE (-cert)
  key_file: string        # AQUEDUCT_TLS_KEY_FILE  (-key)
  require_client_cert: bool  # AQUEDUCT_TLS_REQUIRE_CLIENT_CERT
  client_ca_file: string  # AQUEDUCT_TLS_CLIENT_CA_FILE
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `generate` | `true` | When `true` and `cert_file` is empty, the broker generates an **ephemeral self-signed RSA-2048 cert** at startup. **Never enable in production** — clients reject unknown CAs and browsers reject self-signed WebTransport certs outright. |
| `cert_file` | `""` | PEM-encoded server certificate. Required when `generate: false`. |
| `key_file` | `""` | PEM-encoded private key matching `cert_file`. |
| `require_client_cert` | `false` | Set `true` to enforce mTLS. `tls.Config.ClientAuth = tls.RequireAndVerifyClientCert`. |
| `client_ca_file` | `""` | PEM bundle of CAs allowed to sign client certs. When empty and `require_client_cert` is `true`, the system CA pool is used. |

`MinVersion = tls.VersionTLS13` is always enforced. The WebTransport gateway additionally overrides this in its cloned TLS config so older TLS versions cannot silently downgrade.

The QUIC listener advertises the ALPN identifier `aqueduct-v1`. Cluster mesh peers use `aqueduct-mesh`. WebTransport uses `h3`. See [Protocol §6](protocol-spec.md).

---

## 3. `aal` — Append-Only Log (Encrypted Persistence)

```yaml
aal:
  enabled: bool           # AQUEDUCT_AAL_ENABLED  (-aal sets enabled=true and configures file_path)
  file_path: string       # AQUEDUCT_AAL_FILE_PATH  (-aal)
  key: string             # AQUEDUCT_AAL_KEY
  max_aal_size: int64     # AQUEDUCT_AAL_MAX_SIZE
  retention_period: string
  retention_size: int64
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `enabled` | `false` | When `false`, no AAL writes happen and no replay is attempted at startup. |
| `file_path` | `""` | Required when `enabled: true`. File is opened with mode `0600` (operator-only). |
| `key` | `""` | Base64-encoded 32-byte AES-256 key, or 32 raw bytes. Empty key → unencrypted AAL (still append-only, but plaintext on disk). |
| `max_aal_size` | `104857600` (100 MB) | **Declared only — not enforced in v1.16.0.** `aal.Log.Rotate(maxSize, key)` exists but is currently never called from any production code path, so the AAL file grows unbounded. Apply OS-level rotation (see [Production Deployment §2](production-deployment.md)). |
| `retention_period` | `"24h"` | **Declared only — not enforced.** No in-process scheduler reads this field. Use external rotation to enforce time-based retention. |
| `retention_size` | `1073741824` (1 GB) | **Declared only — not enforced.** No in-process scheduler reads this field. Use external rotation to enforce size-based retention. |

Replay happens **before** the QUIC listener binds. `aal.Replay` walks the file, decrypts records with the same key, and feeds `CmdPublish` frames to `Router.Publish` so durable subscribers and consumer-group offsets are restored.

> **Back up the key separately from the file.** Losing the key makes the AAL file unreadable.

---

## 4. `acl` — Authorization

```yaml
acl:
  enabled: bool           # AQUEDUCT_ACL_ENABLED
  default: string         # AQUEDUCT_ACL_DEFAULT  ("none" or "all")
  rules:
    - client: string      # CN of the client cert
      topic: string       # exact topic or MQTT wildcard (+ / #)
      permission: string  # "publish", "subscribe", "all", "none"
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `enabled` | `false` | When `false`, no authorization check runs and every command is accepted. |
| `default` | `"none"` | Applied when no rule matches the `(client, topic)` tuple. |
| `rules[].client` | — | Common Name from the client's TLS cert. |
| `rules[].topic` | — | Exact topic or wildcard (`sensor/+/temp`, `sensor/#`). |
| `rules[].permission` | — | `publish`, `subscribe`, `all`, or `none`. |

Rules are hashed with the **non-commutative FNV-1a** `CombineHashes(clientID, topicBytes)` and stored in `authz.Engine.rulesPtr`. The hot-path check (`authz.Engine.Allowed`) is lock-free via `atomic.Pointer` — see [Architecture §5](architecture.md).

Hot-reload via gRPC Admin API: `UpdateACL` (`internal/admin/proto/admin.proto`) replaces the rule map atomically. See [Admin API Reference](admin-api.md).

---

## 5. `broker` — Async Fan-Out, Backpressure, Coalesced Writes

```yaml
broker:
  queue_size: int                  # AQUEDUCT_BROKER_QUEUE_SIZE
  backpressure_policy: string      # AQUEDUCT_BROKER_BACKPRESSURE_POLICY
  batch_size: int                  # AQUEDUCT_BROKER_BATCH_SIZE
  flush_interval: duration         # AQUEDUCT_BROKER_FLUSH_INTERVAL
  max_retries: int                 # AQUEDUCT_BROKER_MAX_RETRIES
  priority_ttls: [string, ...]     # (no env override; edit YAML)
  quotas:
    default_publish_rate: int      # AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE
    default_burst_size: int        # AQUEDUCT_BROKER_DEFAULT_BURST_SIZE
    per_client:
      "<client_cn>":
        rate: int
        burst: int
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `queue_size` | `1024` | Per-subscriber bounded channel capacity. Larger queues reduce `drop_oldest` / `drop_newest` triggers but increase memory. |
| `backpressure_policy` | `"drop_oldest"` | `drop_oldest`, `drop_newest`, or `disconnect`. See [Production Deployment §3](production-deployment.md). |
| `batch_size` | `65536` (64 KB) | Coalesced write threshold for subscriber Writer goroutines. |
| `flush_interval` | `50us` | Micro-timer flush interval — bounds latency under low load. |
| `max_retries` | `3` | NACK retry count before a message is routed to `__dlq__<topic>`. |
| `priority_ttls[0..3]` | `nil` (no built-in default) | Per-priority TTL override as `[]string` of 4 durations. **Built-in default is `nil`** — when unset, no per-priority TTL is applied and messages do not expire from this mechanism. The table cell above shows a *configuration example*, not the real default. `"0"` or empty disables the override for that priority. |
| `quotas.default_publish_rate` | `0` | Default publish rate limit (msg/s). `0` = unlimited. |
| `quotas.default_burst_size` | `1000` | Default burst capacity per tenant bucket. |
| `quotas.per_client` | none | Map of `client CN → {rate, burst}`. |

---

## 6. `transport` — Buffer Limits

```yaml
transport:
  max_buf_size: int     # AQUEDUCT_TRANSPORT_MAX_BUF_SIZE
  read_buf_size: int    # AQUEDUCT_TRANSPORT_READ_BUF_SIZE
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `max_buf_size` | `65536` (64 KB) | Maximum buffer before `prepareFrame` rejects with `errOversizedPayload`. Also bounds the per-stream grow cap. |
| `read_buf_size` | `1024` | Initial per-stream read buffer. Doubles up to `max_buf_size` as the buffer fills. |

A separate hard cap `maxPayloadSize = 1 << 20` (1 MB) lives in `internal/broker/router.go` and rejects oversized publishes at the router.

---

## 7. `admin` — gRPC Control Plane

```yaml
admin:
  enabled: bool      # AQUEDUCT_ADMIN_ENABLED
  addr: string       # AQUEDUCT_ADMIN_ADDR
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `enabled` | `false` | Starts the gRPC Admin server on `admin.addr`. |
| `addr` | `":9091"` | TCP listen address. The server reuses the broker's TLS config; clients must present a cert whose CN starts with `admin-`. |

Two RPCs are exposed: `SetClientQuota` and `UpdateACL`. See [Admin API Reference](admin-api.md).

---

## 8. `cluster` — Peers, Discovery, Mesh TLS

```yaml
cluster:
  peers: [string, ...]                # static peer list
  discovery:
    enabled: bool                     # AQUEDUCT_CLUSTER_DISCOVERY_ENABLED
    type: string                      # "dns" (only option)
    host: string                      # AQUEDUCT_CLUSTER_DISCOVERY_HOST
    port: string                      # AQUEDUCT_CLUSTER_DISCOVERY_PORT
    interval: string                  # AQUEDUCT_CLUSTER_DISCOVERY_INTERVAL
  mesh:
    insecure_skip_verify: bool        # AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY
    ca_file: string                   # AQUEDUCT_CLUSTER_MESH_CA_FILE
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `peers` | `[]` | Static `host:port` list. Drained in parallel by `PeerManager` reconnect goroutines. |
| `discovery.enabled` | `false` | Enables `internal/cluster/discovery.go` DNS polling. |
| `discovery.type` | `"dns"` | Only DNS discovery is supported. |
| `discovery.host` | `""` | Headless Service FQDN, e.g. `aqueduct-headless.default.svc.cluster.local`. |
| `discovery.port` | `""` (extracted from `listen_addr` if empty, else `"4242"`) | Port suffix appended to resolved IPs. |
| `discovery.interval` | `"10s"` | Poll interval. Accepts any `time.ParseDuration` string. |
| `mesh.insecure_skip_verify` | `false` | **G402.** Disables peer certificate verification. Default is **secure**. Setting `true` logs a startup warning and is **not safe in production**. |
| `mesh.ca_file` | `""` | PEM CA bundle for verifying peer certificates. When empty and `insecure_skip_verify` is `false`, the system CA pool is used. |

The mesh listener uses ALPN `aqueduct-mesh`. Each peer has its own reconnect goroutine with exponential backoff (`250ms → 30s`). See [Cluster Mesh TLS](cluster-mesh-tls.md) for the hardening checklist.

---

## 9. `tracing` — OpenTelemetry

```yaml
tracing:
  enabled: bool           # AQUEDUCT_TRACING_ENABLED
  service_name: string    # AQUEDUCT_TRACING_SERVICE_NAME
  endpoint: string        # AQUEDUCT_TRACING_ENDPOINT
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `enabled` | `false` | When `false`, the tracer is a zero-cost nil wrapper (~3.4 ns on the hot path). |
| `service_name` | `"aqueduct-broker"` | Used as the OTel `service.name` attribute. |
| `endpoint` | `"localhost:4317"` | OTLP gRPC endpoint. |

When enabled, the broker exports W3C Trace Context through the `ExtTraceContext = 0x01` TLV extension. See [Architecture §12](architecture.md).

---

## 10. `compression` — ZSTD Payload Compression

```yaml
compression:
  enabled: bool
  min_batch_size: int     # bytes
  level: int              # ZSTD level
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `enabled` | `false` | When `true`, `CmdPublishBatch` payloads exceeding `min_batch_size` are ZSTD-compressed before peer forwarding. **Local subscribers always receive the uncompressed payload.** |
| `min_batch_size` | `1024` | Threshold below which compression is skipped. Smaller batches rarely benefit. |
| `level` | `0` | `0` = ZSTD default, `1` = fastest, `3` = default. |

The receiver's cap is `16 × transport.max_buf_size`. Decoded batches above this are rejected.

---

## 11. `webtransport` — Browser Gateway

```yaml
webtransport:
  enabled: bool          # AQUEDUCT_WEBTRANSPORT_ENABLED
  listen_addr: string    # AQUEDUCT_WEBTRANSPORT_LISTEN_ADDR
  path_prefix: string    # AQUEDUCT_WEBTRANSPORT_PATH_PREFIX
```

| Key | Default | Notes |
| :--- | :--- | :--- |
| `enabled` | `false` | Start the HTTP/3 + WebTransport gateway. |
| `listen_addr` | `":4433"` | Distinct UDP port from `listen_addr`. Must not collide. |
| `path_prefix` | `"/aqueduct/wt"` | URL path clients send Extended CONNECT to. |

The gateway reuses the broker's `*tls.Config` and forces `MinVersion = tls.VersionTLS13` and `NextProtos` includes `h3`. The handshake timeout (`WithHandshakeTimeout(...)`) defaults to `10s`. See [Production Deployment §9 / Getting Started §6](getting-started.md) for browser setup.

---

## 12. Environment Variable Cheat-Sheet

Every env override (verified against `internal/config/applyEnvOverrides`):

```text
AQUEDUCT_LISTEN_ADDR
AQUEDUCT_METRICS_ADDR

AQUEDUCT_TLS_GENERATE
AQUEDUCT_TLS_CERT_FILE
AQUEDUCT_TLS_KEY_FILE
AQUEDUCT_TLS_REQUIRE_CLIENT_CERT
AQUEDUCT_TLS_CLIENT_CA_FILE

AQUEDUCT_AAL_ENABLED
AQUEDUCT_AAL_FILE_PATH
AQUEDUCT_AAL_KEY
AQUEDUCT_AAL_MAX_SIZE

AQUEDUCT_ACL_ENABLED
AQUEDUCT_ACL_DEFAULT

AQUEDUCT_ADMIN_ENABLED
AQUEDUCT_ADMIN_ADDR

AQUEDUCT_BROKER_QUEUE_SIZE
AQUEDUCT_BROKER_BACKPRESSURE_POLICY
AQUEDUCT_BROKER_BATCH_SIZE
AQUEDUCT_BROKER_FLUSH_INTERVAL
AQUEDUCT_BROKER_MAX_RETRIES
AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE
AQUEDUCT_BROKER_DEFAULT_BURST_SIZE

AQUEDUCT_TRACING_ENABLED
AQUEDUCT_TRACING_SERVICE_NAME
AQUEDUCT_TRACING_ENDPOINT

AQUEDUCT_TRANSPORT_MAX_BUF_SIZE
AQUEDUCT_TRANSPORT_READ_BUF_SIZE

AQUEDUCT_CLUSTER_DISCOVERY_ENABLED
AQUEDUCT_CLUSTER_DISCOVERY_HOST
AQUEDUCT_CLUSTER_DISCOVERY_PORT
AQUEDUCT_CLUSTER_DISCOVERY_INTERVAL
AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY
AQUEDUCT_CLUSTER_MESH_CA_FILE

AQUEDUCT_COMPRESSION_ENABLED

AQUEDUCT_WEBTRANSPORT_ENABLED
AQUEDUCT_WEBTRANSPORT_LISTEN_ADDR
AQUEDUCT_WEBTRANSPORT_PATH_PREFIX
```

> **Tip:** `priority_ttls`, `cluster.peers`, `acl.rules`, and `broker.quotas.per_client` are arrays / maps and are only editable in `config.yaml` — they have no single-value env override.

---

## 13. CLI Flags

The broker accepts these flags (`cmd/broker/main.go`):

| Flag | Effect |
| :--- | :--- |
| `-config <path>` | YAML config file to load before applying env overrides. |
| `-addr <host:port>` | Overrides `listen_addr`. |
| `-metrics-addr <host:port>` | Overrides `metrics_addr`. |
| `-cert <path>` | Sets `tls.cert_file` and disables `tls.generate`. |
| `-key <path>` | Sets `tls.key_file` and disables `tls.generate`. |
| `-aal <path>` | Enables AAL and sets `aal.file_path`. |

The benchmark tool (`cmd/aqueduct-bench/main.go`) accepts `-addr`, `-c`, `-n`, `-size`, `-topic`, `-timeout`, `-batch`, `-tls-verify`, and `-ca-file` — see its `main.go` for the authoritative list.