# How-to: Troubleshooting (v1.16.0)

This guide maps common Aqueduct symptoms to root causes and fixes. It is **problem-oriented**: start from the symptom you observe, follow the diagnostic step, then apply the remediation. For the underlying architecture see [Architecture & Memory Model](architecture.md).

---

## 1. Broker Will Not Start

### Symptom: `failed to load TLS certificate and key`

```text
ERROR failed to load TLS certificate and key err=open /etc/aqueduct/cert.pem: no such file or directory
```

**Cause.** `tls.generate: false` but `tls.cert_file` / `tls.key_file` are missing or unreadable.

**Fix.** Either point `cert_file` / `key_file` at readable PEM files, or set `tls.generate: true` for ephemeral dev certs (never in production).

### Symptom: `failed to start metrics server`

```text
ERROR failed to start metrics server addr=:9090
```

**Cause.** Another process is bound to `metrics_addr`, or the user lacks permission to bind a low-numbered port.

**Fix.** Override with `-metrics-addr :9090` or `AQUEDUCT_METRICS_ADDR=:19090`. Check `lsof -iTCP:9090`.

### Symptom: `AAL encryption key must be 32 bytes`

```text
ERROR AAL encryption key must be 32 bytes (base64 encoded or raw)
```

**Cause.** `aal.key` was provided but is not a 32-byte AES-256 key (neither base64-decoded to 32 bytes nor exactly 32 raw bytes).

**Fix.** Generate with `openssl rand -base64 32` and paste the result into `aal.key` (with or without whitespace). Confirm with `echo -n '<key>' | base64 -d | wc -c` — must print `32`.

### Symptom: `failed to read cluster mesh CA file`

```text
ERROR failed to read cluster mesh CA file path=/etc/aqueduct/mesh-ca.pem
```

**Cause.** `cluster.mesh.ca_file` set but the file is missing or malformed.

**Fix.** Verify the PEM is readable and contains at least one `BEGIN CERTIFICATE` block. If you don't need a custom CA, leave `ca_file: ""` and let the broker fall back to the system pool — but only after confirming that the system pool actually includes your peer CA.

---

## 2. Connection / mTLS Problems

### Symptom: clients connect but get rejected with auth failures

**Diagnose.** Tail the broker log for `authz denied` or `stream read error`:

```bash
curl -s http://localhost:9090/metrics | grep aqueduct_authz_denied_total
```

**Cause.** Either:

- `acl.enabled: true` and the client's CN does not match any rule, or `acl.default: "none"`.
- A TLS handshake succeeded but the client cert CN is empty (likely `client_ca_file` did not include the signing CA, so the broker treats the peer as `anonymous`).

**Fix.** Add an ACL rule:

```yaml
acl:
  enabled: true
  default: "none"
  rules:
    - client: "<client-CN-from-cert>"
      topic: "<topic>"
      permission: "publish"   # or "subscribe" or "all"
```

Or relax the default:

```yaml
acl:
  default: "all"
```

### Symptom: `aqueduct_admin_requests_total{method="..."}` is rising with `PermissionDenied` log lines

**Cause.** Operators or services are connecting to `:9091` with a cert whose CN does not start with `admin-`.

**Fix.** Reissue the cert with CN `admin-<role>` (e.g. `admin-ops`) and add its signing CA to `tls.client_ca_file`. See [Admin API Reference](admin-api.md).

---

## 3. WebTransport Gateway

### Symptom: browser shows `net::ERR_CERT_AUTHORITY_INVALID` and refuses the WebTransport handshake

**Cause.** The broker is presenting the **ephemeral self-signed dev cert** because `tls.generate: true`. Browsers reject self-signed certs unconditionally.

**Fix.** Disable `tls.generate` and provide a publicly trusted (or locally trusted) cert. For development the simplest path is [`mkcert`](https://github.com/FiloSottile/mkcert):

```bash
mkcert -install
mkcert localhost 127.0.0.1 ::1
# produces localhost+2.pem / localhost+2-key.pem
```

Then:

```yaml
tls:
  generate: false
  cert_file: "/path/to/localhost+2.pem"
  key_file:  "/path/to/localhost+2-key.pem"
webtransport:
  enabled: true
```

### Symptom: `wt handshake timeout` log line + connection drop

**Cause.** The browser did not complete the Extended CONNECT within the gateway's handshake timeout (default `10s`, set via `WithHandshakeTimeout(...)`).

**Fix.** This usually indicates a misconfigured reverse proxy or browser-side issue. Check:

1. UDP/4433 (or the configured port) is open from the client.
2. No transparent proxy is rewriting HTTP/3 frames.
3. The client URL exactly matches `webtransport.path_prefix` (default `/aqueduct/wt`).

### Symptom: browser connects but writes are silently ignored

**Cause.** Browser is writing to a **client-initiated unidirectional** stream, which the gateway does not accept (v1.16.0 limitation — see the roadmap).

**Fix.** Open a **bidirectional** stream and use the same frame layout as a native QUIC client (`[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]`).

---

## 4. Publish / Subscribe Issues

### Symptom: subscriber never receives a published message

**Diagnose.** Publish a probe message and inspect:

```bash
curl -s http://localhost:9090/metrics | grep -E 'aqueduct_messages_(published|delivered)_total'
```

**Possible causes.**

1. **Topic key mismatch.** The publisher's payload and the subscriber's `CmdSubscribe` payload parse to different `topicHashKey` values. This was the v1.16.0 bug fix — `parsePublishTopic` and `parseSubscriptionPayload` must agree. Reproduce in isolation: if `topic:orders` (subscribed) and `orders` (published) both work but `topic:orders` (published) does not, you are on a build that pre-dates the fix.
2. **Subscriber disconnected.** `aqueduct_active_subscribers` is `0`. Inspect the subscriber's stream for `stream.CancelRead(1)` — typically caused by `errOversizedPayload` or `errBufferExceeded` (`transport.max_buf_size` is the bound).
3. **Backpressure dropping.** `aqueduct_messages_dropped_total` is rising. Either raise `broker.queue_size`, switch to `disconnect`, or speed up the subscriber.
4. **Wildcard mismatch.** The subscriber's pattern doesn't actually match. Test with an exact topic first.

### Symptom: `aqueduct_messages_rate_limited_total` is climbing for one client

**Cause.** The client's bucket is depleted. Either:

- The client genuinely publishes faster than its quota.
- The bucket is configured too tight (`broker.quotas.default_publish_rate` or `per_client.<cn>.rate`).

**Fix.** Raise the rate via the Admin API (`SetClientQuota` — see [Admin API Reference](admin-api.md)) or update the YAML and restart.

### Symptom: messages expire before delivery

**Diagnose.** `aqueduct_messages_expired_total{topic, priority}` is non-zero.

**Cause.** `broker.priority_ttls[priority]` is too aggressive, or the subscriber queue is so saturated that messages sit in the queue past their TTL.

**Fix.** Lengthen the TTL or scale out subscribers. The drop is lazy — once the Writer goroutine dequeues a stale message it is silently discarded, not delivered.

---

## 5. Cluster Mesh

### Symptom: `aqueduct_cluster_peers_active` stays at `0`

**Diagnose.** `journalctl -u aqueduct` for handshake errors:

```
ERROR cluster mesh TLS verification disabled ...
WARN  dns discovery: lookup failed host=...
```

**Possible causes.**

1. **`insecure_skip_verify: true` on some peers, `false` on others.** All peers must agree. Re-read [Cluster Mesh TLS §1](cluster-mesh-tls.md).
2. **Cert SAN mismatch.** Peer A's cert SAN list does not include the hostname / IP that Peer B dials. Verify with `openssl x509 -in peer.pem -noout -text | grep -A2 'Subject Alternative Name'`.
3. **DNS poisoning / no record.** `nslookup <headless-service>` from the broker pod returns no A records — usually a misconfigured Service or selector.
4. **UDP blocked.** `nc -uvz broker-b.internal 4242` from each host must succeed. Many cloud firewalls block UDP by default.

### Symptom: mesh forwards the same frame repeatedly (storm)

**Cause.** The `MeshForwarded` bit (Command byte `0x80`) is not being set or is being stripped by a forwarder that copies into a fresh buffer. This should never happen with the in-tree `PeerManager.Forward(...)` — it sets the bit **in place** before write and restores the byte after.

**Fix.** Verify you are running a v1.16.0 binary (the in-place mutation logic is in `internal/cluster/cluster.go`). If the issue persists, capture a wire trace and confirm the bit transitions.

### Symptom: DNS discovery churns every `interval`

**Cause.** The DNS record's TTL is shorter than `cluster.discovery.interval`, and the resolver returns IPs in a different order on each poll.

**Fix.** Set `interval` to at least `2 × dns_ttl`. For Kubernetes Headless Services, the TTL is the EndpointSlice refresh interval (default ~30 s), so `interval: "60s"` is a safer default.

---

## 6. AAL (Append-Only Log)

### Symptom: AAL replay fails on startup

```text
WARN  AAL replay encountered error err=...
```

**Cause.** The encryption key changed since the AAL file was written, or the file was corrupted mid-write.

**Fix.** Confirm `aal.key` matches the key that was in use when the AAL was written. If you rotated the key, the previous AAL is unreadable — keep backups of keys alongside backups of files.

### Symptom: AAL grows without bound

**Cause.** The retention settings are **not enforced** by any in-process scheduler in v1.16.0; `aal.Log.Rotate(maxSize, key)` exists but is **never called** from production code. The fields `aal.max_aal_size`, `aal.retention_period`, and `aal.retention_size` are declared and parsed but have no effect on file size or age. The AAL file grows until the disk is full.

**Fix.** You **must** use OS-level rotation. The broker will not rotate for you:

```bash
# /etc/logrotate.d/aqueduct
/var/log/aqueduct/aal.log {
  daily
  rotate 7
  compress
  missingok
  notifempty
  copytruncate
}
```

Future broker versions will honor `retention_period` and `retention_size` natively and call `aal.Log.Rotate(...)` from a scheduler. Do not rely on the values to take effect today.

---

## 7. Performance Regressions

### Symptom: throughput dropped after a config change

**Diagnose.** `aqueduct_frame_parse_duration_ns` is **registered but currently not observed** in v1.16.0 (it always returns `0`) — see [Metrics Reference §7](metrics.md). Use it only for forward-compatibility. For parser regressions today, instrument the broker externally (e.g. `pprof` CPU profile on `protocol.ParseFrame`) or check that small frames are not being buffered across reads after a `transport.max_buf_size` shrink.

**Fix.** Restore the previous buffer sizes; small reads kill throughput because every read crosses the QUIC stream boundary.

### Symptom: latency p99 grew after enabling compression

**Cause.** ZSTD compress + decompress cycles are CPU-bound. At low batch sizes the compression overhead outweighs the wire savings.

**Fix.** Raise `compression.min_batch_size` (e.g. `4096`) so only meaningful batches are compressed. Compression only applies to the peer-forwarded copy of `CmdPublishBatch` frames — local subscribers are unaffected.

### Symptom: tracing enabled drops throughput

**Diagnose.** `aqueduct_tracing_spans_total` is rising fast.

**Cause.** Every publish now allocates a span. OTel span creation is not free.

**Fix.** Sample at the OTel collector, not in the broker. Configure your collector's tail sampler to keep 1–10 % of traces.

---

## 8. Health & Readiness

| Probe | Endpoint | Expected |
| :--- | :--- | :--- |
| Liveness | `GET /healthz` | `200 OK` |
| Readiness | `GET /healthz` + scrape `/metrics` and confirm `up` | `aqueduct_active_subscribers` is non-zero only if you have subscribers |
| Prometheus scrape | `GET /metrics` | 200 with Prometheus text format |

On Kubernetes:

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 9090
  periodSeconds: 10
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /healthz
    port: 9090
  periodSeconds: 5
  failureThreshold: 2
```

The bundled `docker-compose.yml` already uses `/healthz` for the healthcheck — see the `wget --spider -q http://localhost:9090/healthz` line in [`docker-compose.yml`](../../docker-compose.yml).

---

## 9. Where to Get More Help

- **Architecture details** — [Architecture & Memory Model](architecture.md) explains why each component is built the way it is.
- **Configuration keys** — [Configuration Reference](configuration.md) lists every YAML key with its default, env override, and security implication.
- **Metrics** — [Metrics Reference](metrics.md) lists every Prometheus metric.
- **Admin API** — [Admin API Reference](admin-api.md) documents the gRPC control plane.
- **Protocol bytes** — [Protocol Specification](protocol-spec.md) is the authoritative byte-level reference.