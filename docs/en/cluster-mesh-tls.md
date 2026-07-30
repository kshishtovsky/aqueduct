# How-to: Secure Cluster Mesh with TLS (v1.16.0)

This guide shows how to wire the Aqueduct cluster mesh with proper TLS so peer-to-peer forwarding cannot be intercepted or spoofed. It is **problem-oriented**: each section addresses a specific threat or operational need. For the underlying architecture see [Architecture §6](architecture.md); for the canonical configuration keys see [Configuration Reference](configuration.md).

---

## 1. The Default is Already Secure

Out of the box (`internal/config/config.go`):

```go
type MeshConfig struct {
    InsecureSkipVerify bool   // default false; G402
    CAFile             string // PEM bundle for peer verification
}
```

`PeerManager` dials peers via `quic.DialAddr(ctx, addr, peerTLS, peerQUIC)` with `NextProtos = []string{"aqueduct-mesh"}`. The mesh TLS config is built in `cmd/broker/main.go` from `cluster.mesh`:

- If `insecure_skip_verify: false` (default) and `ca_file: ""`, the system CA pool is loaded.
- If `insecure_skip_verify: false` and `ca_file: "/path/to/ca.pem"`, only that PEM bundle is trusted.
- If `insecure_skip_verify: true`, the broker logs a loud startup warning and skips all chain verification.

> **Never set `insecure_skip_verify: true` in production.** Anyone with network access to the mesh UDP port can inject arbitrary messages. The setting exists solely for self-signed dev meshes (e.g. local Docker Compose with a private CA).

---

## 2. Threat Model

| Threat | Mitigation |
| :--- | :--- |
| **Passive eavesdropping** on inter-broker traffic | TLS 1.3 with forward secrecy — every QUIC connection negotiates fresh keys. |
| **Active MITM** injecting forged frames | Peer certificate chain verification against a trusted CA. Set `ca_file` or use the system pool. |
| **Replay of forwarded frames** | The `MeshForwarded` bit (Command byte `0x80`) is set on every forwarded frame; receivers refuse to re-forward marked frames. |
| **Replay of an old session ticket (0-RTT abuse)** | `quic-go` rejects 0-RTT on the mesh by default; mesh ALPN is `aqueduct-mesh`, distinct from `aqueduct-v1` data traffic. |
| **Rogue node joining the mesh** | Only nodes holding a cert signed by `ca_file` (or a system CA) are accepted. Rotate CA + reissue to add / remove nodes. |
| **DNS poisoning injecting fake peer IPs** | The peer resolver uses the OS resolver; combine with DNSSEC where available. |

---

## 3. Recommended Certificate Topology

For most teams the simplest production topology is:

```
                    ┌─────────────────┐
                    │  Internal CA    │
                    │  (e.g. step-ca, │
                    │  Vault, CFSSL)  │
                    └────────┬────────┘
                             │ signs
        ┌────────────────────┼────────────────────┐
        │                    │                    │
   ┌────▼─────┐         ┌─────▼────┐         ┌─────▼────┐
   │ broker-a │         │ broker-b │         │ broker-c │
   │ cert+key │         │ cert+key │         │ cert+key │
   └──────────┘         └──────────┘         └──────────┘
```

- **One CA** for all peer certs. Pair its public cert as `cluster.mesh.ca_file` on every node.
- **One cert per broker.** Use a stable CN per node (`broker-a`, `broker-b`, …) and add the broker's FQDN and IP in the SAN list.
- **Short validity.** 90 days is typical; rotate via the same CA. Cert rotation requires only a broker restart — the `quic.Config` uses the latest `tls.Config.Certificates` slice on each new dial.
- **Separate from the data-plane cert.** The mesh ALPN is `aqueduct-mesh`; client data traffic uses `aqueduct-v1`. You can issue both from the same CA but using different certs reduces blast radius if either is compromised.

### Generating example certs with `step-ca`

```bash
# Provision the broker-a cert + key
step ca certificate "broker-a" broker-a.crt broker-a.key \
  --san broker-a.internal --san 10.0.0.10 \
  --provisioner "admin" --not-after 2160h

# Distribute the CA root to every broker
step ca root > ca.pem
```

Every broker gets `ca.pem` in `cluster.mesh.ca_file` and its own `broker-X.crt` / `broker-X.key` pair via `tls.cert_file` / `tls.key_file`.

---

## 4. Configuration

```yaml
tls:
  generate: false
  cert_file: "/etc/aqueduct/certs/broker-a.pem"
  key_file:  "/etc/aqueduct/certs/broker-a-key.pem"
  require_client_cert: true               # data plane mTLS
  client_ca_file: "/etc/aqueduct/ca/data-clients.pem"

cluster:
  peers:
    - "broker-b.internal:4242"
    - "broker-c.internal:4242"
  mesh:
    insecure_skip_verify: false            # explicit; never true in production
    ca_file: "/etc/aqueduct/ca/mesh-ca.pem"
```

Two CA bundles are used here:

- `tls.client_ca_file` — CA pool that signs **client** (publisher / subscriber) certs.
- `cluster.mesh.ca_file` — CA pool that signs **peer broker** certs.

You may use a single CA for both; splitting them limits the blast radius if either is compromised.

---

## 5. Verify It Works

After the brokers come up:

```bash
# Mutual handshake confirmation — every mesh peer should be reachable
aqueduct-broker$ journalctl -u aqueduct | grep "broker listening"
# INFO  broker listening addr=:4242
# INFO  cluster federation enabled (static peers) peers=[broker-b.internal:4242 broker-c.internal:4242]
# INFO  cluster mesh TLS using custom CA pool ca_file=/etc/aqueduct/ca/mesh-ca.pem

# Prometheus metric — should track the configured number of peers
curl -s http://localhost:9090/metrics | grep aqueduct_cluster_peers_active
# aqueduct_cluster_peers_active 2
```

If a peer is unreachable, `PeerManager.reconnectLoop` retries with exponential backoff (`250ms → 30s`) and increments `aqueduct_cluster_frames_forwarded_total` only on successful writes.

For DNS-discovered peers, scale the StatefulSet and watch the metric converge within `cluster.discovery.interval` (default `10s`):

```bash
kubectl scale stateset aqueduct --replicas=4
sleep 12
curl -s http://broker-0:9090/metrics | grep aqueduct_cluster_peers_active
```

---

## 6. Rotation & Revocation

The mesh TLS config is reloaded only at startup. To rotate certs:

1. Issue a new cert for the rotating node from the same CA.
2. Restart the broker (`SIGTERM` triggers graceful shutdown via `transport.Broker.Shutdown(ctx)`).
3. The `PeerManager` reconnects with the new cert on the next dial.

For zero-downtime rotation, run **two mesh CAs** in parallel during the rollover window:

1. Issue the new CA. Concat both old + new certs into `cluster.mesh.ca_file` on every node.
2. Reissue each broker's cert from the new CA.
3. Rolling restart the brokers; each new dial uses the new cert.
4. Once all brokers have restarted, drop the old CA from `cluster.mesh.ca_file`.
5. Restart again to apply the trim.

For short-lived rotations (≤ cert validity) the simpler "issue + restart" path is usually fine.

---

## 7. Hardening Checklist

- ✅ `cluster.mesh.insecure_skip_verify: false` (default).
- ✅ `cluster.mesh.ca_file` points at a PEM bundle with every active peer cert's signing CA.
- ✅ Broker cert SAN list includes the broker's FQDN and any DNS name other peers dial (`broker-a.internal`, etc.).
- ✅ Cert validity ≤ 90 days. Calendar reminders for rotation.
- ✅ System clock synchronized (NTP / chrony). TLS handshakes fail when clocks drift outside the cert validity window.
- ✅ UDP/4242 reachable between every pair of brokers. Verify with `nc -uvz broker-b.internal 4242` from each host.
- ✅ DNS discovery backed by a Headless Service **or** an A record set that is rotated together with the cluster.
- ❌ Never enable `insecure_skip_verify: true` outside dev / CI.
- ❌ Never reuse the data-plane client CA as the mesh CA. Separation of duties reduces blast radius.

---

## 8. Failure Modes

| Symptom | Likely Cause |
| :--- | :--- |
| `cluster mesh TLS verification disabled` log line on every start | `insecure_skip_verify: true`. Disable immediately. |
| `aqueduct_cluster_peers_active` stays at `0` | `ca_file` doesn't include the peer's signing CA, or SAN mismatch on the peer cert. Check `quic-go` handshake errors in the broker log. |
| Peers flap (connect → drop → reconnect) | DNS record flapping or upstream firewall tearing down idle QUIC sessions. Increase `quic.Config.MaxIdleTimeout` (currently `30s`) or pin the peer list statically. |
| Cert rotation leaves a node stranded | Old CA was removed from `ca_file` before the node restarted. Re-add temporarily or do a two-CA rollover (see §6). |
| `MeshForwarded` bit frames still arrive from peer | MeshForwarded is enforced at the broker layer (`Broker.handleForwardedFrame` dispatches via `Router.PublishFromPeer` / `Router.PublishBatch` without re-forwarding). Duplicates only happen if the same message is published locally on multiple brokers (a publish is forwarded once per peer). `aqueduct_cluster_frames_received_total` is incremented inside `Router.PublishFromPeer` and therefore counts only `CmdPublish` frames received from peers — `CmdPublishBatch` frames are routed through `Router.PublishBatch` and not counted; see [Metrics Reference §6](metrics.md). |

---

## 9. Reference Implementation

The mesh TLS path is implemented in `cmd/broker/main.go`:

```go
peerTLS := &tls.Config{
    InsecureSkipVerify: cfg.Cluster.Mesh.InsecureSkipVerify,
    NextProtos:         []string{"aqueduct-mesh"},
}

if cfg.Cluster.Mesh.InsecureSkipVerify {
    logger.Warn("cluster mesh TLS verification disabled ...")
} else if cfg.Cluster.Mesh.CAFile != "" {
    caPool := x509.NewCertPool()
    caPool.AppendCertsFromPEM(caPEM)
    peerTLS.RootCAs = caPool
} else {
    systemPool, _ := x509.SystemCertPool()
    peerTLS.RootCAs = systemPool
}
```

`PeerManager.reconnectLoop` (`internal/cluster/cluster.go`) dials via `quic.DialAddr(ctx, p.addr, pm.tlsConf, pm.quicConf)` and rejects any handshake error by waiting for the next backoff window — the broker never falls back to an insecure connection.