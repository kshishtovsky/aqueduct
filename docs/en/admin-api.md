# Reference: Admin API (gRPC, v1.16.0)

The Admin API is a dedicated gRPC control plane that lets operators mutate broker configuration without restarting the process or contending with the message hot path. This document is the formal API reference — **information-oriented**. For usage patterns see [Production Deployment §8](production-deployment.md); for the architectural rationale see [Architecture §5](architecture.md).

The Admin server is implemented in `internal/admin/server.go` and listens on `admin.addr` (default `:9091`). It reuses the broker's `*tls.Config` so the same mTLS infrastructure that protects data traffic protects control traffic.

---

## 1. Endpoint

```text
aqueduct-broker:9091   (configurable via admin.addr / AQUEDUCT_ADMIN_ADDR)
```

The server is **only** started when `admin.enabled: true`. Set the flag in YAML or via `AQUEDUCT_ADMIN_ENABLED=true`.

---

## 2. Transport & Authentication

- **mTLS required.** Every Admin client must present a client certificate during the QUIC + TLS handshake. The broker's `tls.Config` is reused — same `MinVersion = tls.VersionTLS13`, same CA pool.
- **Role-based CN enforcement.** Every Admin RPC is gated by `adminAuthInterceptor`, which inspects `peer.AuthInfo` (a `credentials.TLSInfo`) and verifies that the first peer certificate's `Subject.CommonName` starts with the literal prefix `admin-`. Certificates whose CN does not start with `admin-` receive `codes.PermissionDenied` and a structured `slog.Warn` line. The CN itself is logged as the `caller` field on every successful operation for audit.
- **Audit logging.** Every successful RPC emits a structured `slog.Info` line including the caller CN, the affected client / topic, and the new value.
- **Observability.** Each RPC increments `aqueduct_admin_requests_total{method}` so suspicious spikes surface in Prometheus.

> **Operational note.** The Admin server shares the broker's TLS cert. Issue a *separate* client certificate for operators with CN `admin-<role>` and add its signing CA to `tls.client_ca_file`. Do not reuse regular data-plane client certs.

---

## 3. Service Definition

Defined in `internal/admin/proto/admin.proto`:

```proto
syntax = "proto3";
package admin;

service AdminService {
  rpc SetClientQuota(SetClientQuotaRequest) returns (SetClientQuotaResponse);
  rpc UpdateACL(UpdateACLRequest)         returns (UpdateACLResponse);
}

message SetClientQuotaRequest {
  string client_id = 1;
  int64  rate      = 2;
}

message SetClientQuotaResponse {
  bool success = 1;
}

message ACLRule {
  string client_id   = 1;
  string topic       = 2;
  string permission  = 3;   // "publish" | "subscribe" | "all" | "none"
}

message UpdateACLRequest {
  repeated ACLRule rules = 1;
}

message UpdateACLResponse {
  bool  success     = 1;
  int32 rules_count = 2;
}
```

The generated Go code lives in `internal/admin/proto/admin.pb.go` and `admin_grpc.pb.go`. No regeneration is required for v1.16.0 — the proto schema is stable.

---

## 4. RPC Contracts

### 4.1 `SetClientQuota`

Dynamically updates the per-client token-bucket rate limit.

| Field | Type | Required | Notes |
| :--- | :--- | :--- | :--- |
| `client_id` | `string` | yes | Empty string → `codes.InvalidArgument`. |
| `rate` | `int64` | yes | New steady-state rate (msg/s). `0` disables limiting for this client. |

Response: `SetClientQuotaResponse { success: bool }`. Always `success: true` when the inputs are valid.

Side effects:

- `quotas.Manager.SetRate(client_id, rate, 0)` is called. Existing buckets are mutated atomically (`Bucket.rate.Store(int64(rate))`); new buckets are inserted into the RCU map (`atomic.Pointer[map[string]*Bucket]`).
- The publish hot path continues uninterrupted — `Manager.TryAcquire(clientID)` reads `bucketsPtr` lock-free.

Metrics: `aqueduct_admin_requests_total{method="SetClientQuota"}`.

### 4.2 `UpdateACL`

Atomically replaces the entire ACL rule set.

| Field | Type | Required | Notes |
| :--- | :--- | :--- | :--- |
| `rules` | `repeated ACLRule` | yes | Empty list clears the rule set (default permission still applies). |

Response: `UpdateACLResponse { success: bool, rules_count: int32 }`. `rules_count` reflects the number of unique `(client_id, topic)` composite keys.

Side effects:

- Each rule is hashed with `authz.CombineHashStrings(client_id, topic)` — the same FNV-1a composite hash the hot path uses.
- The new rule map is stored with `authz.Engine.Reload(...)` via `atomic.Pointer.Store(&newRules)`. The publish hot path's `Allowed(...)` call grabs the latest snapshot lock-free.
- Unknown permission strings are normalized to `PermNone`. Valid values: `"publish"`, `"subscribe"`, `"all"`, `"none"` (case-insensitive).

Metrics: `aqueduct_admin_requests_total{method="UpdateACL"}`.

---

## 5. Client Examples

### grpcurl

```bash
grpcurl -cacert /etc/aqueduct/ca.pem \
        -cert     /etc/aqueduct/admin-client.pem \
        -key      /etc/aqueduct/admin-client-key.pem \
        aqueduct-broker:9091 \
        admin.AdminService/SetClientQuota

grpcurl -cacert /etc/aqueduct/ca.pem \
        -cert     /etc/aqueduct/admin-client.pem \
        -key      /etc/aqueduct/admin-client-key.pem \
        -d '{
          "client_id": "service-a",
          "rate": 500
        }' \
        aqueduct-broker:9091 \
        admin.AdminService/SetClientQuota

grpcurl -cacert /etc/aqueduct/ca.pem \
        -cert     /etc/aqueduct/admin-client.pem \
        -key      /etc/aqueduct/admin-client-key.pem \
        -d '{
          "rules": [
            { "client_id": "service-a", "topic": "orders",   "permission": "publish" },
            { "client_id": "service-b", "topic": "orders",   "permission": "subscribe" },
            { "client_id": "analytics", "topic": "sensor/#", "permission": "subscribe" }
          ]
        }' \
        aqueduct-broker:9091 \
        admin.AdminService/UpdateACL
```

### Go

```go
conn, _ := grpc.Dial("aqueduct-broker:9091",
    grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
        Certificates: []tls.Certificate{adminClientCert},
        ServerName:   "aqueduct-broker",
        MinVersion:   tls.VersionTLS13,
    })),
)
defer conn.Close()

c := adminpb.NewAdminServiceClient(conn)
_, err = c.SetClientQuota(ctx, &adminpb.SetClientQuotaRequest{
    ClientId: "service-a",
    Rate:     500,
})
```

---

## 6. Error Mapping

| Condition | gRPC Code | Notes |
| :--- | :--- | :--- |
| Missing client TLS cert | `Unauthenticated` | `missing peer authentication context` / `missing client TLS certificate`. |
| CN does not start with `admin-` | `PermissionDenied` | `access denied: client CN "<cn>" does not have admin role`. |
| Empty `client_id` on `SetClientQuota` | `InvalidArgument` | `client_id cannot be empty`. |
| Auth engine not enabled (Admin server started but `acl.enabled: false`) | `Unavailable` | `authorization engine is not enabled`. |
| Internal cipher / IO failure | `Internal` | Only on rare allocation or TLS-state failures. |

All errors are returned as gRPC `status` errors, not Go panics.

---

## 7. What It Does *Not* Do

The Admin API is intentionally narrow. It does **not**:

- Reload the YAML config. Restart the broker for YAML changes.
- Toggle tracing on / off at runtime. Edit `tracing.enabled` and restart.
- Drain or stop the broker. Send `SIGTERM` for graceful shutdown (`internal/transport/broker.go` `Shutdown(ctx)`).
- Resize AAL retention windows. The keys (`aal.max_aal_size`, `aal.retention_period`, `aal.retention_size`) exist in `config.go` but **are not enforced in v1.16.0** — no scheduler calls `aal.Log.Rotate(...)`. Operators must rotate AAL externally (see [Production Deployment §2](production-deployment.md) and [Troubleshooting §6](troubleshooting.md)).
- Mutate mesh peer topology. Mesh membership is managed via `cluster.peers` (static) or `cluster.discovery` (DNS polling) only.