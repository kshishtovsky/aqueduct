# 参考: Admin API (gRPC Control Plane, v1.12.0+)

本文档详述 Aqueduct 的 gRPC Admin API（独立控制平面）— 用于在运行时动态热重载 Quotas 与 ACL 规则，**不中断**消息处理。

> [!NOTE]
> Diátaxis 分类：**Reference** — 完整 RPC 方法、消息类型与认证机制。

---

## 1. 端点

默认监听于 `:9091`（通过 `admin.addr` 配置）。启用后立即通过 `internal/admin/server.go` 启动 gRPC 服务器。

```yaml
admin:
  enabled: true
  addr: ":9091"
```

---

## 2. 协议

```protobuf
syntax = "proto3";

package admin;

option go_package = "github.com/kshishtovsky/aqueduct/internal/admin/proto;adminpb";

service AdminService {
  rpc SetClientQuota(SetClientQuotaRequest) returns (SetClientQuotaResponse);
  rpc UpdateACL(UpdateACLRequest) returns (UpdateACLResponse);
}

message SetClientQuotaRequest {
  string client_id = 1;
  int64 rate = 2;
}

message SetClientQuotaResponse {
  bool success = 1;
}

message ACLRule {
  string client_id = 1;
  string topic = 2;
  string permission = 3;
}

message UpdateACLRequest {
  repeated ACLRule rules = 1;
}

message UpdateACLResponse {
  bool success = 1;
  int32 rules_count = 2;
}
```

完整源：`internal/admin/proto/admin.proto`。

---

## 3. 认证 (强制 mTLS)

> [!CAUTION]
> Admin API **强制 mTLS**。客户端必须提供有效的 TLS 证书，且证书 Subject Common Name (CN) 必须以 `admin-` 开头。否则 `adminAuthInterceptor` 返回 `codes.PermissionDenied`。

### 实现

```go
// internal/admin/server.go:178
func adminAuthInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
        p, ok := peer.FromContext(ctx)
        if !ok || p.AuthInfo == nil {
            return nil, status.Error(codes.Unauthenticated, "missing peer authentication context")
        }
        tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
        if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
            return nil, status.Error(codes.Unauthenticated, "missing client TLS certificate")
        }
        cn := tlsInfo.State.PeerCertificates[0].Subject.CommonName
        if !strings.HasPrefix(cn, "admin-") {
            return nil, status.Errorf(codes.PermissionDenied, "access denied: client CN %q does not have admin role", cn)
        }
        return handler(ctx, req)
    }
}
```

### 客户端示例 (Go)

```go
package main

import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "os"

    adminpb "github.com/kshishtovsky/aqueduct/internal/admin/proto"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials"
)

func main() {
    // 加载 admin 客户端证书 (CN 必须以 "admin-" 开头)
    cert, err := tls.LoadX509KeyPair("/etc/certs/admin.pem", "/etc/certs/admin-key.pem")
    if err != nil {
        panic(err)
    }

    // 加载 broker CA
    caPEM, _ := os.ReadFile("/etc/certs/ca.pem")
    pool := x509.NewCertPool()
    pool.AppendCertsFromPEM(caPEM)

    tlsConf := &tls.Config{
        Certificates: []tls.Certificate{cert},
        RootCAs:      pool,
        MinVersion:   tls.VersionTLS13,
        ServerName:   "broker",
    }

    conn, err := grpc.NewClient(":9091", grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
    if err != nil {
        panic(err)
    }
    defer conn.Close()

    client := adminpb.NewAdminServiceClient(conn)
    ctx := context.Background()

    // 调用 SetClientQuota
    resp, err := client.SetClientQuota(ctx, &adminpb.SetClientQuotaRequest{
        ClientId: "service-a",
        Rate:     5000,
    })
    if err != nil {
        panic(err)
    }
    _ = resp
}
```

---

## 4. RPC 方法

### 4.1 `SetClientQuota`

热重载指定客户端的令牌桶速率（msg/s）。`Bucket.rate` 是 `atomic.Int64`，因此**热路径零锁**。

**请求**：

```protobuf
message SetClientQuotaRequest {
  string client_id = 1;  // 客户端 ID（来自 mTLS 证书 CN 或其他身份）
  int64 rate = 2;       // 每秒消息数（0 = 无限）
}
```

**响应**：

```protobuf
message SetClientQuotaResponse {
  bool success = 1;
}
```

**实现** (`internal/admin/server.go:105`)：

```go
func (s *Server) SetClientQuota(ctx context.Context, req *adminpb.SetClientQuotaRequest) (*adminpb.SetClientQuotaResponse, error) {
    metrics.AdminRequestsTotal.WithLabelValues("SetClientQuota").Inc()
    callerCN := s.extractCallerCN(ctx)
    s.logger.Info("admin API: SetClientQuota",
        "caller", callerCN,
        "client_id", req.GetClientId(),
        "rate", req.GetRate(),
    )
    if req.GetClientId() == "" {
        return nil, status.Error(codes.InvalidArgument, "client_id cannot be empty")
    }
    if s.quotaManager != nil {
        s.quotaManager.SetRate(req.GetClientId(), int(req.GetRate()), 0)
    }
    return &adminpb.SetClientQuotaResponse{Success: true}, nil
}
```

**校验**：
- `client_id` 非空（否则 `codes.InvalidArgument`）。
- 调用方 CN 以 `admin-` 开头（否则 `codes.PermissionDenied`）。

**热重载路径**：
- `quotas.Manager.SetRate(clientID, rate, burst)` 将 `Bucket.rate` 原子更新。
- `Router.Publish` 每次发布时调用 `quotaManager.Allow(clientID)`，在热路径上读取 `rate` 而无需加锁。

**基准测试**：`BenchmarkACLHotReload` (重建 10,000 条规则) **876 µs/op**。

---

### 4.2 `UpdateACL`

热重载整套 ACL 规则集。通过 **Read-Copy-Update (RCU)** 模式原子替换 `authz.Engine.rulesPtr`，**热路径零锁**。

**请求**：

```protobuf
message ACLRule {
  string client_id = 1;
  string topic = 2;       // 支持 MQTT 通配符 (+ 与 #)
  string permission = 3;  // "publish", "subscribe", "all", "none"
}

message UpdateACLRequest {
  repeated ACLRule rules = 1;
}
```

**响应**：

```protobuf
message UpdateACLResponse {
  bool success = 1;
  int32 rules_count = 2;
}
```

**实现** (`internal/admin/server.go:127`)：

```go
func (s *Server) UpdateACL(ctx context.Context, req *adminpb.UpdateACLRequest) (*adminpb.UpdateACLResponse, error) {
    metrics.AdminRequestsTotal.WithLabelValues("UpdateACL").Inc()
    callerCN := s.extractCallerCN(ctx)
    s.logger.Info("admin API: UpdateACL",
        "caller", callerCN,
        "rules_count", len(req.GetRules()),
    )
    if s.authzEngine == nil {
        return nil, status.Error(codes.Unavailable, "authorization engine is not enabled")
    }

    newRules := make(map[uint64]authz.Permission, len(req.GetRules()))
    for _, r := range req.GetRules() {
        var perm authz.Permission
        switch strings.ToLower(strings.TrimSpace(r.GetPermission())) {
        case "publish":
            perm = authz.PermPublish
        case "subscribe":
            perm = authz.PermSubscribe
        case "all":
            perm = authz.PermAll
        case "none":
            perm = authz.PermNone
        default:
            perm = authz.PermNone
        }
        key := authz.CombineHashStrings(r.GetClientId(), r.GetTopic())
        newRules[key] |= perm
    }

    // Hot-Reload RCU: swap rules map pointer atomically.
    s.authzEngine.Reload(newRules)

    return &adminpb.UpdateACLResponse{
        Success:    true,
        RulesCount: int32(len(newRules)),
    }, nil
}
```

**校验**：
- 调用方 CN 以 `admin-` 开头（否则 `codes.PermissionDenied`）。
- `authz` 引擎必须已启用（否则 `codes.Unavailable`）。
- 无效 `permission` 字符串静默映射为 `PermNone`。

**热重载路径**：
- `authz.Engine.Reload(newRules)` 通过 `e.rulesPtr.Store(&newRules)` 原子替换 map 指针。
- `authz.Engine.Allowed(clientID, topic, perm)` 通过 `rulesPtr.Load()` 读取，热路径无锁。

**基准测试**：`BenchmarkACLCheck` (Lock-Free RCU 热路径) **14.51 ns/op, 0 allocs/op**。

---

## 5. gRPC 状态码

| Code | 触发条件 |
| :--- | :--- |
| `codes.OK` | 成功 |
| `codes.InvalidArgument` | `client_id` 为空 |
| `codes.Unauthenticated` | 缺少对等身份验证或客户端 TLS 证书 |
| `codes.PermissionDenied` | 客户端 CN 不以 `admin-` 开头 |
| `codes.Unavailable` | ACL 引擎未启用（`UpdateACL`） |

---

## 6. 审计日志

每次调用通过 `slog` 输出结构化日志：

```
INFO  admin API: SetClientQuota caller=admin-operator client_id=service-a rate=5000
INFO  admin API: UpdateACL caller=admin-operator rules_count=42
```

拒绝时：

```
WARN  admin API access denied: invalid common name cn=user-bob method=/admin.AdminService/SetClientQuota
```

---

## 7. 指标

| 指标 | 标签 | 说明 |
| :--- | :--- | :--- |
| `aqueduct_admin_requests_total` | `method` | Admin API 请求总数（`SetClientQuota`、`UpdateACL`） |

**示例 PromQL**：

```promql
# Admin API 速率
sum by (method) (rate(aqueduct_admin_requests_total[5m]))
```

---

## 8. 客户端工具

### grpcurl

```bash
grpcurl -cert admin.pem -key admin-key.pem -cacert ca.pem \
  -import-path internal/admin/proto -proto admin.proto \
  broker.local:9091 admin.AdminService/SetClientQuota
```

### gRPC CLI (Go)

使用 `internal/admin/proto/adminpb` 生成的客户端存根（参见上文示例）。

---

## 9. 安全建议

> [!CAUTION]
> 切勿将 Admin API 暴露于公网。仅在内网或经由 SSH 隧道访问。

- [ ] **专用 CA**: 为 Admin 客户端签发独立证书（区别于 broker CA）。
- [ ] **CN 前缀审计**: 定期审计 `admin-*` 证书持有者。
- [ ] **限流**: 在前置代理（Envoy/Nginx）设置每 IP RPC 速率限制。
- [ ] **审计日志**: 通过 Loki 或 ELK 收集 `slog` 输出以追溯配置变更。
- [ ] **多管理员**: 不同管理员使用不同 CN（如 `admin-network-ops`、`admin-security`），便于审计追踪。