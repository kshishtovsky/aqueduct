# 指南: 生产部署与安全 (v1.16.0)

本指南详述在企业生产环境中安全部署 Aqueduct 的最佳实践。

> [!NOTE]
> Diátaxis 分类：**How-to** — 解决具体问题，遵循操作步骤。

---

## 1. 传输安全与 mTLS 配置

生产环境必须强制 **mTLS 1.3** 并提供企业 CA 证书：

```yaml
tls:
  generate: false
  cert_file: "/etc/certs/server.crt"
  key_file: "/etc/certs/server.key"
  require_client_cert: true
  client_ca_file: "/etc/certs/client_ca.pem"
```

`cmd/broker/main.go` 应用 `MinVersion: tls.VersionTLS13`。若未设置 `client_ca_file`，`RequireAndVerifyClientCert` 将使用**系统 CA 池**（Kubernetes 路径下默认为 `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`）。

---

## 2. 加密追加日志 (AAL) 与轮转

### 密钥生成

```bash
openssl rand -base64 32
```

### 配置

```yaml
aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "<BASE64_32_BYTE_KEY>"
  max_aal_size: 104857600 # 已声明但当前不会自动触发轮转
```

### 行为

- **Replay**: Broker 启动时按顺序流式重放历史 `CmdPublish` 帧到路由器，**早于**绑定 UDP 监听端口。
- **加密**: AES-256-GCM，12 字节 Nonce (`[4 字节随机会话前缀 | 8 字节单调计数器]`)。日志文件模式 `0600`。
- **轮转**: 当文件大小 ≥ `max_aal_size` 时调用 `Rotate(maxSize, key)`，原地通过 `Replay` + 替换完成（自动密钥派生）。
- **指标**: `aqueduct_aal_replay_duration_seconds`（Gauge）、`aqueduct_aal_rotations_total`（Counter）。

> [!WARNING]
> AAL 文件可能包含敏感发布载荷。当 `aal.key` 非空时文件与密钥共存于运维配置中 — 限制文件系统权限 (`chmod 600`)。

---

## 3. 慢消费者 Backpressure 调优

根据应用需求选择 backpressure 隔离策略：

- `drop_oldest`: 实时遥测（丢弃旧数据，保留最新）。
- `drop_newest`: 顺序事件流（保留顺序）。
- `disconnect`: 安全严格环境（强制驱逐慢订阅者）。

```yaml
broker:
  queue_size: 2048
  backpressure_policy: "drop_oldest"
  batch_size: 65536
  flush_interval: 50us
  max_retries: 3
  priority_ttls:
    - "500ms"
    - "5s"
    - "0"
    - "0"
  quotas:
    default_publish_rate: 100
    default_burst_size: 1000
```

---

## 4. 操作系统 UDP 限制

提高 OS UDP 缓冲区容量：

```bash
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
ulimit -n 65536
```

---

## 5. 集群部署 (v1.8.0+)

部署多个 Aqueduct broker 组成直接网格（ALPN `aqueduct-mesh`），实现水平扩展。每个 broker 连接到所有其他 broker。

| 节点 | 地址 | 角色 |
|------|------|------|
| Broker A | `192.168.1.10:4242` | Peer |
| Broker B | `192.168.1.11:4242` | Peer |
| Broker C | `192.168.1.12:4242` | Peer |

### 配置

每个节点列出**其他**节点（不包含自身）：

```yaml
cluster:
  peers:
    - "192.168.1.10:4242"
    - "192.168.1.11:4242"
    - "192.168.1.12:4242"
  mesh:
    insecure_skip_verify: false
    ca_file: "/etc/certs/mesh-ca.pem"
```

### 转发行为

- 任意节点上发布的消息转发到所有对等节点。
- MeshForwarded 位（第 7 位，掩码 `0x80`）防止转发循环。
- 无共识或 Leader 选举 — 网格完全去中心化。
- 对等连接使用**独立的** `*tls.Config`（ALPN `aqueduct-mesh`），验证策略由 `cluster.mesh.insecure_skip_verify` 控制。

### 网络要求

- 所有节点必须在配置的端口上通过 UDP 可达。
- 每个节点必须配置自己的 TLS 证书/密钥。
- 3+ 节点拓扑中，每个节点必须列出所有其他对等节点。
- 不保证跨节点的消息顺序（fire-and-forget）。

---

## 6. Kubernetes 部署 (v1.14.0+)

### 为什么选择 Kubernetes？

静态对等列表 (`cluster.peers`) 需要手动协调 — 每个节点必须预先知晓其他节点。Kubernetes StatefulSet 配合 Headless Service 提供**基于 DNS 的动态对等发现**，无需外部依赖（Consul、etcd）。

### Helm Chart（推荐）

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

### Headless Service（DNS 发现必需）

Headless Service (`clusterIP: None`) 为每个 StatefulSet Pod 返回 A 记录：

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

Pod DNS 模式：
- `aqueduct-0.aqueduct-headless.<namespace>.svc.cluster.local`
- `aqueduct-1.aqueduct-headless.<namespace>.svc.cluster.local`
- ...

### DNS 发现

启用发现后，broker 通过 `internal/cluster/discovery.go` 轮询 Headless Service DNS 记录，每 `interval` 与已知 IP 差集对比：

```go
ips, err := net.LookupHost("aqueduct-headless.default.svc.cluster.local")
```

- **扩容**: 新 Pod IP 通过 `AddPeer()` 自动连接。
- **缩容**: 移除的 IP 通过 `RemovePeer()` 断开。
- **零停机时间**: 重连使用指数退避（250 ms → 30 s 上限）。

### 配置

```yaml
cluster:
  peers: []  # 空 — discovery 自动填充
  discovery:
    enabled: true
    type: "dns"
    host: "aqueduct-headless.default.svc.cluster.local"
    port: "4242"
    interval: "10s"
```

### 扩缩容

```bash
# 扩容到 5 个副本
kubectl scale statefulset aqueduct --replicas=5

# 滚动重启（零停机升级）
kubectl rollout restart statefulset aqueduct
```

### 原始 Kubernetes 清单

非 Helm 用户，原始清单在 `deploy/k8s/`：

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

---

## 7. NACK/DLQ 生产配置

配置 NACK 重投与死信队列：

- 在 `config.yaml` 设置 `max_retries`（默认 3）或通过 `AQUEDUCT_BROKER_MAX_RETRIES` 覆盖。
- DLQ 主题遵循 `__dlq__<original_topic>` 模式。
- 监控 `aqueduct_messages_nacked_total{topic}` 与 `aqueduct_messages_dead_lettered_total{topic}`。
- 连接订阅者到 `__dlq__*` 主题进行离线检查。

---

## 8. Admin API 热重载 (v1.12.0+)

启用独立 gRPC Admin 服务器（默认 `:9091`），在运行时热重载 Quotas 与 ACL 规则，**不中断**消息处理：

```yaml
admin:
  enabled: true
  addr: ":9091"
```

> [!IMPORTANT]
> Admin API 强制 mTLS（继承 broker 的 `*tls.Config`，ALPN `aqueduct-v1`）。客户端证书 CN 必须以 `admin-` 开头 — 否则 `adminAuthInterceptor` 返回 `PermissionDenied`。

**RPC 方法**：

| 方法 | 用途 |
| :--- | :--- |
| `SetClientQuota(clientID, rate)` | 热重载每客户端令牌桶速率 |
| `UpdateACL(rules)` | 热重载 ACL 规则集 (RCU) |

详见 `docs/zh/admin-api.md`。

---

## 9. 速率限制配额 (v1.8.0+)

配置每租户速率限制：

- 在 `config.yaml` 设置 `broker.quotas.default_publish_rate` 与 `broker.quotas.default_burst_size`。
- 每客户端覆盖: `broker.quotas.per_client.<client_id>`。
- 监控 `aqueduct_messages_rate_limited_total{client}` 指标。
- 通过 `Admin.SetClientQuota(clientID, rate, burst)` 在运行时热重载 — `Bucket.rate` 是 `atomic.Int64`，零锁热路径重载。

完整 YAML 示例见第 3 节。

---

## 10. OpenTelemetry Tracing (v1.9.0+)

```yaml
tracing:
  enabled: true
  service_name: "aqueduct-broker"
  endpoint: "localhost:4317"
```

启用时初始化 `otlptracegrpc` 批量导出器，连接到 OTLP gRPC 接收器。禁用时（默认）所有 `StartSpan` 调用返回零开销 `func() {}` 结束回调。

**Span 名**：`aqueduct.process` (本地发布)、`aqueduct.forward` (mesh 转发)。

**指标**：`aqueduct_tracing_spans_total`。

---

## 11. 安全检查清单

部署前确认：

- [ ] **TLS**: `tls.generate: false`，提供企业 CA 签名证书。
- [ ] **mTLS**: `tls.require_client_cert: true` 用于客户端身份验证。
- [ ] **WebTransport**: 启用时使用受信证书（非自签）。
- [ ] **AAL**: 生产启用加密；保护密钥与文件（`chmod 600`）。
- [ ] **ACL**: 启用并设置显式规则（`acl.default: "none"`）。
- [ ] **Admin API**: 启用时 mTLS 强制 CN 以 `admin-` 开头。
- [ ] **集群网格 TLS**: `cluster.mesh.insecure_skip_verify: false`，提供 `cluster.mesh.ca_file` 或依赖系统 CA。
- [ ] **速率限制**: 设置 `default_publish_rate` 与 `default_burst_size` 防止 DoS。
- [ ] **监控**: Prometheus 抓取 `/metrics`，Grafana 仪表盘包含 `aqueduct_messages_rate_limited_total`、`aqueduct_messages_dead_lettered_total`、`aqueduct_cluster_peers_active`。
- [ ] **UDP 防火墙**: 开放配置的 broker 端口（默认 4242/UDP）；WebTransport 端口（默认 4433/UDP）。
- [ ] **Backpressure**: 选择适合应用的策略（默认 `drop_oldest`）。
- [ ] **NACK/DLQ**: 设置 `max_retries`，监控 DLQ 指标。