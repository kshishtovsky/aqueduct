# 指南: 生产部署与安全 (v1.14.0)

本指南说明在生产环境中部署与加固 Aqueduct 消息代理的最佳实践。

---

## 1. 传输安全与 mTLS 配置

在生产环境中强制使用 **mTLS 1.3**:

```yaml
tls:
  generate: false
  cert_file: "/etc/certs/server.crt"
  key_file: "/etc/certs/server.key"
  require_client_cert: true
  client_ca_file: "/etc/certs/client_ca.pem"
```

---

## 2. 加密日志 (AAL) 与自动轮转

生成 32 字节 AES-256 加密密钥:

```bash
openssl rand -base64 32
```

配置 `config.yaml`:
```yaml
aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "<BASE64_32_BYTE_KEY>"
  max_aal_size: 104857600 # 100 MB 触发轮转
```

代理启动时自动重放 (Replay) 日志记录以恢复状态。

---

## 3. 慢消费者 Backpressure 调优

- `drop_oldest`: 适合实时遥测数据。
- `drop_newest`: 适合顺序事件流。
- `disconnect`: 安全严格环境，直接断开慢订阅者。

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

## 4. 操作系统 UDP 限制

```bash
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
ulimit -n 65536
```

---

## 5. 集群部署 (v1.8.0+)

部署多个 Aqueduct 代理组成直接网格以实现水平扩展。每个代理连接到所有其他代理。

| 节点 | 地址 | 角色 |
|------|------|------|
| Broker A | `192.168.1.10:4242` | Peer |
| Broker B | `192.168.1.11:4242` | Peer |
| Broker C | `192.168.1.12:4242` | Peer |

### 配置

每个节点列出**其他**节点（不包含自己）：

```yaml
cluster:
  peers:
    - "192.168.1.10:4242"
    - "192.168.1.11:4242"
    - "192.168.1.12:4242"
```

### 转发行为

- 在任何节点上发布的消息都会转发到所有对等节点
- MeshForwarded 位防止转发循环
- 无共识或领导者选举——网格完全去中心化
- 对等连接使用与客户端连接相同的 mTLS 配置

### 网络要求

- 所有节点必须在配置的端口上通过 UDP 可达
- 每个节点必须配置自己的 TLS 证书/密钥
- 对于 3+ 节点拓扑，每个节点必须列出所有其他对等节点
- 不保证跨节点的消息顺序（fire-and-forget）

---

## 6. Kubernetes 部署 (v1.14.0+)

### 为什么选择 Kubernetes？

静态对等列表 (`cluster.peers`) 需要手动协调。StatefulSet 配合 Headless Service 提供**基于 DNS 的动态对等发现**，无需外部依赖（Consul、etcd）。

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
- 等。

### DNS 发现

启用发现后，代理以 `interval` 间隔轮询 Headless Service DNS 记录并与已知 IP 进行对比：

```go
ips, err := net.LookupHost("aqueduct-headless.default.svc.cluster.local")
```

- **扩容**: 新 Pod IP 通过 `AddPeer()` 自动连接
- **缩容**: 移除的 IP 通过 `RemovePeer()` 断开
- **零停机时间**: 使用指数退避重连

### 配置

```yaml
cluster:
  peers: []  # 空 — discovery 自动填充
  discovery:
    enabled: true
    type: "dns"
    host: "aqueduct-headless.default.svc.cluster.local"
    port: 4242
    interval: "10s"
```

### 扩缩容

```bash
kubectl scale statefulset aqueduct --replicas=5
kubectl rollout restart statefulset aqueduct
```

### 原始 Kubernetes 清单

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

---

## 7. NACK/DLQ 生产配置

配置 NACK 重投递和死信队列:

- 在 `config.yaml` 中设置 `max_retries`（默认 3）或通过 `AQUEDUCT_BROKER_MAX_RETRIES` 环境变量
- DLQ 主题遵循 `__dlq__<original_topic>` 模式
- 监控 `aqueduct_messages_nacked_total` 和 `aqueduct_messages_dead_lettered_total` 指标
- 连接订阅者到 `__dlq__*` 主题以进行离线检查

---

## 8. 速率限制配额

配置按租户的速率限制:

- 在 `config.yaml` 中设置 `broker.quotas.default_publish_rate` 和 `broker.quotas.default_burst_size`
- 客户端覆盖: `broker.quotas.per_client.<client_id>`
- 监控 `aqueduct_messages_rate_limited_total` 指标
- 完整的配额配置块见第 3 节的 YAML 示例
