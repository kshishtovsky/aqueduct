# 指南: 集群网格 TLS 配置 (v1.16.0+)

本文档详述 Aqueduct 集群（P2P Federation）网格连接（ALPN `aqueduct-mesh`）的 TLS 配置，包括生产部署的证书签发、最佳实践与安全警告。

> [!NOTE]
> Diátaxis 分类：**How-to** — 解决"如何为集群网格正确配置 mTLS"的具体问题。

---

## 1. 默认行为（v1.16.0）

**v1.16.0 之前**：集群网格连接隐式使用 `tls.Config{InsecureSkipVerify: true}`。这使生产网格易受 MITM 攻击。

**v1.16.0+**：新增 `cluster.mesh` 配置块，默认 `insecure_skip_verify: false`。生产部署必须保持 `false` 并提供受信 CA。

> [!WARNING]
> **`insecure_skip_verify: true` 仅用于自签开发网格**。在可能被攻击者嗅探的网络（任何共享基础设施）上启用即视为漏洞。生产环境必须设为 `false`。

---

## 2. 配置 Schema

```go
// internal/config/config.go:53
type MeshConfig struct {
    InsecureSkipVerify bool   `yaml:"insecure_skip_verify"` // G402 默认 false
    CAFile             string `yaml:"ca_file"`              // PEM CA bundle
}
```

### 完整 YAML

```yaml
cluster:
  peers:
    - "broker-a.internal:4242"
    - "broker-b.internal:4242"
    - "broker-c.internal:4242"
  discovery:
    enabled: false
    type: "dns"
    host: ""
    port: "4242"
    interval: "10s"
  mesh:
    insecure_skip_verify: false  # 生产保持 false
    ca_file: "/etc/aqueduct/mesh-ca.pem"  # PEM bundle
```

### 环境变量覆盖

| 变量 | 覆盖项 |
| :--- | :--- |
| `AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY` | `cluster.mesh.insecure_skip_verify` |
| `AQUEDUCT_CLUSTER_MESH_CA_FILE` | `cluster.mesh.ca_file` |

---

## 3. 证书生命周期

### 3.1 自签开发网格（仅本地测试）

```bash
# 1. 生成 mesh CA
openssl genrsa -out mesh-ca.key 4096
openssl req -new -x509 -days 365 -key mesh-ca.key -out mesh-ca.crt \
  -subj "/CN=Aqueduct Mesh Dev CA"

# 2. 为每个 broker 生成证书
for n in broker-a broker-b broker-c; do
  openssl genrsa -out ${n}.key 2048
  openssl req -new -key ${n}.key -out ${n}.csr \
    -subj "/CN=${n}"
  openssl x509 -req -in ${n}.csr -CA mesh-ca.crt -CAkey mesh-ca.key \
    -CAcreateserial -out ${n}.crt -days 365 \
    -extfile <(echo "subjectAltName=DNS:${n},IP:10.0.0.1")
done

# 3. 合并 CA 为 PEM bundle（用于 cluster.mesh.ca_file）
cat mesh-ca.crt > /etc/aqueduct/mesh-ca.pem
```

### 3.2 证书分发

| 文件 | 部署位置 |
| :--- | :--- |
| `mesh-ca.crt` (`ca_file`) | 所有 broker 节点 |
| `<broker>.crt`, `<broker>.key` | 各自 broker 节点（broker 配置中的 `tls.cert_file`/`tls.key_file`） |

> [!NOTE]
> Aqueduct **broker TLS 证书** (`tls.cert_file`, ALPN `aqueduct-v1`) 与 **网格 CA** (`mesh.ca_file`, ALPN `aqueduct-mesh`) 是分开的。两者可使用同一 CA 签名，但生产部署建议为 broker 客户端和 broker-对等流量使用不同 CA。

---

## 4. ALPN 与握手

| 端点 | ALPN | 验证方 |
| :--- | :--- | :--- |
| 原生 QUIC 客户端 | `aqueduct-v1` | `tls.require_client_cert` |
| 集群网格（broker-to-broker） | `aqueduct-mesh` | `cluster.mesh.insecure_skip_verify` / `RootCAs` |
| WebTransport (浏览器) | `h3` | `tls.require_client_cert` (可选) |

```go
// cmd/broker/main.go:160
peerTLS := &tls.Config{
    InsecureSkipVerify: cfg.Cluster.Mesh.InsecureSkipVerify,
    NextProtos:         []string{"aqueduct-mesh"},
}
```

> [!IMPORTANT]
> `NextProtos: ["aqueduct-mesh"]` 与 broker TLS 的 `NextProtos: ["aqueduct-v1"]` 互不干扰 — 每个 `*quic.Config` 使用各自的 `*tls.Config`。

---

## 5. CA 池解析

`cmd/broker/main.go` 按以下顺序解析 CA：

1. **`cluster.mesh.ca_file`** — 显式提供的 PEM bundle（优先）。
2. **系统 CA 池** — `x509.SystemCertPool()`（若 `ca_file` 为空且 `insecure_skip_verify: false`）。
3. **空池** — `x509.NewCertPool()`（若系统池为 `nil`，在 musl/Alpine 上偶发）。

```go
if cfg.Cluster.Mesh.CAFile != "" {
    caPEM, err := os.ReadFile(cfg.Cluster.Mesh.CAFile)
    caPool := x509.NewCertPool()
    caPool.AppendCertsFromPEM(caPEM)
    peerTLS.RootCAs = caPool
} else {
    systemPool, err := x509.SystemCertPool()
    peerTLS.RootCAs = systemPool
}
```

### 多个 CA

将所有受信 CA 追加到单个 PEM 文件：

```bash
# 内部 mesh CA + 公共 CA（若对等证书由公共 CA 签名）
cat internal-mesh-ca.crt public-ca.crt > /etc/aqueduct/mesh-ca.pem
```

---

## 6. 安全场景

### 6.1 Kubernetes StatefulSet + Headless Service（推荐）

1. 通过 cert-manager 或外部 CA 为每个 broker 节点签发证书，SAN 包含节点 FQDN。
2. 将 `mesh-ca.crt` 通过 ConfigMap 挂载到 `/etc/aqueduct/mesh-ca.pem`。
3. `cluster.mesh.ca_file: "/etc/aqueduct/mesh-ca.pem"`。
4. `cluster.mesh.insecure_skip_verify: false`。

```yaml
# ConfigMap 示例
apiVersion: v1
kind: ConfigMap
metadata:
  name: aqueduct-mesh-ca
data:
  mesh-ca.pem: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
```

### 6.2 静态 IP/主机名（VM/裸机）

1. 为每个 broker 节点签发证书，SAN 包含其 IP 与 DNS 名称。
2. 分发证书与 `mesh-ca.pem` 到所有节点。
3. 配置与 6.1 相同。

### 6.3 开发环境（自签）

```yaml
cluster:
  peers:
    - "127.0.0.1:4242"
    - "127.0.0.1:4243"
  mesh:
    insecure_skip_verify: true  # 仅开发
    ca_file: ""
```

> [!WARNING]
> 开发配置切勿进入生产。CI/CD 流水线应在合并前校验 `cluster.mesh.insecure_skip_verify == false`。

---

## 7. 故障排查

| 症状 | 可能原因 | 修复 |
| :--- | :--- | :--- |
| `cluster mesh TLS using system CA pool` 后对等连接失败 | 证书不由系统 CA 签发 | 设置 `cluster.mesh.ca_file` |
| `cluster mesh TLS verification disabled` 警告 | `insecure_skip_verify: true` | 设为 `false`，提供 `ca_file` |
| `failed to parse cluster mesh CA certificates from PEM` | PEM bundle 格式错误 | 检查 `ca_file` 是 PEM（含 `-----BEGIN CERTIFICATE-----`） |
| 对等日志显示 `certificate verify failed` | SAN 不匹配对等 FQDN | 重新签发证书，SAN 包含对等地址 |
| `aqueduct_cluster_peers_active == 0` | TLS 握手失败 | 启用 `slog` debug 级别查看握手错误 |
| 短暂连接（频繁 reconnect） | 证书过期或 SAN 错误 | 检查 `openssl x509 -in broker.crt -text -noout` |

---

## 8. 证书轮转

Aqueduct 不内置证书轮换。当对等证书过期时，`reconnectLoop` 在握手失败时按指数退避（250 ms → 30 s 上限）无限重试。

**手动轮转流程**：

1. 通过 cert-manager/外部 CA 签发新证书。
2. 将新证书部署到 ConfigMap/Secret。
3. 滚动重启 StatefulSet：`kubectl rollout restart statefulset aqueduct`。
4. 所有 broker 同时重启 — DNS 发现将自动重连。

---

## 9. 测试建议

### 单元测试

`internal/cluster/cluster_test.go` 使用 ALPN `aqueduct-mesh` 验证双向 mesh：

```go
serverTLS = &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"aqueduct-mesh"}}
clientTLS = &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"aqueduct-mesh"}}
```

### 集成测试

部署 3 节点网格并检查 `aqueduct_cluster_peers_active`：

```bash
kubectl exec -it aqueduct-0 -- curl -s http://localhost:9090/metrics | grep cluster_peers_active
# 预期: aqueduct_cluster_peers_active 2.0
```

---

## 10. 安全检查清单

- [ ] **`insecure_skip_verify: false`**：生产环境显式设置。
- [ ] **`ca_file` 已配置**：指向受信 CA bundle。
- [ ] **SAN 匹配**：对等证书 SAN 包含 broker FQDN/IP。
- [ ] **证书未过期**：监控 `notAfter` 字段。
- [ ] **私钥权限**：`chmod 600 <broker>.key`。
- [ ] **审计日志**：监控 `slog` 中的 TLS 握手失败。
- [ ] **mTLS 与 broker 客户端隔离**：客户端 CA 与 mesh CA 分离。