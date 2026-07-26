# 指南: 生产部署与安全 (v1.5.0)

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
```

---

## 4. 操作系统 UDP 限制

```bash
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
ulimit -n 65536
```

---

## 5. 集群部署 (v1.5.0+)

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
