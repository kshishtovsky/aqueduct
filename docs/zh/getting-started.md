# 教程: Aqueduct 入门指南 (v1.16.0)

本教程指导您如何安装、配置、运行 Aqueduct 消息代理并与之交互。

> [!NOTE]
> Diátaxis 分类：**Tutorial** — 引导您完成从零到首个成功运行的步骤。

---

## 环境要求

- **Go 1.23+**（推荐 Go 1.25 stable）
- **Docker & Docker Compose**（可选）
- **Kubernetes 1.28+** 含 `kubectl` 和 `helm`（可选，用于 K8s 部署）
- **现代浏览器** 支持 WebTransport（浏览器客户端示例）：Chrome/Edge ≥ 97，Firefox ≥ 114，或 Safari ≥ 17.4

---

## 1. Docker Compose 快速开始

一键启动 Aqueduct broker、Prometheus 与 Grafana：

```bash
docker compose up -d
```

- **Broker 健康检查**: `http://localhost:9090/healthz`
- **Prometheus UI**: `http://localhost:9091`（容器内 `:9090` 映射到宿主机 `:9091`）
- **Grafana 仪表盘**: `http://localhost:3000` (User: `admin`, Password: `admin`)

停止：

```bash
docker compose down
```

---

## 2. Kubernetes 快速开始 (v1.14.0+)

部署 3 副本集群并启用 DNS 对等发现：

```bash
helm install aqueduct deploy/helm/aqueduct \
  --set replicaCount=3 \
  --set config.cluster.discovery.enabled=true \
  --set config.cluster.discovery.host="aqueduct-headless.default.svc.cluster.local" \
  --set config.cluster.discovery.port=4242
```

验证集群状态：

```bash
kubectl get pods -l app.kubernetes.io/name=aqueduct
kubectl logs statefulset/aqueduct --tail=10 -f
```

动态扩缩容：

```bash
kubectl scale statefulset aqueduct --replicas=5
```

原始清单（不用 Helm）：

```bash
kubectl apply -f deploy/k8s/
```

---

## 3. 配置文件 (`config.yaml`)

```yaml
listen_addr: ":4242"
metrics_addr: ":9090"

tls:
  generate: true
  cert_file: ""
  key_file: ""
  require_client_cert: false
  client_ca_file: ""

aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "dGhpcyBpcyBhIDMyIGJ5dGUgYWVzLTI1NiBrZXkh" # Base64 32 字节密钥
  max_aal_size: 104857600 # 已声明但当前不会自动触发轮转

acl:
  enabled: true
  default: "none"
  rules:
    - client: "sensor-service"
      topic: "sensor/#"
      permission: "publish"
    - client: "analytics-service"
      topic: "sensor/+/temp"
      permission: "subscribe"

admin:
  enabled: true
  addr: ":9091"

broker:
  queue_size: 1024
  backpressure_policy: "drop_oldest" # "drop_oldest", "drop_newest", 或 "disconnect"
  batch_size: 65536
  flush_interval: 50us
  max_retries: 3
  priority_ttls:
    - "500ms"  # 优先级 0 (最高)
    - "5s"     # 优先级 1 (高)
    - "0"      # 优先级 2 (普通 — 不覆盖)
    - "0"      # 优先级 3 (低 — 不覆盖)
  quotas:
    default_publish_rate: 0
    default_burst_size: 1000

transport:
  max_buf_size: 65536
  read_buf_size: 1024

tracing:
  enabled: false
  service_name: "aqueduct-broker"
  endpoint: "localhost:4317"

compression:
  enabled: false
  min_batch_size: 1024

webtransport:
  enabled: false
  listen_addr: ":4433"
  path_prefix: "/aqueduct/wt"

cluster:
  peers:
    - "192.168.1.10:4242"
    - "192.168.1.11:4242"
  mesh:
    insecure_skip_verify: false
    ca_file: ""
```

---

## 4. CLI 标志覆盖

`cmd/broker` 支持以下 CLI 标志覆盖 YAML 配置：

```bash
go run ./cmd/broker/main.go \
  -config config.yaml \
  -addr :4242 \
  -metrics-addr :9090 \
  -cert /etc/certs/cert.pem \
  -key /etc/certs/key.pem \
  -aal /var/log/aqueduct/aal.log
```

| 标志 | 说明 |
| :--- | :--- |
| `-config` | YAML 配置文件路径 |
| `-addr` | Broker UDP 监听地址（覆盖 `listen_addr`） |
| `-metrics-addr` | 指标 HTTP 服务器地址（覆盖 `metrics_addr`） |
| `-cert` | TLS 证书文件（隐式禁用 `tls.generate`） |
| `-key` | TLS 私钥文件（隐式禁用 `tls.generate`） |
| `-aal` | 启用 AAL 并指向指定路径 |

---

## 5. QoS 优先级队列、按优先级 TTL 与通配符

### 优先级 TLV 扩展 (`ExtPriority = 0x03`)
- 优先级范围 `0`（最高/紧急）到 `3`（最低/批量）。
- 关键消息（优先级 0）在订阅者 Writer 队列中**优先**于普通消息发送。

### 按优先级 TTL
- 通过 `config.yaml` 中的 `priority_ttls` 数组配置；没有对应的环境变量。
- 内置默认值为 `nil`，即不设置按优先级 TTL。只有显式配置后，优先级 `P` 的消息才继承 `priority_ttls[P]`；若队列延迟超过 TTL，消息会在出队时丢弃。

### 通配符订阅示例
- `sensor/+/temp`: 匹配 `sensor/room1/temp` 与 `sensor/room2/temp`。
- `sensor/#`: 匹配 `sensor/` 下所有子主题。

---

## 6. NACK 与死信队列 (DLQ)

订阅者可以通过 `CmdNack` (`0x06`) 操作码按偏移量发送 NACK。Payload 为 8 字节 Little-Endian uint64 消息偏移量。Broker 自动重投消息（最多 `max_retries` 次），之后消息路由到死信队列 `__dlq__<topic>`。

---

## 7. WebTransport（浏览器连接）(v1.16.0+)

Aqueduct 内置可选的 HTTP/3 + WebTransport 网关 (ALPN `h3`)，允许浏览器通过 W3C [WebTransport API](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport) 连接。网关复用 Broker 的 mTLS 证书 (ALPN `aqueduct-v1`)，因此单一份根证书同时保护原生客户端与浏览器客户端。

### 7.1 在 `config.yaml` 中启用网关

```yaml
tls:
  generate: false                  # WebTransport 必须关闭 — 浏览器拒绝裸自签证书。
  cert_file: "/etc/aqueduct/fullchain.pem"
  key_file:  "/etc/aqueduct/privkey.pem"

webtransport:
  enabled: true
  listen_addr: ":4433"            # 与 broker.listen_addr 不同的 UDP 端口
  path_prefix: "/aqueduct/wt"     # 客户端发往此路径的 Extended CONNECT
```

本地开发推荐 [`mkcert`](https://github.com/FiloSottile/mkcert)：

```bash
mkcert -install
mkcert localhost 127.0.0.1 ::1
# → localhost+2.pem / localhost+2-key.pem
```

接着把 `tls.cert_file` / `tls.key_file` 指向这些文件。

### 7.2 启动 Broker

```bash
go run ./cmd/broker -config config.yaml
# INFO  webtransport gateway started addr=:4433 path_prefix=/aqueduct/wt
```

### 7.3 浏览器示例

```bash
cd examples/web
go run -mod=mod - <<'EOF'
package main
import ("log"; "net/http")
func main() {
    log.Fatal(http.ListenAndServeTLS(":8443",
        "/path/to/localhost+2.pem",
        "/path/to/localhost+2-key.pem",
        http.FileServer(http.Dir("."))))
}
EOF
```

打开 `https://localhost:8443/index.html`。点击 **Connect** 建立 WebTransport 会话，然后点击 **Open Subscribe Stream** — 来自任意客户端（浏览器、原生 Go、Node.js）的消息都会出现在事件日志中。

### 7.4 浏览器帧格式

与原生 QUIC 客户端完全相同：10 字节首部 `[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]`。Magic = `0x1F`，`CmdSubscribe = 0x02`，`CmdPublish = 0x01`。完整实现见 `examples/web/app.js`（`buildFrame`/`parseFrame`）。

### 7.5 0-RTT

WebTransport 基于 HTTP/3，基于 QUIC，支持 0-RTT。Broker 默认在 QUIC 监听器上启用 `Allow0RTT: true`，`MaxIncomingStreams: 100`。浏览器在持有先前连接的 session ticket 时协商 0-RTT — 首请求与 QUIC 握手落在同一 RTT 内。浏览器透明验证网关证书 — 无需手动步骤。

### 7.6 生产清单（浏览器客户端）

- 使用受信证书（Let's Encrypt 或内部 CA）。
- 证书 SAN 列表包含客户端使用的主机名（例如 `broker.example.com`）。
- 防火墙开放 UDP/443（或配置的端口）— 浏览器仅通过 UDP（不是 TCP）协商 WebTransport。
- 仅在浏览器能出示客户端证书时启用 mTLS；否则设置 `tls.require_client_cert: false`（网关继承 broker 的 TLS 配置）。