# 教程: Aqueduct 入门指南 (v1.16.0)

本教程指导您如何安装、配置和运行 Aqueduct 消息代理。

---

## 环境要求

- **Go 1.23+**
- **Docker & Docker Compose** (可选)
- **Kubernetes 1.28+** 含 `kubectl` 和 `helm`（可选，用于 K8s 部署）

---

## 1. Docker Compose 快速开始

```bash
docker compose up -d
```

- **健康检查**: `http://localhost:9090/healthz`
- **Prometheus**: `http://localhost:9091`
- **Grafana**: `http://localhost:3000` (User: `admin`, Password: `admin`)

---

## 2. Kubernetes 快速开始 (v1.14.0)

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
  key: "dGhpcyBpcyBhIDMyIGJ5dGUgYWVzLTI1NiBrZXkh"
  max_aal_size: 104857600

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

broker:
  queue_size: 1024
  backpressure_policy: "drop_oldest" # "drop_oldest", "drop_newest", 或 "disconnect"
  batch_size: 65536
  flush_interval: 50us
  max_retries: 3
  priority_ttls:
    - "500ms"  # 优先级 0 (最高/紧急)
    - "5s"     # 优先级 1 (高)
    - "0"      # 优先级 2 (普通)
    - "0"      # 优先级 3 (低)
  quotas:
    default_publish_rate: 0
    default_burst_size: 1000

transport:
  max_buf_size: 65536
  read_buf_size: 1024

cluster:
  peers:
    - "192.168.1.10:4242"
    - "192.168.1.11:4242"
```

---

## 4. QoS 优先级队列、按优先级 TTL 与通配符

### 优先级 TLV 扩展 (`ExtPriority = 0x03`)
- 支持 `0`（最高/紧急）到 `3`（最低）共 4 个优先级。
- 紧急消息（优先级 0）在订阅者 Writer 队列中优先发送，超越普通消息。

### 按优先级 TTL (Per-Priority TTL)
- 通过 `config.yaml` 中的 `priority_ttls` 数组配置。
- 堆积超过指定 TTL 的旧消息在出队时被自动延迟丢弃 (`0 allocs/op`)。

### 通配符示例
- `sensor/+/temp`: 匹配 `sensor/room1/temp` 和 `sensor/room2/temp`。
- `sensor/#`: 匹配 `sensor/` 下的所有主题。

---

## 5. NACK 与死信队列 (DLQ)

订阅者可以通过 `CmdNack` (0x05) 操作码按偏移量对消息发送 NACK（否定确认）。代理会自动重新投递消息（最多 `max_retries` 次），之后消息将被路由到死信队列 `__dlq__<topic>`。

---

## 6. WebTransport（浏览器连接）(v1.16.0+)

Aqueduct 内置可选的 HTTP/3 + WebTransport 网关，允许浏览器通过 W3C [WebTransport API](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport) 连接。网关复用 Broker 的 mTLS 证书，因此单一份根证书同时保护原生客户端与浏览器客户端。

### 6.1 在 `config.yaml` 中启用网关

```yaml
tls:
  generate: false                  # WebTransport 必须关闭 —— 浏览器拒绝纯自签证书。
  cert_file: "/etc/aqueduct/fullchain.pem"
  key_file:  "/etc/aqueduct/privkey.pem"

webtransport:
  enabled: true
  listen_addr: ":4433"            # 与 broker.listen_addr 不同的 UDP 端口
  path_prefix: "/aqueduct/wt"     # 客户端发往此路径的 Extended CONNECT
```

本地开发推荐用 [`mkcert`](https://github.com/FiloSottile/mkcert)：

```bash
mkcert -install
mkcert localhost 127.0.0.1 ::1
# → localhost+2.pem / localhost+2-key.pem
```

接着把 `tls.cert_file` / `tls.key_file` 指向这些文件。

### 6.2 启动 Broker

```bash
go run ./cmd/broker -config config.yaml
# INFO  webtransport gateway started addr=:4433 path_prefix=/aqueduct/wt
```

### 6.3 浏览器示例

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

### 6.4 浏览器中的帧格式

与原生 QUIC 客户端完全相同：10 字节首部 `[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]`。Magic = `0x1F`，`CmdSubscribe = 0x02`，`CmdPublish = 0x01`。完整实现见 `examples/web/app.js`（`buildFrame`/`parseFrame`）。

### 6.5 生产部署清单

- 使用受信证书（Let's Encrypt 或内部 CA）。
- 证书 SAN 列表包含客户端使用的主机名（例如 `broker.example.com`）。
- 防火墙开放 UDP/443（或配置端口）。
- 如果客户端没有客户端证书，请保留 `tls.require_client_cert: false` —— 网关继承 broker 的 TLS 策略。
