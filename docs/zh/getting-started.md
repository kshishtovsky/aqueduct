# 教程: Aqueduct 入门指南 (v1.14.0)

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
