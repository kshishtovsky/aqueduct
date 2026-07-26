# 教程: Aqueduct 入门指南 (v1.3.0)

本教程指导您如何安装、配置和运行 Aqueduct 消息代理。

---

## 环境要求

- **Go 1.22+**
- **Docker & Docker Compose** (可选)

---

## 1. Docker Compose 快速开始

```bash
docker compose up -d
```

- **健康检查**: `http://localhost:9090/healthz`
- **Prometheus**: `http://localhost:9091`
- **Grafana**: `http://localhost:3000` (User: `admin`, Password: `admin`)

---

## 2. 配置文件 (`config.yaml`)

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

transport:
  max_buf_size: 65536
  read_buf_size: 1024
```

---

## 3. 使用消息 TTL 与通配符

### 通配符示例
- `sensor/+/temp`: 匹配 `sensor/room1/temp` 和 `sensor/room2/temp`。
- `sensor/#`: 匹配 `sensor/` 下的所有主题。

### 消息 TTL 格式
发送 TTL 为 500 毫秒的消息:
- 消息载荷设置为: `"ttl:500:sensor/room1/temp"`
- 如果队列中的延迟超过 500 毫秒，消息在网络发送前自动丢弃。
