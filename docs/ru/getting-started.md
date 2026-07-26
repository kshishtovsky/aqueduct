# Руководство: Быстрый старт с Aqueduct (v1.8.0)

Это пошаговое руководство поможет вам установить, настроить и запустить мессендж-брокер Aqueduct.

---

## Требования

- **Go 1.23+**
- **Docker & Docker Compose** (опционально)

---

## 1. Запуск через Docker Compose

```bash
docker compose up -d
```

- **Health check**: `http://localhost:9090/healthz`
- **Prometheus**: `http://localhost:9091`
- **Grafana**: `http://localhost:3000` (Логин: `admin`, Пароль: `admin`)

---

## 2. Конфигурация (`config.yaml`)

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
  key: "dGhpcyBpcyBhIDMyIGJ5dGUgYWVzLTI1NiBrZXkh" # Base64 ключ 32 байта
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
  backpressure_policy: "drop_oldest" # "drop_oldest", "drop_newest" или "disconnect"
  batch_size: 65536
  flush_interval: 50us
  max_retries: 3
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

## 3. Wildcard-топики и TTL

### Примеры Wildcard
- `sensor/+/temp`: Совпадает с `sensor/room1/temp` и `sensor/room2/temp`.
- `sensor/#`: Совпадает со всеми подтопиками `sensor/`.

### Формат сообщения с TTL
Публикация сообщения с TTL 500 мс:
- Укажите payload: `"ttl:500:sensor/room1/temp"`
- Сообщение дропается при задержке в очереди свыше 500 мс.

---

## 4. NACK и Dead Letter Queue (DLQ)

Подписчики могут отправить NACK (Negative Acknowledgement) для сообщения по смещению с помощью команды `CmdNack` (0x05). Брокер автоматически выполняет повторную доставку (до `max_retries`), после чего сообщение направляется в очередь недоставленных сообщений `__dlq__<topic>`.
