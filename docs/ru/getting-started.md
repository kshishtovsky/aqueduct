# Руководство: Быстрый старт с Aqueduct (v1.14.0)

Это пошаговое руководство поможет вам установить, настроить и запустить мессендж-брокер Aqueduct.

---

## Требования

- **Go 1.23+**
- **Docker & Docker Compose** (опционально)
- **Kubernetes 1.28+** с `kubectl` и `helm` (опционально, для K8s развёртывания)

---

## 1. Запуск через Docker Compose

```bash
docker compose up -d
```

- **Health check**: `http://localhost:9090/healthz`
- **Prometheus**: `http://localhost:9091`
- **Grafana**: `http://localhost:3000` (Логин: `admin`, Пароль: `admin`)

---

## 2. Быстрый старт в Kubernetes (v1.14.0)

Развёртывание 3-репликового кластера с DNS-обнаружением пиров:

```bash
helm install aqueduct deploy/helm/aqueduct \
  --set replicaCount=3 \
  --set config.cluster.discovery.enabled=true \
  --set config.cluster.discovery.host="aqueduct-headless.default.svc.cluster.local" \
  --set config.cluster.discovery.port=4242
```

Проверка кластера:

```bash
kubectl get pods -l app.kubernetes.io/name=aqueduct
kubectl logs statefulset/aqueduct --tail=10 -f
```

Масштабирование:

```bash
kubectl scale statefulset aqueduct --replicas=5
```

Raw-манифесты (без Helm):

```bash
kubectl apply -f deploy/k8s/
```

---

## 3. Конфигурация (`config.yaml`)

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
  priority_ttls:
    - "500ms"  # Приоритет 0 (Наивысший/Критический)
    - "5s"     # Приоритет 1 (Высокий)
    - "0"      # Приоритет 2 (Обычный)
    - "0"      # Приоритет 3 (Низкий)
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

## 4. Очереди приоритетов (QoS), Per-Priority TTL и Wildcards

### TLV Расширение приоритета (`ExtPriority = 0x03`)
- Поддерживается 4 уровня приоритета (`0` Наивысший до `3` Низший).
- Критические данные (Приоритет 0) передаются в сетевой сокет вне очереди, опережая трафик с более низким приоритетом.

### Per-Priority TTL
- Задается через массив `priority_ttls` в `config.yaml`.
- При застревании сообщения в очереди дольше указанного срока оно автоматически уничтожается при вычитке (`0 allocs/op`).

### Примеры Wildcard
- `sensor/+/temp`: Совпадает с `sensor/room1/temp` и `sensor/room2/temp`.
- `sensor/#`: Совпадает со всеми подтопиками `sensor/`.

---

## 5. NACK и Dead Letter Queue (DLQ)

Подписчики могут отправить NACK (Negative Acknowledgement) для сообщения по смещению с помощью команды `CmdNack` (0x05). Брокер автоматически выполняет повторную доставку (до `max_retries`), после чего сообщение направляется в очередь недоставленных сообщений `__dlq__<topic>`.
