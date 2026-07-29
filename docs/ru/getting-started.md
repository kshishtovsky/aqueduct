# Руководство: Быстрый старт с Aqueduct (v1.16.0)

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

---

## 6. WebTransport (подключение из браузера) (v1.16.0+)

Aqueduct содержит опциональный шлюз HTTP/3 + WebTransport, который позволяет браузерам подключаться через W3C [WebTransport API](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport). Шлюз переиспользует mTLS-сертификат брокера — единый trust root для нативных и браузерных клиентов.

### 6.1 Включение шлюза в `config.yaml`

```yaml
tls:
  generate: false                  # ОБЯЗАТЕЛЬНО для WebTransport — браузеры отвергают самоподписанные сертификаты.
  cert_file: "/etc/aqueduct/fullchain.pem"
  key_file:  "/etc/aqueduct/privkey.pem"

webtransport:
  enabled: true
  listen_addr: ":4433"            # UDP-порт отличный от основного broker.listen_addr
  path_prefix: "/aqueduct/wt"     # клиенты шлют Extended CONNECT сюда
```

Для локальной разработки самый простой путь — [`mkcert`](https://github.com/FiloSottile/mkcert):

```bash
mkcert -install
mkcert localhost 127.0.0.1 ::1
# → localhost+2.pem / localhost+2-key.pem
```

Затем `tls.cert_file` / `tls.key_file` указывают на эти файлы.

### 6.2 Запуск брокера

```bash
go run ./cmd/broker -config config.yaml
# INFO  webtransport gateway started addr=:4433 path_prefix=/aqueduct/wt
```

### 6.3 Браузерный пример

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

Откройте `https://localhost:8443/index.html`. Нажмите **Connect** для открытия WebTransport-сессии, затем **Open Subscribe Stream** — и публикации с любых клиентов (браузер, нативный Go, Node.js) появятся в логе.

### 6.4 Формат фрейма в браузере

Идентичен нативному QUIC-клиенту: 10-байтовый заголовок `[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]`. Magic = `0x1F`, `CmdSubscribe = 0x02`, `CmdPublish = 0x01`. Полная реализация — в `examples/web/app.js` (`buildFrame`/`parseFrame`).

### 6.5 Чек-лист для production

- Публично доверенный сертификат (Let's Encrypt или внутренний CA).
- SAN сертификата содержит хостнейм, по которому ходят клиенты (например `broker.example.com`).
- Откройте UDP/443 (или настроенный порт) на фаерволе.
- Если клиенты не имеют клиентского сертификата, оставьте `tls.require_client_cert: false` — шлюз наследует политику брокера.
