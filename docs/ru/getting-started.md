# Руководство: Быстрый старт с Aqueduct (v1.16.0)

Пошаговое руководство по установке, настройке и запуску мессендж-брокера Aqueduct.

> **Diátaxis:** Учебник (Tutorial) — последовательность действий, которую читатель выполняет шаг за шагом.

---

## Требования

- **Go 1.23+**
- **Docker & Docker Compose** (опционально)
- **Kubernetes 1.28+** с `kubectl` и `helm` (опционально, для K8s-развёртывания)

---

## 1. Запуск через Docker Compose

```bash
docker compose up -d
```

После старта проверьте сервисы:

- **Health check**: `http://localhost:9090/healthz` → `200 OK`
- **Prometheus**: `http://localhost:9091`
- **Grafana**: `http://localhost:3000` (Логин: `admin`, Пароль: `admin`)

> [!NOTE]
> Docker Compose использует `tls.generate: true` (эфемерный самоподписанный сертификат). Браузеры и mTLS-клиенты **не** смогут подключиться без дополнительной настройки. Для WebTransport и продакшена см. шаги ниже.

Остановка стека:

```bash
docker compose down
```

---

## 2. Локальный запуск через `go run`

```bash
go run ./cmd/broker/main.go -config config.yaml
```

Полезные CLI-флаги (`cmd/broker/main.go:37-43`):

| Флаг | Назначение |
| :--- | :--- |
| `-config` | Путь к YAML-конфигу. Пустая строка → только env vars и defaults. |
| `-addr` | UDP-адрес QUIC-листенера (перекрывает `listen_addr`). |
| `-metrics-addr` | HTTP-адрес `/metrics` (перекрывает `metrics_addr`). |
| `-cert` | Путь к TLS-сертификату (отключает `tls.generate`). |
| `-key` | Путь к TLS-приватному ключу (отключает `tls.generate`). |
| `-aal` | Путь к AAL-файлу (включает `aal.enabled` и `aal.file_path`). |

Пример с переопределениями:

```bash
go run ./cmd/broker/main.go \
  -config config.yaml \
  -addr :4242 \
  -metrics-addr :9090
```

---

## 3. Запуск бенчмарка (`aqueduct-bench`)

После старта брокера запустите нагрузочный тест:

```bash
go run ./cmd/aqueduct-bench/main.go \
  -addr 127.0.0.1:4242 \
  -c 10 \
  -n 100000 \
  -size 128 \
  -topic bench
```

| Флаг | Назначение | По умолчанию |
| :--- | :--- | :--- |
| `-addr` | Адрес брокера | `127.0.0.1:4242` |
| `-c` | Количество параллельных воркеров | `10` |
| `-n` | Общее число сообщений | `100000` |
| `-size` | Размер payload (байт) | `128` |
| `-topic` | Имя топика | `bench` |
| `-batch` | Размер батча (1 = single-frame) | `1` |
| `-timeout` | Дедлайн записи per-message | `5s` |
| `-tls-verify` | Включить верификацию TLS | `false` |
| `-ca-file` | CA-бандл для `-tls-verify` | `""` |

> По умолчанию `-tls-verify: false` (для dev-окружения с самоподписанным сертификатом). В production включайте верификацию.

---

## 4. Конфигурация (`config.yaml`)

Полный пример для development:

```yaml
listen_addr: ":4242"
metrics_addr: ":9090"

tls:
  generate: true  # dev: самоподписанный; в production ОБЯЗАТЕЛЬНО false
  cert_file: ""
  key_file: ""
  require_client_cert: false
  client_ca_file: ""

aal:
  enabled: false
  file_path: ""
  key: ""  # Base64 ключ 32 байта для AES-256-GCM
  max_aal_size: 104857600

acl:
  enabled: false
  default: "none"
  rules:
    - client: "service-a"
      topic: "orders"
      permission: "publish"

broker:
  queue_size: 1024
  backpressure_policy: "drop_oldest"
  batch_size: 65536
  flush_interval: "50us"
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

> [!IMPORTANT]
> `priority_ttls` — массив из **ровно 4** элементов (`P0..P3`). Каждый элемент — Go `time.Duration` (`"500ms"`, `"5s"`, `"0"`, `""`). `0` или `""` означает «бессрочно». Лишние элементы игнорируются, недостающие — `0`. **Встроенное значение по умолчанию — `nil` (TTL не применяется); переменная окружения для этого поля отсутствует**, задаётся только через YAML.

Подробный справочник по всем YAML-полям — [`configuration.md`](configuration.md).

---

## 5. Очереди приоритетов (QoS), Per-Priority TTL и Wildcards

### TLV Расширение приоритета (`ExtPriority = 0x03`)

- Поддерживается 4 уровня приоритета (`0` Наивысший до `3` Низший).
- Критические данные (Приоритет 0) передаются в сетевой сокет вне очереди, опережая трафик с более низким приоритетом.
- TLV-блок формируется `protocol.BuildPriorityExtension(priority)`, извлекается `protocol.ExtractPriority(extBlock)`.

### Per-Priority TTL

- Задаётся через массив `priority_ttls` в `config.yaml`.
- При застревании сообщения в очереди дольше указанного срока оно автоматически уничтожается при вычитке (`0 allocs/op`).
- Метрика: `aqueduct_messages_expired_total{topic, priority}`.

### Примеры Wildcard

- `sensor/+/temp`: совпадает с `sensor/room1/temp` и `sensor/room2/temp`. Ровно один сегмент между `/`.
- `sensor/#`: совпадает со всеми подтопиками `sensor/` (включая сам `sensor`).
- `MatchWildcard` работает за **~50 нс/op**, `0 allocs/op`.

---

## 6. Consumer Groups

Конкурирующие подписчики балансируют нагрузку через группы:

```text
# Подписчик 1
topic:orders:group:payment-workers

# Подписчик 2
topic:orders:group:payment-workers
```

Сообщения балансируются через lock-free atomic round-robin. Добавление `:durable:<consumerID>:<offset>` включает durable-режим:

```text
topic:orders:group:payment-workers:durable:worker1:0
```

---

## 7. NACK и Dead Letter Queue (DLQ)

Подписчики могут отправить NACK (Negative Acknowledgement) для сообщения по смещению с помощью команды `CmdNack` (`0x06`). Payload — 8 байт `uint64` (Little-Endian) со смещением.

Брокер автоматически выполняет повторную доставку (до `max_retries` попыток), после чего сообщение направляется в очередь `__dlq__<topic>`. Подписчик на `__dlq__*` получает все poison-pill сообщения.

---

## 8. WebTransport (подключение из браузера) (v1.16.0+)

Aqueduct содержит опциональный шлюз HTTP/3 + WebTransport, который позволяет браузерам подключаться через W3C [WebTransport API](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport). Шлюз переиспользует mTLS-сертификат брокера — единый trust root для нативных и браузерных клиентов.

### 8.1 Включение шлюза в `config.yaml`

```yaml
tls:
  generate: false                  # ОБЯЗАТЕЛЬНО для WebTransport — браузеры отвергают самоподписанные сертификаты.
  cert_file: "/etc/aqueduct/fullchain.pem"
  key_file:  "/etc/aqueduct/privkey.pem"

webtransport:
  enabled: true
  listen_addr: ":4433"            # UDP-порт, отличный от основного listen_addr
  path_prefix: "/aqueduct/wt"     # клиенты шлют Extended CONNECT сюда
```

Для локальной разработки самый простой путь — [`mkcert`](https://github.com/FiloSottile/mkcert):

```bash
mkcert -install
mkcert localhost 127.0.0.1 ::1
# → localhost+2.pem / localhost+2-key.pem
```

Затем `tls.cert_file` / `tls.key_file` указывают на эти файлы.

### 8.2 Запуск брокера

```bash
go run ./cmd/broker -config config.yaml
# INFO  webtransport gateway started addr=:4433 path_prefix=/aqueduct/wt
```

### 8.3 Браузерный пример

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

### 8.4 Формат фрейма в браузере

Идентичен нативному QUIC-клиенту: 10-байтовый заголовок `[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]`. Magic = `0x1F`, `CmdSubscribe = 0x02`, `CmdPublish = 0x01`. Полная реализация — в `examples/web/app.js` (`buildFrame`/`parseFrame`).

### 8.5 Чек-лист для production

- Публично доверенный сертификат (Let's Encrypt или внутренний CA).
- SAN сертификата содержит хостнейм, по которому ходят клиенты (например `broker.example.com`).
- Откройте UDP/443 (или настроенный порт) на файерволе.
- Если клиенты не имеют клиентского сертификата, оставьте `tls.require_client_cert: false` — шлюз наследует политику брокера.

---

## 9. Быстрый старт в Kubernetes (v1.14.0+)

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

Метрики для отслеживания mesh:

```promql
aqueduct_cluster_peers_active
rate(aqueduct_cluster_frames_forwarded_total[1m])
```

Для 3-узлового кластера ожидайте `aqueduct_cluster_peers_active = 2` на каждом узле.

Raw-манифесты (без Helm):

```bash
kubectl apply -f deploy/k8s/
```

---

## 10. Следующие шаги

- Полная конфигурация — [`configuration.md`](configuration.md).
- Admin API — [`admin-api.md`](admin-api.md).
- Mesh TLS — [`cluster-mesh-tls.md`](cluster-mesh-tls.md).
- Метрики — [`metrics.md`](metrics.md).
- Диагностика — [`troubleshooting.md`](troubleshooting.md).
- Бинарный протокол — [`protocol-spec.md`](protocol-spec.md).
- Production hardening — [`production-deployment.md`](production-deployment.md).