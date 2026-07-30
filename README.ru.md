<div align="center">
  <img src="docs/image_readme.png" alt="Aqueduct Banner">
</div>

# Aqueduct

[ [🇬🇧 English](README.md) | 🇷🇺 Русский | [🇨🇳 中文](README.zh.md) ]

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kshishtovsky/aqueduct.svg)](https://pkg.go.dev/github.com/kshishtovsky/aqueduct)

Aqueduct — это сверхвысокопроизводительный брокер сообщений с нулевыми аллокациями памяти на Go поверх **QUIC** (библиотека `quic-go`). Спроектирован для работы с микросекундными задержками (< 1.5 µs), zero-copy бинарным фреймингом и Data-Oriented Design (DoD).

> [!IMPORTANT]
> **Production Ready (v1.16.0)**
> Aqueduct поддерживает **Consumer Groups & Lock-Free Atomic Round-Robin Routing**, **gRPC Control Plane (Admin API)** для **Lock-Free RCU Hot-Reload** квот и правил ACL, **Ленивые очереди приоритетов (QoS Hard Real-Time)**, **Per-Priority TTL**, **Строгую приоритизацию**, **двустороннюю аутентификацию mTLS 1.3**, **авторизацию ACL без аллокаций**, **зашифрованный журнал AAL (AES-256-GCM)** с **восстановлением состояния при старте (Replay)**, **асинхронный Fan-Out с изоляцией медленных потребителей**, **ZSTD-сжатие без аллокаций**, **маршрутизацию по Wildcard-топикам MQTT**, **Direct Mesh Clustering**, **протокольный батчинг без копирования**, **коалесцированную запись подписчиков**, **NACK-переотправку** и **Dead Letter Queues**.

---

## Возможности

- **Consumer Groups & Atomic Round-Robin Routing**: Конкурирующие подписчики (Competing Consumers) объединяются в группы (например `topic:orders:group:payment-workers`). Сообщения балансируются за **`0 allocs/op`** и **`< 10 ns/op`** без мьютексов (`atomic.AddUint64` + modulo). Групповые Durable Offset'ы сохраняются и восстанавливаются на уровне всей группы при фейловере воркеров.
- **Dynamic Control Plane (gRPC Admin API)**: Выделенный gRPC Admin сервер (`:9091`) с валидацией mTLS ролей (`admin-*` CN) для Lock-Free RCU Hot-Reload квот пользователей и правил авторизации ACL без перезапуска брокера и без блокировок горячего пути. RPC: `SetClientQuota(client_id, rate)`, `UpdateACL(rules)`.
- **Транспортный слой QUIC**: Мультиплексирование QUIC с поддержкой 0-RTT, изоляцией стримов и защитой от Amplification атак. ALPN клиентского транспорта: `aqueduct-v1`.
- **WebTransport Gateway (HTTP/3)**: Опциональный листенер `internal/webtransport/` принимает W3C WebTransport API из браузеров на отдельном UDP-порту. Переиспользует mTLS-сертификат брокера (добавляет `h3` в `NextProtos`) — единый trust root для нативных и браузерных клиентов. Браузеры пишут **тот же бинарный формат фреймов**, что и нативные QUIC-клиенты — нулевой overhead на трансляцию. Handshake timeout 10 с (Slowloris защита).
- **Zero-Copy бинарный протокол**: Плоский 10-байтовый заголовок `[Magic:1][Cmd:1][StreamID:4][DataLen:4]` с опциональным блоком TLV-расширений (`ExtTraceContext=0x01`, `ExtCompression=0x02`, `ExtPriority=0x03`, `ExtRetryOffset=0xF0`).
- **Ленивые очереди приоритетов (QoS)**: 4 уровня приоритета сообщений (`0` Highest, `1` High, `2` Normal, `3` Low) в TLV `ExtPriority` (`0x03`). Очереди инициализируются из `sync.Pool` при первом поступлении (`0 allocs/op`). Подписчик с 1 приоритетом потребляет память только под 1 очередь.
- **Строгая приоритизация и защита от голодания**: Writer-горутина опрашивает очереди в строгом порядке `0 -> 1 -> 2 -> 3`. Критические данные передаются вне очереди.
- **Per-Priority TTL**: Опциональная настройка `priority_ttls` (`["500ms", "5s", "0", "0"]`) с принудительной перезаписью `expiresAt`. **По умолчанию массив пуст** — TTL не применяется, сообщения бессрочны. Переменная окружения отсутствует, поле задаётся только через YAML. Устаревшие критические сообщения уничтожаются при извлечении (`aqueduct_messages_expired_total{topic, priority}`).
- **Очистка памяти и рециклинг очередей**: Опустошенные очереди (`len(q) == 0`) автоматически возвращаются в `sync.Pool` и сбрасываются в `nil`.
- **Сжатие данных ZSTD без аллокаций**: Батчевое сжатие ZSTD (`internal/compress`) с TLV-расширением `ExtCompression` (`0x02`) перед межузловым форвардингом.
- **Structure of Arrays (SoA) Роутер**: Внутренняя Pub/Sub маршрутизация на плоских массивах для максимального попадания в L1/L2 кэш CPU. `topicHashKey` — единый источник правды для FNV-1a-хэша топика (фикс коллизий v1.16.0).
- **Асинхронный Fan-Out и Ring Queues**: Персональные неблокирующие каналы и Writer-горутины для каждого подписчика.
- **Изоляция медленных потребителей (Backpressure)**: Политики переполнения очередей (`drop_oldest`, `drop_newest`, `disconnect`) отдельно для каждого приоритета.
- **Атомарный подсчёт ссылок (`MessageRef`)**: Безопасное переиспользование буферов в `sync.Pool` при нулевом счётчике ссылок (`0 allocs/op`).
- **Zero-Copy протокольный батчинг**: Команда `CmdPublishBatch` (0x05) с массовой публикацией без копирования — под-фреймы распаковываются через `unsafe.Slice` прямо в буфер батча (< 4 ns/frame, `0 allocs/op`).
- **Коалесцированная запись подписчиков**: Микро-батчинг с конфигурируемым порогом 64 КБ и микро-таймером 50 µs. 6.67M msg/s пропускная способность.
- **MQTT Wildcard Topics**: Маршрутизация по паттернам `+` и `#` без аллокаций в куче (< 51 ns/op, `0 allocs/op`).
- **Зашифрованный Append-Only Log (AAL)**: AES-256-GCM с уникальными 12-байтными Nonce (4 случайных байта сессии + 8 байт монотонного счётчика), 4-байтными заголовками длины. Поля `retention_period` и `retention_size` декларированы в конфиге, но **не** применяются встроенным планировщиком — ротацию и удержание обеспечивайте внешними средствами (`logrotate`, `cron`-скрипт, k8s-оператор).
- **mTLS и Zero-Allocation ACL**: Двусторонний TLS 1.3, некоммутативная FNV-1a матрица прав доступа с RCU hot-reload.
- **NACK-переотправка и Dead Letter Queues**: Опкод `CmdNack` (0x06) с автоматической переотправкой (до `max_retries`), кэш фреймов на подписчика (FIFO 256), маршрутизация poison pill в `__dlq__<topic>`. `ExtRetryOffset` TLV гарантирует сходимость NACK-счётчика.
- **gRPC Admin API с mTLS**: Динамический hot-reload через `SetClientQuota` / `UpdateACL` — `aqueduct_admin_requests_total{method}`.
- **DNS-based Peer Discovery**: Kubernetes Headless Service → A records → `net.LookupHost` каждые 10 с, RCU-swap списка пиров (`aqueduct_cluster_peers_active`).
- **Mesh TLS**: ALPN `aqueduct-mesh`, проверка через `cluster.mesh.ca_file` или системный пул.
- **Мониторинг Prometheus**: Готовые метрики (`/metrics`) и Docker Compose стек с дашбордом Grafana. Полный список — [`docs/ru/metrics.md`](docs/ru/metrics.md).

---

## Быстрый старт за 2 минуты (Docker Compose)

Запуск Aqueduct, Prometheus и Grafana:

```bash
docker compose up -d
```

Проверка статуса:

- **Health check**: `http://localhost:9090/healthz`
- **Prometheus**: `http://localhost:9091`
- **Grafana**: `http://localhost:3000` (Логин: `admin` / Пароль: `admin`)

Остановка стека:

```bash
docker compose down
```

> Docker Compose использует `tls.generate: true` (эфемерный самоподписанный сертификат). Браузеры и mTLS-клиенты **не** смогут подключиться — для production см. секцию TLS ниже.

---

## Локальный запуск и настройка

```bash
# Запуск с YAML конфигом
go run ./cmd/broker/main.go -config config.yaml

# Запуск с переопределением флагов CLI
go run ./cmd/broker/main.go \
  -config config.yaml \
  -addr :4242 \
  -metrics-addr :9090
```

### Конфигурация (`config.yaml`)

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
  enabled: false
  file_path: ""
  key: "" # Base64 ключ 32 байта для AES-256-GCM
  max_aal_size: 104857600 # 100 MB

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
  priority_ttls: ["500ms", "5s", "0", "0"]
  quotas:
    default_publish_rate: 0  # 0 = безлимитно (значение по умолчанию)
    default_burst_size: 1000

transport:
  max_buf_size: 65536
  read_buf_size: 1024

cluster:
  discovery:
    enabled: false
    host: "aqueduct-headless.default.svc.cluster.local"
    port: "4242"
    interval: "10s"
```

> [!IMPORTANT]
> `priority_ttls` — массив **ровно из 4** строк, индексированных по приоритету (`P0..P3`). Каждое значение — Go `time.Duration` (`"500ms"`, `"5s"`, `"0"`, `""`). **Встроенное значение по умолчанию — `nil`** (нет TTL, все сообщения бессрочны); переменная окружения для переопределения **отсутствует**, поле задаётся только через YAML.

Полный справочник по всем YAML-полям — [`docs/ru/configuration.md`](docs/ru/configuration.md).

### Переопределение переменными окружения

| Переменная окружения | Поле | Пример |
| :--- | :--- | :--- |
| `AQUEDUCT_LISTEN_ADDR` | `listen_addr` | `:4242` |
| `AQUEDUCT_METRICS_ADDR` | `metrics_addr` | `:9090` |
| `AQUEDUCT_TLS_GENERATE` | `tls.generate` | `false` |
| `AQUEDUCT_TLS_CERT_FILE` | `tls.cert_file` | `/etc/certs/cert.pem` |
| `AQUEDUCT_TLS_KEY_FILE` | `tls.key_file` | `/etc/certs/key.pem` |
| `AQUEDUCT_TLS_REQUIRE_CLIENT_CERT` | `tls.require_client_cert` | `true` |
| `AQUEDUCT_TLS_CLIENT_CA_FILE` | `tls.client_ca_file` | `/etc/certs/ca.pem` |
| `AQUEDUCT_AAL_ENABLED` | `aal.enabled` | `true` |
| `AQUEDUCT_AAL_FILE_PATH` | `aal.file_path` | `/var/log/aal.log` |
| `AQUEDUCT_AAL_KEY` | `aal.key` | `base64_encoded_key` |
| `AQUEDUCT_AAL_MAX_SIZE` | `aal.max_aal_size` | `104857600` |
| `AQUEDUCT_ACL_ENABLED` | `acl.enabled` | `true` |
| `AQUEDUCT_ADMIN_ENABLED` | `admin.enabled` | `true` |
| `AQUEDUCT_ADMIN_ADDR` | `admin.addr` | `:9091` |
| `AQUEDUCT_BROKER_QUEUE_SIZE` | `broker.queue_size` | `2048` |
| `AQUEDUCT_BROKER_BACKPRESSURE_POLICY` | `broker.backpressure_policy` | `drop_oldest` |
| `AQUEDUCT_BROKER_BATCH_SIZE` | `broker.batch_size` | `65536` |
| `AQUEDUCT_BROKER_FLUSH_INTERVAL` | `broker.flush_interval` | `50us` |
| `AQUEDUCT_BROKER_MAX_RETRIES` | `broker.max_retries` | `3` |
| `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` | `broker.quotas.default_publish_rate` | `100` |
| `AQUEDUCT_BROKER_DEFAULT_BURST_SIZE` | `broker.quotas.default_burst_size` | `1000` |
| `AQUEDUCT_TRANSPORT_MAX_BUF_SIZE` | `transport.max_buf_size` | `131072` |
| `AQUEDUCT_CLUSTER_DISCOVERY_ENABLED` | `cluster.discovery.enabled` | `true` |
| `AQUEDUCT_CLUSTER_DISCOVERY_HOST` | `cluster.discovery.host` | `aqueduct-headless.default.svc.cluster.local` |
| `AQUEDUCT_CLUSTER_DISCOVERY_PORT` | `cluster.discovery.port` | `4242` |
| `AQUEDUCT_CLUSTER_DISCOVERY_INTERVAL` | `cluster.discovery.interval` | `10s` |
| `AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY` | `cluster.mesh.insecure_skip_verify` | `false` |
| `AQUEDUCT_CLUSTER_MESH_CA_FILE` | `cluster.mesh.ca_file` | `/etc/aqueduct/mesh_ca.pem` |
| `AQUEDUCT_COMPRESSION_ENABLED` | `compression.enabled` | `true` |
| `AQUEDUCT_WEBTRANSPORT_ENABLED` | `webtransport.enabled` | `true` |
| `AQUEDUCT_WEBTRANSPORT_LISTEN_ADDR` | `webtransport.listen_addr` | `:4433` |
| `AQUEDUCT_WEBTRANSPORT_PATH_PREFIX` | `webtransport.path_prefix` | `/aqueduct/wt` |
| `AQUEDUCT_TRACING_ENABLED` | `tracing.enabled` | `false` |
| `AQUEDUCT_TRACING_SERVICE_NAME` | `tracing.service_name` | `aqueduct-broker` |
| `AQUEDUCT_TRACING_ENDPOINT` | `tracing.endpoint` | `otel-collector:4317` |

Полный список — [`docs/ru/configuration.md`](docs/ru/configuration.md#13-полный-справочник-переменных-окружения).

### Нагрузочный тест (`aqueduct-bench`)

```bash
go run ./cmd/aqueduct-bench/main.go \
  -addr 127.0.0.1:4242 \
  -c 10 \
  -n 100000 \
  -size 128 \
  -topic bench
```

> Флаги `-c`, `-n`, `-size` (а не `-streams`/`-messages`/`-payload-size`). Для mTLS-брокеров добавьте `-tls-verify -ca-file /path/to/ca.pem`.

---

## Документация (Фреймворк Diátaxis)

| Тип | Документ |
| :--- | :--- |
| **Tutorial** | [Быстрый старт](docs/ru/getting-started.md) |
| **Reference** | [Спецификация бинарного протокола](docs/ru/protocol-spec.md) |
| **Reference** | [Конфигурация и env vars](docs/ru/configuration.md) |
| **Reference** | [Метрики Prometheus](docs/ru/metrics.md) |
| **Reference** | [gRPC Admin API](docs/ru/admin-api.md) |
| **Explanation** | [Архитектура и модель памяти](docs/ru/architecture.md) |
| **How-to** | [Развёртывание в Production и безопасность](docs/ru/production-deployment.md) |
| **How-to** | [Cluster Mesh и TLS](docs/ru/cluster-mesh-tls.md) |
| **How-to** | [Диагностика и устранение неполадок](docs/ru/troubleshooting.md) |

---

## Развёртывание в Kubernetes (Helm)

Разверните 3-узловой кластер Aqueduct с DNS-обнаружением пиров одной командой:

```bash
helm install aqueduct ./deploy/helm/aqueduct \
  --namespace aqueduct --create-namespace
```

### Как работает обнаружение пиров

При развертывании в Kubernetes Aqueduct использует **DNS-обнаружение пиров** через Headless Service:

1. Каждый под получает стабильное DNS-имя: `aqueduct-0.aqueduct-headless.aqueduct.svc.cluster.local`
2. Headless Service возвращает **A-записи** для всех готовых подов
3. Фоновая горутина `cluster.Discovery` опрашивает DNS каждые 10 секунд (настраивается)
4. Новые поды (скейлап) автоматически подключаются, завершённые поды (скейлдаун) удаляются
5. Используется RCU (Read-Copy-Update) атомарный swap — нулевые блокировки на горячем пути маршрутизации

### Почему DNS вместо K8s API (client-go)

| Аспект | DNS-резолюция | client-go |
| :--- | :--- | :--- |
| Влияние на размер бинарника | 0 MB (stdlib) | ~40 MB |
| Внешние зависимости | Нет | REST-клиент, protobuf, informers |
| Динамические обновления | Автоматически (Headless Service) | Watch + label selector |
| Философия статического бинарника | Да | Нет |

### Mesh TLS

В production настройте `cluster.mesh.insecure_skip_verify: false` и `cluster.mesh.ca_file` с bundle Cluster CA. Подробная инструкция — [`docs/ru/cluster-mesh-tls.md`](docs/ru/cluster-mesh-tls.md).

### Конфигурация

```yaml
cluster:
  discovery:
    enabled: true
    type: "dns"
    host: "aqueduct-headless.aqueduct.svc.cluster.local"
    port: "4242"
    interval: "10s"
```

### Масштабирование

```bash
# Масштаб до 5 реплик
helm upgrade aqueduct ./deploy/helm/aqueduct --set replicaCount=5

# Сокращение до 2 реплик
helm upgrade aqueduct ./deploy/helm/aqueduct --set replicaCount=2
```

DNS-обнаружение автоматически согласовывает P2P mesh — ручная настройка не требуется.

### Raw K8s-манифесты

Для развёртывания без Helm, манифесты находятся в `deploy/k8s/`:

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

---

## Безопасность (сводка)

> [!WARNING]
> **Production-готовность требует строгой настройки TLS, ACL и Admin API.**
>
> - **`tls.generate: true` ТОЛЬКО для разработки.** Браузеры и mTLS-клиенты отвергают самоподписанный сертификат. В production установите `false` и пропишите `tls.cert_file` / `tls.key_file`.
> - **`cluster.mesh.insecure_skip_verify: true` уязвим к MITM-атакам** на mesh-соединения. В production — `false` + `cluster.mesh.ca_file` с bundle Cluster CA.
> - **Admin API требует mTLS** (`tls.require_client_cert: true`). CN клиентского сертификата должно начинаться с `admin-`. Изолируйте Admin CA от пользовательского.
> - **AAL-ключ должен быть 32 байта** (после base64-декодирования). Файл AAL создаётся с правами `0600`. Утечка ключа компрометирует все записи.

Подробное руководство — [`docs/ru/production-deployment.md`](docs/ru/production-deployment.md).

---

## Лицензия

MIT License. Подробнее см. [LICENSE](LICENSE).