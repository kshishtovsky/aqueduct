# Справочник: Конфигурация (v1.16.0)

Полный справочник по YAML-конфигурации брокера Aqueduct и переменным окружения `AQUEDUCT_*`. Все секции и поля приведены в соответствии с Go-структурой `internal/config/config.go::Config`.

> **Diátaxis:** Справочник (Reference) — что конфигурировать, без обоснований.
> Используйте это руководство как API-описание, а не как how-to.

---

## 1. Загрузка и порядок применения

Парсер `internal/config.Load(path)` выполняет строгий порядок:

1. `Config.Default()` — безопасные значения по умолчанию для продакшена и разработки.
2. YAML-файл (если `path != ""`) — `yaml.Unmarshal` накладывается поверх дефолтов. Поля, отсутствующие в YAML, сохраняют значения по умолчанию.
3. Переменные окружения `AQUEDUCT_*` — накладываются поверх YAML.

Команда (`cmd/broker/main.go`) парсит флаги `-config`, `-addr`, `-metrics-addr`, `-cert`, `-key`, `-aal`. Флаги CLI имеют приоритет над YAML и env vars.

| Источник | Приоритет |
| :--- | :--- |
| Флаги CLI (`-addr`, `-metrics-addr`, `-cert`, `-key`, `-aal`) | Высший |
| Переменные окружения `AQUEDUCT_*` | Средний |
| YAML-файл (`-config`) | Низкий |
| `Config.Default()` | Базовый |

---

## 2. Корневые поля

| YAML-ключ | Тип | Значение по умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `listen_addr` | `string` | `:4242` | UDP-адрес QUIC-листенера. |
| `metrics_addr` | `string` | `:9090` | HTTP-адрес для `/metrics` и `/healthz`. |

---

## 3. Секция `tls` (`TLSConfig`)

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `tls.generate` | `bool` | `true` | Если `true` и `cert_file`/`key_file` пусты — генерируется эфемерный самоподписанный сертификат. **Запрещено в production.** |
| `tls.cert_file` | `string` | `""` | Путь к PEM-сертификату сервера. |
| `tls.key_file` | `string` | `""` | Путь к PEM-приватному ключу. |
| `tls.require_client_cert` | `bool` | `false` | Включает mTLS 1.3 (требует валидный клиентский сертификат). |
| `tls.client_ca_file` | `string` | `""` | PEM CA-бандл для проверки клиентских сертификатов. Пусто → системный пул CA. |

> [!WARNING]
> **`tls.generate: true` предназначен только для локальной разработки.** В этом режиме брокер создаёт эфемерный самоподписанный сертификат на 365 дней (`cmd/broker/main.go::generateSelfSignedTLS`). Браузеры отвергают такое соединение по `net::ERR_CERT_AUTHORITY_INVALID`, а WebTransport-шлюз полностью неработоспособен. В production всегда задавайте `generate: false` и прописывайте пути к доверенным сертификатам, выданным внутренним CA или Let's Encrypt.

Константы:

- `MinVersion = tls.VersionTLS13` всегда.
- ALPN для нативного QUIC-транспорта: `aqueduct-v1`.
- ALPN для межкластерного mesh: `aqueduct-mesh`.

---

## 4. Секция `aal` (`AALConfig`)

Append-Only Log с AES-256-GCM-шифрованием.

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `aal.enabled` | `bool` | `false` | Включает запись и replay AAL. |
| `aal.file_path` | `string` | `""` | Путь к файлу AAL. Создаётся с правами `0600`. |
| `aal.key` | `string` | `""` | Base64-кодированный 32-байтный ключ AES-256 (или сырая 32-байтная строка). |
| `aal.max_aal_size` | `int64` | `104857600` (100 MB) | Декларативный размер файла. Метод `aal.Log.Rotate(maxSize, key)` существует, **но в текущей версии не вызывается ни одним планировщиком** — ротацию выполняйте внешними средствами. |
| `aal.retention_period` | `string` | (не задан) | Декларативный период хранения. **Не применяется встроенным кодом** — нет планировщика, который бы удалял/архивировал записи по этому полю. Используйте внешние средства. |
| `aal.retention_size` | `int64` | (не задан) | Декларативный лимит суммарного объёма на диске. **Не применяется встроенным кодом.** Используйте внешние средства. |

> [!WARNING]
> **AAL: ни одно из полей `max_aal_size`, `retention_period`, `retention_size` не обрабатывается встроенным планировщиком.** Брокер только пишет фреймы в `aal.file_path` до тех пор, пока процесс работает. Файл будет расти неограниченно, если не организовать ротацию и очистку извне (`logrotate`, `cron`/`systemd-timer`, k8s `CronJob`, sidecar-утилита). Это поведенческое ограничение текущей версии.

> [!IMPORTANT]
> **Безопасность ключа AAL.** Поле `aal.key` принимает только 32-байтный ключ. Невалидная длина → `aal.OpenEncrypted` возвращает `aal.ErrInvalidKeySize`. Если ключ задан — каждый фрейм шифруется AES-256-GCM с 12-байтным Nonce (`[4 случайных байта сессии][8 байт монотонного счётчика]`). Nonce уникален криптографически. **Утечка ключа компрометирует все записи.**

---

## 5. Секция `acl` (`ACLConfig`)

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `acl.enabled` | `bool` | `false` | Включает движок авторизации. |
| `acl.default` | `string` | `"none"` | Политика по умолчанию: `"none"` или `"all"`. |
| `acl.rules[]` | `[]ACLRuleConfig` | `nil` | Список явных правил. |

### Элемент `acl.rules[]` (`ACLRuleConfig`)

| YAML-ключ | Тип | Описание |
| :--- | :--- | :--- |
| `client` | `string` | Идентификатор клиента (CN из TLS-сертификата). |
| `topic` | `string` | MQTT-паттерн топика (`+`, `#` поддерживаются в matchWildcard движке authz). |
| `permission` | `string` | `"publish"`, `"subscribe"` или `"all"`. |

---

## 6. Секция `admin` (`AdminConfig`)

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `admin.enabled` | `bool` | `false` | Включает gRPC Admin API на отдельном TCP-порту. |
| `admin.addr` | `string` | `:9091` | Адрес gRPC Admin API. |

> Подробное описание RPC, аутентификации и примеров клиента — см. [`admin-api.md`](admin-api.md).

---

## 7. Секция `broker` (`BrokerConfig`)

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `broker.queue_size` | `int` | `1024` | Ёмкость per-subscriber очереди. |
| `broker.backpressure_policy` | `string` | `"drop_oldest"` | Политика переполнения: `"drop_oldest"`, `"drop_newest"`, `"disconnect"`. |
| `broker.batch_size` | `int` | `65536` | Порог коалесцированной записи (байт). |
| `broker.flush_interval` | `duration` | `50us` | Микро-таймер сброса батча. |
| `broker.max_retries` | `int` | `3` | Максимум NACK-повторов перед DLQ. |
| `broker.priority_ttls` | `[]string` | `nil` (TTL не применяется) | Опциональный массив из 4 длительностей для уровней 0..3. `0`/`""` = бессрочно. По умолчанию массив пуст — `priority_ttls` не задан, per-priority TTL не действует, все сообщения бессрочны. Переменная окружения для переопределения **отсутствует** — поле доступно только через YAML. |

### Подсекция `broker.quotas` (`QuotasConfig`)

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `broker.quotas.default_publish_rate` | `int` | `0` (безлимитно) | Default token-bucket refill (msg/sec). Встроенное значение по умолчанию — `0` = безлимитно; token-bucket активируется только если задано `> 0` или установлена переменная `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE`. |
| `broker.quotas.default_burst_size` | `int` | `1000` | Burst-ёмкость default-корзины. |
| `broker.quotas.per_client.<client_id>.rate` | `int` | — | Per-tenant override rate. |
| `broker.quotas.per_client.<client_id>.burst` | `int` | — | Per-tenant override burst. |

---

## 8. Секция `transport` (`TransportConfig`)

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `transport.max_buf_size` | `int` | `65536` | Максимальный буфер per-stream (байт). |
| `transport.read_buf_size` | `int` | `1024` | Начальный read-буфер per-stream (байт). |

`max_buf_size` ограничивает `payload_len` (`transport/broker.go::prepareFrame`). Превышение → `stream.CancelRead(1)` + `stream.CancelWrite(1)`.

---

## 9. Секция `cluster` (`ClusterConfig`)

| YAML-ключ | Тип | Описание |
| :--- | :--- | :--- |
| `cluster.peers` | `[]string` | Статический список адресов пиров `["node-b:4242", "node-c:4242"]`. |
| `cluster.discovery.*` | `DiscoveryConfig` | Параметры DNS-based peer discovery. |
| `cluster.mesh.*` | `MeshConfig` | Параметры TLS верификации mesh. |

### Подсекция `cluster.discovery`

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `cluster.discovery.enabled` | `bool` | `false` | Включает фоновый goroutine `cluster.Discovery`. |
| `cluster.discovery.type` | `string` | `"dns"` | Тип discovery (только `"dns"` в MVP). |
| `cluster.discovery.host` | `string` | `""` | FQDN Headless Service. |
| `cluster.discovery.port` | `string` | `"4242"` | Порт для резолвленных IP. Пусто → извлекается из `listen_addr`. |
| `cluster.discovery.interval` | `string` | `"10s"` | Интервал DNS-опроса. |

### Подсекция `cluster.mesh`

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `cluster.mesh.insecure_skip_verify` | `bool` | `false` | Отключает верификацию TLS сертификата пира (опасно). |
| `cluster.mesh.ca_file` | `string` | `""` | PEM-бандл CA, подписавшего сертификаты mesh-пиров. Пусто → системный пул. |

> [!WARNING]
> **`cluster.mesh.insecure_skip_verify: true` отключает проверку TLS-сертификата mesh-узла и оставляет mesh открытым для MITM-атак.** В production установите `false`, разверните внутренний Cluster CA и пропишите `cluster.mesh.ca_file`. В dev-окружениях с самоподписанными сертификатами допустимо `true`, но **никогда не в production**.

Полное руководство — [`cluster-mesh-tls.md`](cluster-mesh-tls.md).

---

## 10. Секция `compression` (`CompressionConfig`)

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `compression.enabled` | `bool` | `false` | Включает ZSTD-сжатие батчей перед mesh-форвардингом. |
| `compression.min_batch_size` | `int` | `1024` | Минимальный размер батча для применения сжатия (байт). |
| `compression.level` | `int` | `0` | Уровень ZSTD (0 = default, 1 = fastest, 3 = default). |

Алгоритм: только `AlgoZSTD = 1`. На стороне получателя `transport/broker.go::decompressFrame` декомпрессит батчи независимо от размера.

---

## 11. Секция `tracing` (`TracingConfig`)

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `tracing.enabled` | `bool` | `false` | Включает OTLP-export через gRPC. |
| `tracing.service_name` | `string` | `"aqueduct-broker"` | Имя сервиса в спанах. |
| `tracing.endpoint` | `string` | `"localhost:4317"` | OTLP gRPC endpoint. |

При `enabled: false` трейсер становится nil-safe inline no-op (~3.4 нс). При `enabled: true` используется batched OTLP gRPC exporter.

---

## 12. Секция `webtransport` (`WebTransportConfig`)

| YAML-ключ | Тип | По умолчанию | Описание |
| :--- | :--- | :--- | :--- |
| `webtransport.enabled` | `bool` | `false` | Включает WebTransport (HTTP/3) шлюз на отдельном UDP-порту. |
| `webtransport.listen_addr` | `string` | `":4433"` | UDP-адрес шлюза (должен отличаться от `listen_addr`). |
| `webtransport.path_prefix` | `string` | `"/aqueduct/wt"` | URL для Extended CONNECT. |

> [!WARNING]
> Шлюз WebTransport **наследует mTLS-сертификат брокера** (`tls.cert_file` / `tls.key_file`). Браузеры требуют публично доверенный или явно добавленный в trust store сертификат. `tls.generate: true` (самоподписанный) делает шлюз неработоспособным.

---

## 13. Полный справочник переменных окружения

Парсер `internal/config/config.go::applyEnvOverrides` обрабатывает переменные `AQUEDUCT_*`:

| Переменная | Поле | Парсер |
| :--- | :--- | :--- |
| `AQUEDUCT_LISTEN_ADDR` | `listen_addr` | `envString` |
| `AQUEDUCT_METRICS_ADDR` | `metrics_addr` | `envString` |
| `AQUEDUCT_TLS_GENERATE` | `tls.generate` | `envBool` |
| `AQUEDUCT_TLS_CERT_FILE` | `tls.cert_file` | `envString` |
| `AQUEDUCT_TLS_KEY_FILE` | `tls.key_file` | `envString` |
| `AQUEDUCT_TLS_REQUIRE_CLIENT_CERT` | `tls.require_client_cert` | `envBool` |
| `AQUEDUCT_TLS_CLIENT_CA_FILE` | `tls.client_ca_file` | `envString` |
| `AQUEDUCT_AAL_ENABLED` | `aal.enabled` | `envBool` |
| `AQUEDUCT_AAL_FILE_PATH` | `aal.file_path` | `envString` |
| `AQUEDUCT_AAL_KEY` | `aal.key` | `envString` |
| `AQUEDUCT_AAL_MAX_SIZE` | `aal.max_aal_size` | `envInt64` (>0) |
| `AQUEDUCT_ACL_ENABLED` | `acl.enabled` | `envBool` |
| `AQUEDUCT_ACL_DEFAULT` | `acl.default` | `envString` |
| `AQUEDUCT_ADMIN_ENABLED` | `admin.enabled` | `envBool` |
| `AQUEDUCT_ADMIN_ADDR` | `admin.addr` | `envString` |
| `AQUEDUCT_BROKER_BACKPRESSURE_POLICY` | `broker.backpressure_policy` | `envString` |
| `AQUEDUCT_BROKER_QUEUE_SIZE` | `broker.queue_size` | `envPositiveInt` |
| `AQUEDUCT_BROKER_BATCH_SIZE` | `broker.batch_size` | `envPositiveInt` |
| `AQUEDUCT_BROKER_FLUSH_INTERVAL` | `broker.flush_interval` | `envDuration` |
| `AQUEDUCT_BROKER_MAX_RETRIES` | `broker.max_retries` | `envPositiveInt` |
| `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` | `broker.quotas.default_publish_rate` | `envNonNegativeInt` |
| `AQUEDUCT_BROKER_DEFAULT_BURST_SIZE` | `broker.quotas.default_burst_size` | `envPositiveInt` |
| `AQUEDUCT_TRANSPORT_MAX_BUF_SIZE` | `transport.max_buf_size` | `envPositiveInt` |
| `AQUEDUCT_TRANSPORT_READ_BUF_SIZE` | `transport.read_buf_size` | `envPositiveInt` |
| `AQUEDUCT_TRACING_ENABLED` | `tracing.enabled` | `envBool` |
| `AQUEDUCT_TRACING_SERVICE_NAME` | `tracing.service_name` | `envString` |
| `AQUEDUCT_TRACING_ENDPOINT` | `tracing.endpoint` | `envString` |
| `AQUEDUCT_CLUSTER_DISCOVERY_ENABLED` | `cluster.discovery.enabled` | `envBool` |
| `AQUEDUCT_CLUSTER_DISCOVERY_HOST` | `cluster.discovery.host` | `envString` |
| `AQUEDUCT_CLUSTER_DISCOVERY_PORT` | `cluster.discovery.port` | `envString` |
| `AQUEDUCT_CLUSTER_DISCOVERY_INTERVAL` | `cluster.discovery.interval` | `envString` |
| `AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY` | `cluster.mesh.insecure_skip_verify` | `envBool` |
| `AQUEDUCT_CLUSTER_MESH_CA_FILE` | `cluster.mesh.ca_file` | `envString` |
| `AQUEDUCT_COMPRESSION_ENABLED` | `compression.enabled` | `envBool` |
| `AQUEDUCT_WEBTRANSPORT_ENABLED` | `webtransport.enabled` | `envBool` |
| `AQUEDUCT_WEBTRANSPORT_LISTEN_ADDR` | `webtransport.listen_addr` | `envString` |
| `AQUEDUCT_WEBTRANSPORT_PATH_PREFIX` | `webtransport.path_prefix` | `envString` |

Парсеры:

- `envString` — записывает значение, если непустое.
- `envBool` — `parseBool()` принимает `true`/`1`/`yes`/`on` и `false`/`0`/`no`/`off`; остальное → текущее значение.
- `envPositiveInt` — записывает только если `> 0`.
- `envNonNegativeInt` — записывает только если `>= 0`.
- `envDuration` — `time.ParseDuration`, записывает только если `> 0`.
- `envInt64` — `strconv.ParseInt`, записывает только если `> 0`.

---

## 14. Полный пример `config.yaml`

```yaml
listen_addr: ":4242"
metrics_addr: ":9090"

tls:
  generate: false
  cert_file: "/etc/aqueduct/server.crt"
  key_file: "/etc/aqueduct/server.key"
  require_client_cert: true
  client_ca_file: "/etc/aqueduct/client_ca.pem"

aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "<BASE64_32_BYTE_KEY>"
  max_aal_size: 104857600
  retention_period: "24h"
  retention_size: 1073741824

acl:
  enabled: true
  default: "none"
  rules:
    - client: "publisher-service"
      topic: "orders"
      permission: "publish"
    - client: "analytics-service"
      topic: "orders"
      permission: "subscribe"

admin:
  enabled: true
  addr: ":9091"

broker:
  queue_size: 1024
  backpressure_policy: "drop_oldest"
  batch_size: 65536
  flush_interval: "50us"
  max_retries: 3
  priority_ttls: ["500ms", "5s", "0", "0"]
  quotas:
    default_publish_rate: 1000
    default_burst_size: 1000
    per_client:
      noisy-tenant:
        rate: 100
        burst: 100

transport:
  max_buf_size: 65536
  read_buf_size: 1024

cluster:
  peers: []
  discovery:
    enabled: true
    type: "dns"
    host: "aqueduct-headless.default.svc.cluster.local"
    port: "4242"
    interval: "10s"
  mesh:
    insecure_skip_verify: false
    ca_file: "/etc/aqueduct/mesh_ca.pem"

tracing:
  enabled: false
  service_name: "aqueduct-broker"
  endpoint: "localhost:4317"

compression:
  enabled: true
  min_batch_size: 1024
  level: 3

webtransport:
  enabled: true
  listen_addr: ":4433"
  path_prefix: "/aqueduct/wt"
```

---

## 15. Проверка конфигурации

Перед запуском боевого узла прогоните YAML через `go run ./cmd/broker -config config.yaml` в dev-окружении. Брокер выполняет валидацию:

1. `config.Load` → `yaml.Unmarshal` (синтаксические ошибки YAML → fatal).
2. `aal.OpenEncrypted` → отказ, если `aal.key` декодирован и его длина ≠ 32 байт.
3. `quic.ListenAddr` → отказ, если порт занят или UDP недоступен.
4. `admin.Server.Start` → отказ, если `:9091` занят.

Ошибки валидации выводятся через `slog.Logger.Error` и приводят к `os.Exit(1)`.