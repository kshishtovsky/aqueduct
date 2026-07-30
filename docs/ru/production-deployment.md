# Руководство: Безопасность и развертывание в Production (v1.16.0)

Настоящее руководство описывает лучшие практики безопасного развёртывания Aqueduct в промышленной среде.

> **Diátaxis:** Практическое руководство (How-to) — конкретные шаги и конфигурации для production-сценариев.

---

## 1. Безопасность транспорта и настройка mTLS

В боевой среде обязательно используйте **mTLS 1.3** с проверкой сертификатов клиентов:

```yaml
tls:
  generate: false
  cert_file: "/etc/certs/server.crt"
  key_file: "/etc/certs/server.key"
  require_client_cert: true
  client_ca_file: "/etc/certs/client_ca.pem"
```

> [!WARNING]
> **`tls.generate: true` предназначен только для локальной разработки.** В этом режиме брокер генерирует эфемерный самоподписанный сертификат на 365 дней (`cmd/broker/main.go::generateSelfSignedTLS`). Браузеры отвергают такое соединение (`net::ERR_CERT_AUTHORITY_INVALID`), WebTransport-шлюз полностью неработоспособен. В production всегда устанавливайте `generate: false` и прописывайте пути к доверенным сертификатам.

ALPN:

- Клиентский трафик: `aqueduct-v1`
- Межкластерный mesh: `aqueduct-mesh`

---

## 2. Шифрование логов (AAL) и Ротация

### Генерация ключа

```bash
openssl rand -base64 32
```

### Конфигурация

```yaml
aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "<BASE64_32_BYTE_KEY>"
  max_aal_size: 104857600     # декларативно; встроенной ротации НЕТ
  retention_period: "24h"     # декларативно; встроенного планировщика НЕТ
  retention_size: 1073741824  # декларативно; встроенного планировщика НЕТ
```

> [!WARNING]
> **AAL ротация и retention не выполняются встроенным кодом.** Метод `aal.Log.Rotate(maxSize, key)` существует в коде, но **не вызывается** ни планировщиком, ни при записи, ни при старте. Поля `max_aal_size`, `retention_period`, `retention_size` считываются парсером конфигурации, но не обрабатываются. Без внешнего управления файл AAL будет расти неограниченно в течение всего времени жизни процесса. Настройте ротацию и очистку внешними средствами (`logrotate` с `copytruncate`, k8s sidecar, `systemd-timer`/`cron`), иначе диск переполнится.

При старте брокер выполняет Replay кадра из AAL для полного восстановления состояния **до открытия UDP-порта** (`transport/broker.go:178`).

### Метрики

- `aqueduct_aal_rotations_total` — **неактивная метрика**: зарегистрирована в реестре Prometheus, но ни один код-путь её не инкрементирует (счётчик всегда `0`). Не используйте её как сигнал — ротаций нет.
- `aqueduct_aal_replay_duration_seconds` — длительность Replay при старте. Обновляется в `transport/broker.go`.
- `aqueduct_aal_backfill_frames_total` — количество доставленных исторических фреймов при backfill durable-подписчика.

> [!IMPORTANT]
> **Безопасность ключа.** `aal.key` должен быть ровно 32 байта (после base64-декодирования). Невалидная длина → `aal.ErrInvalidKeySize` на старте. Каждый фрейм шифруется AES-256-GCM с 12-байтным Nonce (4 случайных байта сессии + 8 байт монотонного счётчика). Файл создаётся с правами `0600`. **Утечка ключа компрометирует все записи.**

---

## 3. Настройка Backpressure и очередей

Выбор политики при медленных потребителях:

- `drop_oldest` — для систем реального времени (телеметрия).
- `drop_newest` — для последовательных событий.
- `disconnect` — отключение зависших подписчиков (политика `disconnect`).

```yaml
broker:
  queue_size: 2048
  backpressure_policy: "drop_oldest"
  batch_size: 65536
  flush_interval: "50us"
  max_retries: 3
  quotas:
    default_publish_rate: 100
    default_burst_size: 1000
```

Per-priority TTL:

```yaml
broker:
  priority_ttls:
    - "500ms"  # P0 — Наивысший
    - "5s"     # P1 — Высокий
    - "0"      # P2 — Обычный (бессрочно)
    - "0"      # P3 — Низкий (бессрочно)
```

---

## 4. Системные лимиты ОС (`sysctl`)

Увеличение буферов UDP и числа файловых дескрипторов:

```bash
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
sysctl -w net.core.netdev_max_backlog=250000
ulimit -n 65536
```

> На Linux для QUIC также рекомендуется увеличить `net.core.optmem_max` до ~50000.

---

## 5. Кластерное развертывание

Разверните несколько брокеров Aqueduct в прямом mesh для горизонтального масштабирования.

| Узел | Адрес | Роль |
| :--- | :--- | :--- |
| Broker A | `192.168.1.10:4242` | Peer |
| Broker B | `192.168.1.11:4242` | Peer |
| Broker C | `192.168.1.12:4242` | Peer |

### Конфигурация

Каждый узел указывает **другие** узлы (не себя):

```yaml
cluster:
  peers:
    - "192.168.1.10:4242"
    - "192.168.1.11:4242"
    - "192.168.1.12:4242"
  mesh:
    insecure_skip_verify: false
    ca_file: "/etc/aqueduct/mesh_ca.pem"
```

### Поведение пересылки

- Сообщение, опубликованное на любом узле, пересылается всем пирам через `PeerManager.Forward`.
- MeshForwarded бит (бит 7 Command-байта, `0x80`) предотвращает циклы пересылки.
- Нет консенсуса или выборов лидера — mesh полностью децентрализован.
- Peer-соединения используют `aqueduct-mesh` ALPN.

### Требования к сети

- Все узлы должны быть доступны по UDP на настроенном порту.
- Каждый узел должен иметь собственные TLS-сертификаты, подписанные Cluster CA.
- Для топологий с 3+ узлами каждый узел должен перечислить всех пиров.
- Нет гарантий порядка сообщений между узлами (fire-and-forget).

> [!WARNING]
> **`cluster.mesh.insecure_skip_verify: true` отключает верификацию TLS-сертификата mesh-узла** и делает mesh уязвимым к MITM-атакам. В production установите `false`, разверните выделенный Cluster CA и пропишите `cluster.mesh.ca_file`.

Подробная инструкция по генерации CA и сертификатов — [`cluster-mesh-tls.md`](cluster-mesh-tls.md).

---

## 6. Развёртывание в Kubernetes

### Почему Kubernetes?

Статические списки пиров (`cluster.peers`) требуют ручной координации. StatefulSet с Headless Service обеспечивает **динамическое обнаружение пиров через DNS** без внешних зависимостей (Consul, etcd).

### Helm Chart (рекомендуется)

```bash
helm install aqueduct deploy/helm/aqueduct \
  --set replicaCount=3 \
  --set config.cluster.peers[0]="aqueduct-0.aqueduct-headless.default.svc.cluster.local:4242" \
  --set config.cluster.peers[1]="aqueduct-1.aqueduct-headless.default.svc.cluster.local:4242" \
  --set config.cluster.peers[2]="aqueduct-2.aqueduct-headless.default.svc.cluster.local:4242" \
  --set config.cluster.discovery.enabled=true \
  --set config.cluster.discovery.host="aqueduct-headless.default.svc.cluster.local" \
  --set config.cluster.discovery.port=4242 \
  --set config.cluster.discovery.interval="10s"
```

### Headless Service (обязательно для DNS Discovery)

Headless Service (`clusterIP: None`) возвращает A-записи для каждого пода StatefulSet:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: aqueduct-headless
spec:
  clusterIP: None
  ports:
    - name: quic
      port: 4242
  selector:
    app.kubernetes.io/name: aqueduct
```

DNS-паттерны подов:

- `aqueduct-0.aqueduct-headless.<namespace>.svc.cluster.local`
- `aqueduct-1.aqueduct-headless.<namespace>.svc.cluster.local`
- и т.д.

### DNS Discovery

При включённом обнаружении брокер опрашивает DNS-запись Headless Service с интервалом `interval` и вычисляет разницу:

```go
ips, err := net.LookupHost("aqueduct-headless.default.svc.cluster.local")
```

- **Масштабирование вверх**: Новые IP подов автоматически подключаются через `AddPeer()`.
- **Масштабирование вниз**: Удалённые IP отключаются через `RemovePeer()`.
- **Нулевой даунтайм**: Переиспользуется экспоненциальная задержка переподключения.

### Конфигурация

```yaml
cluster:
  peers: []  # пусто — discovery заполняет автоматически
  discovery:
    enabled: true
    type: "dns"
    host: "aqueduct-headless.default.svc.cluster.local"
    port: "4242"
    interval: "10s"
```

### Масштабирование

```bash
kubectl scale statefulset aqueduct --replicas=5
kubectl rollout restart statefulset aqueduct
```

### Raw-манифесты Kubernetes

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
```

---

## 7. gRPC Admin API

Включите динамический hot-reload квот и ACL:

```yaml
tls:
  require_client_cert: true
  client_ca_file: "/etc/aqueduct/client_ca.pem"

admin:
  enabled: true
  addr: ":9091"
```

API использует mTLS; CN клиентского сертификата должен начинаться с `admin-`. RPC:

- `SetClientQuota(client_id, rate)` — обновление token-bucket.
- `UpdateACL(rules)` — полная замена ACL-матрицы через RCU-swap.

Полное описание и примеры клиента — [`admin-api.md`](admin-api.md).

> [!WARNING]
> **Изолируйте Admin CA от пользовательского.** Admin API использует тот же `client_ca_file`, что и клиентский mTLS. Используйте выделенный CA-бандл для admin-сертификатов и защитите сетевой доступ к `admin.addr` через NetworkPolicy.

---

## 8. WebTransport в Production

Включение HTTP/3 + WebTransport для браузерных клиентов:

```yaml
tls:
  generate: false
  cert_file: "/etc/aqueduct/fullchain.pem"
  key_file: "/etc/aqueduct/privkey.pem"

webtransport:
  enabled: true
  listen_addr: ":4433"
  path_prefix: "/aqueduct/wt"
```

Требования:

1. Публично доверенный или явно добавленный в trust store сертификат.
2. Откройте UDP/443 (или настроенный порт) на файерволе.
3. WebTransport не работает с `tls.generate: true`.

---

## 9. OpenTelemetry Distributed Tracing

```yaml
tracing:
  enabled: true
  service_name: "aqueduct-broker"
  endpoint: "otel-collector:4317"
```

При `enabled: false` трейсер — nil-safe inline no-op (~3.4 нс). При `enabled: true` — batched OTLP gRPC exporter.

Метрика: `aqueduct_tracing_spans_total`.

---

## 10. NACK/DLQ в Production

Настройка повторной доставки NACK и Dead Letter Queue:

- Установите `max_retries` (по умолчанию 3) в `config.yaml` или через `AQUEDUCT_BROKER_MAX_RETRIES`.
- DLQ топики следуют шаблону `__dlq__<original_topic>`.
- Отслеживайте метрики `aqueduct_messages_nacked_total{topic}` и `aqueduct_messages_dead_lettered_total{topic}`.
- Подключите подписчика к топикам `__dlq__*` для автономного просмотра.
- Внутри брокера `ExtRetryOffset` TLV (`0xF0`) гарантирует сходимость счётчика NACK к `max_retries` и триггер DLQ.

---

## 11. Квоты Rate Limiting

Настройка ограничения скорости для каждого клиента:

```yaml
broker:
  quotas:
    default_publish_rate: 1000
    default_burst_size: 1000
    per_client:
      noisy-tenant:
        rate: 100
        burst: 50
```

- Индивидуальные настройки: `broker.quotas.per_client.<client_id>`.
- Динамическое обновление через Admin API `SetClientQuota`.
- Метрика: `aqueduct_messages_rate_limited_total{client}`.

---

## 12. Compression (межузловой форвардинг)

```yaml
compression:
  enabled: true
  min_batch_size: 1024
  level: 3
```

Применяется только к батчам, пересылаемым между mesh-пирами. Compression TLV (`ExtCompression = 0x02`) добавляется в фрейм для уведомления получателя.

---

## 13. AAL Retention (требует внешнего управления)

```yaml
aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "<BASE64_KEY>"
  max_aal_size: 104857600      # декларативно; встроенной ротации нет
  retention_period: "24h"        # декларативно; встроенного планировщика нет
  retention_size: 1073741824     # декларативно; встроенного планировщика нет
```

> [!WARNING]
> **В этой версии брокер НЕ выполняет AAL-ротацию и НЕ применяет retention.** Поля `max_aal_size`, `retention_period`, `retention_size` принимаются парсером конфигурации (см. `internal/config/config.go:81-83`), но никакого планировщика или фоновой горутины, использующей их, в кодовой базе нет. Метод `aal.Log.Rotate` существует, но не вызывается ни одним код-путем. Следовательно, по умолчанию файл AAL растёт неограниченно в течение всего времени жизни процесса. **Для ограничения объёма и времени хранения используйте внешние средства:**
>
> - `logrotate` с опцией `copytruncate` (с учётом того, что активный файл открыт брокером).
> - `cron` / `systemd-timer` со скриптом архивирования и обрезки.
> - Kubernetes `CronJob` или sidecar-контейнер.
> - Метрика `aqueduct_aal_rotations_total` остаётся `0` — это **неактивная метрика** (зарегистрирована, но ни разу не инкрементируется). Не сигнализируйте по ней.

---

## 14. Production чек-лист

- [ ] `tls.generate: false`, валидный production-сертификат
- [ ] `tls.require_client_cert: true` для mTLS
- [ ] Изолированный Cluster CA для mesh (`cluster.mesh.ca_file`)
- [ ] `cluster.mesh.insecure_skip_verify: false`
- [ ] `aal.enabled: true` с валидным 32-байтным ключом
- [ ] `acl.enabled: true` с явными правилами
- [ ] `admin.enabled: true` с mTLS и `admin-` CN в CA
- [ ] WebTransport: публично доверенный сертификат
- [ ] Scrape `/metrics` Prometheus'ом
- [ ] Алерт на `aqueduct_cluster_peers_active` ниже ожидаемого
- [ ] Алерт на рост `aqueduct_messages_dead_lettered_total`
- [ ] `sysctl` UDP-буферы увеличены
- [ ] Headless Service в Kubernetes для DNS discovery