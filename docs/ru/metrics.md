# Справочник: Метрики Prometheus (v1.16.0)

Полный справочник по метрикам, экспортируемым брокером Aqueduct через `/metrics`. Все метрики определены в `internal/metrics/metrics.go` и регистрируются в `prometheus.MustRegister` на старте.

> **Diátaxis:** Справочник (Reference) — какие метрики доступны, без рекомендаций по алертам.

---

## 1. Эндпоинты HTTP

`internal/metrics/server.go::StartServer` запускает HTTP-сервер на `metrics_addr` (по умолчанию `:9090`):

| Путь | Назначение |
| :--- | :--- |
| `/metrics` | Prometheus scrape endpoint (`promhttp.Handler()`). |
| `/healthz` | Liveness probe — всегда `200 OK`. |

Защитные настройки HTTP-сервера:

```go
ReadHeaderTimeout: 5 * time.Second,
ReadTimeout:       10 * time.Second,
WriteTimeout:      10 * time.Second,
IdleTimeout:       60 * time.Second,
```

`ReadHeaderTimeout` блокирует Slowloris-атаки (G112).

---

## 2. Счётчики (Counter / CounterVec)

| Метрика | Тип | Labels | Help |
| :--- | :--- | :--- | :--- |
| `aqueduct_messages_published_total` | CounterVec | `topic` | Всего опубликованных сообщений на топик. |
| `aqueduct_messages_delivered_total` | CounterVec | `topic` | Всего доставленных сообщений на топик. |
| `aqueduct_authz_denied_total` | CounterVec | `client`, `topic` | Всего отказов ACL по клиенту и топику. |
| `aqueduct_messages_dropped_total` | CounterVec | `topic`, `policy` | Всего сообщений, отброшенных backpressure. |
| `aqueduct_messages_expired_total` | CounterVec | `topic`, `priority` | Всего сообщений с истёкшим TTL. |
| `aqueduct_messages_nacked_total` | CounterVec | `topic` | Всего NACK-сообщений. |
| `aqueduct_messages_dead_lettered_total` | CounterVec | `topic` | Всего сообщений, отправленных в DLQ. |
| `aqueduct_messages_rate_limited_total` | CounterVec | `client` | Всего сообщений, отклонённых token-bucket. |
| `aqueduct_slow_consumers_disconnected_total` | Counter | — | Всего медленных подписчиков, отключённых по политике `disconnect`. |
| `aqueduct_aal_rotations_total` | Counter | — | **Неактивная метрика.** Зарегистрирована в Prometheus, но в коде нет ни одного `.Inc()` на этом счётчике (см. `internal/metrics/metrics.go:77-82,177`). Всегда возвращает `0`. Не сигнализируйте по ней: встроенного планировщика ротации AAL нет. |
| `aqueduct_aal_backfill_frames_total` | Counter | — | Всего исторических AAL-фреймов, доставленных подписчику при backfill. |
| `aqueduct_cluster_frames_forwarded_total` | Counter | — | Всего фреймов, пересланных пирам. |
| `aqueduct_cluster_frames_received_total` | Counter | — | Всего mesh-фреймов, полученных от пиров. |
| `aqueduct_tracing_spans_total` | Counter | — | Всего созданных OTel-спанов. |
| `aqueduct_admin_requests_total` | CounterVec | `method` | Всего запросов gRPC Admin API по методу. |

### Что мониторить

- **Throughput**: `rate(aqueduct_messages_published_total[5m])` vs. `rate(aqueduct_messages_delivered_total[5m])`. Сильное расхождение → подписчики не успевают потреблять.
- **Loss budget**: `rate(aqueduct_messages_dropped_total[5m]) + rate(aqueduct_messages_expired_total[5m])`. Ненулевое значение означает backpressure.
- **Rate limit hit**: `rate(aqueduct_messages_rate_limited_total[5m])`. Аномальные всплески → misbehaving tenant.
- **DLQ inflow**: `rate(aqueduct_messages_dead_lettered_total[5m]) > 0` — стабильно растущий поток требует расследования.

---

## 3. Датчики (Gauge / GaugeVec)

| Метрика | Тип | Labels | Help |
| :--- | :--- | :--- | :--- |
| `aqueduct_active_subscribers` | Gauge | — | Текущее количество активных подписчиков. |
| `aqueduct_durable_subscriptions_active` | Gauge | — | Текущее количество активных durable-подписок. |
| `aqueduct_consumer_offset` | GaugeVec | `consumer`, `topic` | Текущий подтверждённый offset подписчика. |
| `aqueduct_aal_replay_duration_seconds` | Gauge | — | Длительность Replay AAL при старте. |
| `aqueduct_cluster_peers_active` | Gauge | — | Текущее количество активных mesh-пиров. |

Эти метрики читайте через point-in-time (не rate):

- `aqueduct_active_subscribers` — общее число подписчиков. Падение ниже ожидаемого → отвал клиентов.
- `aqueduct_cluster_peers_active` — для N-узлового кластера ожидается `N - 1`. Отклонение → DNS discovery или reconnect-loop.
- `aqueduct_consumer_offset` — отставание durable consumer'а: `rtr(slope)` показывает скорость ack.

---

## 4. Гистограммы (Histogram)

| Метрика | Тип | Buckets | Help |
| :--- | :--- | :--- | :--- |
| `aqueduct_frame_parse_duration_ns` | Histogram | `prometheus.DefBuckets` | **Неактивная метрика.** Зарегистрирована в Prometheus, но в коде нет ни одного `.Observe()` на этом гистограмме (см. `internal/metrics/metrics.go:31-37,171`). Все bucket'ы возвращают `0`. Не используйте её как источник p99 латентности парсера. |

Buckets по умолчанию: `{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}`. Поскольку метрика неактивна, `histogram_quantile(...)` всегда даёт `NaN`.

---

## 5. Метрики Admin API

| Метрика | Метки | Где инкрементируется |
| :--- | :--- | :--- |
| `aqueduct_admin_requests_total{method="SetClientQuota"}` | `method` | `internal/admin/server.go::SetClientQuota` |
| `aqueduct_admin_requests_total{method="UpdateACL"}` | `method` | `internal/admin/server.go::UpdateACL` |

Неудачные вызовы (с `codes.Unauthenticated` / `codes.PermissionDenied`) **не** инкрементируют — interceptor отвергает их до метода.

---

## 6. Прометеевский конфиг (`prometheus.yml`)

Минимальный scrape job для брокера:

```yaml
scrape_configs:
  - job_name: aqueduct
    scrape_interval: 15s
    static_configs:
      - targets: ["aqueduct-broker:9090"]
        labels:
          service: aqueduct
```

---

## 7. Grafana

`docker-compose.yml` поднимает Prometheus и Grafana:

- Prometheus: `http://localhost:9091`
- Grafana: `http://localhost:3000` (Логин: `admin`, Пароль: `admin`)

Дашборд поставляется в `deploy/grafana/` (если присутствует) или конструируется вручную из метрик выше. Базовые панели:

- **Throughput**: `rate(aqueduct_messages_published_total[1m])` + `rate(aqueduct_messages_delivered_total[1m])`.
- **Latency p99**: ~~`histogram_quantile(0.99, rate(aqueduct_frame_parse_duration_ns_bucket[5m]))`~~ — **не работает**, метрика неактивна (см. §4). Вместо этого используйте внешний профайлер (`go tool pprof`, OTLP-трейсы) или собственные `Timer`-обёртки в клиентском коде.
- **Backpressure**: `rate(aqueduct_messages_dropped_total[1m])` с разбивкой по `policy`.
- **Mesh Health**: `aqueduct_cluster_peers_active` vs `aqueduct_cluster_frames_forwarded_total`.

---

## 8. Кардиальность и лейблы

Брокер **не использует user-controlled label values**: все лейблы формируются из операторских правил ACL или CN сертификата. Тем не менее:

- Неограниченное количество `client_id` или `topic` в ACL rules → линейный рост cardinality. Prometheus рекомендует держать cardinality < 10K на метрику.
- `topic` лейблы в `messages_*_total` принимают значения от publisher'ов. Для production удерживайте множество топиков ограниченным (high-cardinality топики ломают Prometheus).
- `client` лейблы извлекаются из TLS CN — ограничены количеством выданных клиентских сертификатов.

---

## 9. Стандартные Process-метрики

Используется `prometheus/client_golang` — go runtime метрики (`go_goroutines`, `go_gc_duration_seconds`, `process_cpu_seconds_total`) экспортируются автоматически. Дополнительной настройки не требуется.