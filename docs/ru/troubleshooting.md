# Справочник: Диагностика и устранение неполадок (v1.16.0)

Практическое руководство по диагностике проблем и их решению в Aqueduct.

> **Diátaxis:** Практическое руководство (How-to) — конкретные команды и метрики для диагностики.

---

## 1. Диагностические эндпоинты

| Эндпоинт | Порт | Назначение |
| :--- | :--- | :--- |
| `/healthz` | `:9090` | Liveness probe. `200 OK` означает, что HTTP-сервер живой. |
| `/metrics` | `:9090` | Prometheus scrape endpoint. Полный список — [`metrics.md`](metrics.md). |

> `/healthz` **не** проверяет QUIC-листенер или состояние роутера. Это простой HTTP 200.

---

## 2. Диагностика по логам

Брокер использует `log/slog` с text handler на stderr (`cmd/broker/main.go:45`). Уровни: `INFO`, `WARN`, `ERROR`. Структурированные поля:

- `addr` — сетевой адрес;
- `stream_id` — идентификатор QUIC-стрима;
- `client_id` — CN TLS-сертификата;
- `remote` — remote-адрес клиента;
- `caller` — CN Admin API клиента;
- `err` — ошибка.

Типичные сообщения:

| Сообщение | Условие | Действие |
| :--- | :--- | :--- |
| `broker listening addr=:4242` | Успешный bind UDP. | OK |
| `connection accepted remote=... 0rtt=...` | Установлен QUIC handshake. | OK; `0rtt=true` — клиент использовал session ticket. |
| `Using ephemeral self-signed certificate` | `tls.generate: true`. | **Warning**; в production отключите. |
| `admin API access denied: invalid common name` | Admin API вызван без `admin-` CN. | Проверьте CN клиентского сертификата. |
| `cluster mesh TLS verification disabled` | `insecure_skip_verify: true`. | **Warning**; в production отключите. |
| `peer added / peer removed` | Mesh dynamic peer change. | OK; reconcile с ожидаемым числом пиров. |
| `AAL replay completed records_replayed=N` | Успешный Replay при старте. | OK; N должно соответствовать числу записей в AAL. |
| `frame parse error` | Malformed frame на проводе. | Проверьте клиент-источник. |
| `oversized payload or memory limit exceeded` | `payload_len > max_buf_size`. | Увеличьте `transport.max_buf_size` или уменьшите payload. |
| `accept stream error` | Ошибка принятия стрима (сеть закрыта). | Сетевая диагностика. |
| `broker stopped cleanly` | Graceful shutdown. | OK |

---

## 3. Диагностика подключения клиентов

### 3.1 Клиент не подключается

```bash
# 1. Проверьте, что UDP-порт доступен
ss -ulpn | grep 4242

# 2. Проверьте healthcheck
curl -v http://localhost:9090/healthz

# 3. Проверьте логи broker'а
# Ищите "connection accepted" — если нет, handshake не дошёл.
journalctl -u aqueduct -f
```

Типичные ошибки:

| Симптом | Вероятная причина |
| :--- | :--- |
| `connection refused` | Порт закрыт firewall'ом или брокер не запущен. |
| `TLS handshake error` | Несоответствие ALPN, `aqueduct-v1` отсутствует в `NextProtos`. |
| `certificate verify failed` | `tls.require_client_cert: true`, но клиент не передал сертификат, или CA не совпадает. |
| `i/o timeout` | UDP firewall блокирует ответы. Откройте порт. |

### 3.2 0-RTT не работает

`Allow0RTT: true` уже установлен в `internal/transport/broker.go:185`. Проверьте:

- Клиент должен использовать `tls.Config{ClientSessionCache: ...}` или эквивалент для кэширования session tickets.
- В логе `0rtt=true` означает успешный 0-RTT resumption.

---

## 4. Диагностика публикации и подписки

### 4.1 Сообщения не доставляются

1. **Подписчик зарегистрирован?** Проверьте `aqueduct_active_subscribers`:

   ```promql
   aqueduct_active_subscribers
   ```

   Должно быть > 0.

2. **ACL отвергает?** Проверьте `rate(aqueduct_authz_denied_total[1m])` — ненулевое значение означает отказ.

3. **Rate limit?** Проверьте `rate(aqueduct_messages_rate_limited_total[1m])`. Если клиент превышает квоту, увеличьте `broker.quotas.default_publish_rate` или добавьте per-client override.

4. **Backpressure?** Проверьте `rate(aqueduct_messages_dropped_total[1m])`. Если подписчик не успевает, увеличьте `broker.queue_size` или смените `backpressure_policy`.

5. **TTL?** Проверьте `rate(aqueduct_messages_expired_total[1m])`. Сообщения с приоритетом 0/1 могут истекать по `priority_ttls`.

6. **DLQ?** Проверьте `rate(aqueduct_messages_dead_lettered_total[1m])`. Сообщения с `max_retries` NACK'ами уходят в `__dlq__<topic>`.

### 4.2 Publisher получает ошибку

| Ошибка | Причина |
| :--- | :--- |
| `payload exceeds maximum frame size` | `payload_len > 1 MB` (`maxPayloadSize` в router.go:25). |
| `aqueduct_messages_dropped_total` | Slow consumer на subscriber стороне. |
| `aqueduct_messages_expired_total` | TTL истёк до доставки (низкий приоритет + долгий backpressure). |

### 4.3 Подписчик не получает сообщения по wildcard

1. Проверьте формат pattern:

   - `+` — ровно один сегмент.
   - `#` — ноль или более сегментов в конце.

2. Сегменты разделяются `/`. Топик `sensor.room1.temp` имеет 3 сегмента.

3. `sensor/+/temp` НЕ совпадает с `sensor/room1/temp/sub` (только один сегмент между `/`).

4. `sensor/#` совпадает с `sensor/room1`, `sensor/room1/temp`, и т. д.

### 4.4 Consumer Group не балансирует

- Убедитесь, что **все** подписчики используют одинаковый `<group_id>` в `CmdSubscribe` payload.
- `atomic.AddUint64` в `ConsumerGroup.counter` даёт lock-free round-robin — проверьте, что в `topicGroups` для группы есть > 1 индекс.
- Долговечный (`durable`) подписчик с устаревшим offset может "перетягивать" все сообщения на себя.

---

## 5. Диагностика mesh

### 5.1 Узел не подключается к mesh

1. Проверьте логи на старте: `peer added / peer removed`.
2. Проверьте `aqueduct_cluster_peers_active` — для N-узлового mesh ожидается `N - 1` на каждом узле.
3. Проверьте `peer_tls`:

   ```bash
   openssl s_client -connect peer:4242 -alpn aqueduct-mesh -cert client.crt -key client.key -CAfile mesh_ca.pem
   ```

4. DNS Discovery: `dig aqueduct-headless.default.svc.cluster.local A`.

### 5.2 Forwarding задерживается

`aqueduct_cluster_frames_forwarded_total` показывает cumulative count. Rate:

```promql
rate(aqueduct_cluster_frames_forwarded_total[5m])
```

Нулевой rate + растущий `published_total` → пересылка сломана. Проверьте `cluster.mesh.insecure_skip_verify` (false в production), `aqueduct_cluster_peers_active` (должно быть > 0) и firewall на UDP.

### 5.3 Reconnect storm

Если `aqueduct_cluster_peers_active` колеблется — у ping-pong подключений:

- Слишком короткий backoff между диалами.
- Нестабильный DNS resolution.
- Firewall теряет UDP-пакеты.

Проверьте UDP MTU и `netstat -s` на потерю пакетов:

```bash
netstat -s | grep -i "packet loss"
```

---

## 6. Диагностика AAL

### 6.1 Replay долгий

- `aqueduct_aal_replay_duration_seconds` — длительность Replay на старте.
- Если > 30 секунд: файл AAL слишком большой. Уменьшите размер файла внешней ротацией (`logrotate`/`copytruncate` или k8s-sidecar). Поле `aal.max_aal_size` декларативно — **встроенной ротации нет**, поэтому изменение YAML не уменьшит существующий файл. Метрика `aqueduct_aal_rotations_total` всегда `0` (зарегистрирована, но не инкрементируется).

### 6.2 AAL повреждён

`internal/aal/aal.go::Replay` восстанавливается при ошибке best-effort:

```go
if recLen <= 0 || recLen > 10*1024*1024 {
    consumed++  // skip 1 byte and continue
    continue
}
```

Битые записи молча пропускаются — это допустимо для fire-and-forget лога. Если **все** записи не читаются:

1. Проверьте, что `aal.key` совпадает с тем, что использовался при записи.
2. Проверьте режим файла: `chmod 600 /var/log/aqueduct/aal.log`.
3. Проверьте диск: `dmesg | grep -i "I/O error"`.

### 6.3 Ключ неверный

```
aal: encryption key must be 32 bytes for AES-256
```

`aal.key` должен быть 32 байта после base64-декодирования. Сгенерируйте снова:

```bash
openssl rand -base64 32
```

---

## 7. Диагностика TLS

### 7.1 Сертификат отвергнут

1. Проверьте цепочку:

   ```bash
   openssl verify -CAfile /etc/aqueduct/client_ca.pem /etc/aqueduct/server.crt
   ```

2. Проверьте SAN:

   ```bash
   openssl x509 -in /etc/aqueduct/server.crt -text -noout | grep -A1 "Subject Alternative Name"
   ```

3. Проверьте expiry:

   ```bash
   openssl x509 -in /etc/aqueduct/server.crt -noout -dates
   ```

### 7.2 mTLS отвергает клиента

Включите debug в клиенте и найдите `certificate verify failed`:

```bash
GODEBUG=tls=1 ./client
```

Типичные причины:

- CN клиента не в CA (`client_ca_file`).
- Сертификат клиента истёк.
- SAN клиента не покрывает адрес, по которому идёт подключение (если включена hostname verification).

### 7.3 ALPN mismatch

Брокер ожидает `aqueduct-v1` (клиенты) и `aqueduct-mesh` (пиры). Если клиент шлёт `h2` или `h3` — handshake отвергается. Установите:

```go
tls.Config.NextProtos = []string{"aqueduct-v1"}
```

---

## 8. Диагностика WebTransport

### 8.1 Браузер не подключается

1. Сертификат должен быть публично доверенным или явно добавленным в trust store.
2. UDP firewall должен пропускать выбранный порт (`webtransport.listen_addr`).
3. HTTP/3 ALPN (`h3`) должен быть в `NextProtos`. `cmd/broker/main.go` добавляет его автоматически.
4. `:path` в JS должен совпадать с `webtransport.path_prefix` (по умолчанию `/aqueduct/wt`).

### 8.2 Handshake timeout

`handshakeTimeout = 10 * time.Second` в `internal/webtransport/server.go:62`. Если браузер не отправит Extended CONNECT за 10 секунд — соединение закрывается.

Проверьте:

- Network latency (особенно мобильные сети).
- Slowloris-атаки — Sentry в логе `wt handshake timeout`.

### 8.3 Frame errors из браузера

Браузер пишет тот же формат, что и нативные клиенты. Если `frame parse error`:

- Проверьте, что `buildFrame()` в `examples/web/app.js` использует `MagicByte = 0x1F`.
- Проверьте порядок байтов в `StreamID` / `DataLen` — Little-Endian.

---

## 9. Диагностика Admin API

### 9.1 `PermissionDenied`

```json
{"code": 7, "message": "access denied: client CN \"user-1\" does not have admin role"}
```

Решение: переиздайте клиентский сертификат с CN, начинающимся с `admin-`.

### 9.2 `Unavailable`

```json
{"code": 14, "message": "authorization engine is not enabled"}
```

`UpdateACL` требует `authz.Engine`. Если `acl.enabled: false`, движок не инициализирован. `cmd/broker/main.go:308` создаёт engine автоматически при `admin.enabled: true`, но если вы обходите `main.go` — убедитесь, что engine передан в `admin.NewServer`.

### 9.3 gRPC deadline exceeded

Admin API требует TLS handshake. Если deadline < 1 секунды, может не успеть. Увеличьте:

```go
ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
```

---

## 10. Диагностика WebTransport из Node.js / Go

Пример минимального клиента на Go (`examples/go/main.go`):

```go
conn, err := quic.DialAddr(ctx, "broker.example.com:4433", &tls.Config{
    NextProtos: []string{"h3"},
    RootCAs:    pool,
}, nil)
if err != nil { panic(err) }
```

Если клиент зависает — проверьте, что `:4433` UDP-порт открыт на firewall.

---

## 11. Performance troubleshooting

### 11.1 Пропускная способность ниже ожидаемой

1. **Bottleneck в Publish**: `rate(aqueduct_messages_published_total[1m])` — если publisher упирается в лимит, увеличьте `broker.quotas.default_publish_rate`.

2. **Bottleneck в Deliver**: `rate(aqueduct_messages_delivered_total[1m])` — если подписчик не успевает, увеличьте `broker.queue_size` или добавьте реплик брокера.

3. **Bottleneck в Parse**: `histogram_quantile(0.99, rate(aqueduct_frame_parse_duration_ns_bucket[5m]))` — **метрика неактивна** (зарегистрирована, но `Observe` не вызывается), `histogram_quantile` вернёт `NaN`. Используйте `go tool pprof` для профилирования парсера (`internal/protocol/frame.go`) или оборачивайте парсинг собственным `Timer` в клиентском коде. Ожидаемые значения на горячем пути — десятки наносекунд.

4. **GC pressure**: `go_gc_duration_seconds` и `go_gc_pause_seconds`. Aqueduct стремится к 0 allocs/op; если растёт — посмотрите `protocol.ReleaseBuffer` — он не вызывается.

### 11.2 Латентность выросла

1. **Buffer pressure**: проверьте `aqueduct_active_subscribers` × `broker.queue_size`. Если subscriber не успевает, увеличьте размер или примените `disconnect`.

2. **TLS resumption**: проверьте `0rtt=true` в логе. Если 0-RTT не используется, латентность растёт на RTT.

3. **DNS discovery**: слишком короткий interval (например, 1s) может нагружать DNS. Default 10s.

---

## 12. Часто встречающиеся ошибки в логах

### `frame parse error: invalid magic byte`

Клиент отправил фрейм с `Magic != 0x1F`. Проверьте реализацию клиента.

### `frame parse error: unknown command`

Opcode за пределами `CmdPublish..CmdNack`. Возможно, клиент использует устаревший протокол или неправильно интерпретирует Control Bits.

### `frame parse error: extensions exceed declared data length`

TLV-блок не помещается в `DataLen`. Скорее всего повреждённый блок.

### `frame truncated: data exceeds buffer length`

Брокер не получил полный фрейм. Это нормально для частичных TCP/UDP-датаграмм; transport-loop ждёт остаток.

### `payload length exceeds maxBufSize`

`payload_len` > `transport.max_buf_size` (64 KB default). Увеличьте `max_buf_size` или уменьшите payload.

### `buffer too short for header`

Меньше 10 байт. Скорее всего, мусор от сетевой диагностики или UDP-фрагментация.

---

## 13. Контакты и баг-репорты

- Issues: <https://github.com/kshishtovsky/aqueduct/issues>
- Сбор метрик при баге:

  ```bash
  # 1. Снимите stack trace
  curl -s http://localhost:9090/debug/pprof/goroutine?debug=2 > goroutines.txt

  # 2. Снимите метрики
  curl -s http://localhost:9090/metrics > metrics.txt

  # 3. Сделайте config (без ключей!)
  cp config.yaml /tmp/config-no-secrets.yaml
  sed -i 's/key:.*/key: "<redacted>"/' /tmp/config-no-secrets.yaml

  # 4. Соберите в tar
  tar czf aqueduct-debug.tgz \
      /var/log/aqueduct/aqueduct.log \
      goroutines.txt metrics.txt /tmp/config-no-secrets.yaml
  ```