# Объяснение: Архитектура и модель памяти (v1.16.0)

Документ описывает архитектурные принципы, Data-Oriented Design (DoD), механизмы безопасности и стратегии управления памятью с нулевыми аллокациями в Aqueduct.

> **Diátaxis:** Объяснение (Explanation) — почему архитектура устроена именно так. Без пошаговых how-to.

---

## 1. Почему QUIC вместо TCP?

Традиционные брокеры на TCP страдают от проблемы Head-of-Line (HoL) blocking при мультиплексировании топиков в одном соединении. Потеря одного пакета останавливает передачу для всех топиков.

Aqueduct использует **QUIC** (`quic-go`):

- **Изоляция на уровне стримов**: Потеря пакетов в одном топике не блокирует другие топики.
- **0-RTT Resumption**: Устраняет задержки TLS при повторных подключениях. `quic.Config.Allow0RTT = true` в `internal/transport/broker.go:185`.
- **Транспорт UDP**: Снижает задержки ядра ОС.

---

## 2. Structure of Arrays (SoA) и Ленивые очереди приоритетов (QoS)

Стандартная Go-реализация подписчиков через `map[string][]*Subscriber` ухудшает cache-locality: указатели разбросаны по куче. Aqueduct использует схему **Structure of Arrays (SoA)** с ленивыми кольцевыми каналами приоритетов и Writer-горутинами на каждого подписчика (`internal/broker/router.go`):

```go
type Router struct {
    mu sync.RWMutex

    // SoA плоские параллельные массивы
    streamIDs []uint32               // Stream ID подписчиков
    streams   []*quic.Stream         // Указатели на QUIC-стримы
    topics    []string               // Имена топиков
    active    []bool                 // Флаги активности
    queues    []*[4]chan *MessageRef // Указатель на 4 ленивые очереди приоритетов (0=Highest .. 3=Low)
    notifyChs []chan struct{}        // Per-subscriber notification
    subMus    []*sync.RWMutex        // Per-subscriber RWMutex для ленивой инициализации и очистки
    cancels   []context.CancelFunc   // Хэндлы отмены Writer-горутин

    // topicIndex maps FNV-1a hash of topic name to slice of indices in flat arrays.
    topicIndex map[uint64][]int
    groups     map[uint64][]*ConsumerGroup
    queuePool  sync.Pool
}
```

### Ленивая аллокация и Строгая приоритизация

1. **Ленивая инициализация:** При подписке `queues[idx]` — указатель `*[4]chan *MessageRef` со всеми 4 значениями `nil`. Канал выделяется из `r.queuePool` (`0 allocs/op`) только при поступлении сообщения приоритета `P`.
2. **Строгий опрос приоритетов:** Writer-горутина вызывает `fetchNextMessage`, который опрашивает очереди в строгом порядке `0 -> 1 -> 2 -> 3`. Критические алерты передаются вне очереди.
3. **Per-Priority TTL:** Для каждого приоритета вычисляется свой `expiresAt` (из `priority_ttls`). При извлечении сообщения с просроченным `expiresAt` оно уничтожается до записи в сокет (`aqueduct_messages_expired_total{topic, priority}`).
4. **Рециклинг памяти:** Когда очередь опустошается (`len(q) == 0`), `cleanupEmptyQueue` возвращает её в `r.queuePool` и сбрасывает указатель в `nil`. Подписчик с 1 приоритетом потребляет память под 1 очередь.

---

## 3. TopicHash: единый источник правды (v1.16.0)

До v1.16.0 существовала критическая ошибка: подписчики и publisher'ы использовали **разные** формулы FNV-1a-хэша для ключа топика. Один путь вызывал `authz.CombineHashes(clientID, topicBytes)`, другой — `authz.CombineHashStrings("topic", topic)`. Это приводило к коллизиям хэшей, при которых сообщение уходило не тому подписчику (или не уходило никому).

В v1.16.0 введена функция `topicHashKey(topic string) uint64` (`broker/router.go:1554`), которая **единственная** вычисляет хэш топика для SoA-таблиц:

```go
func topicHashKey(topic string) uint64 {
    return authz.CombineHashStrings("topic", topic)
}
```

Все hot-path маршруты — `Subscribe`, `Publish`, `PublishFromPeer`, `PublishBatch` — вызывают **только** `topicHashKey()`. Если где-то в коде остался inlined вызов `authz.CombineHashStrings("topic", ...)`, это регрессия — фиксируйте через PR.

Дополнительно: `parsePublishTopic` (`router.go:1541`) и `parseSubscriptionPayload` стрипают префикс `topic:` детерминированно, гарантируя что **routing key всегда post-strip clean topic name**, а не raw payload.

---

## 4. Атомарный подсчёт ссылок (`MessageRef`)

Безопасное переиспользование буферов в `sync.Pool` без гонок данных (`internal/broker/msg_ref.go`):

```go
type MessageRef struct {
    buf       *[]byte    // буфер из пула (только для родителя)
    frame     []byte     // zero-copy под-срез буфера родителя (для потомков батча)
    ref       atomic.Int32
    expiresAt int64      // unix nano, 0 = бессрочно
    offset    uint64     // смещение в топике
    parent    *MessageRef // указатель на родителя (nil для родителей)
}
```

- `AcquireMessageRef` создаёт обёртку с `ref = 1`.
- Для каждого подписчика `Retain()` увеличивает `ref`.
- Каждая Writer-горутина после отправки в сеть вызывает `Release()`.
- При `ref.Add(-1) == 0` буфер возвращается в `protocol.ReleaseBuffer`, а `MessageRef` — в `msgRefPool` (**`0 allocs/op`**).
- **Nested Reference Counting (v1.6.0+)**: для батч-сообщений используется иерархия «родитель-потомок». Родительский `MessageRef` создаётся для буфера батча с `ref = 1 + кол-во фреймов`. Каждый потомок создаётся через `AcquireChildMessageRef()` с полем `frame []byte`, указывающим на под-срез в буфере родителя. При `Release()` потомка, когда его ref достигает 0, вызывается `parent.Release()`. Жизненный цикл `buf` родителя управляется через `protocol.ReleaseBuffer`.

---

## 5. Wildcard Topic Matching без аллокаций

- `+` совпадает с одним сегментом топика между `/`.
- `#` совпадает со всеми последующими сегментами.
- `MatchWildcard(pattern, topic []byte)` (`internal/broker/wildcard.go`) работает за **50.41 ns/op** при **`0 allocs/op`**.

---

## 6. Архитектура безопасности (mTLS, FNV-1a ACL, AES-GCM AAL)

1. **mTLS 1.3**: Валидация сертификатов клиентов по пулу `client_ca_file`. CN клиентского сертификата используется как `client_id` для ACL и Admin API (`admin-*` префикс).
2. **Некоммутативный ACL**: `authz.CombineHashes(clientID + ":" + topic)` через FNV-1a исключает XOR-коммутативность (`hash(A, B) != hash(B, A)`). Контрактная проверка в `authz_test.go::TestNonCommutativity`.
3. **AES-256-GCM AAL и Replay**: Шифрование журналов с 12-байтным Nonce (4-байтный случайный префикс сессии + 8 байт монотонного счётчика) и 4-байтным заголовком длины. Replay выполняется **до открытия UDP-порта** — `b.ReplayAAL(ctx, b.aalPath, b.aalKey)` в `transport/broker.go:178`.

### RCU Hot-Reload

`authz.Engine.rulesPtr` — `atomic.Pointer[map[uint64]Permission]`. `Reload()` атомарно подменяет map; проверки в `Allowed()` читают snapshot без блокировок. Это позволяет gRPC Admin API обновлять ACL без паузы на горячем пути.

---

## 7. Direct Mesh Clustering (P2P Federation) и DNS Discovery

Aqueduct поддерживает формирование кластера из брокеров, соединённых через прямую P2P QUIC-сеть. Центральный координатор или консенсус (Raft/Paxos) отсутствуют — пересылка сообщений работает по принципу fire-and-forget.

### PeerManager (RCU-паттерн, v1.14.0+)

`internal/cluster/cluster.go::PeerManager`:

```go
type PeerManager struct {
    peers   atomic.Pointer[peerSlice]   // атомарный снимок без блокировок
    mu      sync.Mutex                  // только для записи (AddPeer/RemovePeer)
    addrSet map[string]context.CancelFunc
}
```

- **Чтение** (`Forward()`, `PeerCount()`): Атомарный указатель — ноль блокировок
- **Запись** (`AddPeer()`, `RemovePeer()`): Создание нового слайса, атомарная замена
- **Динамическое управление пирами**:
  - `AddPeer(ctx, addr)` — подключение к новому пиру, добавление в атомарный снимок
  - `RemovePeer(addr)` — закрытие соединения, удаление из атомарного снимка
  - `PeerCount()` — текущее количество пиров (атомарное чтение)

### DNS Discovery (v1.14.0+)

Для Kubernetes StatefulSet модуль `Discovery` (`internal/cluster/discovery.go`) опрашивает DNS-записи Headless Service:

```go
type Discovery struct {
    resolver Resolver            // инжектируемый интерфейс для тестов
    hostname string              // FQDN Headless Service
    port     string              // port suffix
    interval time.Duration       // polling interval (default 10s)
    knownIPs map[string]struct{} // быстрое отслеживание изменений
}
```

- Опрос `net.LookupHost(hostname)` каждые `interval` (по умолчанию 10с)
- Вычисление diff с `knownIPs` → вызов `AddPeer()`/`RemovePeer()` только при изменениях
- `normalize()` дедуплицирует IP и валидирует через `net.ParseIP`
- Интерфейс `Resolver` позволяет мокать DNS в тестах

### MeshForwarded Bit

Один бит в Command-байте протокола (бит 7, маска `0x80`) помечает фрейм как уже пересланный. Получающие пиры проверяют этот бит и пропускают повторную пересылку, предотвращая широковещательные штормы в многоузловой топологии.

### Zero-Copy Forwarding

`PeerManager.Forward` (cluster.go:238) читает атомарный снимок пиров, затем **мутирует MeshForwarded-бит на месте** в общем буфере (0 аллокаций в куче) и пишет напрямую в QUIC-поток каждого пира. После записи бит восстанавливается:

```go
orig := rawBuf[1]
rawBuf[1] = orig | byte(protocol.MeshForwardedBit)
_, werr := s.Write(rawBuf)
rawBuf[1] = orig
```

### Интеграция с Router

При обработке локального сообщения в `Router.publishWithClientID` (router.go:925):

1. `hasPeers` проверяется **до** раннего возврата при `len(indices) == 0` — иначе сообщения не форвардятся в кластер при отсутствии локальных подписчиков.
2. Сообщение доставляется локальным подписчикам через SoA fan-out.
3. Вызывается `PeerManager.Forward()` для отправки всем пирам.
4. Получающий пир вызывает `Router.PublishFromPeer()`, который диспатчит только локально (без повторной пересылки).

---

## 8. WebTransport Gateway (HTTP/3, v1.16.0+)

Браузеры не могут использовать нативный ALPN брокера `aqueduct-v1` — они поддерживают только HTTP/3 + W3C WebTransport API. Шлюз транслирует на транспортном уровне без изменений в протоколе (`internal/webtransport/`):

```text
   ┌─────────────┐     HTTP/3     ┌──────────────────────┐     QUIC bidi     ┌───────────────────┐
   │ Браузер     │ ─ WebTransport► │ internal/webtransport│ ────────────────► │ internal/transport│
   │ (W3C API)   │     streams    │ (этот пакет)         │  *quic.Stream    │ (broker)          │
   └─────────────┘                └──────────────────────┘                   └───────────────────┘
                                             │                                        │
                                             └─── повторно используется через broker.HandleStream ────┘
```

Гарантии дизайна:

- **Один TLS-конфиг — два листенера.** Шлюз вызывает `http3.ConfigureTLSConfig` поверх `*tls.Config` брокера, поэтому одним mTLS-сертификатом защищены оба порта. `h3` добавляется в `NextProtos` без мутации основного конфига (`cmd/broker/main.go:337-347`).
- **Hijack handshake-стрима.** `responseWriter.HTTPStream()` возвращает базовый `*http3.Stream`. Мы отправляем `200 OK` для завершения WebTransport Extended CONNECT handshake, затем в цикле вызываем `conn.AcceptStream()` и передаём каждый bidi-стрим в `broker.HandleStream(ctx, conn, s, clientID)` — ту же функцию, что вызывает нативная QUIC-сессия.
- **Без трансляции протокола.** Браузер пишет `[Magic:1][Cmd:1][StreamID:4][DataLen:4][Payload:N]` в `WebTransportBidirectionalStream`; парсер фреймов брокера обрабатывает это без изменений.
- **Синхронный handshake timeout.** Через `WithHandshakeTimeout(...)` (по умолчанию 10s) мы блокируем Slowloris-атаки.
- **0-RTT по умолчанию.** `Allow0RTT: true` в `quic.Config`.

Подробнее — [`configuration.md`](configuration.md#12-секция-webtransport-webtransportconfig) и [`troubleshooting.md`](troubleshooting.md#8-диагностика-webtransport).

---

## 9. Батчинг и коалесцированная запись

### Проблема: OS PPS лимиты

`quic.Stream.Write()` имеет высокую стоимость одного вызова (syscall, пакетизация, крипто). Отправка одного фрейма за раз ограничивает пропускную способность ~300k RPS.

### Решение: Умный батчинг

#### 9.1 Протокольный батчинг (`CmdPublishBatch`)

Команда `0x05` упаковывает множество стандартных фреймов в плоский массив байт внутри одного QUIC-запроса:

```text
+--------------------------+
| CmdPublishBatch Frame    |
| +----------------------+ |
| | Sub-frame 1..N       | |
| +----------------------+ |
+--------------------------+
```

Под-фреймы распаковываются через `unsafe.Slice` с pointer arithmetic (`internal/protocol/frame.go::ParseBatch`) — каждый под-слайс указывает напрямую в буфер родительского батча (**zero-copy unpack**, `0 allocs/op`).

#### 9.2 Вложенный подсчёт ссылок (Nested RC)

1. **Parent `MessageRef`** создаётся для буфера батча (`ref = 1 + кол-во фреймов`)
2. **Child `MessageRef`** создаются для каждого под-фрейма через `AcquireChildMessageRef()` — каждый child хранит `frame []byte`, указывающий в буфер parent
3. На `Release()`: когда child ref → 0, вызывается `parent.Release()`. Когда parent ref → 0, буфер возвращается в `sync.Pool`
4. Все операции — `atomic.Int32`, без блокировок на горячем пути

#### 9.3 Коалесцированная запись (Coalesced Writer)

Writer-горутина подписчика аккумулирует outgoing фреймы и сбрасывает их батчем при:

1. **Порог размера**: накоплено > `batch_size` (по умолчанию 64 КБ)
2. **Микро-таймер**: единый переиспользуемый `time.Timer` сбрасывается после первого фрейма и стреляет через `flush_interval` (по умолчанию 50 µs)

```yaml
broker:
  batch_size: 65536
  flush_interval: "50us"
```

#### 9.4 Бенчмарки

| Сценарий | Пропускная способность | allocs/op |
| :--- | :--- | :--- |
| **BatchUnpack** (1000 фреймов) | 19.9 GB/s | **0** |
| **BatchPublish** (100 сообщ.) | 6.67M msg/s, 921 MB/s | **0** |
| Single vs Batch (на сообщение) | ~150 ns/msg (batch) vs ~920 ns/msg (single) | **0** |

---

## 10. NACK-редиливери и очереди мёртвых сообщений (DLQ)

Aqueduct поддерживает механизм отрицательного подтверждения (NACK) для надёжной доставки:

### Протокол NACK (`CmdNack`)

- **Опкод**: `0x06` (`CmdNack`)
- **Полезная нагрузка**: 8-байтовый uint64 со смещением сообщения (Little-Endian)
- При получении брокер находит исходное сообщение по `ExtRetryOffset` TLV и планирует повторную доставку

### Автоматическая повторная доставка

- Каждое сообщение имеет внутренний счётчик попыток (`nackCounters map[nackKey]int8`).
- `max_retries` по умолчанию: 3.
- После каждого NACK сообщение повторно ставится в очередь подписчика.
- После исчерпания `max_retries`: сообщение направляется в Dead Letter Queue `__dlq__<topic>`.

### Кэш фреймов на подписчика

- Ограниченный FIFO-кэш (256 записей) на каждого подписчика.
- Хранит отображение `offset → topic` для O(1) поиска при редиливери.
- Предотвращает истощение памяти от злонамеренных NACK.

### Dead Letter Queue

- После исчерпания `max_retries` poison pill направляется в `__dlq__<оригинальный_топик>`.
- Подписчики DLQ используют стандартные семантики подписки на паттерн `__dlq__`.

### Метрики

| Метрика | Описание |
| :--- | :--- |
| `aqueduct_messages_nacked_total` | Всего NACK-сообщений (по топикам). |
| `aqueduct_messages_dead_lettered_total` | Всего сообщений в DLQ (по топикам). |

### Беспроводной путь

- `NackByStream` направляет NACK через буферизированный канал — ноль блокировок на горячем пути.
- Канал подписчика развязывает получение NACK от обработки редиливери.

---

## 11. Slab-аллокатор

Aqueduct заменяет `sync.Pool` для буферов `*[]byte` на горячем пути на высокопроизводительный slab-аллокатор (`internal/mem/`):

### Дизайн

- **Предварительно выделенные арены**: непрерывная память на класс размера
- **Классы размеров**: 128B, 256B, 512B, 2KB, 8KB, 32KB
- **Безблокировочный free-list**: Treiber stack (атомарный CAS) для аллокации и деаллокации
- **Нулевое давление на GC**: Память арен никогда не сканируется сборщиком мусора

### Производительность

| Метрика | Значение |
| :--- | :--- |
| Латентность аллокации | ~15 ns/op (без конкуренции) |
| Аллокаций на операцию | 0 (предварительно выделено) |
| Влияние на GC | Отсутствует |

### Интеграция

- `slab.Allocate(size) → *[]byte` заменяет `pool.Get().(*[]byte)`
- `slab.Deallocate(buf)` заменяет `pool.Put(buf)`
- Для размеров вне классов slab используется heap-аллокация

---

## 12. Per-Tenant Rate Limiting (Token Bucket)

Lock-free rate limiting на арендатора через алгоритм Token Bucket (`internal/quotas/bucket.go`):

### Дизайн

- **Lock-Free Token Bucket**: Атомарные операции для потребления и пополнения токенов (`atomic.Int64` для `tokens` и `rate`).
- **Фоновое пополнение**: Тикер 100 ms пополняет все корзины.
- **Изоляция арендаторов**: Каждый клиент имеет независимую корзину токенов.

### Производительность

| Метрика | Значение |
| :--- | :--- |
| Проверка (без конкуренции) | 2.1 ns/op |
| Аллокаций на операцию | 0 |

### Конфигурация

```yaml
broker:
  quotas:
    default_publish_rate: 1000  # сообщений в секунду
    default_burst_size: 100     # burst capacity
    per_client:
      noisy-tenant:
        rate: 100
        burst: 50
```

Переменные окружения:

| Переменная | Поле |
| :--- | :--- |
| `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` | `broker.quotas.default_publish_rate` |
| `AQUEDUCT_BROKER_DEFAULT_BURST_SIZE` | `broker.quotas.default_burst_size` |

### Интеграция

- Проверка в `Router.publishWithClientID` перед отправкой сообщения.
- При превышении лимита сообщение отбрасывается, счётчик увеличивается.
- Метрика: `aqueduct_messages_rate_limited_total{client}`.
- Per-client overrides управляются через gRPC Admin API (`SetClientQuota`).