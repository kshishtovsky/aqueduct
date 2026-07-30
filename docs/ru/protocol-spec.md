# Справочник: Спецификация бинарного протокола (v1.16.0)

Формальная спецификация zero-copy бинарного сетевого протокола Aqueduct.

---

## 1. Формат сетевого кадра (Frame Wire Format)

Протокол использует 10-байтовый бинарный заголовок в формате **Little-Endian**, за которым следует опциональный блок TLV-расширений и полезная нагрузка:

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Magic (0x1F)  | Command (1B)  |         Stream ID (4B)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Stream ID    |        Payload/Data Length (4B)               |
|  continued    |                                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| [Ext Total Len (2B) | Type | Len | Value ... ][ Payload N ...]|
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Поля заголовка

| Смещение | Поле | Тип | Описание |
| :--- | :--- | :--- | :--- |
| `0` | `Magic` | `uint8` | Сигнатура протокола `0x1F` (unit separator). Константа `protocol.MagicByte`. |
| `1` | `Command` | `uint8` | Код команды + управляющие биты (Бит 7: MeshForwarded `0x80`, Бит 6: HasExtensions `0x40`). |
| `2-5` | `StreamID` | `uint32` | Идентификатор QUIC-стрима (Little-Endian). |
| `6-9` | `PayloadLen` | `uint32` | Длина `ExtBlockSize + PayloadLen` (Little-Endian). На проводе покрывает весь блок данных от смещения 10. |
| `10..` | `ExtBlock` | `[]byte` | Опциональный блок TLV-расширений (если `Command & 0x40 != 0`). |
| `10+Ext..` | `Payload` | `[]byte` | Полезная нагрузка. |

### Константы

| Имя | Значение |
| :--- | :--- |
| `protocol.MagicByte` | `0x1F` |
| `protocol.HeaderSize` | `10` байт |
| `protocol.MeshForwardedBit` | `0x80` (бит 7) |
| `protocol.HasExtensionsBit` | `0x40` (бит 6) |
| `protocol.MaxExtTotalLen` | `1024` байт |

---

## 2. Команды протокола и управляющие биты

| Код | Название | Описание | Формат Payload |
| :--- | :--- | :--- | :--- |
| `0x01` | `CmdPublish` | Публикация сообщения в топик | `[ttl:<ms>:]<topic_name>` или сырые данные |
| `0x02` | `CmdSubscribe` | Подписка стрима на топик или Consumer Group | `topic:<name>[:group:<group_id>][:durable:<client_id>:<offset>]` |
| `0x03` | `CmdUnsubscribe` | Отмена подписки | `topic:<topic_name>` |
| `0x04` | `CmdAck` | Подтверждение доставки по смещению | 8 байт uint64 (Little-Endian) — offset сообщения |
| `0x05` | `CmdPublishBatch` | Пакетная публикация под-фреймов | Плоский массив стандартных фреймов `[Magic][Cmd][StreamID][Len][Payload]...` |
| `0x06` | `CmdNack` | Отрицательное подтверждение по смещению сообщения | 8 байт uint64 (Little-Endian) — offset сообщения |

> Константы определены в `internal/protocol/frame.go` через `iota`: `CmdPublish=1, CmdSubscribe=2, CmdUnsubscribe=3, CmdAck=4, CmdPublishBatch=5, CmdNack=6`. Конкретный handler для каждого opcode устанавливается вызывающей стороной через parse route.

### Алгоритм парсинга opcode

`ParseFrame` (`internal/protocol/frame.go:85`) выполняет строгую проверку:

1. `buf[0] == MagicByte` (иначе `invalid magic byte`).
2. `cmd := Command(buf[1]) & ^MeshForwardedBit & ^HasExtensionsBit` — оба управляющих бита маскируются **перед** проверкой диапазона.
3. Диапазон opcode: `CmdPublish <= cmd <= CmdNack`. Всё остальное → `unknown command`.

`OpcodeOf(cmd)` (frame.go:176) делает то же самое для произвольного Command:

```go
cmd & ^MeshForwardedBit & ^HasExtensionsBit
```

### Управляющие биты (Биты 6 и 7)

- **MeshForwarded (Бит 7, `0x80`)**: Устанавливается при пересылке между узлами кластера (`PeerManager.Forward` мутирует `rawBuf[1]` in-place, см. `internal/cluster/cluster.go:251-263`). Получатель проверяет `protocol.IsForwarded(cmd)` и диспатчит через `Router.PublishFromPeer`, **не** пересылая дальше. Это предотвращает петлевые штормы.
- **HasExtensions (Бит 6, `0x40`)**: Указывает на наличие TLV-блока сразу за 10-байтовым заголовком. Бит устанавливается `SerializeFrameWithExtensions` (frame.go:267).

Парсер корректно работает со старыми клиентами, которые не знают о `HasExtensions`: они маскируют только `MeshForwarded`, видят неизвестный opcode (с установленным битом 6) и корректно сдвигаются на `DataLen` байт — wire alignment сохраняется.

---

## 3. Синтаксис подписки Consumer Groups (v1.13.0+)

Конкурирующие подписчики объединяются в группу через `:group:<group_id>` в payload `CmdSubscribe`:

- **Обычная групповая подписка**: `topic:orders:group:payment-workers`
- **Durable-подписка группы**: `topic:orders:group:payment-workers:durable:worker1:0`

Сообщения балансируются между воркерами группы через **Lock-Free Atomic Round-Robin**:

- `atomic.Uint64` в `ConsumerGroup.counter` (`broker/router.go:186`).
- `atomic.Pointer[[]int]` в `ConsumerGroup.members` хранит snapshot активных индексов в SoA-массивах роутера.
- Чтение на горячем пути без блокировок: `< 10 ns/op`, `0 allocs/op`.
- Групповые Durable Offset'ы синхронизируются через `CmdAck` с `consumer_id == group_id`.

При `AckOffset(consumerID, topic, offset)` (router.go:480) роутер обновляет offset **и** для индивидуального подписчика, **и** для всей группы (если `consumerID` совпадает с `groupID`).

---

## 4. Блок TLV-расширений (TLV Extensions)

При `Command & 0x40 != 0` полезная нагрузка начинается с 2-байтовой общей длины `ExtTotalLen`, за которой следуют упакованные записи `[Type: 1B][Length: 1B][Value: N Bytes]`:

```text
+--------------------+----------+----------+--------------------+
| ExtTotalLen (2B)   | Type (1B)| Len (1B) | Value (N Bytes)    |
+--------------------+----------+----------+--------------------+
| Type (1B) | Len (1B) | Value (N Bytes) | ...                |
+-----------+----------+--------------------+
```

### Зарегистрированные типы

| Тип расширения | Type ID | Длина Value | Описание |
| :--- | :--- | :--- | :--- |
| `ExtTraceContext` | `0x01` | 25 байт | Контекст трассировки OpenTelemetry `[TraceID: 16B][SpanID: 8B][TraceFlags: 1B]` |
| `ExtCompression` | `0x02` | 5 байт | Метаданные ZSTD-сжатия `[Algo: 1B][UncompressedSize: 4B]`. `Algo=1` для ZSTD. |
| `ExtPriority` | `0x03` | 1 байт | Уровень приоритета QoS (`0` Highest, `1` High, `2` Normal, `3` Low). |
| `ExtRetryOffset` | `0xF0` | 8 байт | (внутреннее) Original offset для NACK-редиливери: `[OriginalOffset: 8B]`. Используется delivery-loop'ом для гарантии сходимости NACK-счётчика к `max_retries` и триггера DLQ. |

### Константы TLV-блока

```go
ExtHeaderLen      = 2   // длина ExtTotalLen prefix
ExtEntryHeader    = 2   // Type (1B) + Len (1B)
MaxExtTotalLen    = 1024 // жёсткий лимит (DoS protection)
```

`ParseTLVEntry` (`internal/protocol/extensions.go:94`) выполняет bounds-check перед `unsafe.Slice` и возвращает `ErrExtEntryTruncated` при усечённой записи. Неизвестные типы молча пропускаются (`FindExtension` ищет конкретный тип).

### Zero-Copy парсинг

`FindExtension` (extensions.go:116) и `ExtractTraceContext` (extensions.go:192) возвращают под-срез, указывающий **напрямую** в исходный буфер — zero-alloc:

```go
// SAFE: valStart+l <= end проверено выше
return unsafe.Slice(&extBlock[valStart], l), true
```

`ExtractTraceContext` возвращает `TraceID [16]byte`, `SpanID [8]byte`, `TraceFlags byte` — все указывают в `extBlock`. Время выполнения: **~4 ns**.

---

## 5. Сообщения TTL и Приоритеты QoS

### Inline TTL (legacy)

В payload `CmdPublish` может быть устаревший inline TTL:

```
ttl:<миллисекунды>:<данные_сообщения>
```

`parseTTL` (`broker/router.go`) парсит формат и устанавливает `expiresAt`.

### Per-Priority TTL (v1.13.0+)

`config.yaml`:

```yaml
broker:
  priority_ttls: ["500ms", "5s", "0", "0"]
```

| Индекс | Уровень | Семантика |
| :--- | :--- | :--- |
| 0 | `PriorityHighest` | Критические алерты |
| 1 | `PriorityHigh` | Высокий приоритет |
| 2 | `PriorityNormal` | По умолчанию |
| 3 | `PriorityLow` | Фоновые задачи |

`Router.publishWithClientID` (router.go:925) переписывает `expiresAt` сообщения в соответствии с `priority_ttls[pLevel]`. Устаревшие сообщения отбрасываются при dequeue и инкрементируют `aqueduct_messages_expired_total{topic, priority}`.

### TLV `ExtPriority`

Новый формат — TLV-расширение `0x03`:

```
[ExtPriority TLV entry: 1 байт = 0..3]
```

`BuildPriorityExtension(priority)` (extensions.go:170) создаёт slab-аллоцированный блок. `ExtractPriority(extBlock)` возвращает приоритет и `ok` флаг. Значение вне 0..3 → `DefaultPriority` (= 2).

---

## 6. Формат записей в файле AAL

Append-Only Log (AES-256-GCM при заданном ключе):

```text
+------------------+------------------+------------------------+
| 4 байта Длина    | 12 байт Nonce    | Шифртекст (AEAD Seal)  |
| (Little-Endian)  | (Session+Counter)| [12 + 16 + N байт]     |
+------------------+------------------+------------------------+
```

### Без шифрования

```text
+------------------+-----------------------------------+
| 4 байта Длина    | Сырой фрейм (CmdPublish и т.д.)  |
| (Little-Endian)  |                                   |
+------------------+-----------------------------------+
```

### Nonce (только при шифровании)

```text
+------------------------+------------------------+
| 4 байта: session prefix| 8 байт: counter (BE)   |
| (random при старте)    | (atomic.Uint64 монотонный) |
+------------------------+------------------------+
```

Криптографически уникальный 12-байтный Nonce: 4 случайных байта сессии + 8 монотонных байт счётчика. Файл создаётся с правами `0600` (G302).

`Replay` (`internal/aal/aal.go:138`) читает файл блоками по 64 КБ, вызывает `gcm.Open` для каждой записи, и для corrupt records применяет best-effort resync (advance 1 byte). Возвращает количество успешно дешифрованных фреймов.

---

## 7. WebTransport Transport Binding (v1.16.0+)

Браузерные клиенты подключаются через W3C [WebTransport API](https://www.w3.org/TR/webtransport/), инкапсулирующий HTTP/3 поверх QUIC. Кадрирование **идентично** нативному QUIC-транспорту — шлюз WebTransport (`internal/webtransport/`) переводит только HTTP/3-слой.

### 7.1 Установка соединения

Browser JS:

```js
const transport = new WebTransport("https://broker.example.com:4433/aqueduct/wt");
await transport.ready;
```

Серверная сторона: `webtransport.Gateway.handleConn` вызывает `http3.NewRawServerConn`, завершает обмен HTTP/3 SETTINGS, затем разбирает входящий Extended CONNECT и проверяет:

- `:method = CONNECT`
- `:scheme = https`
- `:authority = broker.example.com:4433`
- `:path = /aqueduct/wt` (настраивается через `webtransport.path_prefix`)
- `:protocol = webtransport`

Ответ `200 OK` завершает handshake. Последующие bidi-стримы на том же QUIC-соединении несут бинарные фреймы брокера.

### 7.2 ALPN

WebTransport использует `NextProtos: ["h3", "aqueduct-v1"]` (где `h3` — стандартный HTTP/3 ALPN, `aqueduct-v1` нужен шлюзу для совместимости). `cmd/broker/main.go:346` автоматически добавляет `h3` в `NextProtos`, если его нет.

`MinVersion = tls.VersionTLS13` — обязательно для всех WebTransport-листенеров.

### 7.3 Маппинг стримов

| Тип стрима WebTransport | Пара в брокере |
| :------------------------ | :------------------ |
| Server-initiated uni | (зарезервирован для HTTP/3 control, игнорируется шлюзом) |
| Client-initiated uni | (отбрасывается — стримы с неизвестным типом по RFC 9298) |
| Client-initiated bidi | `*quic.Stream` из `conn.AcceptStream()` → `transport.Broker.HandleStream(...)` |
| Server-initiated bidi | (handshake request stream, остаётся открытым для capsule protocol) |

После handshake каждый клиентский bidi-стрим передаётся в `transport.Broker.HandleStream(ctx, conn, stream, clientID)` — тот же метод, что использует нативный QUIC-транспорт.

### 7.4 0-RTT и TLS

QUIC-слой под HTTP/3 договаривается о 0-RTT при условии `quic.Config.Allow0RTT = true` на обеих сторонах. Шлюз включает это по умолчанию (`Allow0RTT: true` в `internal/transport/broker.go:185`). Браузеры сохраняют session ticket при первом подключении и переиспользуют его.

Брокер принудительно выставляет TLS 1.3 (`MinVersion = tls.VersionTLS13`) на WebTransport-листенере — старые версии TLS молча отвергаются.

### 7.5 Handshake timeout

`handshakeTimeout = 10 * time.Second` в `internal/webtransport/server.go:62` — Slowloris защита. Незавершённый handshake получает `stream.CancelRead(1)` + `conn.CloseWithError(ErrCodeRequestRejected, "wt handshake timeout")`.

### 7.6 Формат фрейма из браузера

Браузер, пишущий в WebTransport bidi-стрим, формирует **тот же бинарный layout**, что и нативные клиенты:

```
[Magic: 1 байт = 0x1F][Cmd: 1 байт][StreamID: 4 байта (Little-Endian)][DataLen: 4 байта (Little-Endian)][Payload: N байт]
```

Идентичные константы (см. §1, §2). Идентичное TLV-кодирование расширений (см. §4). Реализация в `examples/web/app.js` (`buildFrame()` / `parseFrame()`) соответствует этому layout.