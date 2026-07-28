# Справочник: Спецификация бинарного протокола (v1.13.0)

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
| Stream ID cntd|        Payload/Data Length (4B)               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Payload Length| [Ext Total Len (2B)] | [Type] [Len] [Value..] |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Payload Data (N Bytes) ...                                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Поля заголовка

| Смещение | Поле | Тип | Описание |
| :--- | :--- | :--- | :--- |
| `0` | `Magic` | `uint8` | Сигнатура протокола `0x1F` (unit separator). |
| `1` | `Command` | `uint8` | Код команды + управляющие биты (Бит 7: MeshForwarded, Бит 6: HasExtensions). |
| `2-5` | `StreamID` | `uint32` | Идентификатор QUIC-стрима (Little-Endian). |
| `6-9` | `PayloadLen`| `uint32` | Длина ExtBlock (если Бит 6=1) + Payload (Little-Endian). |
| `10..` | `ExtBlock` | `[]byte` | Опциональный блок TLV-расширений (при Command & `0x40` != 0). |
| `10+Ext..`| `Payload` | `[]byte` | Полезная нагрузка. |

---

## 2. Команды протокола и управляющие биты

| Код | Название | Описание | Формат Payload |
| :--- | :--- | :--- | :--- |
| `0x01` | `CmdPublish` | Публикация сообщения в топик | `[ttl:<ms>:]<topic_name>` или данные |
| `0x02` | `CmdSubscribe` | Подписка стрима на топик или Consumer Group | `topic:<name>[:group:<group_id>][:durable:<client_id>:<offset>]` |
| `0x03` | `CmdUnsubscribe`| Отмена подписки | `topic:<topic_name>` |
| `0x04` | `CmdPublishBatch` | Пакетная публикация под-фреймов | Плоский массив стандартных фреймов `[Magic][Cmd][StreamID][Len][Payload]...` |
| `0x05` | `CmdNack` | Отрицательное подтверждение по смещению сообщения | `[offset: 8]` — 8 байт uint64 Little-Endian offset сообщения |

### Синтаксис подписки Consumer Groups (`v1.13.0`)
Конкурирующие подписчики объединяются в группу, указывая `:group:<group_id>` в полезной нагрузке `CmdSubscribe`:
- **Обычная подписка в группу**: `topic:orders:group:payment-workers`
- **Durable-подписка группы**: `topic:orders:group:payment-workers:durable:worker1:0`

Сообщения в топике балансируются между воркерами группы через **Lock-Free Atomic Round-Robin** (`0 allocs/op`, `< 10 ns/op`). Групповые Durable Offset'ы синхронизируются на уровне всей группы при получении `CmdAck`.

### Управляющие биты (Биты 6 и 7)

- **Флаг MeshForwarded (Бит 7, `0x80`)**: Устанавливается при пересылке между узлами кластера для защиты от петлевых штормов.
- **Флаг HasExtensions (Бит 6, `0x40`)**: Устанавливается при наличии блока TLV-расширений сразу за 10-байтовым заголовком.

При `Command & 0x40 != 0` полезная нагрузка начинается с 2-байтовой общей длины `ExtTotalLen`, за которой следуют упакованные записи `[Type: 1B][Length: 1B][Value: N Bytes]`:

## 3. Блок TLV-расширений (TLV Extensions)

При `Command & 0x40 != 0` полезная нагрузка начинается с 2-байтовой общей длины `ExtTotalLen`, за которой следуют упакованные записи `[Type: 1B][Length: 1B][Value: N Bytes]`:

| Тип расширения | Type ID | Длина Value | Описание |
| :--- | :--- | :--- | :--- |
| `ExtTraceContext` | `0x01` | 25 байт | Контекст трассировки OpenTelemetry `[TraceID: 16B][SpanID: 8B][TraceFlags: 1B]` |
| `ExtCompression` | `0x02` | 5 байт | Метаданные ZSTD-сжатия `[Algo: 1B][UncompressedSize: 4B]` |
| `ExtPriority` | `0x03` | 1 байт | Уровень приоритета QoS (`0` Highest, `1` High, `2` Normal, `3` Low) |

---

## 4. Сообщения TTL и Приоритеты QoS

- **Флаг приоритета TLV (`ExtPriority = 0x03`)**: Содержит 1 байт приоритета (`0` наивысший, `3` низший). Writer-горутины обрабатывают очереди в строгом порядке `0 -> 1 -> 2 -> 3`.
- **Per-Priority TTL**: Настройка `priority_ttls` (`["500ms", "5s", "0", "0"]`) принудительно переписывает `expiresAt` и удаляет устаревшие сообщения при извлечении из очереди.
- **Inline TTL в заголовке**: `ttl:<миллисекунды>:<данные_сообщения>` (Запасной формат).

---

## 5. Формат записей в файле AAL

```text
+-------------------+-------------------+-------------------+
| 4 байта Длина     | 12 байт Nonce     | Зашифрованный     |
| (Little-Endian)   | (Сессия+Счетчик)  | Кадр (AEAD Seal)  |
+-------------------+-------------------+-------------------+
```

---

## 6. WebTransport Transport Binding (v1.16.0+)

Браузерные клиенты подключаются через W3C [WebTransport API](https://www.w3.org/TR/webtransport/), инкапсулирующий HTTP/3 поверх QUIC. Кадрирование **идентично** нативному QUIC-транспорту — шлюз WebTransport (`internal/webtransport/`) переводит только HTTP/3-слой.

### 6.1 Установка соединения

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

Ответ 200 OK завершает handshake. Последующие bidirectional-стримы на том же QUIC-соединении несут бинарные фреймы брокера.

### 6.2 Маппинг стримов

| Тип стрима WebTransport | Пара в брокере |
| :------------------------ | :------------------ |
| Server-initiated uni | (зарезервирован для HTTP/3 control, игнорируется шлюзом) |
| Client-initiated uni | (отбрасывается — стримы с неизвестным типом по RFC 9298) |
| Client-initiated bidi | `*quic.Stream` из `conn.AcceptStream()` |
| Server-initiated bidi | (сам request-стрим handshake, остаётся открытым для capsule) |

После handshake каждый клиентский bidi-стрим передаётся в `transport.Broker.HandleStream(ctx, conn, stream, clientID)` — тот же метод, что использует нативный QUIC-транспорт.

### 6.3 0-RTT и TLS

QUIC-слой под HTTP/3 договаривается о 0-RTT при условии `quic.Config.Allow0RTT = true` на обеих сторонах. Шлюз включает это по умолчанию. Браузеры сохраняют session ticket при первом подключении и переиспользуют его.

Брокер принудительно выставляет TLS 1.3 (`MinVersion = tls.VersionTLS13`) на WebTransport-листенере даже если операторский сертификат это отключает — старые версии TLS молча отвергаются.

### 6.4 Формат фрейма из браузера

Браузер, пишущий в WebTransport bidi-стрим, формирует **тот же бинарный layout**, что и нативные клиенты:

```
[Magic: 1 байт = 0x1F][Cmd: 1 байт][StreamID: 4 байта (little-endian)][DataLen: 4 байта (little-endian)][Payload: N байт]
```

Идентичные константы (см. §1, §2). Идентичное TLV-кодирование расширений (см. §3). Реализация в `examples/web/app.js` (`buildFrame()` / `parseFrame()`) соответствует этому layout.
