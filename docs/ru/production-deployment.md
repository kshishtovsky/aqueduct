# Руководство: Безопасность и развертывание в Production (v1.11.0)

Настоящее руководство описывает лучшие практики безопасного развертывания Aqueduct в промышленной среде.

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

---

## 2. Шифрование логов (AAL) и Ротация

Генерация 32-байтового ключа AES-256:

```bash
openssl rand -base64 32
```

Настройка `config.yaml`:
```yaml
aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "<BASE64_32_BYTE_KEY>"
  max_aal_size: 104857600 # 100 МБ до запуска ротации
```

При старте брокер выполняет Replay кадра из AAL для полного восстановления состояния до открытия UDP-порта.

---

## 3. Настройка Backpressure и очередей

Выбор политики при медленных потребителях:

- `drop_oldest`: Для систем реального времени (телеметрия).
- `drop_newest`: Для последовательных событий.
- `disconnect`: Отключение зависших подписчиков.

```yaml
broker:
  queue_size: 2048
  backpressure_policy: "drop_oldest"
  batch_size: 65536
  flush_interval: 50us
  max_retries: 3
  quotas:
    default_publish_rate: 100
    default_burst_size: 1000
```

---

## 4. Системные лимиты ОС (`sysctl`)

Увеличение буферов UDP:

```bash
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
ulimit -n 65536
```

---

## 5. Кластерное развертывание (v1.8.0+)

Разверните несколько брокеров Aqueduct в прямом mesh для горизонтального масштабирования.

| Узел | Адрес | Роль |
|------|-------|------|
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
```

### Поведение пересылки

- Сообщение, опубликованное на любом узле, пересылается всем пирам
- MeshForwarded бит предотвращает циклы пересылки
- Нет консенсуса или выборов лидера — mesh полностью децентрализован
- Peer-соединения используют ту же mTLS конфигурацию, что и клиентские соединения

### Требования к сети

- Все узлы должны быть доступны по UDP на настроенном порту
- Каждый узел должен иметь собственные TLS-сертификаты
- Для топологий с 3+ узлами каждый узел должен перечислить всех пиров
- Нет гарантий порядка сообщений между узлами (fire-and-forget)

---

## 6. NACK/DLQ в Production

Настройка повторной доставки NACK и Dead Letter Queue:

- Установите `max_retries` (по умолчанию 3) в `config.yaml` или через `AQUEDUCT_BROKER_MAX_RETRIES`
- DLQ топики следуют шаблону `__dlq__<original_topic>`
- Отслеживайте метрики `aqueduct_messages_nacked_total` и `aqueduct_messages_dead_lettered_total`
- Подключите подписчика к топикам `__dlq__*` для автономного просмотра

---

## 7. Квоты Rate Limiting

Настройка ограничения скорости для каждого клиента:

- Установите `broker.quotas.default_publish_rate` и `broker.quotas.default_burst_size` в `config.yaml`
- Индивидуальные настройки: `broker.quotas.per_client.<client_id>`
- Отслеживайте метрику `aqueduct_messages_rate_limited_total`
- Полный блок конфигурации квот приведен в YAML-примере Раздела 3
