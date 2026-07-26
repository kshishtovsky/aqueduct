# Aqueduct

[ [English](README.md) | Русский | [中文](README.zh.md) ]

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kshishtovsky/aqueduct.svg)](https://pkg.go.dev/github.com/kshishtovsky/aqueduct)

Aqueduct — это сверхвысокопроизводительный брокер сообщений с нулевыми аллокациями памяти на Go поверх **QUIC** (библиотека `quic-go`). Спроектирован для работы с микросекундными задержками (< 1.5 µs), zero-copy бинарным фреймингом и Data-Oriented Design (DoD).

> [!IMPORTANT]
> **Production Ready (v1.3.0)**
> Aqueduct поддерживает **двустороннюю аутентификацию mTLS 1.3**, **авторизацию ACL без аллокаций**, **зашифрованный журнал AAL (AES-256-GCM)** с **восстановлением состояния при старте (Replay)**, **асинхронный Fan-Out с изоляцией медленных потребителей**, **Message TTL** и **маршрутизацию по Wildcard-топикам MQTT**.

---

## Возможности

- **Транспортный слой QUIC**: Мультиплексирование QUIC с поддержкой 0-RTT, изоляцией стримов и защитой от Amplification атак.
- **Zero-Copy бинарный протокол**: Плоский 10-байтовый заголовок (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`) с безопасностью выравнивания байт (Little-Endian).
- **Structure of Arrays (SoA) Роутер**: Внутренняя Pub/Sub маршрутизация на плоских массивах для максимального попадания в L1/L2 кэш CPU.
- **Асинхронный Fan-Out и Ring Queues**: Персональные неблокирующие каналы и Writer-горутины для каждого подписчика.
- **Изоляция медленных потребителей (Backpressure)**: Настраиваемые политики переполнения очередей (`drop_oldest`, `drop_newest`, `disconnect`).
- **Атомарный подсчет ссылок (`MessageRef`)**: Безопасное переиспользование буферов в `sync.Pool` при нулевом счетчике ссылок (`0 allocs/op`).
- **MQTT Wildcard Topics**: Маршрутизация по паттернам `+` и `#` без аллокаций в куче (< 51 ns/op, `0 allocs/op`).
- **Time-To-Live (TTL) сообщений**: Ленивое удаление истекших сообщений при извлечении из очереди (`ttl:<ms>:<payload>`).
- **Зашифрованный Append-Only Log (AAL)**: Логирование в AES-256-GCM с уникальными 12-байтными Nonce (4-байтовый случайный префикс сессии) и длинами записей.
- **AAL Replay при старте**: Полное восстановление состояния до открытия QUIC UDP сокета.
- **Ротация файлов AAL**: Автоматическое сжатие при превышении размера `max_aal_size`.
- **mTLS и Zero-Allocation ACL**: Двусторонний TLS 1.3 и некоммутативная FNV-1a матрица прав доступа.
- **Мониторинг Prometheus**: Готовые метрики (`/metrics`) и Docker Compose стек с дашбордом Grafana.

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

transport:
  max_buf_size: 65536
  read_buf_size: 1024
```

---

## Документация (Фреймворк Diátaxis)

- **Учебное руководство**: [Быстрый старт](docs/ru/getting-started.md)
- **Справочник**: [Спецификация бинарного протокола](docs/ru/protocol-spec.md)
- **Объяснение**: [Архитектура и модель памяти](docs/ru/architecture.md)
- **Практическое руководство**: [Развертывание в Production и безопасность](docs/ru/production-deployment.md)

---

## Лицензия

MIT License. Подробнее см. [LICENSE](LICENSE).
