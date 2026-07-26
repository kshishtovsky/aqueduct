# Aqueduct

[ [English](README.md) | Русский | [中文](README.zh.md) ]

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kshishtovsky/aqueduct.svg)](https://pkg.go.dev/github.com/kshishtovsky/aqueduct)

Aqueduct — это сверхбыстрый мессендж-брокер с нулевыми аллокациями памяти (Zero-Allocation), написанный на Go поверх протокола **QUIC** (библиотека `quic-go`). Спроектирован с расчетом на Data-Oriented Design (DoD) и наносекундные задержки (< 1.5 мкс).

> [!IMPORTANT]
> **Production Ready (v1.0.0)**
> Aqueduct строго требует **TLS 1.3**, поддерживает защиту от OOM/DoS атак через лимитирование стримов (`maxBufSize`), включает логирование AAL и загрузку конфигурации YAML/ENV.

---

## Возможности

- **Транспорт QUIC**: Мультиплексирование стримов, поддержка 0-RTT, изоляция ошибок и защита от Amplification-атак.
- **Zero-Copy бинарный протокол**: Минималистичный 10-байтовый заголовок (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`) с прямым парсингом из сетевых буферов.
- **SoA Роутер**: Внутрипамятьная подсистема Pub/Sub на основе Structure of Arrays (SoA) для максимальной локальности CPU кэша L1/L2.
- **Append-Only Logging (AAL)**: Нулевые аллокации при записи на диск (`0 allocs/op`) напрямую из сетевых буферов в Page Cache ОС.
- **Memory Hardening**: Защита от OOM атак на уровне стримов.
- **Конфигурация YAML + ENV**: Загрузка `config.yaml` с переопределением через переменные окружения `AQUEDUCT_*`.
- **Прометеус и Grafana**: HTTP сервер (`:9090`) с `/metrics` и `/healthz` и готовым стеком Docker Compose.

---

## Быстрый старт за 2 минуты (Docker Compose)

Запустите брокер Aqueduct, Prometheus и Grafana одной командой:

```bash
docker compose up -d
```

Доступные эндпоинты:
- **Health Check брокера**: `http://localhost:9090/healthz`
- **Метрики Prometheus**: `http://localhost:9091`
- **Дашборд Grafana**: `http://localhost:3000` (Логин: `admin` / Пароль: `admin`)

Остановка стека:
```bash
docker compose down
```

---

## Конфигурация (`config.yaml`)

```yaml
listen_addr: ":4242"
metrics_addr: ":9090"

tls:
  generate: true
  cert_file: ""
  key_file: ""

aal:
  enabled: false
  file_path: ""

transport:
  max_buf_size: 65536
  read_buf_size: 1024
```

### Переменные окружения (ENV)

| Переменная | Поле `config.yaml` | Пример |
| :--- | :--- | :--- |
| `AQUEDUCT_LISTEN_ADDR` | `listen_addr` | `:4242` |
| `AQUEDUCT_METRICS_ADDR` | `metrics_addr` | `:9090` |
| `AQUEDUCT_TLS_GENERATE` | `tls.generate` | `false` |
| `AQUEDUCT_TLS_CERT_FILE` | `tls.cert_file` | `/etc/certs/cert.pem` |
| `AQUEDUCT_TLS_KEY_FILE` | `tls.key_file` | `/etc/certs/key.pem` |
| `AQUEDUCT_AAL_ENABLED` | `aal.enabled` | `true` |
| `AQUEDUCT_AAL_FILE_PATH` | `aal.file_path` | `/var/log/aal.log` |
| `AQUEDUCT_TRANSPORT_MAX_BUF_SIZE` | `transport.max_buf_size` | `131072` |

---

## Примеры клиентского кода

Примеры клиентских скриптов:

- [Пример на Go](examples/go/main.go) — Клиент на `quic-go`.
- [Пример на Python](examples/python/client.py) — Асинхронный клиент на `aioquic`.
- [Пример на Node.js](examples/nodejs/client.js) — Формирование бинарных фреймов.

---

## Производительность и Бенчмарки

| Бенчмарк | Задержка / Пропускная способность | Память / Операцию | Аллокации |
| :--- | :--- | :--- | :--- |
| `BenchmarkRouterPublishWithAAL` | **1403 ns/op** (10.69 MB/s) | **0 B/op** | **0 allocs/op** |
| `BenchmarkQUICRoundTrip` | **2448 ns/op** (56.37 MB/s) | **53 B/op** | **1 alloc/op** |

---

## Документация

- [Быстрый старт](docs/ru/getting-started.md)
- [Production развертывание](docs/ru/production-deployment.md)
- [Спецификация бинарного протокола](docs/ru/protocol-spec.md)
- [Архитектура и модель памяти](docs/ru/architecture.md)
