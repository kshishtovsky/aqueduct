# Руководство: Безопасность и развертывание в Production (v1.3.0)

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
```

---

## 4. Системные лимиты ОС (`sysctl`)

Увеличение буферов UDP:

```bash
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
ulimit -n 65536
```
