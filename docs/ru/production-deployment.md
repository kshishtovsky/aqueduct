# Руководство: Развертывание в Production

В данном руководстве описан процесс развертывания Aqueduct в продуктивной среде с сертификатами TLS 1.3, логированием AAL, службой systemd и мониторингом Prometheus.

## 1. Подготовка TLS 1.3 сертификатов

Aqueduct строго требует протокол **TLS 1.3**.

Пример генерации сертификата для тестирования:

```bash
openssl req -x509 -newkey rsa:4090 -keyout key.pem -out cert.pem -sha256 -days 365 -nodes \
  -subj "/CN=broker.example.com"
```

## 2. Настройка Append-Only Logging (AAL)

AAL сбрасывает публикуемые фреймы на диск с нулевыми аллокациями памяти (`0 allocs/op`).

Создайте директорию для логов и настройте права доступа:

```bash
sudo mkdir -p /var/log/aqueduct
sudo chown -R aqueduct:aqueduct /var/log/aqueduct
```

## 3. Создание службы Systemd

Создайте файл `/etc/systemd/system/aqueduct.service`:

```ini
[Unit]
Description=Aqueduct QUIC Message Broker
After=network.target

[Service]
Type=simple
User=aqueduct
Group=aqueduct
ExecStart=/usr/local/bin/aqueduct-broker \
  -addr :4242 \
  -metrics-addr :9090 \
  -cert /etc/aqueduct/cert.pem \
  -key /etc/aqueduct/key.pem \
  -aal /var/log/aqueduct/publish.log
Restart=always
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Перезагрузите демон systemd и запустите службу:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now aqueduct
```

## 4. Мониторинг Prometheus

Aqueduct экспортирует метрики на порту `:9090/metrics`.

Добавьте секцию в `/etc/prometheus/prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'aqueduct'
    static_configs:
      - targets: ['localhost:9090']
```

### Экспортируемые метрики

| Метрика | Тип | Описание |
| :--- | :--- | :--- |
| `aqueduct_messages_published_total` | Counter | Общее количество опубликованных сообщений по топикам |
| `aqueduct_messages_delivered_total` | Counter | Общее количество доставленных сообщений по топикам |
| `aqueduct_active_subscribers` | Gauge | Текущее количество активных подписчиков |
