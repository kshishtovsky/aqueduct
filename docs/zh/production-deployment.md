# 操作指南: 生产环境部署

本指南说明如何在生产环境中部署 Aqueduct，包括配置 TLS 1.3 证书、追加日志（AAL）、systemd 服务管理以及 Prometheus 监控集成。

## 1. 配置 TLS 1.3 证书

Aqueduct 严格要求 **TLS 1.3** 协议。生产环境中请使用权威 PKI 或 Let's Encrypt 证书。

测试证书生成示例：

```bash
openssl req -x509 -newkey rsa:4090 -keyout key.pem -out cert.pem -sha256 -days 365 -nodes \
  -subj "/CN=broker.example.com"
```

## 2. 配置追加日志 (AAL)

开启 AAL 可将发布的帧同步写入磁盘，且不增加堆内存分配（`0 allocs/op`）。

创建日志目录并配置权限：

```bash
sudo mkdir -p /var/log/aqueduct
sudo chown -R aqueduct:aqueduct /var/log/aqueduct
```

## 3. 配置 Systemd 服务

创建服务文件 `/etc/systemd/system/aqueduct.service`:

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

加载 systemd 守护进程并启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now aqueduct
```

## 4. 配置 Prometheus 监控

Aqueduct 在 `:9090/metrics` 暴露 Prometheus 监控指标。

在 `/etc/prometheus/prometheus.yml` 中添加配置：

```yaml
scrape_configs:
  - job_name: 'aqueduct'
    static_configs:
      - targets: ['localhost:9090']
```

### 指标说明

| 指标名称 | 类型 | 描述 |
| :--- | :--- | :--- |
| `aqueduct_messages_published_total` | Counter | 各主题发布的消息总数 |
| `aqueduct_messages_delivered_total` | Counter | 各主题投递成功的消息总数 |
| `aqueduct_active_subscribers` | Gauge | 当前活跃订阅者总数 |
