# How-To Guide: Production Deployment

This guide explains how to deploy Aqueduct in a production environment with custom TLS 1.3 certificates, Append-Only Logging (AAL), systemd service management, and Prometheus metrics integration.

## 1. Generating or Procuring TLS 1.3 Certificates

Aqueduct strictly enforces **TLS 1.3**. Self-signed certificates are rejected by production clients. Obtain certificates from Let's Encrypt or your enterprise PKI.

For testing with custom certificates:

```bash
openssl req -x509 -newkey rsa:4090 -keyout key.pem -out cert.pem -sha256 -days 365 -nodes \
  -subj "/CN=broker.example.com"
```

## 2. Setting Up Append-Only Logging (AAL)

Enable AAL to persist published messages to disk synchronously without heap memory allocations (`0 allocs/op`).

Create a dedicated log directory with appropriate write permissions:

```bash
sudo mkdir -p /var/log/aqueduct
sudo chown -R aqueduct:aqueduct /var/log/aqueduct
```

## 3. Creating a Systemd Service

Create `/etc/systemd/system/aqueduct.service`:

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

Reload systemd and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now aqueduct
```

## 4. Configuring Prometheus Monitoring

Aqueduct exports standard Prometheus metrics at `:9090/metrics`.

Add the following scrape configuration to `/etc/prometheus/prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'aqueduct'
    static_configs:
      - targets: ['localhost:9090']
```

### Exported Metrics

| Metric | Type | Description |
| :--- | :--- | :--- |
| `aqueduct_messages_published_total` | Counter | Total published messages by topic |
| `aqueduct_messages_delivered_total` | Counter | Total delivered messages by topic |
| `aqueduct_active_subscribers` | Gauge | Current number of active subscribers |

## 5. Security & Hardening Checklist

- [x] Verify TLS 1.3 configuration (`-cert` and `-key` specified).
- [x] Enable Append-Only Logging (`-aal`).
- [x] Restrict HTTP metrics endpoint (`:9090`) behind firewall or internal network.
- [x] Set system file descriptor limits (`LimitNOFILE=65536`).
