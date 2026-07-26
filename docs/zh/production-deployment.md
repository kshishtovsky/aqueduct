# 指南: 生产部署与安全 (v1.3.0)

本指南说明在生产环境中部署与加固 Aqueduct 消息代理的最佳实践。

---

## 1. 传输安全与 mTLS 配置

在生产环境中强制使用 **mTLS 1.3**:

```yaml
tls:
  generate: false
  cert_file: "/etc/certs/server.crt"
  key_file: "/etc/certs/server.key"
  require_client_cert: true
  client_ca_file: "/etc/certs/client_ca.pem"
```

---

## 2. 加密日志 (AAL) 与自动轮转

生成 32 字节 AES-256 加密密钥:

```bash
openssl rand -base64 32
```

配置 `config.yaml`:
```yaml
aal:
  enabled: true
  file_path: "/var/log/aqueduct/aal.log"
  key: "<BASE64_32_BYTE_KEY>"
  max_aal_size: 104857600 # 100 MB 触发轮转
```

代理启动时自动重放 (Replay) 日志记录以恢复状态。

---

## 3. 慢消费者 Backpressure 调优

- `drop_oldest`: 适合实时遥测数据。
- `drop_newest`: 适合顺序事件流。
- `disconnect`: 安全严格环境，直接断开慢订阅者。

```yaml
broker:
  queue_size: 2048
  backpressure_policy: "drop_oldest"
```

---

## 4. 操作系统 UDP 限制

```bash
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
ulimit -n 65536
```
