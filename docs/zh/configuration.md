# 参考: 配置参考 (v1.16.0)

本文档详述 `config.yaml` 的所有可用字段、`AQUEDUCT_*` 环境变量覆盖、默认值与安全考量。

> [!NOTE]
> Diátaxis 分类：**Reference** — 完整字段表与约束。

---

## 1. 完整配置 Schema

```yaml
# === 网络 ===
listen_addr: ":4242"            # Broker UDP 监听地址
metrics_addr: ":9090"          # 指标 HTTP 服务器地址 (/metrics, /healthz)

# === TLS / mTLS ===
tls:
  generate: true               # 若 true，使用自签证书（仅开发）
  cert_file: ""                # TLS 证书 PEM 路径（生产必需）
  key_file: ""                 # TLS 私钥 PEM 路径（生产必需）
  require_client_cert: false   # 强制 mTLS
  client_ca_file: ""           # 客户端 CA 证书 PEM 路径

# === 加密追加日志 (AAL) ===
aal:
  enabled: false
  file_path: ""                # AAL 文件路径
  key: ""                      # Base64 32 字节 AES-256-GCM 密钥
  max_aal_size: 104857600      # 已声明但当前不会自动触发轮转
  retention_period: "24h"      # 已声明但 v1.16.0 未执行
  retention_size: 1073741824   # 已声明但 v1.16.0 未执行

# === ACL 授权 ===
acl:
  enabled: false
  default: "none"              # "none" 或 "all"
  rules:
    - client: "service-a"
      topic: "orders"
      permission: "publish"    # "publish", "subscribe", "all", "none"

# === Admin API (gRPC) ===
admin:
  enabled: false
  addr: ":9091"                # gRPC Admin 服务器 TCP 地址

# === 路由 / 队列 / 批处理 ===
broker:
  queue_size: 1024             # 每订阅者 channel 容量
  backpressure_policy: "drop_oldest" # "drop_oldest", "drop_newest", "disconnect"
  batch_size: 65536            # 写合并字节阈值 (64 KB)
  flush_interval: "50us"       # 写合并微定时器 (50 µs)
  max_retries: 3               # NACK 重试上限 (超限 → DLQ)
  priority_ttls:               # 可选；内置默认值为 nil（不设置按优先级 TTL）
    - "500ms"  # Priority 0 (Highest)，示例值
    - "5s"     # Priority 1 (High)，示例值
    - "0"      # Priority 2 (Normal)
    - "0"      # Priority 3 (Low)
  quotas:
    default_publish_rate: 0    # 每租户速率 (msg/s, 0 = 无限)
    default_burst_size: 1000   # 每租户突发容量
    per_client:                # 可选每客户端覆盖
      service-a:
        rate: 100
        burst: 50

# === 传输层 ===
transport:
  max_buf_size: 65536          # 最大帧缓冲 (字节)
  read_buf_size: 1024          # 初始读取缓冲 (字节)

# === OpenTelemetry Tracing ===
tracing:
  enabled: false               # 禁用时零开销 (~3.4 ns/op)
  service_name: "aqueduct-broker"
  endpoint: "localhost:4317"   # OTLP gRPC 接收器

# === ZSTD 批处理压缩 ===
compression:
  enabled: false
  min_batch_size: 1024         # 字节；< 该值不压缩
  level: 0                     # ZSTD 压缩级别 (0=default)

# === WebTransport (HTTP/3) 网关 ===
webtransport:
  enabled: false
  listen_addr: ":4433"         # 不同于 listen_addr 的 UDP 端口
  path_prefix: "/aqueduct/wt"  # 客户端 Extended CONNECT 路径

# === 集群 (P2P Federation) ===
cluster:
  peers: []                    # 静态对等地址 (["host:4242"])
  discovery:
    enabled: false
    type: "dns"                # 仅 "dns" 支持
    host: ""                   # Headless Service FQDN
    port: "4242"
    interval: "10s"
  mesh:
    insecure_skip_verify: false  # 生产保持 false
    ca_file: ""                  # 网格 CA PEM 路径
```

---

## 2. 环境变量覆盖 (`AQUEDUCT_*`)

所有环境变量覆盖通过 `internal/config/applyEnvOverrides` 应用。优先级从高到低为：**CLI 标志 > 环境变量 > YAML > 内置默认值**。

### 监听

| 环境变量 | 覆盖项 | 类型 |
| :--- | :--- | :--- |
| `AQUEDUCT_LISTEN_ADDR` | `listen_addr` | string |
| `AQUEDUCT_METRICS_ADDR` | `metrics_addr` | string |

### TLS

| 环境变量 | 覆盖项 | 类型 |
| :--- | :--- | :--- |
| `AQUEDUCT_TLS_GENERATE` | `tls.generate` | bool (`true`/`false`/`1`/`0`/`yes`/`no`) |
| `AQUEDUCT_TLS_CERT_FILE` | `tls.cert_file` | string |
| `AQUEDUCT_TLS_KEY_FILE` | `tls.key_file` | string |
| `AQUEDUCT_TLS_REQUIRE_CLIENT_CERT` | `tls.require_client_cert` | bool |
| `AQUEDUCT_TLS_CLIENT_CA_FILE` | `tls.client_ca_file` | string |

### AAL

| 环境变量 | 覆盖项 | 类型 |
| :--- | :--- | :--- |
| `AQUEDUCT_AAL_ENABLED` | `aal.enabled` | bool |
| `AQUEDUCT_AAL_FILE_PATH` | `aal.file_path` | string |
| `AQUEDUCT_AAL_KEY` | `aal.key` | string (Base64 32 字节) |
| `AQUEDUCT_AAL_MAX_SIZE` | `aal.max_aal_size` | int64 (> 0) |

### ACL

| 环境变量 | 覆盖项 | 类型 |
| :--- | :--- | :--- |
| `AQUEDUCT_ACL_ENABLED` | `acl.enabled` | bool |
| `AQUEDUCT_ACL_DEFAULT` | `acl.default` | string (`"none"`/`"all"`) |

### Admin API

| 环境变量 | 覆盖项 | 类型 |
| :--- | :--- | :--- |
| `AQUEDUCT_ADMIN_ENABLED` | `admin.enabled` | bool |
| `AQUEDUCT_ADMIN_ADDR` | `admin.addr` | string |

### Broker

| 环境变量 | 覆盖项 | 类型 | 约束 |
| :--- | :--- | :--- | :--- |
| `AQUEDUCT_BROKER_BACKPRESSURE_POLICY` | `broker.backpressure_policy` | string | `drop_oldest`/`drop_newest`/`disconnect` |
| `AQUEDUCT_BROKER_QUEUE_SIZE` | `broker.queue_size` | int (> 0) | |
| `AQUEDUCT_BROKER_BATCH_SIZE` | `broker.batch_size` | int (> 0) | 写合并字节阈值 |
| `AQUEDUCT_BROKER_FLUSH_INTERVAL` | `broker.flush_interval` | duration (`"50us"`, `"1ms"`) | |
| `AQUEDUCT_BROKER_MAX_RETRIES` | `broker.max_retries` | int (> 0) | |
| `AQUEDUCT_BROKER_DEFAULT_PUBLISH_RATE` | `broker.quotas.default_publish_rate` | int (>= 0) | 0 = 无限 |
| `AQUEDUCT_BROKER_DEFAULT_BURST_SIZE` | `broker.quotas.default_burst_size` | int (> 0) | |

### Transport

| 环境变量 | 覆盖项 | 类型 |
| :--- | :--- | :--- |
| `AQUEDUCT_TRANSPORT_MAX_BUF_SIZE` | `transport.max_buf_size` | int (> 0) |
| `AQUEDUCT_TRANSPORT_READ_BUF_SIZE` | `transport.read_buf_size` | int (> 0) |

### Tracing

| 环境变量 | 覆盖项 | 类型 |
| :--- | :--- | :--- |
| `AQUEDUCT_TRACING_ENABLED` | `tracing.enabled` | bool |
| `AQUEDUCT_TRACING_SERVICE_NAME` | `tracing.service_name` | string |
| `AQUEDUCT_TRACING_ENDPOINT` | `tracing.endpoint` | string |

### Cluster / Discovery / Mesh TLS

| 环境变量 | 覆盖项 | 类型 |
| :--- | :--- | :--- |
| `AQUEDUCT_CLUSTER_DISCOVERY_ENABLED` | `cluster.discovery.enabled` | bool |
| `AQUEDUCT_CLUSTER_DISCOVERY_HOST` | `cluster.discovery.host` | string |
| `AQUEDUCT_CLUSTER_DISCOVERY_PORT` | `cluster.discovery.port` | string |
| `AQUEDUCT_CLUSTER_DISCOVERY_INTERVAL` | `cluster.discovery.interval` | string (`time.ParseDuration`) |
| `AQUEDUCT_CLUSTER_MESH_INSECURE_SKIP_VERIFY` | `cluster.mesh.insecure_skip_verify` | bool |
| `AQUEDUCT_CLUSTER_MESH_CA_FILE` | `cluster.mesh.ca_file` | string |

### Compression & WebTransport

| 环境变量 | 覆盖项 | 类型 |
| :--- | :--- | :--- |
| `AQUEDUCT_COMPRESSION_ENABLED` | `compression.enabled` | bool |
| `AQUEDUCT_WEBTRANSPORT_ENABLED` | `webtransport.enabled` | bool |
| `AQUEDUCT_WEBTRANSPORT_LISTEN_ADDR` | `webtransport.listen_addr` | string |
| `AQUEDUCT_WEBTRANSPORT_PATH_PREFIX` | `webtransport.path_prefix` | string |

---

## 3. 默认值

`internal/config.Default()` 提供生产/开发安全的默认值：

| 字段 | 默认值 |
| :--- | :--- |
| `listen_addr` | `:4242` |
| `metrics_addr` | `:9090` |
| `tls.generate` | `true` |
| `tls.require_client_cert` | `false` |
| `aal.enabled` | `false` |
| `aal.max_aal_size` | `104857600` (100 MB；已声明，当前不自动轮转) |
| `aal.retention_period` | `"24h"`（已声明，v1.16.0 未执行） |
| `aal.retention_size` | `1073741824` (1 GB；已声明，v1.16.0 未执行) |
| `acl.enabled` | `false` |
| `acl.default` | `"none"` |
| `admin.enabled` | `false` |
| `admin.addr` | `:9091` |
| `broker.queue_size` | `1024` |
| `broker.backpressure_policy` | `"drop_oldest"` |
| `broker.batch_size` | `65536` (64 KB) |
| `broker.flush_interval` | `50us` |
| `broker.max_retries` | `3` |
| `broker.priority_ttls` | `nil`（不设置按优先级 TTL；仅可通过 YAML 配置） |
| `broker.quotas.default_publish_rate` | `0` (无限) |
| `broker.quotas.default_burst_size` | `1000` |
| `transport.max_buf_size` | `65536` |
| `transport.read_buf_size` | `1024` |
| `compression.enabled` | `false` |
| `compression.min_batch_size` | `1024` |
| `compression.level` | `0` (zstd default) |
| `tracing.enabled` | `false` |
| `tracing.service_name` | `"aqueduct-broker"` |
| `tracing.endpoint` | `"localhost:4317"` |
| `webtransport.enabled` | `false` |
| `webtransport.listen_addr` | `:4433` |
| `webtransport.path_prefix` | `"/aqueduct/wt"` |
| `cluster.mesh.insecure_skip_verify` | `false` |

---

## 4. 安全考量

> [!WARNING]
> **集群网格 TLS**: `cluster.mesh.insecure_skip_verify: true` 禁用对等证书验证，使网格易受 MITM 攻击。生产环境**必须**保持 `false` 并通过 `cluster.mesh.ca_file` 或系统 CA 池提供受信 CA。

> [!WARNING]
> **AAL 保留策略**：v1.16.0 会解析 `max_aal_size`、`retention_period` 与 `retention_size`，但生产代码不会调用 `aal.Log.Rotate`，因此这些字段不会自动限制文件大小或保留时间。请使用 `logrotate`、cron 或 sidecar 执行外部轮转，并监控磁盘空间。

> [!WARNING]
> **AAL 加密**: 当 `aal.key` 非空时，AAL 文件与密钥共存于运维配置。文件系统权限应限制为 `chmod 600`。

> [!WARNING]
> **WebTransport**: 浏览器拒绝裸自签证书。生产环境 `tls.generate: false` 并使用受信证书（Let's Encrypt 或内部 CA）。

> [!CAUTION]
> **Admin API**: 启用时强制 mTLS，客户端证书 CN 必须以 `admin-` 开头。否则 RPC 返回 `codes.PermissionDenied`。

### 最小特权 ACL 示例

```yaml
acl:
  enabled: true
  default: "none"  # 拒绝未匹配任何规则的客户端
  rules:
    - client: "sensor-service"
      topic: "sensor/#"
      permission: "publish"
    - client: "analytics-service"
      topic: "sensor/+/temp"
      permission: "subscribe"
    - client: "backup-worker"
      topic: "#"
      permission: "all"
```

通配符 `+`（单层）与 `#`（多层）与 MQTT 主题路由语义一致。

---

## 5. 配置加载顺序

`internal/config.Load(path)` 按以下顺序应用：

1. **`Default()`** — 安全默认值
2. **YAML 文件** (若提供 `-config` 路径)
3. **环境变量覆盖** (`AQUEDUCT_*`)
4. **CLI 标志覆盖** (`cmd/broker/main.go`)

后步骤覆盖前步骤。CLI 标志为最终覆盖层。

```bash
go run ./cmd/broker/main.go \
  -config config.yaml \
  -addr :4242 \               # 覆盖 YAML listen_addr
  -cert /etc/certs/cert.pem \  # 覆盖 YAML tls.cert_file
  -aal /var/log/aal.log        # 启用 AAL + 设置文件路径
```

---

## 6. 验证

启动时若配置错误（如 TLS 证书缺失、ACL 格式非法），broker 立即 `os.Exit(1)` 并通过 `slog` 输出诊断信息。常用排查：

- `WARN  Using ephemeral self-signed certificate. Do not use in production.` — `tls.generate: true`，生产应设为 `false`。
- `ERROR failed to load TLS certificate and key` — 检查 `cert_file`/`key_file` 路径与权限。
- `ERROR failed to parse client CA certificates from PEM` — `client_ca_file` 不是有效 PEM。
- `ERROR AAL encryption key must be 32 bytes` — `aal.key` 必须 Base64 解码后恰好 32 字节。
- `WARN cluster mesh TLS verification disabled` — `cluster.mesh.insecure_skip_verify: true`，生产应设为 `false`。