# 指南: 故障排查 (v1.16.0)

本文档提供 Aqueduct 常见运维问题的诊断流程与修复方案。

> [!NOTE]
> Diátaxis 分类：**How-to** — 解决具体问题，遵循操作步骤。

---

## 1. 启动失败

### 1.1 `failed to load TLS certificate and key`

**症状**：Broker 启动失败，`os.Exit(1)`。

**原因**：`tls.cert_file` 或 `tls.key_file` 路径错误或文件不可读。

**修复**：
```bash
ls -la /etc/certs/cert.pem /etc/certs/key.pem
openssl x509 -in /etc/certs/cert.pem -text -noout | head -20
```

### 1.2 `failed to parse client CA certificates from PEM`

**症状**：Broker 启动失败。

**原因**：`tls.client_ca_file` 不是有效 PEM 格式。

**修复**：
```bash
# 验证 PEM 内容
head -1 /etc/certs/client_ca.pem
# 必须以 "-----BEGIN CERTIFICATE-----" 开头
```

### 1.3 `AAL encryption key must be 32 bytes`

**症状**：Broker 启动失败。

**原因**：`aal.key` Base64 解码后不是 32 字节。

**修复**：
```bash
# 重新生成 32 字节密钥
openssl rand -base64 32
# 复制到 config.yaml 的 aal.key
```

### 1.4 `Using ephemeral self-signed certificate`

**症状**：Broker 启动但输出警告。

**原因**：`tls.generate: true`（默认），使用自签证书。

**修复**：生产环境必须设为 `false`：
```yaml
tls:
  generate: false
  cert_file: "/etc/certs/cert.pem"
  key_file: "/etc/certs/key.pem"
```

### 1.5 `failed to start webtransport listener`

**症状**：Broker 启动失败，WebTransport 网关无法绑定。

**原因**：端口已被占用或证书无效。

**修复**：
```bash
# 检查端口占用
ss -ulpn | grep 4433
# 检查证书 SAN 包含 hostname
openssl x509 -in /etc/certs/cert.pem -text -noout | grep -A1 "Subject Alternative Name"
```

---

## 2. 连接失败

### 2.1 客户端无法连接

**症状**：`aqueduct-bench` 或原生客户端报 `connection error`。

**诊断步骤**：

```bash
# 1. 验证 broker 正在监听
ss -ulpn | grep 4242
# 预期: udp UNCONN 0 0 *:4242 *:* users:(("broker",pid=...))

# 2. 验证健康检查端点
curl -sv http://localhost:9090/healthz
# 预期: HTTP/1.1 200 OK, body: OK

# 3. 验证 QUIC ALPN 协商
# (需要 quic-go 工具或浏览器开发者工具)
```

**常见原因**：

- **防火墙阻挡 UDP**：`quic-go` 需要 UDP（不是 TCP）。检查 `iptables` / `nftables` / 安全组。
- **错误的 ALPN**：客户端必须设置 `NextProtos: ["aqueduct-v1"]`。
- **TLS 验证失败**：自签证书时客户端必须 `InsecureSkipVerify: true`（开发）或安装 CA（生产）。

### 2.2 WebTransport 浏览器无法连接

**症状**：浏览器报 `WebTransport connection failed`。

**诊断**：

1. 打开浏览器开发者工具 → Network → 过滤 "Connect" 请求。
2. 检查响应状态码与错误消息。

**常见原因**：

- **UDP 阻挡**：浏览器仅通过 UDP 协商 WebTransport。检查防火墙。
- **证书不被信任**：浏览器拒绝裸自签证书。生产使用受信 CA（Let's Encrypt 或内部 CA）。
- **路径错误**：默认 `path_prefix: /aqueduct/wt`。客户端必须发 Extended CONNECT 到该路径。
- **握手超时**：默认 10 秒。若客户端延迟超过，超时关闭。检查网络延迟。

**修复**：使用 [`mkcert`](https://github.com/FiloSottile/mkcert) 生成本地受信证书。

---

## 3. 性能问题

### 3.1 吞吐量低于预期

**诊断**：

```bash
# 1. 检查 CPU 利用率
top -p $(pgrep broker)

# 2. 检查帧解析 P99
histogram_quantile(0.99, rate(aqueduct_frame_parse_duration_ns_bucket[5m]))
# 预期: < 1000 ns

# 3. 检查 Backpressure 丢弃
rate(aqueduct_messages_dropped_total[5m])
# 若 > 0：订阅者队列溢出，调整 queue_size 或 backpressure_policy

# 4. 检查 GC 暂停
# Go runtime: GODEBUG=gctrace=1
```

**常见原因与修复**：

| 症状 | 原因 | 修复 |
| :--- | :--- | :--- |
| `frame_parse_duration_ns > 10 µs` | 帧过大 / 缓冲区不足 | 增加 `transport.max_buf_size` |
| `messages_dropped_total > 0` | 订阅者消费速度不足 | 增加 `queue_size` 或使用 `drop_oldest` |
| 高 GC 暂停 | 大对象分配 | 启用 `tracing.enabled: false` 与 `compression.enabled: false` 减少开销 |
| 内存增长无界 | AAL 写入失败 | 检查磁盘空间与 `aal.max_aal_size` |

### 3.2 高延迟

**诊断**：

```bash
# 1. 检查 QUIC 握手延迟
# 0-RTT 启用？quic.Config.Allow0RTT = true (默认)

# 2. 检查内核 UDP 缓冲区
sysctl net.core.rmem_max net.core.wmem_max
# 预期: ≥ 25000000 (25 MB)

# 3. 检查 backpressure 队列深度
# 通过 aqueduct_active_subscribers 与 _messages_dropped_total
```

**修复**：
```bash
# 提高 UDP 缓冲区
sysctl -w net.core.rmem_max=25000000
sysctl -w net.core.wmem_max=25000000
```

### 3.3 内存使用过高

**诊断**：

```bash
# 1. RSS（Resident Set Size）
ps -p $(pgrep broker) -o rss,vsz

# 2. Go runtime 内存统计
curl -s http://localhost:9090/debug/pprof/heap > heap.prof
go tool pprof heap.prof
```

**常见原因**：

- **大量慢订阅者**：每个订阅者都有 `queue_size` 字节 channel。N 个慢订阅者 × `queue_size` × 最大帧大小 = 潜在积压。
- **AAL 文件无限增长**：`max_aal_size` 未触发（检查 `aqueduct_aal_rotations_total`）。
- **无界 NACK 缓存**：每个订阅者 256 条目 FIFO。如果 NACK 速率高于处理速率，缓存保持满。

**修复**：
- 减小 `broker.queue_size`。
- 增加 `broker.backpressure_policy: "disconnect"` 驱逐慢订阅者。
- 启用 AAL 加密并设置合理的 `max_aal_size`。

---

## 4. 集群问题

### 4.1 对等节点频繁断开重连

**症状**：`aqueduct_cluster_peers_active` 在 0 与 N 之间波动。

**诊断**：

```bash
# 1. 检查 broker 日志中的 TLS 握手错误
kubectl logs aqueduct-0 | grep -i "tls\|certificate\|handshake"

# 2. 检查 mesh CA 文件存在性
ls -la /etc/aqueduct/mesh-ca.pem

# 3. 检查证书是否过期
openssl x509 -in /etc/certs/cert.pem -checkend 2592000
# 输出 "Certificate will not expire" 表示 ≥ 30 天有效
```

**常见原因**：

- **证书过期**：检查 `notAfter`。
- **SAN 不匹配**：对等证书 SAN 不包含对等地址的 FQDN/IP。
- **网络分区**：防火墙/路由问题阻断 UDP。
- **CA 文件缺失**：`cluster.mesh.ca_file` 路径错误或文件不可读。

### 4.2 DNS 发现不工作

**症状**：启用 `cluster.discovery.enabled: true` 但新 Pod 未连接。

**诊断**：

```bash
# 1. 验证 Headless Service 返回 A 记录
nslookup aqueduct-headless.default.svc.cluster.local

# 2. 在 broker Pod 内验证 DNS 解析
kubectl exec -it aqueduct-0 -- nslookup aqueduct-headless.default.svc.cluster.local

# 3. 检查 discovery 日志
kubectl logs aqueduct-0 | grep -i "discovery"
```

**常见原因**：

- **Headless Service 未配置**：`clusterIP: None` 必须设置。
- **DNS 传播延迟**：Kubernetes DNS 缓存；K8s 1.28+ 默认较低。
- **端口错误**：`cluster.discovery.port` 必须与 broker 监听端口一致。
- **链接本地地址**：169.254.x.x 被 `normalize()` 过滤。若 Pod 只在该子网有 IP，集群将无对等。

### 4.3 消息未跨节点转发

**症状**：在节点 A 发布，节点 B 的订阅者未收到。

**诊断**：

```bash
# 1. 验证 MeshForwarded 位未被错误设置
# 重新发布消息应从原始节点转发

# 2. 检查 PeerManager 状态
curl -s http://localhost:9090/metrics | grep aqueduct_cluster
# 预期: aqueduct_cluster_peers_active ≥ 2
# 预期: aqueduct_cluster_frames_forwarded_total > 0

# 3. 检查发布者与订阅者主题名一致
# 记住：v1.16.0 parsePublishTopic 规范化 topic: 前缀
```

**常见原因**：

- **主题名不规范**：发布 `topic:orders` 与订阅 `orders` 在 v1.16.0+ 已路由到同一槽位，但客户端可能在 TLV 扩展上有所不同。
- **PeerManager 未启动**：`cluster.peers` 为空且 `cluster.discovery.enabled: false`。
- **本地订阅者已接收**：本地 `topicIndex` 槽已存在则不重复分发到对等（`PublishFromPeer` 仅本地派发）。

---

## 5. ACL 与授权

### 5.1 `aqueduct_authz_denied_total` 高频增长

**症状**：客户端连接但消息被静默丢弃。

**诊断**：

```bash
# 1. 找出拒绝热点
topk(5, rate(aqueduct_authz_denied_total[5m]))

# 2. 检查 ACL 规则
grep -A20 "^acl:" config.yaml

# 3. 验证客户端 CN/ID 与 ACL rules.client 匹配
```

**修复**：

```yaml
acl:
  enabled: true
  default: "all"  # 临时放宽调试；生产改回 "none"
  rules:
    - client: "service-a"
      topic: "orders"
      permission: "publish"
```

或通过 Admin API：

```bash
grpcurl -cert admin.pem -key admin-key.pem ... \
  admin.AdminService/UpdateACL
```

### 5.2 Admin API 拒绝请求

**症状**：`codes.PermissionDenied` 返回给 Admin 客户端。

**诊断**：

```bash
# 检查客户端证书 CN
openssl x509 -in admin.pem -text -noout | grep "Subject:"
# 必须以 "admin-" 开头
```

**修复**：重新签发证书，CN 前缀为 `admin-`。

---

## 6. AAL 与 Replay

### 6.1 AAL 文件损坏

**症状**：启动 Replay 失败或跳过记录。

**诊断**：

```bash
# 1. 检查 AAL 文件权限
ls -la /var/log/aqueduct/aal.log

# 2. 检查磁盘空间
df -h /var/log
```

**行为**：`decodeReplayChunk` 在遇到损坏记录时按 1 字节步进 best-effort resync。完整文件损坏会丢失后续记录。

**修复**：
```bash
# 备份与轮转
mv /var/log/aqueduct/aal.log /var/log/aqueduct/aal.log.bak
# Broker 下次启动将创建新文件
```

### 6.2 Replay 耗时过长

**症状**：Broker 启动延迟。

**诊断**：

```bash
# AAL 重放耗时
curl -s http://localhost:9090/metrics | grep aqueduct_aal_replay_duration_seconds
```

**修复**：
- 减小 AAL 文件大小（设置更小的 `max_aal_size`）。
- 启用加密（AAL 加密时 Replay 解密开销可忽略）。
- 监控重放帧数；超过 10M 帧需考虑外部持久化方案。

---

## 7. 调试工具

### 7.1 pprof

```bash
# CPU profile
curl -s "http://localhost:9090/debug/pprof/profile?seconds=30" > cpu.prof
go tool pprof cpu.prof

# 堆 profile
curl -s "http://localhost:9090/debug/pprof/heap" > heap.prof
go tool pprof heap.prof

# Goroutine dump
curl -s "http://localhost:9090/debug/pprof/goroutine?debug=2"
```

### 7.2 帧级调试

```bash
# 启用详细日志级别
slog.SetLevel: debug

# 或通过环境变量（slog 默认支持 LOG_LEVEL=debug）
```

### 7.3 aqueduct-bench 基准测试

```bash
# 压力测试：1000 并发，1000000 请求，4 KB 载荷
go run ./cmd/aqueduct-bench/main.go \
  -addr 127.0.0.1:4242 \
  -c 1000 \
  -n 1000000 \
  -size 4096 \
  -batch 1

# 启用 TLS 验证（生产环境）
go run ./cmd/aqueduct-bench/main.go \
  -tls-verify \
  -ca-file /etc/certs/ca.pem
```

---

## 8. 性能基准参考

下表列出 v1.16.0 实测基准（loopback 仅供参考）：

| 场景 | 预期结果 |
| :--- | :--- |
| 单帧 Publish | ~920 ns/op, 0 allocs/op |
| 批量 Publish (100 msgs) | ~150 ns/msg, 6.67M msg/s, 0 allocs/op |
| 帧解析 | < 10 ns/op, 0 allocs/op |
| TLV 扩展解析 | 5.2 ns/op, 0 allocs/op |
| 通配符匹配 | 50.41 ns/op, 0 allocs/op |
| Slab 分配 | ~15 ns/op, 0 allocs/op |
| 令牌桶检查 | 2.1 ns/op, 0 allocs/op |
| ACL 热路径 | 14.51 ns/op, 0 allocs/op |
| WT 握手 (mTLS + Extended CONNECT) | ~3.3 ms/handshake |
| WT Publish 端到端 | ~1.25 ms/op (loopback) |

若实测偏离基线超过 2 倍，启用 pprofile 收集 CPU/堆 profile 并提交 issue。

---

## 9. 常见误诊

| 误诊 | 实际 |
| :--- | :--- |
| "TLS 失败，因为 mTLS 未启用" | mTLS 是可选的；单边 TLS（仅 broker 证书）即可工作 |
| "WebTransport 不工作，因为 ALPN 错误" | ALPN 是 `h3`，不是 `aqueduct-v1` — 两者并存 |
| "消息未投递，因为订阅失败" | 实际可能是 ACL 拒绝；检查 `aqueduct_authz_denied_total` |
| "集群不工作，因为 DNS 未解析" | `cluster.peers` 静态列表优先；DNS 仅在 `discovery.enabled: true` 时启用 |
| "延迟高，因为 broker 慢" | 实际可能是客户端 `quic.Config` 设置不当（如 `MaxIncomingStreams` 过低） |
| "AAL 不写，因为 `enabled: false`" | 检查默认值；生产需显式 `aal.enabled: true` |

---

## 10. 获取帮助

- **GitHub Issues**: https://github.com/kshishtovsky/aqueduct/issues
- **示例客户端**: `examples/web/`（浏览器 WebTransport）
- **基准测试**: `cmd/aqueduct-bench/`
- **架构文档**: `docs/zh/architecture.md`
- **协议规范**: `docs/zh/protocol-spec.md`