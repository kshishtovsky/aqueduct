# Aqueduct

[ [English](README.md) | [Русский](README.ru.md) | 中文 ]

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kshishtovsky/aqueduct.svg)](https://pkg.go.dev/github.com/kshishtovsky/aqueduct)

Aqueduct 是一个基于 **QUIC**（通过 `quic-go`）使用 Go 语言构建的超高性能、零内存分配消息代理。专为极低延迟（< 1.5 微秒）、零拷贝二进制解析和面向数据设计（DoD）而设计。

> [!IMPORTANT]
> **生产就绪 (v1.3.0)**
> Aqueduct 支持 **mTLS 1.3 传输身份验证**、**零分配 ACL 授权**、**AES-256-GCM 加密日志 (AAL)** 与 **启动状态恢复 (Replay)**、**异步 Fan-Out 与慢消费者隔离**、**消息 TTL** 以及 **MQTT 通配符主题路由**。

---

## 核心特性

- **QUIC 传输层**: 具备 0-RTT 连接建立、流隔离和放大攻击保护。
- **零拷贝二进制协议**: 扁平 10 字节二进制首部 (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`)，支持跨平台 Little-Endian 安全解析。
- **Structure of Arrays (SoA) 路由器**: 内存 Pub/Sub 路由，使用连续数组优化 CPU L1/L2 缓存命中率。
- **异步 Fan-Out 与环形队列**: 每个订阅者拥有非阻塞队列与独立的 Writer 协程。
- **慢消费者隔离 (Backpressure)**: 可配置队列溢出策略 (`drop_oldest`, `drop_newest`, `disconnect`)。
- **原子引用计数 (`MessageRef`)**: 安全的零分配 `sync.Pool` 缓冲区回收 (`0 allocs/op`)。
- **MQTT 通配符主题路由**: 支持单级 (`+`) 和多级 (`#`) 通配符匹配 (< 51 ns/op, `0 allocs/op`)。
- **消息生存时间 (TTL)**: 出队时延迟过期 (`ttl:<ms>:<payload>`)。
- **加密追加日志 (AAL)**: AES-256-GCM 加密与 12 字节 Nonce (4 字节随机会话前缀) 和长度前缀记录。
- **启动 AAL 重放 (Replay)**: 在打开 QUIC UDP 监听套接字前恢复状态。
- **AAL 日志轮转**: 超过 `max_aal_size` 时自动压缩。
- **mTLS 与零分配 ACL**: 双向 TLS 1.3 认证与非交换 FNV-1a 组合哈希权限矩阵。
- **Prometheus 监控**: 提供 `/metrics` 端点和 Ready-to-use Grafana 仪表盘。

---

## 2 分钟快速开始 (Docker Compose)

```bash
docker compose up -d
```

- **健康检查**: `http://localhost:9090/healthz`
- **Prometheus**: `http://localhost:9091`
- **Grafana**: `http://localhost:3000` (User: `admin` / Password: `admin`)

```bash
docker compose down
```

---

## 文档 (Diátaxis 框架)

- **教程**: [入门指南](docs/zh/getting-started.md)
- **参考**: [二进制协议规范](docs/zh/protocol-spec.md)
- **解释**: [架构与内存模型](docs/zh/architecture.md)
- **指南**: [生产部署与安全](docs/zh/production-deployment.md)

---

## 许可证

MIT License. 详见 [LICENSE](LICENSE)。
