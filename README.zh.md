# Aqueduct

[ [English](README.md) | [Русский](README.ru.md) | 中文 ]

Aqueduct 是一款基于 Go 语言和 **QUIC** 协议（基于 `quic-go`）构建的超高性能、零堆内存分配（Zero-Allocation）消息代理（Message Broker）。基于面向数据设计（Data-Oriented Design, DoD）理念打造，提供极低延迟（< 1.5 微秒）与零拷贝二进制帧解析。

> [!IMPORTANT]
> **生产就绪 (v1.0.0)**
> Aqueduct 严格要求使用 **TLS 1.3** 协议，内置基于 `maxBufSize` 限制的流级别防 OOM / DoS 攻击保护，并支持追加日志（Append-Only Logging, AAL）磁盘落盘。

---

## 核心特性

- **QUIC 传输层**: 基于 QUIC 多路复用，支持 0-RTT 快速连接建立、流隔离与放大攻击防护。
- **零拷贝二进制协议**: 扁平化 10 字节二进制帧头 (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`)，指针运算零内存分配。
- **SoA (Structure of Arrays) 路由器**: 内存中 Pub/Sub 路由使用扁平并行数组，最大化 CPU L1/L2 缓存命中率。
- **追加日志 (AAL)**: 零堆内存分配（`0 allocs/op`）同步写入 OS 页缓存（Page Cache）。
- **内存加固 (Memory Hardening)**: 严格的流级别缓冲区限制，防止 oversized payload 导致的内存溢出。
- **Prometheus 监控**: 内置 HTTP 服务 (`:9090`)，提供 `/metrics` 与 `/healthz` 接口。

---

## 快速开始

### 环境要求

- **Go**: 1.22+
- **操作系统**: Linux / macOS

### 运行 Broker

使用标准命令行参数启动代理服务：

```bash
# 开发模式（使用临时自签名 TLS 证书）
go run ./cmd/broker/main.go -addr :4242

# 生产模式（配置 TLS 证书与 AAL 日志）
go run ./cmd/broker/main.go \
  -cert /path/to/cert.pem \
  -key /path/to/key.pem \
  -aal /path/to/aqueduct.log \
  -addr :4242 \
  -metrics-addr :9090
```

### 命令行参数

| 参数 | 默认值 | 描述 |
| :--- | :--- | :--- |
| `-addr` | `:4242` | QUIC 代理服务 UDP 监听地址 |
| `-metrics-addr` | `:9090` | Prometheus 指标与健康检查 HTTP 地址 |
| `-cert` | `""` | TLS 1.3 证书文件路径 |
| `-key` | `""` | TLS 1.3 私钥文件路径 |
| `-aal` | `""` | 追加日志（AAL）文件路径 |

> [!WARNING]
> 未提供 `-cert` 和 `-key` 参数时，Aqueduct 将生成临时自签名证书并输出 `WARN` 日志。请勿在生产环境中使用临时证书。

---

## 架构与设计亮点

1. **热路径零内存分配**: 采用 `sync.Pool` 缓冲区池，直接从网络缓冲区解析二进制帧，无需额外分配。
2. **缓存友好路由**: 订阅者存储在并行扁平切片中 (`streamIDs`, `streams`, `topics`, `active`)，显著提升批量分发时的 CPU 缓存命中率。
3. **零拷贝 AAL 写入**: 发布消息直接通过 `os.File.Write` 系统调用落盘，保持 `0 allocs/op`。

---

## 性能与基准测试

基准测试环境: AMD Ryzen 5 5500U (Linux amd64):

| 基准测试项目 | 延迟 / 吞吐量 | 内存消耗 | 堆内存分配次数 |
| :--- | :--- | :--- | :--- |
| `BenchmarkRouterPublishWithAAL` | **1403 ns/op** (10.69 MB/s) | **0 B/op** | **0 allocs/op** |
| `BenchmarkQUICRoundTrip` | **2448 ns/op** (56.37 MB/s) | **53 B/op** | **1 alloc/op** |

---

## 文档目录

基于 Diátaxis 框架编写的完整文档：

- [快速入门教程](docs/zh/getting-started.md) — 适合新用户的逐步指导。
- [生产部署指南](docs/zh/production-deployment.md) — TLS 1.3、AAL 与 Prometheus 监控配置。
- [二进制协议规范](docs/zh/protocol-spec.md) — 帧头格式与命令码说明。
- [架构与内存模型](docs/zh/architecture.md) — SoA 与零拷贝设计深度解析。
