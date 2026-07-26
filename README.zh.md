# Aqueduct

[ [English](README.md) | [Русский](README.ru.md) | 中文 ]

[![CI](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml/badge.svg)](https://github.com/kshishtovsky/aqueduct/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/kshishtovsky/aqueduct.svg)](https://pkg.go.dev/github.com/kshishtovsky/aqueduct)

Aqueduct 是一款基于 Go 语言和 **QUIC** 协议（基于 `quic-go`）构建的超高性能、零堆内存分配（Zero-Allocation）消息代理（Message Broker）。基于面向数据设计（DoD）理念打造，提供极低延迟（< 1.5 微秒）与零拷贝二进制帧解析。

> [!IMPORTANT]
> **生产就绪 (v1.0.0)**
> Aqueduct 严格要求使用 **TLS 1.3** 协议，内置流级别防 OOM / DoS 攻击保护，并支持追加日志（AAL）落盘与 YAML/ENV 灵活配置。

---

## 核心特性

- **QUIC 传输层**: 基于 QUIC 多路复用，支持 0-RTT 快速连接建立、流隔离与放大攻击防护。
- **零拷贝二进制协议**: 扁平化 10 字节二进制帧头 (`[Magic:1] [Cmd:1] [StreamID:4] [PayloadLen:4]`)。
- **SoA (Structure of Arrays) 路由器**: 内存中 Pub/Sub 路由使用扁平并行数组，最大化 CPU L1/L2 缓存命中率。
- **追加日志 (AAL)**: 零堆内存分配（`0 allocs/op`）同步写入 OS 页缓存。
- **内存加固 (Memory Hardening)**: 严格的流级别缓冲区限制。
- **灵活配置**: 支持 `config.yaml` 配置文件与 `AQUEDUCT_*` 环境变量覆盖。
- **Prometheus & Grafana**: 内置 HTTP 指标接口 (`:9090`)，提供一键启动 Docker Compose 监控栈。

---

## 2 分钟快速开始 (Docker Compose)

启动 Aqueduct Broker、Prometheus 及 Grafana：

```bash
docker compose up -d
```

服务接口说明:
- **Broker 健康检查**: `http://localhost:9090/healthz`
- **Prometheus 监控**: `http://localhost:9091`
- **Grafana 仪表盘**: `http://localhost:3000` (账号: `admin` / 密码: `admin`)

停止服务栈：
```bash
docker compose down
```

---

## 配置文件 (`config.yaml`)

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

### 环境变量覆盖

| 环境变量 | 覆盖配置项 | 示例 |
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

## 性能压测工具 (`aqueduct-bench`)

```bash
# 编译压测工具
go build -o bin/aqueduct-bench ./cmd/aqueduct-bench

# 执行压测 (10 并发, 100,000 请求, 128 字节 Payload)
./bin/aqueduct-bench -addr 127.0.0.1:4242 -c 10 -n 100000 -size 128 -topic bench
```

---

## 客户端代码示例

- [Go 客户端示例](examples/go/main.go) — `quic-go` 原生实现。
- [Python 客户端示例](examples/python/client.py) — `aioquic` 异步客户端。
- [Node.js Buffer 示例](examples/nodejs/client.js) — 二进制帧构建示例。

---

## 性能与基准测试

| 基准测试项目 | 延迟 / 吞吐量 | 内存消耗 | 堆内存分配次数 |
| :--- | :--- | :--- | :--- |
| `BenchmarkRouterPublishWithAAL` | **1403 ns/op** (10.69 MB/s) | **0 B/op** | **0 allocs/op** |
| `BenchmarkQUICRoundTrip` | **2448 ns/op** (56.37 MB/s) | **53 B/op** | **1 alloc/op** |

---

## 文档目录

- [快速入门教程](docs/zh/getting-started.md)
- [生产部署指南](docs/zh/production-deployment.md)
- [二进制协议规范](docs/zh/protocol-spec.md)
- [架构与内存模型](docs/zh/architecture.md)
