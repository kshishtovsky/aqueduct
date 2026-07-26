# 教程: Aqueduct 快速入门指南

本教程引导您配置、编译并运行您的首个 Aqueduct QUIC 消息代理实例，订阅主题并发布二进制消息。

## 环境准备

开始前，请确保您的系统安装了：
- **Go 1.22+**
- 终端环境（推荐 Linux 或 macOS）

## 步骤 1: 克隆与编译 Broker

克隆代码仓库并编译可执行文件：

```bash
git clone https://github.com/kshishtovsky/aqueduct.git
cd aqueduct
go build -o bin/broker ./cmd/broker
```

## 步骤 2: 在开发模式下运行 Broker

运行编译好的二进制文件：

```bash
./bin/broker -addr :4242 -metrics-addr :9090
```

您将看到类似如下的日志输出：

```text
2026/07/26 04:42:00 WARN Using ephemeral self-signed certificate. Do not use in production.
2026/07/26 04:42:00 INFO metrics server started addr=:9090
2026/07/26 04:42:00 INFO broker listening addr=127.0.0.1:4242
2026/07/26 04:42:00 INFO broker started addr=127.0.0.1:4242
```

## 步骤 3: 验证健康检查接口

打开第二个终端窗口测试健康检查接口：

```bash
curl http://localhost:9090/healthz
# 输出: OK
```

## 步骤 4: 通过 Go 客户端代码发布与订阅

创建简单的客户端文件 `example_client.go` 连接 QUIC 代理并发送消息：

```go
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

func main() {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true, // 仅用于开发自签名证书
		NextProtos:         []string{"aqueduct-v1"},
		MinVersion:         tls.VersionTLS13,
	}

	conn, err := quic.DialAddr(context.Background(), "127.0.0.1:4242", tlsConf, nil)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}

	// 1. 打开流并订阅
	subStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("打开流失败: %v", err)
	}

	subPayload := []byte("topic:orders")
	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 1, subPayload)
	_, _ = subStream.Write(*subBuf)
	protocol.ReleaseBuffer(subBuf)

	fmt.Println("已订阅主题 'orders'。等待消息中...")

	// 2. 打开流并发布消息
	pubStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("打开发布流失败: %v", err)
	}

	pubPayload := []byte("orders")
	pubBuf := protocol.SerializeFrame(protocol.CmdPublish, 2, pubPayload)
	_, _ = pubStream.Write(*pubBuf)
	protocol.ReleaseBuffer(pubBuf)

	// 3. 订阅者读取投递的消息
	readBuf := make([]byte, 1024)
	n, err := subStream.Read(readBuf)
	if err != nil {
		log.Fatalf("读取投递消息失败: %v", err)
	}

	frame, err := protocol.ParseFrame(readBuf[:n])
	if err != nil {
		log.Fatalf("解析帧失败: %v", err)
	}

	fmt.Printf("成功收到主题 '%s' 投递的消息\n", string(frame.Payload))
}
```

## 下一步

完成首个代理运行后，请参阅 [生产部署指南](production-deployment.md) 配置 TLS 证书、追加日志（AAL）与 Prometheus 监控。
