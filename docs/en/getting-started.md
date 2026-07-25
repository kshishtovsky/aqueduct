# Tutorial: Getting Started with Aqueduct

This tutorial guides you through setting up and running your first Aqueduct QUIC message broker instance, subscribing to a topic, and publishing binary messages.

## Prerequisites

Before starting, ensure you have:
- **Go 1.22+** installed on your system
- A terminal environment (Linux or macOS recommended)

## Step 1: Clone and Build the Broker

Clone the repository and compile the broker binary:

```bash
git clone https://github.com/kshishtovsky/aqueduct.git
cd aqueduct
go build -o bin/broker ./cmd/broker
```

## Step 2: Start the Broker in Development Mode

Run the compiled broker binary:

```bash
./bin/broker -addr :4242 -metrics-addr :9090
```

You should see output similar to:

```text
2026/07/26 04:42:00 WARN Using ephemeral self-signed certificate. Do not use in production.
2026/07/26 04:42:00 INFO metrics server started addr=:9090
2026/07/26 04:42:00 INFO broker listening addr=127.0.0.1:4242
2026/07/26 04:42:00 INFO broker started addr=127.0.0.1:4242
```

## Step 3: Verify Health Endpoint

Open a second terminal window and test the health check endpoint:

```bash
curl http://localhost:9090/healthz
# Output: OK
```

## Step 4: Publish and Subscribe via Go Client Code

Create a small Go program (`example_client.go`) to connect to Aqueduct over QUIC and send pub/sub frames:

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
		InsecureSkipVerify: true, // For self-signed dev certificate
		NextProtos:         []string{"aqueduct-v1"},
		MinVersion:         tls.VersionTLS13,
	}

	conn, err := quic.DialAddr(context.Background(), "127.0.0.1:4242", tlsConf, nil)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	// 1. Open Stream and Subscribe
	subStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("failed to open stream: %v", err)
	}

	subPayload := []byte("topic:orders")
	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 1, subPayload)
	_, _ = subStream.Write(*subBuf)
	protocol.ReleaseBuffer(subBuf)

	fmt.Println("Subscribed to topic 'orders'. Waiting for messages...")

	// 2. Open Stream and Publish Message
	pubStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("failed to open pub stream: %v", err)
	}

	pubPayload := []byte("orders")
	pubBuf := protocol.SerializeFrame(protocol.CmdPublish, 2, pubPayload)
	_, _ = pubStream.Write(*pubBuf)
	protocol.ReleaseBuffer(pubBuf)

	// 3. Read Message on Subscriber Stream
	readBuf := make([]byte, 1024)
	n, err := subStream.Read(readBuf)
	if err != nil {
		log.Fatalf("failed to read delivered message: %v", err)
	}

	frame, err := protocol.ParseFrame(readBuf[:n])
	if err != nil {
		log.Fatalf("failed to parse delivered frame: %v", err)
	}

	fmt.Printf("Received delivered message on topic '%s'\n", string(frame.Payload))
}
```

## Next Steps

Now that you have run your first broker and communicated over QUIC, proceed to the [Production Deployment Guide](production-deployment.md) to configure TLS certificates, Append-Only Logging, and Prometheus metrics.
