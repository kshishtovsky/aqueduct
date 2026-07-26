// Package main demonstrates a native Go client communicating with the Aqueduct QUIC broker.
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
		InsecureSkipVerify: true, // For development self-signed cert
		NextProtos:         []string{"aqueduct-v1"},
		MinVersion:         tls.VersionTLS13,
	}

	conn, err := quic.DialAddr(context.Background(), "127.0.0.1:4242", tlsConf, nil)
	if err != nil {
		log.Fatalf("failed to dial aqueduct broker: %v", err)
	}

	// 1. Subscribe stream
	subStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("failed to open sub stream: %v", err)
	}

	subPayload := []byte("topic:orders")
	subBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 1, subPayload)
	if _, err := subStream.Write(*subBuf); err != nil {
		log.Fatalf("write subscribe frame failed: %v", err)
	}
	protocol.ReleaseBuffer(subBuf)

	fmt.Println("[Go Client] Subscribed to topic 'orders'.")

	// 2. Publish stream
	pubStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("failed to open pub stream: %v", err)
	}

	pubPayload := []byte("orders")
	pubBuf := protocol.SerializeFrame(protocol.CmdPublish, 2, pubPayload)
	if _, err := pubStream.Write(*pubBuf); err != nil {
		log.Fatalf("write publish frame failed: %v", err)
	}
	protocol.ReleaseBuffer(pubBuf)

	fmt.Println("[Go Client] Published message to topic 'orders'.")

	// 3. Read message on subscriber stream
	buf := make([]byte, 1024)
	n, err := subStream.Read(buf)
	if err != nil {
		log.Fatalf("read delivered frame failed: %v", err)
	}

	frame, err := protocol.ParseFrame(buf[:n])
	if err != nil {
		log.Fatalf("parse frame failed: %v", err)
	}

	fmt.Printf("[Go Client] Received frame: Cmd=%d, StreamID=%d, Topic/Payload=%q\n",
		frame.Command, frame.StreamID, string(frame.Payload))
}
