package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/quic-go/quic-go"
)

const (
	magicByte    = 0x1F
	headerSize   = 10
	cmdPublish   = 0x01
	cmdSubscribe = 0x02
)

func serializeFrame(cmd byte, streamID uint32, payload []byte) []byte {
	buf := make([]byte, headerSize+len(payload))
	buf[0] = magicByte
	buf[1] = cmd
	binary.LittleEndian.PutUint32(buf[2:6], streamID)
	binary.LittleEndian.PutUint32(buf[6:10], uint32(len(payload)))
	copy(buf[headerSize:], payload)
	return buf
}

func parseFrame(data []byte) (cmd byte, streamID uint32, payload []byte, err error) {
	if len(data) < headerSize {
		return 0, 0, nil, fmt.Errorf("frame too short")
	}
	if data[0] != magicByte {
		return 0, 0, nil, fmt.Errorf("invalid magic byte")
	}
	cmd = data[1]
	streamID = binary.LittleEndian.Uint32(data[2:6])
	payloadLen := binary.LittleEndian.Uint32(data[6:10])
	if uint32(len(data)) < headerSize+payloadLen {
		return 0, 0, nil, fmt.Errorf("payload truncated")
	}
	return cmd, streamID, data[headerSize : headerSize+payloadLen], nil
}

func main() {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"aqueduct-v1"},
		MinVersion:         tls.VersionTLS13,
	}

	conn, err := quic.DialAddr(context.Background(), "127.0.0.1:4242", tlsConf, nil)
	if err != nil {
		log.Fatalf("failed to dial aqueduct broker: %v", err)
	}

	subStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("failed to open sub stream: %v", err)
	}

	subPayload := []byte("topic:orders")
	subBuf := serializeFrame(cmdSubscribe, 1, subPayload)
	if _, err := subStream.Write(subBuf); err != nil {
		log.Fatalf("write subscribe frame failed: %v", err)
	}

	fmt.Println("[Go Client] Subscribed to topic 'orders'.")

	pubStream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		log.Fatalf("failed to open pub stream: %v", err)
	}

	pubPayload := []byte("orders")
	pubBuf := serializeFrame(cmdPublish, 2, pubPayload)
	if _, err := pubStream.Write(pubBuf); err != nil {
		log.Fatalf("write publish frame failed: %v", err)
	}

	fmt.Println("[Go Client] Published message to topic 'orders'.")

	buf := make([]byte, 1024)
	n, err := subStream.Read(buf)
	if err != nil {
		log.Fatalf("read delivered frame failed: %v", err)
	}

	cmd, streamID, payload, err := parseFrame(buf[:n])
	if err != nil {
		log.Fatalf("parse frame failed: %v", err)
	}

	fmt.Printf("[Go Client] Received frame: Cmd=%d, StreamID=%d, Topic/Payload=%q\n",
		cmd, streamID, string(payload))
}
