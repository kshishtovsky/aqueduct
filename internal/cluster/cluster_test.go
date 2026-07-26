package cluster_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/broker"
	"github.com/kshishtovsky/aqueduct/internal/cluster"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

// genSelfSigned generates a self-signed TLS key pair for testing.
func genSelfSigned(t testing.TB) (serverTLS *tls.Config, clientTLS *tls.Config) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen rsa key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	cert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
	}
	serverTLS = &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"aqueduct-mesh"}}
	clientTLS = &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"aqueduct-mesh"}}
	return serverTLS, clientTLS
}

// TestPeerManagerForward verifies that PeerManager connects to a peer and forwards frames.
func TestPeerManagerForward(t *testing.T) {
	sTLS, cTLS := genSelfSigned(t)
	quicConf := &quic.Config{MaxIdleTimeout: 5 * time.Second}

	// Start a listener that acts as the peer node.
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var received atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Accept one connection and count received frames.
	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		for {
			n, err := stream.Read(buf)
			if err != nil || n == 0 {
				return
			}
			if protocol.IsForwarded(protocol.Command(buf[1])) {
				received.Add(1)
			}
		}
	}()

	pm := cluster.New(ctx, []string{ln.Addr().String()}, cTLS, quicConf)
	defer pm.Close()

	// Wait for connection to establish.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pm.ActivePeers() > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if pm.ActivePeers() == 0 {
		t.Fatal("expected at least 1 active peer after dial")
	}

	// Build a publish frame and forward it.
	payload := []byte("mesh/test/topic")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 0, payload)
	pm.Forward(*buf, true)
	protocol.ReleaseBuffer(buf)

	// Allow forwarding to land.
	time.Sleep(200 * time.Millisecond)

	if received.Load() == 0 {
		t.Error("expected peer to receive forwarded frame with MeshForwarded bit set")
	}
}

// TestMeshStormProtection verifies that receiving a MeshForwarded frame does NOT re-forward it.
// The PeerManager.Forward is a one-way write — there is no feedback loop in the protocol itself.
// This test verifies the MeshForwarded bit is correctly round-tripped by checking IsForwarded().
func TestMeshStormProtection(t *testing.T) {
	payload := []byte("orders/new")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 0, payload)
	defer protocol.ReleaseBuffer(buf)

	// Simulate what Forward(rawBuf, true) does: set MeshForwardedBit on byte[1].
	forwardedBuf := make([]byte, len(*buf))
	copy(forwardedBuf, *buf)
	forwardedBuf[1] |= byte(protocol.MeshForwardedBit)

	f, err := protocol.ParseFrame(forwardedBuf)
	if err != nil {
		t.Fatalf("ParseFrame failed: %v", err)
	}

	if !protocol.IsForwarded(f.Command) {
		t.Error("expected IsForwarded to return true for frame with MeshForwardedBit set")
	}

	// OpcodeOf strips the bit — the opcode must remain CmdPublish.
	if protocol.OpcodeOf(f.Command) != protocol.CmdPublish {
		t.Errorf("expected opcode CmdPublish, got %d", protocol.OpcodeOf(f.Command))
	}
}

// Test2NodeMeshForwarding is a minimal integration test: 2 nodes, subscribe on node 2,
// publish on node 1, verify node 2 subscriber gets the message via mesh forwarding.
func Test2NodeMeshForwarding(t *testing.T) {
	sTLS, cTLS := genSelfSigned(t)
	qConf := &quic.Config{MaxIdleTimeout: 10 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start 2 listeners.
	ln1, err := quic.ListenAddr("127.0.0.1:0", sTLS, qConf)
	if err != nil {
		t.Fatalf("listen node1: %v", err)
	}
	defer ln1.Close()
	ln2, err := quic.ListenAddr("127.0.0.1:0", sTLS, qConf)
	if err != nil {
		t.Fatalf("listen node2: %v", err)
	}
	defer ln2.Close()

	addr1, addr2 := ln1.Addr().String(), ln2.Addr().String()

	// Node 2: router + PeerManager (peers: node1).
	pm2 := cluster.New(ctx, []string{addr1}, cTLS, qConf)
	router2 := broker.NewRouter(nil, broker.WithPeerForwarder(pm2))
	go acceptLoop(ctx, router2, ln2)

	// Node 1: router + PeerManager (peers: node2).
	pm1 := cluster.New(ctx, []string{addr2}, cTLS, qConf)
	router1 := broker.NewRouter(nil, broker.WithPeerForwarder(pm1))
	go acceptLoop(ctx, router1, ln1)

	// Wait for peer connections.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		a1, a2 := pm1.ActivePeers(), pm2.ActivePeers()
		if a1 >= 1 && a2 >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if pm1.ActivePeers() < 1 {
		t.Fatal("node1 did not connect to node2")
	}
	if pm2.ActivePeers() < 1 {
		t.Fatal("node2 did not connect to node1")
	}

	// Subscribe on node 2: write subscribe frame directly via QUIC stream.
	subConn, err := quic.DialAddr(ctx, addr2, cTLS, qConf)
	if err != nil {
		t.Fatalf("dial node2: %v", err)
	}
	defer subConn.CloseWithError(0, "done")
	subStream, err := subConn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub stream: %v", err)
	}
	sendBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 1, []byte("topic:test"))
	_, _ = subStream.Write(*sendBuf)
	protocol.ReleaseBuffer(sendBuf)
	time.Sleep(500 * time.Millisecond)

	// Publish on node 1's router. This tests the full path:
	// router1.Publish → pm1.Forward → QUIC → node2 acceptLoop → router2.PublishFromPeer → subscriber.
	_ = router1.Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 4,
		Payload:    []byte("test"),
	})

	// The subscriber on node2 should receive the forwarded message.
	subStream.SetReadDeadline(time.Now().Add(5 * time.Second))
	readBuf := make([]byte, 4096)
	n, err := subStream.Read(readBuf)
	if err != nil {
		t.Fatalf("subscriber on node2 did not receive message: %v", err)
	}
	f, pe := protocol.ParseFrame(readBuf[:n])
	if pe != nil {
		t.Fatalf("parse: %v", pe)
	}
	if string(f.Payload) != "test" {
		t.Errorf("expected 'test', got %q", f.Payload)
	}
	if protocol.IsForwarded(f.Command) {
		t.Error("delivered frame must not have MeshForwarded bit set")
	}
}

// acceptLoop is a reusable accept-dispatch goroutine used by integration tests.
func acceptLoop(ctx context.Context, router *broker.Router, ln *quic.Listener) {
	for {
		c, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		go func() {
			for {
				stream, err := c.AcceptStream(ctx)
				if err != nil {
					return
				}
				go func(s *quic.Stream) {
					buf := make([]byte, 4096)
					n, err := s.Read(buf)
					if err != nil || n < protocol.HeaderSize {
						return
					}
					frame, err := protocol.ParseFrame(buf[:n])
					if err != nil {
						return
					}
					if protocol.IsForwarded(frame.Command) {
						if protocol.OpcodeOf(frame.Command) == protocol.CmdPublish {
							_ = router.PublishFromPeer(ctx, frame)
						}
					} else if frame.Command == protocol.CmdSubscribe {
						_ = router.Subscribe(ctx, s, frame)
					} else if frame.Command == protocol.CmdPublish {
						_ = router.Publish(ctx, frame)
					}
				}(stream)
			}
		}()
	}
}

// Test3NodeClusterMeshForwarding spins up 3 nodes, subscribes on node 3,
// publishes on node 1, and verifies delivery on node 3 via mesh forwarding.
func Test3NodeClusterMeshForwarding(t *testing.T) {
	sTLS, cTLS := genSelfSigned(t)
	qConf := &quic.Config{MaxIdleTimeout: 15 * time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start 3 listeners.
	var listeners []*quic.Listener
	var addrs []string
	for i := 0; i < 3; i++ {
		ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, qConf)
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		defer ln.Close()
		listeners = append(listeners, ln)
		addrs = append(addrs, ln.Addr().String())
	}

	// Create 3 nodes, each with a router + PeerManager + acceptLoop.
	var routers []*broker.Router
	var managers []*cluster.PeerManager
	for i := 0; i < 3; i++ {
		peers := make([]string, 0, 2)
		for j := range addrs {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}
		pm := cluster.New(ctx, peers, cTLS, qConf)
		router := broker.NewRouter(nil, broker.WithPeerForwarder(pm))
		go acceptLoop(ctx, router, listeners[i])
		routers = append(routers, router)
		managers = append(managers, pm)
	}

	// Wait for all peers to connect.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		allGood := true
		for _, pm := range managers {
			if pm.ActivePeers() < 2 {
				allGood = false
				break
			}
		}
		if allGood {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i, pm := range managers {
		if pm.ActivePeers() < 2 {
			t.Fatalf("node%d: %d peers (expected 2)", i+1, pm.ActivePeers())
		}
	}

	// Subscriber connects to node 3 (index 2).
	subConn, err := quic.DialAddr(ctx, addrs[2], cTLS, qConf)
	if err != nil {
		t.Fatalf("dial node3: %v", err)
	}
	defer subConn.CloseWithError(0, "done")
	subStream, err := subConn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open sub stream: %v", err)
	}
	sendBuf := protocol.SerializeFrame(protocol.CmdSubscribe, 1, []byte("topic:cluster3"))
	_, _ = subStream.Write(*sendBuf)
	protocol.ReleaseBuffer(sendBuf)
	time.Sleep(500 * time.Millisecond)

	// Publisher publishes on node 1 (index 0) via router.
	_ = routers[0].Publish(ctx, protocol.Frame{
		Command:    protocol.CmdPublish,
		PayloadLen: 8,
		Payload:    []byte("cluster3"),
	})

	// Verify subscriber on node 3 gets the message.
	subStream.SetReadDeadline(time.Now().Add(5 * time.Second))
	readBuf := make([]byte, 4096)
	n, err := subStream.Read(readBuf)
	if err != nil {
		t.Fatalf("subscriber on node3 did not receive message: %v", err)
	}
	f, pe := protocol.ParseFrame(readBuf[:n])
	if pe != nil {
		t.Fatalf("parse: %v", pe)
	}
	if string(f.Payload) != "cluster3" {
		t.Errorf("expected 'cluster3', got %q", f.Payload)
	}
	if protocol.IsForwarded(f.Command) {
		t.Error("delivered frame must not have MeshForwarded bit set")
	}
}

// TestPeerManagerForwardRaw verifies the ForwardRaw helper writes raw bytes to peers.
func TestPeerManagerForwardRaw(t *testing.T) {
	sTLS, cTLS := genSelfSigned(t)
	qConf := &quic.Config{MaxIdleTimeout: 5 * time.Second}

	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, qConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var received atomic.Int32
	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		n, err := stream.Read(buf)
		if err != nil || n == 0 {
			return
		}
		if protocol.IsForwarded(protocol.Command(buf[1])) {
			received.Add(1)
		}
	}()

	pm := cluster.New(ctx, []string{ln.Addr().String()}, cTLS, qConf)
	defer pm.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pm.ActivePeers() > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if pm.ActivePeers() == 0 {
		t.Fatal("expected active peer")
	}

	payload := []byte("raw/forward/test")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 0, payload)
	// Set MeshForwarded bit manually, then use ForwardRaw
	(*buf)[1] |= byte(protocol.MeshForwardedBit)
	_ = pm.ForwardRaw(*buf)
	protocol.ReleaseBuffer(buf)

	time.Sleep(200 * time.Millisecond)
	if received.Load() == 0 {
		t.Error("expected ForwardRaw frame to be received with MeshForwarded bit")
	}
}

// TestPeerManagerClose verifies Close stops reconnect loops and marks no active peers.
func TestPeerManagerClose(t *testing.T) {
	sTLS, cTLS := genSelfSigned(t)
	qConf := &quic.Config{MaxIdleTimeout: 5 * time.Second}

	// Start a dummy listener so PeerManager can connect.
	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, qConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pm := cluster.New(ctx, []string{ln.Addr().String()}, cTLS, qConf)

	// Wait briefly for connection attempt.
	time.Sleep(500 * time.Millisecond)

	// Close should stop all goroutines and set active peers to 0.
	pm.Close()
	if pm.ActivePeers() != 0 {
		t.Errorf("expected 0 active peers after close, got %d", pm.ActivePeers())
	}
}

// TestPeerManagerForwardNoBit verifies forwarding without setting the MeshForwarded bit.
func TestPeerManagerForwardNoBit(t *testing.T) {
	sTLS, cTLS := genSelfSigned(t)
	qConf := &quic.Config{MaxIdleTimeout: 5 * time.Second}

	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, qConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var receivedNormal, receivedForwarded atomic.Int32
	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 1024)
		for {
			n, err := stream.Read(buf)
			if err != nil || n == 0 {
				return
			}
			if protocol.IsForwarded(protocol.Command(buf[1])) {
				receivedForwarded.Add(1)
			} else {
				receivedNormal.Add(1)
			}
		}
	}()

	pm := cluster.New(ctx, []string{ln.Addr().String()}, cTLS, qConf)
	defer pm.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pm.ActivePeers() > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if pm.ActivePeers() == 0 {
		t.Fatal("expected active peer")
	}

	payload := []byte("normal/topic")
	// Forward WITHOUT mesh forwarded bit
	buf := protocol.SerializeFrame(protocol.CmdPublish, 0, payload)
	pm.Forward(*buf, false)
	protocol.ReleaseBuffer(buf)

	time.Sleep(200 * time.Millisecond)
	if receivedNormal.Load() == 0 {
		t.Error("expected normal frame (without mesh bit) to be received")
	}
	if receivedForwarded.Load() != 0 {
		t.Error("expected no forwarded frames when addForwardedBit=false")
	}
}

// BenchmarkMeshForward measures zero-copy peer forwarding overhead.
func BenchmarkMeshForward(b *testing.B) {
	sTLS, cTLS := genSelfSigned(b)
	quicConf := &quic.Config{MaxIdleTimeout: 10 * time.Second}

	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, quicConf)
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		// Drain the stream so peer writes don't block.
		buf := make([]byte, 4096)
		for {
			if _, err := stream.Read(buf); err != nil {
				return
			}
		}
	}()

	pm := cluster.New(ctx, []string{ln.Addr().String()}, cTLS, quicConf)
	defer pm.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pm.ActivePeers() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pm.ActivePeers() == 0 {
		b.Skip("no peer connected — skipping BenchmarkMeshForward")
	}

	payload := []byte("bench/topic/data")
	buf := protocol.SerializeFrame(protocol.CmdPublish, 0, payload)
	defer protocol.ReleaseBuffer(buf)
	rawBuf := *buf

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.Forward(rawBuf, true)
	}
}
