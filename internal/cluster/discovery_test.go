package cluster_test

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/cluster"
	"github.com/quic-go/quic-go"
)

// mockResolver returns IPs from its current snapshot, which can be swapped atomically.
type mockResolver struct {
	mu  sync.RWMutex
	ips []string
	err error
}

func newMockResolver(ips []string) *mockResolver {
	return &mockResolver{ips: ips}
}

func (m *mockResolver) LookupHost(_ context.Context, _ string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.err != nil {
		return nil, m.err
	}
	out := make([]string, len(m.ips))
	copy(out, m.ips)
	return out, nil
}

func (m *mockResolver) setIPs(ips []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ips = ips
}

func (m *mockResolver) setErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// dummyPeerManager creates a PeerManager with a QUIC listener that accepts connections.
// It returns the manager and a function to count connected peers.
func dummyPeerManager(ctx context.Context, t *testing.T) *cluster.PeerManager {
	t.Helper()
	sTLS, cTLS := genSelfSigned(t)
	qConf := &quic.Config{MaxIdleTimeout: 5 * time.Second}

	ln, err := quic.ListenAddr("127.0.0.1:0", sTLS, qConf)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go dummyAcceptLoop(ctx, ln)

	pm := cluster.NewWithLogger(ctx, []string{}, cTLS, qConf, slog.Default())
	t.Cleanup(func() { pm.Close() })
	return pm
}

// dummyAcceptLoop accepts connections in a loop and drains every accepted
// stream until EOF. Splitting this into a helper keeps dummyPeerManager well
// under Sonar's cognitive-complexity threshold.
func dummyAcceptLoop(ctx context.Context, ln *quic.Listener) {
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		go dummyAcceptStreams(ctx, conn)
	}
}

func dummyAcceptStreams(ctx context.Context, conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go drainStream(stream)
	}
}

func drainStream(s *quic.Stream) {
	buf := make([]byte, 4096)
	for {
		if _, err := s.Read(buf); err != nil {
			return
		}
	}
}

// TestDiscoveryInitialResolution verifies that Start resolves DNS immediately
// and adds discovered peers. Refactored to helpers to keep cognitive
// complexity under Sonar's threshold.
func TestDiscoveryInitialResolution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver := newMockResolver([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})
	pm := dummyPeerManager(ctx, t)

	disc := startDiscovery(t, ctx, resolver, pm, cluster.DiscoveryConfig{
		Host:     "aqueduct-headless.default.svc.cluster.local",
		Port:     "4242",
		Interval: 1 * time.Hour,
	})
	time.Sleep(200 * time.Millisecond)

	peers := disc.KnownPeers()
	if len(peers) != 3 {
		t.Fatalf("expected 3 known peers, got %d: %v", len(peers), peers)
	}
	assertAllPeersUsePort(t, peers, "4242")
}

// assertAllPeersUsePort fails the test if any peer address does not carry
// the expected port suffix.
func assertAllPeersUsePort(t *testing.T, peers []string, want string) {
	t.Helper()
	for _, p := range peers {
		_, port, err := net.SplitHostPort(p)
		if err != nil {
			t.Fatalf("invalid peer addr %q: %v", p, err)
		}
		if port != want {
			t.Errorf("expected port %s, got %s", want, port)
		}
	}
}

// TestDiscoveryNewPeerAdded verifies that a new IP resolved after initial
// resolution is added to the peer list.
func TestDiscoveryNewPeerAdded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver := newMockResolver([]string{"10.0.0.1", "10.0.0.2"})
	pm := dummyPeerManager(ctx, t)

	disc := cluster.NewDiscovery(resolver, cluster.DiscoveryConfig{
		Enabled:  true,
		Host:     "aqueduct-headless.default.svc.cluster.local",
		Port:     "4242",
		Interval: 1 * time.Hour,
	}, pm, slog.Default())

	disc.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	if len(disc.KnownPeers()) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(disc.KnownPeers()))
	}

	// Simulate scale-up: new pod with IP 10.0.0.3 appears.
	resolver.setIPs([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})
	disc.ResolveOnce(ctx)
	time.Sleep(100 * time.Millisecond)

	peers := disc.KnownPeers()
	if len(peers) != 3 {
		t.Fatalf("expected 3 peers after scale-up, got %d: %v", len(peers), peers)
	}

	found := false
	for _, p := range peers {
		host, _, _ := net.SplitHostPort(p)
		if host == "10.0.0.3" {
			found = true
			break
		}
	}
	if !found {
		t.Error("new peer 10.0.0.3 not found in known peers")
	}
}

// TestDiscoveryPeerRemoved verifies that when an IP disappears from DNS,
// the corresponding peer is removed. Refactored to a helper to keep
// cognitive complexity under Sonar's threshold.
func TestDiscoveryPeerRemoved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver := newMockResolver([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})
	pm := dummyPeerManager(ctx, t)

	disc := startDiscovery(t, ctx, resolver, pm, cluster.DiscoveryConfig{
		Host:     "aqueduct-headless.default.svc.cluster.local",
		Port:     "4242",
		Interval: 1 * time.Hour,
	})
	time.Sleep(200 * time.Millisecond)
	assertKnownPeerCount(t, disc, 3)

	// Simulate scale-down: pod with IP 10.0.0.2 is terminated.
	resolver.setIPs([]string{"10.0.0.1", "10.0.0.3"})
	disc.ResolveOnce(ctx)
	time.Sleep(100 * time.Millisecond)

	peers := disc.KnownPeers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers after scale-down, got %d: %v", len(peers), peers)
	}
	for _, p := range peers {
		host, _, _ := net.SplitHostPort(p)
		if host == "10.0.0.2" {
			t.Error("peer 10.0.0.2 should have been removed")
		}
	}
}

// startDiscovery is a test helper that wires a Discovery instance with the
// standard aqueduct-headless config, overriding only the host/port/interval.
func startDiscovery(t *testing.T, ctx context.Context, resolver *mockResolver, pm *cluster.PeerManager, cfg cluster.DiscoveryConfig) *cluster.Discovery {
	t.Helper()
	cfg.Enabled = true
	disc := cluster.NewDiscovery(resolver, cfg, pm, slog.Default())
	disc.Start(ctx)
	return disc
}

// assertKnownPeerCount fails the test when the discovery reports the wrong
// number of known peers.
func assertKnownPeerCount(t *testing.T, disc *cluster.Discovery, want int) {
	t.Helper()
	got := len(disc.KnownPeers())
	if got != want {
		t.Fatalf("expected %d known peers, got %d: %v", want, got, disc.KnownPeers())
	}
}

// TestDiscoveryDuplicateIPs verifies that duplicate IPs from DNS are normalized.
func TestDiscoveryDuplicateIPs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// DNS can return duplicates (A records for statefulset pods).
	resolver := newMockResolver([]string{"10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.2"})
	pm := dummyPeerManager(ctx, t)

	disc := cluster.NewDiscovery(resolver, cluster.DiscoveryConfig{
		Enabled:  true,
		Host:     "aqueduct-headless.default.svc.cluster.local",
		Port:     "4242",
		Interval: 1 * time.Hour,
	}, pm, slog.Default())

	disc.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	peers := disc.KnownPeers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 deduplicated peers, got %d: %v", len(peers), peers)
	}
}

// TestDiscoveryDNSError verifies that DNS errors are handled gracefully
// without crashing or losing the current peer set.
func TestDiscoveryDNSError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver := newMockResolver([]string{"10.0.0.1"})
	pm := dummyPeerManager(ctx, t)

	disc := cluster.NewDiscovery(resolver, cluster.DiscoveryConfig{
		Enabled:  true,
		Host:     "aqueduct-headless.default.svc.cluster.local",
		Port:     "4242",
		Interval: 1 * time.Hour,
	}, pm, slog.Default())

	disc.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	if len(disc.KnownPeers()) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(disc.KnownPeers()))
	}

	// DNS fails — known peers should remain unchanged.
	resolver.setErr(net.ErrClosed)
	disc.ResolveOnce(ctx)
	time.Sleep(100 * time.Millisecond)

	if len(disc.KnownPeers()) != 1 {
		t.Errorf("peer set changed after DNS error: expected 1, got %d", len(disc.KnownPeers()))
	}
}

// TestDiscoveryNormalizeIPv6 verifies IPv6 addresses are properly normalized.
func TestDiscoveryNormalizeIPv6(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver := newMockResolver([]string{
		"fd00::1",
		"FD00::1", // same address, different case
		"10.0.0.1",
		"fe80::1%eth0", // link-local with zone — should be skipped (unparseable)
	})
	pm := dummyPeerManager(ctx, t)

	disc := cluster.NewDiscovery(resolver, cluster.DiscoveryConfig{
		Enabled:  true,
		Host:     "aqueduct-headless.default.svc.cluster.local",
		Port:     "4242",
		Interval: 1 * time.Hour,
	}, pm, slog.Default())

	disc.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	peers := disc.KnownPeers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 normalized peers, got %d: %v", len(peers), peers)
	}
}

// TestDiscoveryPollingLoop verifies the background ticker triggers re-resolution.
func TestDiscoveryPollingLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var resolveCount atomic.Int32
	resolver := &countingResolver{
		mock:  newMockResolver([]string{"10.0.0.1"}),
		count: &resolveCount,
	}
	pm := dummyPeerManager(ctx, t)

	disc := cluster.NewDiscovery(resolver, cluster.DiscoveryConfig{
		Enabled:  true,
		Host:     "aqueduct-headless.default.svc.cluster.local",
		Port:     "4242",
		Interval: 100 * time.Millisecond, // fast ticker
	}, pm, slog.Default())

	disc.Start(ctx)
	time.Sleep(500 * time.Millisecond) // should trigger ~4-5 resolutions

	count := resolveCount.Load()
	if count < 3 {
		t.Errorf("expected at least 3 DNS resolutions from polling, got %d", count)
	}
}

// TestDiscoveryNoOpWhenSameIPs verifies no AddPeer/RemovePeer when DNS is stable.
func TestDiscoveryNoOpWhenSameIPs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resolver := newMockResolver([]string{"10.0.0.1", "10.0.0.2"})
	pm := dummyPeerManager(ctx, t)

	disc := cluster.NewDiscovery(resolver, cluster.DiscoveryConfig{
		Enabled:  true,
		Host:     "aqueduct-headless.default.svc.cluster.local",
		Port:     "4242",
		Interval: 1 * time.Hour,
	}, pm, slog.Default())

	disc.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	initialCount := len(disc.KnownPeers())

	// Resolve again with same IPs — peer count should not change.
	disc.ResolveOnce(ctx)
	time.Sleep(100 * time.Millisecond)

	if len(disc.KnownPeers()) != initialCount {
		t.Errorf("peer count changed with identical DNS results: %d → %d",
			initialCount, len(disc.KnownPeers()))
	}
}

// countingResolver wraps mockResolver and counts Lookups.
type countingResolver struct {
	mock  *mockResolver
	count *atomic.Int32
}

func (c *countingResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	c.count.Add(1)
	return c.mock.LookupHost(ctx, host)
}
