package cluster

import (
	"context"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"
)

// Resolver abstracts DNS resolution for testability.
// The default implementation uses net.DefaultResolver (system DNS).
type Resolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// DefaultResolver wraps the system DNS resolver.
type DefaultResolver struct{}

func (DefaultResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

// DiscoveryConfig holds DNS-based peer discovery parameters.
type DiscoveryConfig struct {
	Enabled  bool          // whether discovery is active
	Host     string        // headless service FQDN e.g. "aqueduct-headless.default.svc.cluster.local"
	Port     string        // port suffix e.g. "4242"
	Interval time.Duration // polling interval
}

// Discovery periodically resolves a Kubernetes Headless Service hostname
// via DNS and reconciles the PeerManager peer list using RCU (atomic swap).
//
// Algorithm:
//  1. Every N seconds, resolve host → []IP via net.LookupHost.
//  2. Sort + deduplicate the result set.
//  3. Diff against the currently known set.
//  4. For IPs not in current set → PeerManager.AddPeer.
//  5. For IPs in current set but not in resolved set → PeerManager.RemovePeer.
//
// Why DNS over K8s API (client-go):
//   - client-go adds ~40MB to the binary (REST client, informers, protobuf).
//   - DNS resolution uses only net.LookupHost — zero external dependencies.
//   - Headless Services in Kubernetes automatically create SRV/A records for all
//     ready pods, providing the same discovery as API label selectors.
//   - Preserves the "single static binary" philosophy.
type Discovery struct {
	resolver Resolver
	hostname string
	port     string
	interval time.Duration
	pm       *PeerManager
	logger   *slog.Logger
	knownIPs map[string]struct{}
	mu       sync.Mutex // protects knownIPs (only used by the polling goroutine)
}

// NewDiscovery creates a Discovery that will periodically resolve hostname and
// reconcile peers on the given PeerManager.
func NewDiscovery(resolver Resolver, cfg DiscoveryConfig, pm *PeerManager, logger *slog.Logger) *Discovery {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Interval == 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.Port == "" {
		cfg.Port = "4242"
	}
	return &Discovery{
		resolver: resolver,
		hostname: cfg.Host,
		port:     cfg.Port,
		interval: cfg.Interval,
		pm:       pm,
		logger:   logger,
		knownIPs: make(map[string]struct{}),
	}
}

// Start begins the background DNS polling loop. It resolves immediately on start,
// then repeats every d.interval. Cancel ctx to stop.
func (d *Discovery) Start(ctx context.Context) {
	// Immediate first resolution.
	d.resolve(ctx)

	go func() {
		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.resolve(ctx)
			}
		}
	}()
}

// resolve performs a single DNS lookup and reconciles the peer list.
func (d *Discovery) resolve(ctx context.Context) {
	ips, err := d.resolver.LookupHost(ctx, d.hostname)
	if err != nil {
		d.logger.Warn("dns discovery: lookup failed", "host", d.hostname, "err", err)
		return
	}

	normalized := d.normalize(ips)

	d.mu.Lock()
	defer d.mu.Unlock()

	// Find IPs to add (in resolved but not in known).
	for ip := range normalized {
		if _, exists := d.knownIPs[ip]; !exists {
			addr := net.JoinHostPort(ip, d.port)
			d.pm.AddPeer(ctx, addr)
			d.logger.Info("dns discovery: peer discovered", "addr", addr)
		}
	}

	// Find IPs to remove (in known but not in resolved).
	for ip := range d.knownIPs {
		if _, exists := normalized[ip]; !exists {
			addr := net.JoinHostPort(ip, d.port)
			d.pm.RemovePeer(addr)
			d.logger.Info("dns discovery: peer lost", "addr", addr)
		}
	}

	d.knownIPs = normalized
}

// normalize deduplicates and sorts a list of IP strings.
func (d *Discovery) normalize(ips []string) map[string]struct{} {
	set := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		// Parse to normalize IPv4/IPv6 representations, then re-stringify.
		parsed := net.ParseIP(ip)
		if parsed == nil {
			continue // skip unparseable entries
		}
		set[parsed.String()] = struct{}{}
	}
	return set
}

// KnownPeers returns a sorted copy of the currently discovered peer addresses.
func (d *Discovery) KnownPeers() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	peers := make([]string, 0, len(d.knownIPs))
	for ip := range d.knownIPs {
		peers = append(peers, net.JoinHostPort(ip, d.port))
	}
	sort.Strings(peers)
	return peers
}

// ResolveOnce performs a single resolution + reconciliation (exported for testing).
func (d *Discovery) ResolveOnce(ctx context.Context) {
	d.resolve(ctx)
}
