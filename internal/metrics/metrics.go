package metrics

import (
	"hash/fnv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// maxUniqueLabels is the hard cap on unique label values per metric family.
// When exceeded, new values are hashed into a single overflow bucket to prevent
// unbounded memory growth from high-cardinality attacker-controlled labels.
const maxUniqueLabels = 4096

// labelBounded is a concurrent-safe map that caps the number of unique label
// values. New values beyond the cap are collapsed into an overflow bucket.
type labelBounded struct {
	mu       sync.RWMutex
	mapping  map[string]string
	count    int
	overflow string
}

func newLabelBounded() *labelBounded {
	return &labelBounded{
		mapping:  make(map[string]string, 64),
		overflow: "__overflow__",
	}
}

// sanitize returns a bounded label value. Known values pass through instantly.
// Unknown values are admitted until the cap is hit, then mapped to the overflow bucket.
func (b *labelBounded) sanitize(val string) string {
	b.mu.RLock()
	if mapped, ok := b.mapping[val]; ok {
		b.mu.RUnlock()
		return mapped
	}
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	// Re-check after acquiring write lock.
	if mapped, ok := b.mapping[val]; ok {
		return mapped
	}

	if b.count >= maxUniqueLabels {
		// Hash to detect collisions but always return overflow bucket.
		h := fnv.New32a()
		_, _ = h.Write([]byte(val))
		b.mapping[val] = b.overflow
		return b.overflow
	}

	b.mapping[val] = val
	b.count++
	return val
}

var (
	// Bounded label sanitizers for high-cardinality labels.
	sanitizeTopic    = newLabelBounded()
	sanitizeClient   = newLabelBounded()
	sanitizeConsumer = newLabelBounded()

	MessagesPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aqueduct_messages_published_total",
			Help: "Total number of messages published per topic",
		},
		[]string{"topic"},
	)

	MessagesDelivered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aqueduct_messages_delivered_total",
			Help: "Total number of messages delivered per topic",
		},
		[]string{"topic"},
	)

	ActiveSubscribers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aqueduct_active_subscribers",
			Help: "Current number of active subscribers across all topics",
		},
	)

	FrameParseDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aqueduct_frame_parse_duration_ns",
			Help:    "Histogram of frame parse duration in nanoseconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	AuthzDenied = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aqueduct_authz_denied_total",
			Help: "Total number of authorization denials per client and topic",
		},
		[]string{"client", "topic"},
	)

	MessagesDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aqueduct_messages_dropped_total",
			Help: "Total number of messages dropped due to slow consumer backpressure",
		},
		[]string{"topic", "policy"},
	)

	SlowConsumersDisconnected = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aqueduct_slow_consumers_disconnected_total",
			Help: "Total number of slow consumers disconnected due to queue overflow",
		},
	)

	AALReplayDuration = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aqueduct_aal_replay_duration_seconds",
			Help: "Duration of AAL log replay on startup in seconds",
		},
	)

	MessagesExpired = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aqueduct_messages_expired_total",
			Help: "Total number of messages dropped due to TTL expiry",
		},
		[]string{"topic", "priority"},
	)

	AALRotations = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aqueduct_aal_rotations_total",
			Help: "Total number of AAL log rotations performed",
		},
	)

	DurableSubscribersActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aqueduct_durable_subscriptions_active",
			Help: "Current number of active durable subscriptions across all topics",
		},
	)

	ConsumerOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aqueduct_consumer_offset",
			Help: "Current acknowledged consumer offset per consumer and topic",
		},
		[]string{"consumer", "topic"},
	)

	AALBackfillFrames = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aqueduct_aal_backfill_frames_total",
			Help: "Total number of historical AAL frames replayed during subscriber backfill",
		},
	)

	ClusterPeersActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aqueduct_cluster_peers_active",
			Help: "Current number of active peer connections in the cluster",
		},
	)

	ClusterFramesForwarded = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aqueduct_cluster_frames_forwarded_total",
			Help: "Total number of frames forwarded to peer nodes",
		},
	)

	ClusterFramesReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aqueduct_cluster_frames_received_total",
			Help: "Total number of mesh-forwarded frames received from peer nodes",
		},
	)

	MessagesNacked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aqueduct_messages_nacked_total",
			Help: "Total number of messages nacked (negative acknowledged) per topic",
		},
		[]string{"topic"},
	)

	MessagesRateLimited = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aqueduct_messages_rate_limited_total",
			Help: "Total number of messages rate limited (dropped) per client",
		},
		[]string{"client"},
	)

	MessagesDeadLettered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aqueduct_messages_dead_lettered_total",
			Help: "Total number of messages moved to DLQ per topic",
		},
		[]string{"topic"},
	)

	TracingSpansTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aqueduct_tracing_spans_total",
			Help: "Total number of tracing spans created",
		},
	)

	AdminRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aqueduct_admin_requests_total",
			Help: "Total number of gRPC Admin API requests per method",
		},
		[]string{"method"},
	)
)

// SanitizeTopic returns a cardinality-bounded topic label value.
func SanitizeTopic(topic string) string { return sanitizeTopic.sanitize(topic) }

// SanitizeClient returns a cardinality-bounded client label value.
func SanitizeClient(client string) string { return sanitizeClient.sanitize(client) }

// SanitizeConsumer returns a cardinality-bounded consumer label value.
func SanitizeConsumer(consumer string) string { return sanitizeConsumer.sanitize(consumer) }

func init() {
	prometheus.MustRegister(MessagesPublished)
	prometheus.MustRegister(MessagesDelivered)
	prometheus.MustRegister(ActiveSubscribers)
	prometheus.MustRegister(FrameParseDuration)
	prometheus.MustRegister(AuthzDenied)
	prometheus.MustRegister(MessagesDropped)
	prometheus.MustRegister(SlowConsumersDisconnected)
	prometheus.MustRegister(AALReplayDuration)
	prometheus.MustRegister(MessagesExpired)
	prometheus.MustRegister(AALRotations)
	prometheus.MustRegister(DurableSubscribersActive)
	prometheus.MustRegister(ConsumerOffset)
	prometheus.MustRegister(AALBackfillFrames)
	prometheus.MustRegister(ClusterPeersActive)
	prometheus.MustRegister(ClusterFramesForwarded)
	prometheus.MustRegister(ClusterFramesReceived)
	prometheus.MustRegister(MessagesNacked)
	prometheus.MustRegister(MessagesDeadLettered)
	prometheus.MustRegister(MessagesRateLimited)
	prometheus.MustRegister(TracingSpansTotal)
	prometheus.MustRegister(AdminRequestsTotal)
}
