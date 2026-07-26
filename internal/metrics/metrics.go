package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
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
		[]string{"topic"},
	)

	AALRotations = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aqueduct_aal_rotations_total",
			Help: "Total number of AAL log rotations performed",
		},
	)
)

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
}
