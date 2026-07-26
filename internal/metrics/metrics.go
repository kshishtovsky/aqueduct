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
)

func init() {
	prometheus.MustRegister(MessagesPublished)
	prometheus.MustRegister(MessagesDelivered)
	prometheus.MustRegister(ActiveSubscribers)
	prometheus.MustRegister(FrameParseDuration)
	prometheus.MustRegister(AuthzDenied)
}
