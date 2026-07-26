package broker

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/metrics"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

const defaultQueueSize = 1024
const maxPayloadSize = 1 << 20 // 1 MB max payload per message

var (
	errTopicRequired = errors.New("subscribe payload must contain a topic")
	errTopicEmpty    = errors.New("topic cannot be empty")
)

// BackpressurePolicy defines how queue overflow is handled for slow consumers.
type BackpressurePolicy uint8

const (
	PolicyDropOldest BackpressurePolicy = iota
	PolicyDropNewest
	PolicyDisconnect
)

func ParseBackpressurePolicy(s string) BackpressurePolicy {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "drop_newest":
		return PolicyDropNewest
	case "disconnect":
		return PolicyDisconnect
	default:
		return PolicyDropOldest
	}
}

func (p BackpressurePolicy) String() string {
	switch p {
	case PolicyDropNewest:
		return "drop_newest"
	case PolicyDisconnect:
		return "disconnect"
	default:
		return "drop_oldest"
	}
}

// RouterMetrics is the interface for publishing metrics from the router.
type RouterMetrics interface {
	OnPublish(topic string)
	OnDeliver(topic string)
	SetActiveSubscribers(n float64)
}

// RouterOption configures the Router.
type RouterOption func(*Router)

// WithQueueSize sets the per-subscriber bounded channel queue size.
func WithQueueSize(n int) RouterOption {
	return func(r *Router) {
		if n > 0 {
			r.queueSize = n
		}
	}
}

// WithBackpressurePolicy sets the slow consumer isolation policy.
func WithBackpressurePolicy(p BackpressurePolicy) RouterOption {
	return func(r *Router) {
		r.policy = p
	}
}

// WildcardSub stores a wildcard pattern registration and subscriber index.
type WildcardSub struct {
	pattern []byte
	idx     int
}

// Router implements In-Memory Direct Mesh Routing using Structure of Arrays (SoA).
// Each subscriber has a dedicated non-blocking bounded queue and Writer goroutine.
type Router struct {
	mu sync.RWMutex

	// SoA flat arrays — parallel indices refer to the same subscriber.
	streamIDs []uint32               // stream ID per subscriber slot
	streams   []*quic.Stream         // QUIC stream pointer per slot
	topics    []string               // topic name per slot
	active    []bool                 // true if subscriber slot is live
	queues    []chan *MessageRef     // per-subscriber non-blocking ring queue
	cancels   []context.CancelFunc   // per-subscriber Writer goroutine cancel handle

	// topicIndex maps FNV-1a hash of topic name to slice of indices in flat arrays.
	topicIndex map[uint64][]int

	// wildcardSubs holds wildcard topic patterns (+ and #).
	wildcardSubs []WildcardSub

	metrics   RouterMetrics
	queueSize int
	policy    BackpressurePolicy

	wg sync.WaitGroup
}

// NewRouter creates a Router with optional metrics collector and configuration options.
func NewRouter(m RouterMetrics, opts ...RouterOption) *Router {
	r := &Router{
		topicIndex: make(map[uint64][]int),
		metrics:    m,
		queueSize:  defaultQueueSize,
		policy:     PolicyDropOldest,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Subscribe registers a QUIC stream as a subscriber for the topic parsed from
// frame.Payload and spawns a dedicated Writer goroutine. Expected payload format: "topic:<name>".
func (r *Router) Subscribe(ctx context.Context, stream *quic.Stream, frame protocol.Frame) error {
	if stream == nil {
		return errors.New("nil stream")
	}
	topic, err := extractTopic(frame.Payload)
	if err != nil {
		return err
	}

	q := make(chan *MessageRef, r.queueSize)
	subCtx, cancel := context.WithCancel(ctx)
	topicHash := authz.CombineHashStrings("topic", topic)

	r.mu.Lock()
	idx := len(r.streamIDs)
	r.streamIDs = append(r.streamIDs, frame.StreamID)
	r.streams = append(r.streams, stream)
	r.topics = append(r.topics, topic)
	r.active = append(r.active, true)
	r.queues = append(r.queues, q)
	r.cancels = append(r.cancels, cancel)
	r.topicIndex[topicHash] = append(r.topicIndex[topicHash], idx)

	if IsWildcardTopic(topic) {
		r.wildcardSubs = append(r.wildcardSubs, WildcardSub{
			pattern: []byte(topic),
			idx:     idx,
		})
	}

	if r.metrics != nil {
		r.metrics.SetActiveSubscribers(float64(r.countActiveLocked()))
	}
	r.mu.Unlock()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.runSubscriberWriter(subCtx, stream, topic, q)
	}()

	return nil
}

// runSubscriberWriter drains the subscriber's queue, checks TTL expiration lazily,
// writes frames to the QUIC stream, and releases MessageRef reference counts upon completion.
func (r *Router) runSubscriberWriter(ctx context.Context, stream *quic.Stream, topic string, q chan *MessageRef) {
	for {
		select {
		case <-ctx.Done():
			r.drainQueue(q)
			return
		case msgRef, ok := <-q:
			if !ok {
				return
			}
			nowNano := time.Now().UnixNano()
			if msgRef.IsExpired(nowNano) {
				msgRef.Release()
				metrics.MessagesExpired.WithLabelValues(topic).Inc()
				continue
			}

			buf := msgRef.Buf()
			if len(buf) > 0 {
				if _, err := stream.Write(buf); err != nil {
					msgRef.Release()
					r.drainQueue(q)
					return
				}
				if r.metrics != nil {
					r.metrics.OnDeliver(topic)
				}
			}
			msgRef.Release()
		}
	}
}

// drainQueue releases all remaining MessageRefs in the channel when a subscriber disconnects.
func (r *Router) drainQueue(q chan *MessageRef) {
	for {
		select {
		case msgRef, ok := <-q:
			if ok && msgRef != nil {
				msgRef.Release()
			}
		default:
			return
		}
	}
}

// Publish non-blockingly dispatches a message to all matching subscriber queues (exact & wildcard).
// Operates with 0 heap allocations and nano-second publisher latency.
func (r *Router) Publish(ctx context.Context, frame protocol.Frame) error {
	if frame.PayloadLen > maxPayloadSize {
		return errors.New("payload exceeds maximum frame size")
	}

	expiresAt, cleanPayload := parseTTL(frame.Payload)
	topicHash := authz.CombineHashes("topic", cleanPayload)

	r.mu.RLock()
	indices := r.topicIndex[topicHash]
	hasWildcards := len(r.wildcardSubs) > 0

	if len(indices) == 0 && !hasWildcards {
		r.mu.RUnlock()
		return nil
	}

	if r.metrics != nil {
		r.metrics.OnPublish(string(cleanPayload))
	}

	// Create single frame buffer pooled via sync.Pool
	buf := protocol.SerializeFrame(protocol.CmdPublish, 0, cleanPayload)
	msgRef := AcquireMessageRef(buf)
	msgRef.SetExpiresAt(expiresAt)

	var inlineDC [4]int
	disconnectIndices := inlineDC[:0]

	// 1. Dispatch to exact topic match subscribers
	for _, idx := range indices {
		if idx < len(r.active) && r.active[idx] {
			q := r.queues[idx]
			msgRef.Retain()

			select {
			case q <- msgRef:
				// Successfully enqueued
			default:
				topicName := r.topics[idx]
				dc := r.handleOverflow(idx, topicName, q, msgRef)
				if dc >= 0 {
					disconnectIndices = append(disconnectIndices, dc)
				}
			}
		}
	}

	// 2. Dispatch to matching wildcard subscribers (+ and #)
	if hasWildcards {
		for _, wSub := range r.wildcardSubs {
			idx := wSub.idx
			if idx < len(r.active) && r.active[idx] {
				if MatchWildcard(wSub.pattern, cleanPayload) {
					q := r.queues[idx]
					msgRef.Retain()

					select {
					case q <- msgRef:
					default:
						topicName := r.topics[idx]
						dc := r.handleOverflow(idx, topicName, q, msgRef)
						if dc >= 0 {
							disconnectIndices = append(disconnectIndices, dc)
						}
					}
				}
			}
		}
	}
	r.mu.RUnlock()

	// Release initial reference count held by publisher
	msgRef.Release()

	if len(disconnectIndices) > 0 {
		for _, idx := range disconnectIndices {
			r.disconnectSubscriber(idx)
		}
	}

	return nil
}

// handleOverflow applies the configured backpressure policy when a subscriber queue is full.
func (r *Router) handleOverflow(idx int, topic string, q chan *MessageRef, msgRef *MessageRef) int {
	switch r.policy {
	case PolicyDropOldest:
		select {
		case oldest := <-q:
			if oldest != nil {
				oldest.Release()
			}
			metrics.MessagesDropped.WithLabelValues(topic, "drop_oldest").Inc()
			select {
			case q <- msgRef:
			default:
				msgRef.Release()
			}
		default:
			select {
			case q <- msgRef:
			default:
				msgRef.Release()
			}
		}
		return -1

	case PolicyDropNewest:
		msgRef.Release()
		metrics.MessagesDropped.WithLabelValues(topic, "drop_newest").Inc()
		return -1

	case PolicyDisconnect:
		msgRef.Release()
		metrics.SlowConsumersDisconnected.Inc()
		return idx

	default:
		msgRef.Release()
		return -1
	}
}

// disconnectSubscriber closes the QUIC stream and cancels the Writer goroutine for a slow consumer.
func (r *Router) disconnectSubscriber(idx int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if idx < len(r.active) && r.active[idx] {
		r.active[idx] = false
		if cancel := r.cancels[idx]; cancel != nil {
			cancel()
		}
		if stream := r.streams[idx]; stream != nil {
			_ = stream.Close()
		}
		if r.metrics != nil {
			r.metrics.SetActiveSubscribers(float64(r.countActiveLocked()))
		}
	}
}

// Unsubscribe removes all active subscriber registrations for streamID.
func (r *Router) Unsubscribe(streamID uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, sid := range r.streamIDs {
		if sid == streamID && r.active[i] {
			r.active[i] = false
			if cancel := r.cancels[i]; cancel != nil {
				cancel()
			}
		}
	}

	if r.metrics != nil {
		r.metrics.SetActiveSubscribers(float64(r.countActiveLocked()))
	}
}

// Close drains all active subscriber queues and waits for Writer goroutines to exit.
func (r *Router) Close() {
	r.mu.Lock()
	for i := range r.active {
		if r.active[i] {
			r.active[i] = false
			if cancel := r.cancels[i]; cancel != nil {
				cancel()
			}
		}
	}
	r.mu.Unlock()

	r.wg.Wait()
}

func (r *Router) countActiveLocked() int {
	count := 0
	for _, a := range r.active {
		if a {
			count++
		}
	}
	return count
}

func extractTopic(payload []byte) (string, error) {
	if len(payload) < 6 || string(payload[:6]) != "topic:" {
		return "", errTopicRequired
	}
	topic := string(payload[6:])
	if topic == "" {
		return "", errTopicEmpty
	}
	return topic, nil
}

// parseTTL parses optional "ttl:<ms>:<payload>" format and returns unix nanosecond expiration.
func parseTTL(payload []byte) (int64, []byte) {
	if bytes.HasPrefix(payload, []byte("ttl:")) {
		idx := bytes.IndexByte(payload[4:], ':')
		if idx > 0 {
			msStr := string(payload[4 : 4+idx])
			if ms, err := strconv.ParseInt(msStr, 10, 64); err == nil {
				exp := time.Now().Add(time.Duration(ms) * time.Millisecond).UnixNano()
				return exp, payload[4+idx+1:]
			}
		}
	}
	return 0, payload
}
