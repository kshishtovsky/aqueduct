package broker

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/quic-go/quic-go"
)

const maxPayloadSize = 1 << 20 // 1 MB max payload per message

var errTopicRequired = errors.New("subscribe payload must contain a topic")
var errTopicEmpty = errors.New("topic cannot be empty")

// RouterMetrics is the interface for publishing metrics from the router.
type RouterMetrics interface {
	OnPublish(topic string)
	OnDeliver(topic string)
	SetActiveSubscribers(n float64)
}

// Router implements In-Memory Direct Mesh Routing using Structure of Arrays (SoA).
// Subscribers are stored in flat parallel slices for L1/L2 cache locality.
// Topic lookup uses a map[string][]int (topic → indices into the flat arrays).
//
// SoA layout avoids the pointer-chasing penalty of map[string][]*Subscriber.
// Iterating over a single field (e.g., streams[i] for batch writes) is
// sequential in memory, maximizing L1/L2 cache hit rate.
type Router struct {
	mu sync.RWMutex

	// SoA flat arrays — parallel indices refer to the same subscriber.
	streamIDs []uint32       // stream ID per subscriber slot
	streams   []*quic.Stream // QUIC stream pointer per slot
	topics    []string       // topic name per slot
	active    []bool         // true if subscriber slot is live

	// topicIndex maps topic name to slice of indices in the flat arrays.
	topicIndex map[string][]int

	metrics RouterMetrics
}

// NewRouter creates a Router with optional metrics collector.
func NewRouter(metrics RouterMetrics) *Router {
	return &Router{
		topicIndex: make(map[string][]int),
		metrics:    metrics,
	}
}

// Subscribe registers a QUIC stream as a subscriber for the topic parsed from
// frame.Payload. Expected payload format: "topic:<name>".
func (r *Router) Subscribe(_ context.Context, stream *quic.Stream, frame protocol.Frame) error {
	if stream == nil {
		return errors.New("nil stream")
	}
	topic, err := extractTopic(frame.Payload)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	idx := len(r.streamIDs)
	r.streamIDs = append(r.streamIDs, frame.StreamID)
	r.streams = append(r.streams, stream)
	r.topics = append(r.topics, topic)
	r.active = append(r.active, true)
	r.topicIndex[topic] = append(r.topicIndex[topic], idx)

	if r.metrics != nil {
		r.metrics.SetActiveSubscribers(float64(r.countActiveLocked()))
	}

	return nil
}

// Publish fans out a message to all active subscribers of the topic identified
// by frame.Payload (raw topic name, no "topic:" prefix needed).
//
// Zero-Copy Send strategy:
//   - SerializeFrame pulls a buffer from sync.Pool.
//   - stream.Write copies data into quic-go's internal send buffer synchronously.
//   - ReleaseBuffer returns the pool buffer immediately after Write returns.
//
// SAFETY: quic-go's Stream.Write is synchronous — once it returns without error,
// the data has been copied into the QUIC stack's internal buffers and the
// original buffer can be safely recycled.
func (r *Router) Publish(ctx context.Context, frame protocol.Frame) error {
	if frame.PayloadLen > maxPayloadSize {
		return errors.New("payload exceeds maximum frame size")
	}

	// Take a snapshot of indices under read lock, then release.
	r.mu.RLock()
	indices := r.topicIndex[string(frame.Payload)]
	idxs := make([]int, len(indices))
	copy(idxs, indices)
	r.mu.RUnlock()

	if len(idxs) == 0 {
		return nil
	}

	if r.metrics != nil {
		r.metrics.OnPublish(string(frame.Payload))
	}

	var lastErr error
	for _, idx := range idxs {
		r.mu.RLock()
		if idx >= len(r.streams) || !r.active[idx] {
			r.mu.RUnlock()
			continue
		}
		stream := r.streams[idx]
		streamID := r.streamIDs[idx]
		r.mu.RUnlock()

		buf := protocol.SerializeFrame(protocol.CmdPublish, streamID, frame.Payload)
		_, err := stream.Write(*buf)
		protocol.ReleaseBuffer(buf)

		if err != nil {
			r.markInactive(idx)
			lastErr = err
			continue
		}

		if r.metrics != nil {
			r.metrics.OnDeliver(string(frame.Payload))
		}
	}

	return lastErr
}

// markInactive sets a subscriber slot to inactive.
func (r *Router) markInactive(idx int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if idx < len(r.active) {
		r.active[idx] = false
	}
	if r.metrics != nil {
		r.metrics.SetActiveSubscribers(float64(r.countActiveLocked()))
	}
}

// Unsubscribe removes a stream from all topic subscriptions by stream ID.
func (r *Router) Unsubscribe(streamID uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := len(r.streamIDs) - 1; i >= 0; i-- {
		if r.streamIDs[i] == streamID {
			r.active[i] = false
			topic := r.topics[i]
			r.topicIndex[topic] = removeIndex(r.topicIndex[topic], i)
			if len(r.topicIndex[topic]) == 0 {
				delete(r.topicIndex, topic)
			}
		}
	}

	if r.metrics != nil {
		r.metrics.SetActiveSubscribers(float64(r.countActiveLocked()))
	}
}

// countActiveLocked returns the number of active subscriber slots.
// Caller must hold r.mu.
func (r *Router) countActiveLocked() int {
	count := 0
	for _, a := range r.active {
		if a {
			count++
		}
	}
	return count
}

// removeIndex removes the element at position i from s by swapping with the last element.
func removeIndex(s []int, i int) []int {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

// extractTopic parses "topic:<name>" from payload bytes.
func extractTopic(payload []byte) (string, error) {
	if len(payload) == 0 {
		return "", errTopicRequired
	}
	payloadStr := string(payload)
	const prefix = "topic:"
	if len(payloadStr) < len(prefix) || payloadStr[:len(prefix)] != prefix {
		return "", errTopicRequired
	}
	topic := payloadStr[len(prefix):]
	if topic == "" {
		return "", errTopicEmpty
	}
	return topic, nil
}

// Compile-time check: *quic.Stream must satisfy io.Writer.
var _ io.Writer = (*quic.Stream)(nil)
