package broker

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kshishtovsky/aqueduct/internal/aal"
	"github.com/kshishtovsky/aqueduct/internal/authz"
	"github.com/kshishtovsky/aqueduct/internal/metrics"
	"github.com/kshishtovsky/aqueduct/internal/protocol"
	"github.com/kshishtovsky/aqueduct/internal/quotas"
	"github.com/quic-go/quic-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const defaultQueueSize = 1024
const maxPayloadSize = 1 << 20   // 1 MB max payload per message
const defaultMaxRetries = 3      // default max NACK retries before DLQ
const defaultNackCacheSize = 256 // max frame cache entries per subscriber for NACK replay

// Default batch configuration for coalesced writes.
const (
	defaultBatchSize     = 64 * 1024             // 64 KB
	defaultFlushInterval = 50 * time.Microsecond // 50 µs
)

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
	OnRateLimited(clientID string)
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

// WithAALPath provides path and key for AAL backfill replay workers.
func WithAALPath(path string, key []byte) RouterOption {
	return func(r *Router) {
		r.aalPath = path
		r.aalKey = key
	}
}

// PeerForwarder is the interface satisfied by cluster.PeerManager.
// It is abstracted here to avoid an import cycle.
type PeerForwarder interface {
	// Forward sends rawBuf zero-copy to all connected peers.
	// addForwardedBit=true sets the MeshForwardedBit in the wire frame.
	Forward(rawBuf []byte, addForwardedBit bool)
	ActivePeers() int
}

// WithPeerForwarder plugs a cluster PeerManager into the Router for inter-node forwarding.
func WithPeerForwarder(f PeerForwarder) RouterOption {
	return func(r *Router) {
		r.peerForwarder = f
	}
}

// CompressionEngine is the interface for batch payload compression.
type CompressionEngine interface {
	Compress(src []byte) ([]byte, error)
	Decompress(src []byte, uncompressedSize int) ([]byte, error)
	ReleaseBuf(buf []byte)
}

// WithBatchSize sets the coalesced write batch size in bytes for subscriber writers.
// Must be > 0. Default: 64 KB.
func WithBatchSize(n int) RouterOption {
	return func(r *Router) {
		if n > 0 {
			r.batchSize = n
		}
	}
}

// WithFlushInterval sets the maximum time to wait before flushing accumulated messages.
// Must be > 0. Default: 50 µs.
func WithFlushInterval(d time.Duration) RouterOption {
	return func(r *Router) {
		if d > 0 {
			r.flushInterval = d
		}
	}
}

// WithCompression enables batch payload compression for peer forwarding.
// engine provides ZSTD compression, minBatchSize is the minimum payload size
// in bytes before compression is applied (default 1024).
func WithCompression(engine CompressionEngine, minBatchSize int) RouterOption {
	return func(r *Router) {
		r.compression = engine
		r.compressionMinSize = minBatchSize
	}
}

// WildcardSub stores a wildcard pattern registration and subscriber index.
type WildcardSub struct {
	pattern []byte
	idx     int
}

// nackKey uniquely identifies a message for retry tracking.
type nackKey struct {
	topicHash uint64
	offset    uint64
}

// ConsumerGroup manages lock-free atomic round-robin dispatch among competing consumers.
type ConsumerGroup struct {
	groupID string
	topic   string
	counter atomic.Uint64
	members atomic.Pointer[[]int] // RCU snapshot of active subscriber slot indices
}

// SubscriptionSpec holds parsed parameters from a Subscribe command.
type SubscriptionSpec struct {
	Topic           string
	IsDurable       bool
	ConsumerID      string
	RequestedOffset uint64
	GroupID         string
}

// Router implements In-Memory Direct Mesh Routing using Structure of Arrays (SoA).
// Each subscriber has a dedicated non-blocking bounded queue and Writer goroutine.
type Router struct {
	mu sync.RWMutex

	// SoA flat arrays — parallel indices refer to the same subscriber.
	streamIDs []uint32              // stream ID per subscriber slot
	streams   []*quic.Stream        // QUIC stream pointer per slot
	topics    []string              // topic name per slot
	active    []bool                 // true if subscriber slot is live
	queues    []*[4]chan *MessageRef // per-subscriber 4 priority ring queues pointer (lazy allocated)
	notifyChs []chan struct{}        // per-subscriber notification handle
	subMus    []*sync.RWMutex       // per-subscriber RWMutex for lazy queue pool init/cleanup
	cancels   []context.CancelFunc  // per-subscriber Writer goroutine cancel handle

	queuePool sync.Pool // pool of chan *MessageRef of size queueSize

	// topicIndex maps FNV-1a hash of topic name to slice of indices in flat arrays.
	topicIndex map[uint64][]int

	// subGroups stores GroupID per subscriber slot ("" if individual subscriber).
	subGroups []string

	// groups maps FNV-1a hash of topic name to slice of ConsumerGroup instances.
	groups map[uint64][]*ConsumerGroup

	// wildcardSubs holds wildcard topic patterns (+ and #).
	wildcardSubs []WildcardSub

	// topicOffsets tracks 64-bit monotonic sequence counter per topic.
	topicOffsets map[uint64]*atomic.Uint64

	// durableOffsets tracks acknowledged consumer offset per (consumerID + topic).
	durableOffsets map[uint64]uint64

	// nackChs delivers NACK offsets from processStream to subscriber writer goroutines.
	nackChs []chan uint64

	// nackCounters tracks retry count for nacked messages (topicHash+offset → count).
	// Only populated for messages that receive at least one NACK.
	// Cleared when message moves to DLQ.
	nackCounters map[nackKey]int8
	nackMu       sync.Mutex

	maxRetries   int
	quotaManager *quotas.Manager
	aalPath      string
	aalKey       []byte

	peerForwarder PeerForwarder

	metrics       RouterMetrics
	queueSize     int
	policy        BackpressurePolicy
	batchSize     int
	flushInterval time.Duration

	priorityTTLs [4]time.Duration // per-priority TTL thresholds

	compression        CompressionEngine
	compressionMinSize int

	wg sync.WaitGroup
}

// WithPriorityTTLs sets the per-priority TTL thresholds (array of 4 durations).
func WithPriorityTTLs(ttls [4]time.Duration) RouterOption {
	return func(r *Router) {
		r.priorityTTLs = ttls
	}
}

// NewRouter creates a Router with optional metrics collector and configuration options.
// WithMaxRetries sets the maximum NACK retry count before a message is moved to DLQ.
func WithMaxRetries(n int) RouterOption {
	return func(r *Router) {
		if n > 0 {
			r.maxRetries = n
		}
	}
}

func WithQuotas(qm *quotas.Manager) RouterOption {
	return func(r *Router) {
		r.quotaManager = qm
	}
}

func NewRouter(m RouterMetrics, opts ...RouterOption) *Router {
	r := &Router{
		topicIndex:     make(map[uint64][]int),
		topicOffsets:   make(map[uint64]*atomic.Uint64),
		durableOffsets: make(map[uint64]uint64),
		groups:         make(map[uint64][]*ConsumerGroup),
		nackCounters:   make(map[nackKey]int8),
		metrics:        m,
		queueSize:      defaultQueueSize,
		policy:         PolicyDropOldest,
		batchSize:      defaultBatchSize,
		flushInterval:  defaultFlushInterval,
		maxRetries:     defaultMaxRetries,
	}
	for _, opt := range opts {
		opt(r)
	}
	r.queuePool.New = func() any {
		ch := make(chan *MessageRef, r.queueSize)
		return ch
	}
	return r
}

func (r *Router) getQueueFromPool() chan *MessageRef {
	return r.queuePool.Get().(chan *MessageRef)
}

func (r *Router) returnQueueToPool(ch chan *MessageRef) {
	if ch == nil {
		return
	}
	for {
		select {
		case msgRef, ok := <-ch:
			if ok && msgRef != nil {
				msgRef.Release()
			}
		default:
			goto DRAINED
		}
	}
DRAINED:
	r.queuePool.Put(ch)
}

// Subscribe registers a QUIC stream as a subscriber for the topic parsed from
// frame.Payload and spawns a dedicated Writer goroutine. Expected payload format: "topic:<name>[:durable:<consumerID>:<offset>]".
func (r *Router) Subscribe(ctx context.Context, stream *quic.Stream, frame protocol.Frame) error {
	if stream == nil {
		return errors.New("nil stream")
	}
	spec, err := parseSubscriptionPayload(frame.Payload)
	if err != nil {
		return err
	}

	nackCh := make(chan uint64, 8)
	notifyCh := make(chan struct{}, 1)
	subCtx, cancel := context.WithCancel(ctx)
	topicHash := authz.CombineHashStrings("topic", spec.Topic)

	subQueues := &[4]chan *MessageRef{}
	subMu := &sync.RWMutex{}

	r.mu.Lock()
	idx := len(r.streamIDs)
	r.streamIDs = append(r.streamIDs, frame.StreamID)
	r.streams = append(r.streams, stream)
	r.topics = append(r.topics, spec.Topic)
	r.active = append(r.active, true)
	r.queues = append(r.queues, subQueues)
	r.notifyChs = append(r.notifyChs, notifyCh)
	r.subMus = append(r.subMus, subMu)
	r.nackChs = append(r.nackChs, nackCh)
	r.cancels = append(r.cancels, cancel)
	r.subGroups = append(r.subGroups, spec.GroupID)

	if spec.GroupID != "" {
		var cg *ConsumerGroup
		for _, g := range r.groups[topicHash] {
			if g.groupID == spec.GroupID {
				cg = g
				break
			}
		}
		if cg == nil {
			cg = &ConsumerGroup{
				groupID: spec.GroupID,
				topic:   spec.Topic,
			}
			r.groups[topicHash] = append(r.groups[topicHash], cg)
		}
		r.rebuildGroupMembersLocked(cg)

		groupDurableKey := authz.CombineHashStrings(spec.GroupID, spec.Topic)
		if spec.IsDurable || spec.RequestedOffset > 0 {
			if spec.RequestedOffset == 0 {
				spec.RequestedOffset = r.durableOffsets[groupDurableKey]
			} else {
				r.durableOffsets[groupDurableKey] = spec.RequestedOffset
			}
			metrics.ConsumerOffset.WithLabelValues(spec.GroupID, spec.Topic).Set(float64(r.durableOffsets[groupDurableKey]))
		}
	} else {
		r.topicIndex[topicHash] = append(r.topicIndex[topicHash], idx)
	}

	if IsWildcardTopic(spec.Topic) {
		r.wildcardSubs = append(r.wildcardSubs, WildcardSub{
			pattern: []byte(spec.Topic),
			idx:     idx,
		})
	}

	if spec.IsDurable && spec.GroupID == "" {
		durableKey := authz.CombineHashStrings(spec.ConsumerID, spec.Topic)
		r.durableOffsets[durableKey] = spec.RequestedOffset
		metrics.DurableSubscribersActive.Inc()
		metrics.ConsumerOffset.WithLabelValues(spec.ConsumerID, spec.Topic).Set(float64(spec.RequestedOffset))
	}

	if r.metrics != nil {
		r.metrics.SetActiveSubscribers(float64(r.countActiveLocked()))
	}
	r.mu.Unlock()

	// Check if backfill replay is required for reconnecting durable subscriber
	latestOffset := r.GetTopicOffset(spec.Topic)
	if spec.IsDurable && spec.RequestedOffset < latestOffset && r.aalPath != "" {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.runBackfillWorker(subCtx, spec.Topic, spec.ConsumerID, spec.RequestedOffset, latestOffset, idx)
		}()
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.runSubscriberWriter(subCtx, stream, spec.Topic, subMu, subQueues, notifyCh, nackCh)
	}()

	return nil
}

// TopicOfStream returns the topic name for a subscriber stream, or false if not found.
func (r *Router) TopicOfStream(streamID uint32) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for i, sid := range r.streamIDs {
		if sid == streamID && r.active[i] {
			return r.topics[i], true
		}
	}
	return "", false
}

// NackByStream routes a NACK offset to the subscriber's writer goroutine.
// Uses non-blocking send — drops if the nack channel is full (should not happen in practice).
func (r *Router) NackByStream(streamID uint32, offset uint64) {
	r.mu.RLock()
	for i, sid := range r.streamIDs {
		if sid == streamID && r.active[i] && i < len(r.nackChs) {
			ch := r.nackChs[i]
			r.mu.RUnlock()
			select {
			case ch <- offset:
			default:
			}
			return
		}
	}
	r.mu.RUnlock()
}

// rebuildGroupMembersLocked updates the active subscriber index snapshot for a ConsumerGroup.
// Must be called with r.mu held.
func (r *Router) rebuildGroupMembersLocked(cg *ConsumerGroup) {
	var members []int
	for i, groupID := range r.subGroups {
		if groupID == cg.groupID && i < len(r.active) && r.active[i] && r.topics[i] == cg.topic {
			members = append(members, i)
		}
	}
	cg.members.Store(&members)
}

// AckOffset updates the acknowledged consumer offset for a durable subscriber or consumer group.
func (r *Router) AckOffset(consumerID, topic string, offset uint64) {
	durableKey := authz.CombineHashStrings(consumerID, topic)
	r.mu.Lock()
	r.durableOffsets[durableKey] = offset
	for i, groupID := range r.subGroups {
		if groupID != "" && r.topics[i] == topic {
			groupKey := authz.CombineHashStrings(groupID, topic)
			if groupID == consumerID || (r.active[i] && r.subGroups[i] == consumerID) {
				r.durableOffsets[groupKey] = offset
				metrics.ConsumerOffset.WithLabelValues(groupID, topic).Set(float64(offset))
			}
		}
	}
	r.mu.Unlock()

	metrics.ConsumerOffset.WithLabelValues(consumerID, topic).Set(float64(offset))
}

// GetGroupOffset returns the acknowledged offset for a consumer group on topic.
func (r *Router) GetGroupOffset(groupID, topic string) uint64 {
	return r.GetConsumerOffset(groupID, topic)
}

// SetGroupOffset explicitly sets the group offset for groupID on topic.
func (r *Router) SetGroupOffset(groupID, topic string, offset uint64) {
	r.AckOffset(groupID, topic, offset)
}

// GetTopicOffset returns the current monotonic offset for topic.
func (r *Router) GetTopicOffset(topic string) uint64 {
	tHash := authz.CombineHashStrings("topic", topic)
	r.mu.RLock()
	counter, exists := r.topicOffsets[tHash]
	r.mu.RUnlock()
	if !exists || counter == nil {
		return 0
	}
	return counter.Load()
}

// GetConsumerOffset returns the acknowledged offset for consumerID on topic.
func (r *Router) GetConsumerOffset(consumerID, topic string) uint64 {
	durableKey := authz.CombineHashStrings(consumerID, topic)
	r.mu.RLock()
	offset := r.durableOffsets[durableKey]
	r.mu.RUnlock()
	return offset
}

// runBackfillWorker streams historical AAL records from disk into the subscriber queue.
func (r *Router) runBackfillWorker(ctx context.Context, topic, consumerID string, startOffset, endOffset uint64, idx int) {
	if r.aalPath == "" {
		return
	}
	var currentMsgOffset uint64 = 0
	_, _ = aal.Replay(r.aalPath, r.aalKey, func(frameBytes []byte) error {
		select {
		case <-ctx.Done():
			return errors.New("backfill cancelled")
		default:
		}
		frame, parseErr := protocol.ParseFrame(frameBytes)
		if parseErr != nil {
			return nil
		}
		if frame.Command == protocol.CmdPublish {
			tExp, cleanPayload := parseTTL(frame.Payload)
			if string(cleanPayload) == topic {
				currentMsgOffset++
				if currentMsgOffset > startOffset && currentMsgOffset <= endOffset {
					buf := protocol.SerializeFrame(protocol.CmdPublish, 0, cleanPayload)
					msgRef := AcquireMessageRef(buf)
					msgRef.SetExpiresAt(tExp)
					msgRef.SetOffset(currentMsgOffset)
					msgRef.Retain()
					dc := r.enqueueToSubscriber(idx, msgRef, protocol.PriorityNormal)
					if dc >= 0 {
						r.disconnectSubscriber(dc)
					}
					metrics.AALBackfillFrames.Inc()
				}
			}
		}
		return nil
	})
}

// batchAccumulator coalesces small messages into larger writes for QUIC efficiency.
// It accumulates frames until either the buffer reaches maxSize or the flush timer fires.
// Timer lifecycle: stopped after flush, Reset() on first message arrival.
// All operations are zero-allocation after initial construction.
type batchAccumulator struct {
	maxSize int
	buf     []byte
	msgRefs []*MessageRef // pending MessageRefs, released atomically on flush
	timer   *time.Timer
}

func newBatchAccumulator(maxSize int, flushInterval time.Duration) *batchAccumulator {
	acc := &batchAccumulator{
		maxSize: maxSize,
		buf:     make([]byte, 0, maxSize),
		msgRefs: make([]*MessageRef, 0, 32),
		timer:   time.NewTimer(flushInterval),
	}
	// Stop timer immediately; it will be Reset() on first message.
	if !acc.timer.Stop() {
		<-acc.timer.C
	}
	return acc
}

// flush writes all accumulated messages to the stream and releases their refs.
// On write error, it releases all pending refs and returns the error.
// After flush, the accumulator is ready for reuse (zero allocation reset).
func (acc *batchAccumulator) flush(stream *quic.Stream) error {
	if len(acc.buf) == 0 {
		return nil
	}

	buf := acc.buf
	refs := acc.msgRefs

	// Reset accumulator slices for reuse (zero allocation).
	acc.buf = acc.buf[:0]
	acc.msgRefs = acc.msgRefs[:0]

	if _, err := stream.Write(buf); err != nil {
		for _, m := range refs {
			m.Release()
		}
		return err
	}
	for _, m := range refs {
		m.Release()
	}
	return nil
}

// enqueueToSubscriber lazily fetches or acquires a channel for priority level p,
// enqueues msgRef, signals notifyCh, and handles backpressure overflow for level p.
func (r *Router) enqueueToSubscriber(idx int, msgRef *MessageRef, p uint8) int {
	if p > protocol.PriorityLow {
		p = protocol.DefaultPriority
	}

	r.mu.RLock()
	if idx >= len(r.subMus) || !r.active[idx] {
		r.mu.RUnlock()
		return -1
	}
	subMu := r.subMus[idx]
	subQueues := r.queues[idx]
	notifyCh := r.notifyChs[idx]
	topicName := r.topics[idx]
	r.mu.RUnlock()

	subMu.RLock()
	q := subQueues[p]
	if q == nil {
		subMu.RUnlock()
		subMu.Lock()
		q = subQueues[p]
		if q == nil {
			q = r.getQueueFromPool()
			subQueues[p] = q
		}
		subMu.Unlock()
		subMu.RLock()
	}

	dc := -1
	select {
	case q <- msgRef:
	default:
		dc = r.handleOverflow(idx, topicName, q, msgRef)
	}
	subMu.RUnlock()

	select {
	case notifyCh <- struct{}{}:
	default:
	}

	return dc
}

// fetchNextMessage polls priority queues in strict priority order (0 -> 1 -> 2 -> 3).
// If a message is dequeued from a queue that becomes empty, it triggers cleanup.
func (r *Router) fetchNextMessage(subMu *sync.RWMutex, subQueues *[4]chan *MessageRef) (*MessageRef, uint8) {
	for p := 0; p < 4; p++ {
		subMu.RLock()
		q := subQueues[p]
		if q == nil {
			subMu.RUnlock()
			continue
		}

		select {
		case msgRef, ok := <-q:
			if !ok || msgRef == nil {
				subMu.RUnlock()
				continue
			}

			if len(q) == 0 {
				subMu.RUnlock()
				r.cleanupEmptyQueue(subMu, subQueues, p, q)
			} else {
				subMu.RUnlock()
			}
			return msgRef, uint8(p)
		default:
			subMu.RUnlock()
		}
	}
	return nil, 0
}

func (r *Router) cleanupEmptyQueue(subMu *sync.RWMutex, subQueues *[4]chan *MessageRef, p int, q chan *MessageRef) {
	subMu.Lock()
	defer subMu.Unlock()

	if subQueues[p] == q && len(q) == 0 {
		subQueues[p] = nil
		r.returnQueueToPool(q)
	}
}

func (r *Router) drainSubscriberQueues(subMu *sync.RWMutex, subQueues *[4]chan *MessageRef) {
	subMu.Lock()
	defer subMu.Unlock()

	for p := 0; p < 4; p++ {
		q := subQueues[p]
		if q != nil {
			subQueues[p] = nil
			r.returnQueueToPool(q)
		}
	}
}

// runSubscriberWriter drains the subscriber's priority queues, checks TTL expiration lazily,
// coalesces small messages into batched writes, and releases MessageRef counts upon delivery.
func (r *Router) runSubscriberWriter(ctx context.Context, stream *quic.Stream, topic string, subMu *sync.RWMutex, subQueues *[4]chan *MessageRef, notifyCh chan struct{}, nackCh chan uint64) {
	acc := newBatchAccumulator(r.batchSize, r.flushInterval)
	defer func() {
		_ = acc.flush(stream)
		if !acc.timer.Stop() {
			select {
			case <-acc.timer.C:
			default:
			}
		}
		r.drainSubscriberQueues(subMu, subQueues)
	}()

	// Bounded frame cache for NACK replay: offset → topic name.
	frameCache := make(map[uint64]string)
	frameCacheKeys := make([]uint64, 0, defaultNackCacheSize)

	storeFrame := func(offset uint64, topicName string) {
		if _, exists := frameCache[offset]; exists {
			return
		}
		if len(frameCacheKeys) >= defaultNackCacheSize {
			oldKey := frameCacheKeys[0]
			delete(frameCache, oldKey)
			frameCacheKeys = frameCacheKeys[1:]
		}
		frameCache[offset] = topicName
		frameCacheKeys = append(frameCacheKeys, offset)
	}

	handleNack := func(nackOffset uint64) {
		topicName, ok := frameCache[nackOffset]
		if !ok {
			return
		}
		topicHash := authz.CombineHashStrings("topic", topicName)
		key := nackKey{topicHash: topicHash, offset: nackOffset}

		r.nackMu.Lock()
		count := r.nackCounters[key]
		count++
		r.nackCounters[key] = count
		r.nackMu.Unlock()

		metrics.MessagesNacked.WithLabelValues(topicName).Inc()

		if int(count) < r.maxRetries {
			_ = r.Publish(ctx, protocol.Frame{
				Command:    protocol.CmdPublish,
				PayloadLen: uint32(len(topicName)),
				Payload:    []byte(topicName),
			})
			return
		}

		dlqTopic := "__dlq__" + topicName
		metrics.MessagesDeadLettered.WithLabelValues(topicName).Inc()
		r.nackMu.Lock()
		delete(r.nackCounters, key)
		r.nackMu.Unlock()

		_ = r.Publish(ctx, protocol.Frame{
			Command:    protocol.CmdPublish,
			PayloadLen: uint32(len(dlqTopic)),
			Payload:    []byte(dlqTopic),
		})
	}

	for {
		msgRef, pLevel := r.fetchNextMessage(subMu, subQueues)
		if msgRef != nil {
			nowNano := time.Now().UnixNano()
			if msgRef.IsExpired(nowNano) {
				msgRef.Release()
				pStr := strconv.Itoa(int(pLevel))
				metrics.MessagesExpired.WithLabelValues(topic, pStr).Inc()
				continue
			}

			buf := msgRef.Buf()
			if len(buf) == 0 {
				msgRef.Release()
				continue
			}

			storeFrame(msgRef.Offset(), topic)

			if len(acc.buf) > 0 && len(acc.buf)+len(buf) > acc.maxSize {
				if err := acc.flush(stream); err != nil {
					msgRef.Release()
					return
				}
			}

			firstMsg := len(acc.buf) == 0
			acc.buf = append(acc.buf, buf...)
			acc.msgRefs = append(acc.msgRefs, msgRef)

			if firstMsg {
				acc.timer.Reset(r.flushInterval)
			}

			if len(acc.buf) >= acc.maxSize {
				if err := acc.flush(stream); err != nil {
					return
				}
			}

			if r.metrics != nil {
				r.metrics.OnDeliver(topic)
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-acc.timer.C:
			if err := acc.flush(stream); err != nil {
				return
			}
		case nackOffset := <-nackCh:
			handleNack(nackOffset)
		case <-notifyCh:
		}
	}
}

// Publish non-blockingly dispatches a message to all matching subscriber queues (exact & wildcard).
// Operates with 0 heap allocations and nano-second publisher latency.
func (r *Router) Publish(ctx context.Context, frame protocol.Frame) error {
	return r.publishWithClientID(ctx, frame, "")
}

// QuotaManager returns the quota manager associated with the router.
func (r *Router) QuotaManager() *quotas.Manager {
	return r.quotaManager
}

// PublishWithClientID routes a published frame and checks rate limits for the given clientID.
func (r *Router) PublishWithClientID(ctx context.Context, frame protocol.Frame, clientID string) error {
	return r.publishWithClientID(ctx, frame, clientID)
}

func (r *Router) publishWithClientID(ctx context.Context, frame protocol.Frame, clientID string) error {
	if frame.PayloadLen > maxPayloadSize {
		return errors.New("payload exceeds maximum frame size")
	}

	// Add topic attribute to the active tracing span (if any)
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.String("messaging.destination", string(frame.Payload)))
	}

	if r.quotaManager != nil && !r.quotaManager.TryAcquire(clientID) {
		if r.metrics != nil {
			r.metrics.OnRateLimited(clientID)
		}
		return nil
	}

	pLevel := protocol.DefaultPriority
	if frame.HasExtensions() {
		if p, ok := protocol.ExtractPriority(frame.Extensions); ok {
			pLevel = p
		}
	}

	var expiresAt int64
	var cleanPayload []byte
	if r.priorityTTLs[pLevel] > 0 {
		_, cleanPayload = parseTTL(frame.Payload)
		expiresAt = time.Now().UnixNano() + r.priorityTTLs[pLevel].Nanoseconds()
	} else {
		expiresAt, cleanPayload = parseTTL(frame.Payload)
	}

	topicHash := authz.CombineHashes("topic", cleanPayload)

	r.mu.RLock()
	counter, exists := r.topicOffsets[topicHash]
	if !exists {
		r.mu.RUnlock()
		r.mu.Lock()
		counter, exists = r.topicOffsets[topicHash]
		if !exists {
			counter = &atomic.Uint64{}
			r.topicOffsets[topicHash] = counter
		}
		r.mu.Unlock()
		r.mu.RLock()
	}
	r.mu.RUnlock()

	msgOffset := counter.Add(1)

	r.mu.RLock()
	indices := r.topicIndex[topicHash]
	hasWildcards := len(r.wildcardSubs) > 0
	hasPeers := r.peerForwarder != nil && r.peerForwarder.ActivePeers() > 0
	hasGroups := len(r.groups[topicHash]) > 0

	// Early return only if no local subscribers, no groups, AND no peers to forward to.
	if len(indices) == 0 && !hasWildcards && !hasPeers && !hasGroups {
		r.mu.RUnlock()
		return nil
	}

	if r.metrics != nil {
		r.metrics.OnPublish(string(cleanPayload))
	}

	// Preserve TLV extensions in the serialized frame for subscriber delivery
	// and peer forwarding. The original frame byte slice is used when HasExtensions
	// is true; otherwise, we create a new frame from the clean payload.
	var buf *[]byte
	if frame.HasExtensions() {
		buf = protocol.SerializeFrameWithExtensions(protocol.CmdPublish, 0, frame.Extensions, cleanPayload)
	} else {
		buf = protocol.SerializeFrame(protocol.CmdPublish, 0, cleanPayload)
	}
	msgRef := AcquireMessageRef(buf)
	msgRef.SetExpiresAt(expiresAt)
	msgRef.SetOffset(msgOffset)

	// 1. Forward to peer nodes zero-copy (before local dispatch).
	//    Must happen before Release so buf is still live.
	if hasPeers {
		r.peerForwarder.Forward(*buf, true)
	}

	var inlineDC [4]int
	disconnectIndices := inlineDC[:0]

	// 2. Dispatch to exact topic match subscribers
	for _, idx := range indices {
		if idx < len(r.active) && r.active[idx] {
			msgRef.Retain()
			dc := r.enqueueToSubscriber(idx, msgRef, pLevel)
			if dc >= 0 {
				disconnectIndices = append(disconnectIndices, dc)
			}
		}
	}

	// 3. Dispatch to matching wildcard subscribers (+ and #)
	if hasWildcards {
		for _, wSub := range r.wildcardSubs {
			idx := wSub.idx
			if idx < len(r.active) && r.active[idx] {
				if MatchWildcard(wSub.pattern, cleanPayload) {
					msgRef.Retain()
					dc := r.enqueueToSubscriber(idx, msgRef, pLevel)
					if dc >= 0 {
						disconnectIndices = append(disconnectIndices, dc)
					}
				}
			}
		}
	}

	// 4. Dispatch to Consumer Groups (Lock-Free Atomic Round-Robin per group)
	if hasGroups {
		for _, g := range r.groups[topicHash] {
			membersPtr := g.members.Load()
			if membersPtr != nil && len(*membersPtr) > 0 {
				members := *membersPtr
				n := uint64(len(members))
				val := g.counter.Add(1)
				chosenIdx := members[(val-1)%n]
				if chosenIdx < len(r.active) && r.active[chosenIdx] {
					msgRef.Retain()
					dc := r.enqueueToSubscriber(chosenIdx, msgRef, pLevel)
					if dc >= 0 {
						disconnectIndices = append(disconnectIndices, dc)
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

// PublishFromPeer routes a frame received from a peer node to local subscribers only.
// It NEVER re-forwards to peers, preventing mesh broadcast storms.
func (r *Router) PublishFromPeer(ctx context.Context, frame protocol.Frame) error {
	metrics.ClusterFramesReceived.Inc()
	// Strip the MeshForwarded bit to get the real opcode; then publish locally.
	frame.Command = protocol.OpcodeOf(frame.Command)
	return r.publishLocal(ctx, frame)
}

// publishLocal is Publish without peer forwarding — used both by Publish (after
// forwarding externally) and by PublishFromPeer.
func (r *Router) publishLocal(ctx context.Context, frame protocol.Frame) error {
	if frame.PayloadLen > maxPayloadSize {
		return errors.New("payload exceeds maximum frame size")
	}

	pLevel := protocol.DefaultPriority
	if frame.HasExtensions() {
		if p, ok := protocol.ExtractPriority(frame.Extensions); ok {
			pLevel = p
		}
	}

	var expiresAt int64
	var cleanPayload []byte
	if r.priorityTTLs[pLevel] > 0 {
		_, cleanPayload = parseTTL(frame.Payload)
		expiresAt = time.Now().UnixNano() + r.priorityTTLs[pLevel].Nanoseconds()
	} else {
		expiresAt, cleanPayload = parseTTL(frame.Payload)
	}

	topicHash := authz.CombineHashes("topic", cleanPayload)

	r.mu.RLock()
	counter, exists := r.topicOffsets[topicHash]
	if !exists {
		r.mu.RUnlock()
		r.mu.Lock()
		counter, exists = r.topicOffsets[topicHash]
		if !exists {
			counter = &atomic.Uint64{}
			r.topicOffsets[topicHash] = counter
		}
		r.mu.Unlock()
		r.mu.RLock()
	}
	r.mu.RUnlock()

	msgOffset := counter.Add(1)

	r.mu.RLock()
	indices := r.topicIndex[topicHash]
	hasWildcards := len(r.wildcardSubs) > 0
	hasGroups := len(r.groups[topicHash]) > 0

	if len(indices) == 0 && !hasWildcards && !hasGroups {
		r.mu.RUnlock()
		return nil
	}

	if r.metrics != nil {
		r.metrics.OnPublish(string(cleanPayload))
	}

	var buf *[]byte
	if frame.HasExtensions() {
		buf = protocol.SerializeFrameWithExtensions(protocol.CmdPublish, 0, frame.Extensions, cleanPayload)
	} else {
		buf = protocol.SerializeFrame(protocol.CmdPublish, 0, cleanPayload)
	}
	msgRef := AcquireMessageRef(buf)
	msgRef.SetExpiresAt(expiresAt)
	msgRef.SetOffset(msgOffset)

	var inlineDC [4]int
	disconnectIndices := inlineDC[:0]

	for _, idx := range indices {
		if idx < len(r.active) && r.active[idx] {
			msgRef.Retain()
			dc := r.enqueueToSubscriber(idx, msgRef, pLevel)
			if dc >= 0 {
				disconnectIndices = append(disconnectIndices, dc)
			}
		}
	}

	if hasWildcards {
		for _, wSub := range r.wildcardSubs {
			idx := wSub.idx
			if idx < len(r.active) && r.active[idx] {
				if MatchWildcard(wSub.pattern, cleanPayload) {
					msgRef.Retain()
					dc := r.enqueueToSubscriber(idx, msgRef, pLevel)
					if dc >= 0 {
						disconnectIndices = append(disconnectIndices, dc)
					}
				}
			}
		}
	}

	if hasGroups {
		for _, g := range r.groups[topicHash] {
			membersPtr := g.members.Load()
			if membersPtr != nil && len(*membersPtr) > 0 {
				members := *membersPtr
				n := uint64(len(members))
				val := g.counter.Add(1)
				chosenIdx := members[(val-1)%n]
				if chosenIdx < len(r.active) && r.active[chosenIdx] {
					msgRef.Retain()
					dc := r.enqueueToSubscriber(chosenIdx, msgRef, pLevel)
					if dc >= 0 {
						disconnectIndices = append(disconnectIndices, dc)
					}
				}
			}
		}
	}
	r.mu.RUnlock()

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

// PublishBatch unpacks a CmdPublishBatch frame and publishes each sub-message individually.
// Uses nested reference counting: creates a parent MessageRef for the batch buffer,
// then child MessageRefs for each sub-frame pointing into the parent's buffer (zero-copy).
//
// Each sub-frame is a standard frame (Magic, Cmd, StreamID, Len, Payload) whose Payload
// contains the topic for routing. Sub-frames are NOT copied — they are zero-copy sub-slices
// of the parent batch buffer.
//
// If compression is enabled (via WithCompression) and the batch payload exceeds the minimum
// size threshold, the peer-forwarded copy is compressed with ZSTD and tagged with a Compression
// TLV extension. Local subscribers always receive the uncompressed payload.
func (r *Router) PublishBatch(ctx context.Context, frame protocol.Frame) error {
	if frame.PayloadLen > maxPayloadSize {
		return errors.New("batch payload exceeds maximum frame size")
	}

	// Serialize the batch frame into a pooled buffer (parent buffer for all sub-frames).
	// Preserve extensions so batch-forwarded frames carry TLV context to peers.
	var batchBuf *[]byte
	if frame.HasExtensions() {
		batchBuf = protocol.SerializeFrameWithExtensions(protocol.CmdPublishBatch, 0, frame.Extensions, frame.Payload)
	} else {
		batchBuf = protocol.SerializeFrame(protocol.CmdPublishBatch, 0, frame.Payload)
	}
	batchMsgRef := AcquireMessageRef(batchBuf)

	hasPeers := r.peerForwarder != nil && r.peerForwarder.ActivePeers() > 0

	r.mu.RLock()
	var inlineDC [4]int
	disconnectIndices := inlineDC[:0]

	// Parse batch: iterate through sub-frames, find subscribers, and dispatch.
	_ = protocol.ParseBatch(frame.Payload, func(subFrame []byte) error {
		sub, subErr := protocol.ParseFrame(subFrame)
		if subErr != nil {
			return nil // skip malformed sub-frames
		}
		_, subTopic := parseTTL(sub.Payload)
		subTopicHash := authz.CombineHashes("topic", subTopic)

		subIndices := r.topicIndex[subTopicHash]
		subHasWildcards := false
		for _, wSub := range r.wildcardSubs {
			if MatchWildcard(wSub.pattern, subTopic) {
				subHasWildcards = true
				break
			}
		}
		subHasGroups := len(r.groups[subTopicHash]) > 0
		if len(subIndices) == 0 && !subHasWildcards && !subHasGroups {
			return nil
		}

		pLevel := protocol.DefaultPriority
		if sub.HasExtensions() {
			if p, ok := protocol.ExtractPriority(sub.Extensions); ok {
				pLevel = p
			}
		}

		child := AcquireChildMessageRef(batchMsgRef, subFrame, 0, 0)

		for _, idx := range subIndices {
			if idx < len(r.active) && r.active[idx] {
				child.Retain()
				dc := r.enqueueToSubscriber(idx, child, pLevel)
				if dc >= 0 {
					disconnectIndices = append(disconnectIndices, dc)
				}
			}
		}
		for _, wSub := range r.wildcardSubs {
			idx := wSub.idx
			if idx < len(r.active) && r.active[idx] {
				if MatchWildcard(wSub.pattern, subTopic) {
					child.Retain()
					dc := r.enqueueToSubscriber(idx, child, pLevel)
					if dc >= 0 {
						disconnectIndices = append(disconnectIndices, dc)
					}
				}
			}
		}
		if subHasGroups {
			for _, g := range r.groups[subTopicHash] {
				membersPtr := g.members.Load()
				if membersPtr != nil && len(*membersPtr) > 0 {
					members := *membersPtr
					n := uint64(len(members))
					val := g.counter.Add(1)
					chosenIdx := members[(val-1)%n]
					if chosenIdx < len(r.active) && r.active[chosenIdx] {
						child.Retain()
						dc := r.enqueueToSubscriber(chosenIdx, child, pLevel)
						if dc >= 0 {
							disconnectIndices = append(disconnectIndices, dc)
						}
					}
				}
			}
		}
		child.Release()
		return nil
	})
	r.mu.RUnlock()

	if len(disconnectIndices) > 0 {
		for _, idx := range disconnectIndices {
			r.disconnectSubscriber(idx)
		}
	}

	// If compression is enabled and the batch is large enough, compress the payload
	// for peer forwarding. Local subscribers already received uncompressed data above.
	var forwardBuf *[]byte
	if hasPeers && r.compression != nil && int(frame.PayloadLen) >= r.compressionMinSize {
		compressed, err := r.compression.Compress(frame.Payload)
		if err == nil {
			mergedExt := protocol.BuildMergedExtensionsWithCompression(frame.Extensions, frame.PayloadLen)
			forwardBuf = protocol.SerializeFrameWithExtensions(protocol.CmdPublishBatch, 0, mergedExt, compressed)
			protocol.ReleaseExtensions(mergedExt)
			r.compression.ReleaseBuf(compressed)
		}
	}

	if forwardBuf != nil {
		r.peerForwarder.Forward(*forwardBuf, true)
		protocol.ReleaseBuffer(forwardBuf)
	} else if hasPeers {
		r.peerForwarder.Forward(*batchBuf, true)
	}

	batchMsgRef.Release()
	return nil
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
		if idx < len(r.subGroups) && r.subGroups[idx] != "" {
			groupID := r.subGroups[idx]
			topic := r.topics[idx]
			topicHash := authz.CombineHashStrings("topic", topic)
			for _, cg := range r.groups[topicHash] {
				if cg.groupID == groupID {
					r.rebuildGroupMembersLocked(cg)
				}
			}
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
			if i < len(r.subGroups) && r.subGroups[i] != "" {
				groupID := r.subGroups[i]
				topic := r.topics[i]
				topicHash := authz.CombineHashStrings("topic", topic)
				for _, cg := range r.groups[topicHash] {
					if cg.groupID == groupID {
						r.rebuildGroupMembersLocked(cg)
					}
				}
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

func parseSubscriptionPayload(payload []byte) (SubscriptionSpec, error) {
	if len(payload) < 6 || string(payload[:6]) != "topic:" {
		return SubscriptionSpec{}, errTopicRequired
	}
	raw := string(payload[6:])
	if raw == "" {
		return SubscriptionSpec{}, errTopicEmpty
	}

	spec := SubscriptionSpec{}

	if idx := strings.Index(raw, ":group:"); idx != -1 {
		rest := raw[idx+7:]
		groupID := rest
		endIdx := strings.IndexByte(rest, ':')
		if endIdx != -1 {
			groupID = rest[:endIdx]
			raw = raw[:idx] + rest[endIdx:]
		} else {
			raw = raw[:idx]
		}
		spec.GroupID = groupID
	}

	parts := strings.Split(raw, ":durable:")
	if len(parts) == 1 {
		spec.Topic = parts[0]
		return spec, nil
	}

	spec.Topic = parts[0]
	durParts := strings.Split(parts[1], ":")
	if len(durParts) < 2 {
		spec.IsDurable = true
		spec.ConsumerID = parts[1]
		return spec, nil
	}

	spec.ConsumerID = durParts[0]
	offset, _ := strconv.ParseUint(durParts[1], 10, 64)
	spec.IsDurable = true
	spec.RequestedOffset = offset

	return spec, nil
}

func extractTopic(payload []byte) (string, error) {
	spec, err := parseSubscriptionPayload(payload)
	if err != nil {
		return "", err
	}
	return spec.Topic, nil
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
