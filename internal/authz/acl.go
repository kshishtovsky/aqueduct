// Package authz implements a high-performance, zero-allocation ACL engine.
package authz

import "sync/atomic"

// Permission represents a bitmask of allowed actions.
type Permission uint8

const (
	PermNone      Permission = 0
	PermPublish   Permission = 1 << 0 // 0x01
	PermSubscribe Permission = 1 << 1 // 0x02
	PermAll       Permission = PermPublish | PermSubscribe
)

// FNV-1a 64-bit hash constants.
const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// CombineHashes computes a non-commutative 64-bit FNV-1a composite hash of clientID and topic.
// It guarantees zero heap allocations on hot paths and prevents XOR commutativity vulnerabilities
// (e.g. CombineHashes("A", []byte("B")) != CombineHashes("B", []byte("A"))).
func CombineHashes(clientID string, topicBytes []byte) uint64 {
	hash := uint64(offset64)
	for i := 0; i < len(clientID); i++ {
		hash ^= uint64(clientID[i])
		hash *= prime64
	}
	hash ^= uint64(':')
	hash *= prime64
	for i := 0; i < len(topicBytes); i++ {
		hash ^= uint64(topicBytes[i])
		hash *= prime64
	}
	return hash
}

// CombineHashStrings computes a non-commutative 64-bit FNV-1a composite hash of clientID and topic strings.
func CombineHashStrings(clientID, topic string) uint64 {
	hash := uint64(offset64)
	for i := 0; i < len(clientID); i++ {
		hash ^= uint64(clientID[i])
		hash *= prime64
	}
	hash ^= uint64(':')
	hash *= prime64
	for i := 0; i < len(topic); i++ {
		hash ^= uint64(topic[i])
		hash *= prime64
	}
	return hash
}

// Engine provides lock-free, zero-allocation O(1) permission checks with RCU hot-reloading.
type Engine struct {
	rulesPtr    atomic.Pointer[map[uint64]Permission]
	defaultPerm Permission
}

// NewEngine creates an Engine initialized with compiled rules and a default permission.
func NewEngine(rules map[uint64]Permission, defaultPerm Permission) *Engine {
	e := &Engine{
		defaultPerm: defaultPerm,
	}
	if rules == nil {
		rules = make(map[uint64]Permission)
	}
	e.rulesPtr.Store(&rules)
	return e
}

// Allowed performs a zero-allocation O(1) permission check for a clientID and topic.
// It executes lock-free via atomic.Pointer (RCU).
func (e *Engine) Allowed(clientID string, topicBytes []byte, required Permission) bool {
	if e == nil {
		return true
	}
	rulesMap := e.rulesPtr.Load()
	if rulesMap == nil {
		return (e.defaultPerm & required) != 0
	}
	key := CombineHashes(clientID, topicBytes)

	perm, ok := (*rulesMap)[key]
	if !ok {
		return (e.defaultPerm & required) != 0
	}
	return (perm & required) != 0
}

// Reload replaces the rule map atomically using RCU without locking hot paths.
func (e *Engine) Reload(newRules map[uint64]Permission) {
	if e == nil {
		return
	}
	if newRules == nil {
		newRules = make(map[uint64]Permission)
	}
	e.rulesPtr.Store(&newRules)
}

// RulesCount returns the current count of active rules.
func (e *Engine) RulesCount() int {
	if e == nil {
		return 0
	}
	rulesMap := e.rulesPtr.Load()
	if rulesMap == nil {
		return 0
	}
	return len(*rulesMap)
}

// Builder constructs an Engine.
type Builder struct {
	rules       map[uint64]Permission
	defaultPerm Permission
}

// NewBuilder initializes a new ACL Builder.
func NewBuilder(defaultPerm Permission) *Builder {
	return &Builder{
		rules:       make(map[uint64]Permission),
		defaultPerm: defaultPerm,
	}
}

// Allow registers permission for a specific clientID and topic.
func (b *Builder) Allow(clientID, topic string, perm Permission) *Builder {
	key := CombineHashStrings(clientID, topic)
	b.rules[key] |= perm
	return b
}

// Build compiles the rules into an Engine.
func (b *Builder) Build() *Engine {
	return NewEngine(b.rules, b.defaultPerm)
}
