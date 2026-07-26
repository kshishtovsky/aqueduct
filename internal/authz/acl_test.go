package authz

import (
	"testing"
)

func TestCombineHashesAndStrings(t *testing.T) {
	client := "service-a"
	topic := "orders"

	h1 := CombineHashStrings(client, topic)
	h2 := CombineHashes(client, []byte(topic))

	if h1 != h2 {
		t.Errorf("CombineHashStrings(%q, %q) = %d != CombineHashes = %d", client, topic, h1, h2)
	}

	// Test non-commutativity (security requirement: key(A,B) != key(B,A))
	hReverse := CombineHashStrings(topic, client)
	if h1 == hReverse {
		t.Errorf("vulnerability detected: CombineHashStrings is commutative! %d == %d", h1, hReverse)
	}
}

func TestEnginePermissions(t *testing.T) {
	builder := NewBuilder(PermNone)
	builder.Allow("service-a", "orders", PermPublish)
	builder.Allow("service-b", "orders", PermSubscribe)
	builder.Allow("service-c", "orders", PermAll)

	engine := builder.Build()

	clientA := "service-a"
	clientB := "service-b"
	clientC := "service-c"
	clientD := "service-d"

	topicOrders := []byte("orders")
	topicPayments := []byte("payments")

	// Service A tests
	if !engine.Allowed(clientA, topicOrders, PermPublish) {
		t.Error("service-a should be allowed to publish to orders")
	}
	if engine.Allowed(clientA, topicOrders, PermSubscribe) {
		t.Error("service-a should NOT be allowed to subscribe to orders")
	}

	// Commutative vulnerability test: client "orders" accessing topic "service-a" MUST be denied
	if engine.Allowed("orders", []byte("service-a"), PermPublish) {
		t.Error("vulnerability: client 'orders' for topic 'service-a' was incorrectly granted permission!")
	}

	// Service B tests
	if engine.Allowed(clientB, topicOrders, PermPublish) {
		t.Error("service-b should NOT be allowed to publish to orders")
	}
	if !engine.Allowed(clientB, topicOrders, PermSubscribe) {
		t.Error("service-b should be allowed to subscribe to orders")
	}

	// Service C tests
	if !engine.Allowed(clientC, topicOrders, PermPublish) || !engine.Allowed(clientC, topicOrders, PermSubscribe) {
		t.Error("service-c should be allowed both publish and subscribe to orders")
	}

	// Service D (unregistered) tests with PermNone default
	if engine.Allowed(clientD, topicOrders, PermPublish) {
		t.Error("unregistered client-d should be denied")
	}

	// Unregistered topic test
	if engine.Allowed(clientA, topicPayments, PermPublish) {
		t.Error("service-a should be denied for unregistered topic payments")
	}
}

func TestNilEngineAllowed(t *testing.T) {
	var nilEngine *Engine
	if !nilEngine.Allowed("client-a", []byte("orders"), PermPublish) {
		t.Error("nil engine should allow all actions")
	}
}

func BenchmarkACLCheck(b *testing.B) {
	engine := NewBuilder(PermNone).
		Allow("service-a", "orders", PermPublish).
		Build()

	clientStr := "service-a"
	topicBytes := []byte("orders")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = engine.Allowed(clientStr, topicBytes, PermPublish)
	}
}
