package authz

import (
	"testing"
)

func TestHashBytesAndString(t *testing.T) {
	str := "orders"
	bytes := []byte(str)

	h1 := HashString(str)
	h2 := HashBytes(bytes)

	if h1 != h2 {
		t.Errorf("HashString(%q) = %d != HashBytes = %d", str, h1, h2)
	}
}

func TestEnginePermissions(t *testing.T) {
	builder := NewBuilder(PermNone)
	builder.Allow("service-a", "orders", PermPublish)
	builder.Allow("service-b", "orders", PermSubscribe)
	builder.Allow("service-c", "orders", PermAll)

	engine := builder.Build()

	clientA := HashString("service-a")
	clientB := HashString("service-b")
	clientC := HashString("service-c")
	clientD := HashString("service-d")

	topicOrders := []byte("orders")
	topicPayments := []byte("payments")

	// Service A tests
	if !engine.Allowed(clientA, topicOrders, PermPublish) {
		t.Error("service-a should be allowed to publish to orders")
	}
	if engine.Allowed(clientA, topicOrders, PermSubscribe) {
		t.Error("service-a should NOT be allowed to subscribe to orders")
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
	if !nilEngine.Allowed(123, []byte("orders"), PermPublish) {
		t.Error("nil engine should allow all actions")
	}
}

func BenchmarkACLCheck(b *testing.B) {
	engine := NewBuilder(PermNone).
		Allow("service-a", "orders", PermPublish).
		Build()

	clientIDHash := HashString("service-a")
	topicBytes := []byte("orders")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = engine.Allowed(clientIDHash, topicBytes, PermPublish)
	}
}
