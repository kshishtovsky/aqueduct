package tracing

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func newTestTracer() (*Tracer, *tracetest.InMemoryExporter) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	return &Tracer{
		provider: tp,
		tracer:   tp.Tracer("test"),
		logger:   slog.Default(),
	}, exp
}

func TestTracerDisabled(t *testing.T) {
	tracer, err := New(Config{Enabled: false}, slog.Default())
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	if tracer.IsEnabled() {
		t.Fatal("expected tracer to be disabled")
	}

	ctx, end := tracer.StartSpan(context.Background(), "test")
	end()
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	ctx, end = tracer.StartSpanWithTraceContext(context.Background(), "test2", nil, nil, 0)
	end()
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	if err := tracer.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

func TestTracerEnabled(t *testing.T) {
	tracer, exp := newTestTracer()

	if !tracer.IsEnabled() {
		t.Fatal("expected tracer to be enabled")
	}

	ctx, end := tracer.StartSpan(context.Background(), "test-span",
		oteltrace.WithAttributes(attribute.String("key", "value")),
	)
	AddSpanAttributes(ctx, attribute.Int("count", 42))
	end()
	tp := tracer.provider
	_ = tp.ForceFlush(context.Background())

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if spans[0].Name != "test-span" {
		t.Fatalf("span name = %q, want %q", spans[0].Name, "test-span")
	}

	if err := tracer.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

func TestStartSpanWithTraceContext(t *testing.T) {
	tracer, exp := newTestTracer()

	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	for i := range traceID {
		traceID[i] = byte(i)
	}
	for i := range spanID {
		spanID[i] = byte(i + 16)
	}

	_, end := tracer.StartSpanWithTraceContext(context.Background(), "child", traceID, spanID, 1)
	end()
	_ = tracer.provider.ForceFlush(context.Background())

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}

	sc := spans[0].SpanContext
	if sc.TraceID()[0] != 0 || sc.TraceID()[15] != 15 {
		t.Fatal("trace ID mismatch from remote span context")
	}

	if err := tracer.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

func TestStartSpanWithTraceContextInvalid(t *testing.T) {
	tracer, exp := newTestTracer()

	_, end := tracer.StartSpanWithTraceContext(context.Background(), "invalid", nil, nil, 0)
	end()
	_ = tracer.provider.ForceFlush(context.Background())

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if spans[0].Name != "invalid" {
		t.Fatalf("span name = %q, want %q", spans[0].Name, "invalid")
	}

	if err := tracer.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

func TestStartSpanReturnsEndFunc(t *testing.T) {
	tracer, err := New(Config{Enabled: false}, slog.Default())
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	_, end := tracer.StartSpan(context.Background(), "test")
	if end == nil {
		t.Fatal("expected non-nil end function")
	}
	end()

	_, end = tracer.StartSpanWithTraceContext(context.Background(), "test2", nil, nil, 0)
	if end == nil {
		t.Fatal("expected non-nil end function")
	}
	end()
}

func BenchmarkTracerDisabled(b *testing.B) {
	tracer, err := New(Config{Enabled: false}, slog.Default())
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, end := tracer.StartSpan(ctx, "bench")
		end()
	}
}

func BenchmarkTracerDisabledWithTraceContext(b *testing.B) {
	tracer, err := New(Config{Enabled: false}, slog.Default())
	if err != nil {
		b.Fatal(err)
	}

	traceID := make([]byte, 16)
	spanID := make([]byte, 8)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, end := tracer.StartSpanWithTraceContext(ctx, "bench", traceID, spanID, 1)
		end()
	}
}
