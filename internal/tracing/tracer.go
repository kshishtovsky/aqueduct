package tracing

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	// SpanNameProcess is the span name for message processing in the broker.
	SpanNameProcess = "aqueduct.process"
	// SpanNameForward is the span name for cluster forwarding.
	SpanNameForward = "aqueduct.forward"
)

// Tracer is a nil-safe, config-gated wrapper around OpenTelemetry.
// When tracing is disabled, all operations are no-ops with zero overhead.
type Tracer struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	logger   *slog.Logger
}

// Config holds tracing configuration.
type Config struct {
	Enabled     bool   `yaml:"enabled"`
	ServiceName string `yaml:"service_name"`
	Endpoint    string `yaml:"endpoint"`
}

// New creates a Tracer based on config. If tracing is disabled, returns a
// nil-safe no-op tracer. When enabled, it initializes the OTLP exporter.
func New(cfg Config, logger *slog.Logger) (*Tracer, error) {
	if !cfg.Enabled {
		return &Tracer{}, nil
	}

	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "aqueduct-broker"
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.8.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: create resource: %w", err)
	}

	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "localhost:4317"
	}

	exp, err := otlptracegrpc.New(context.Background(),
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: create exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracer := tp.Tracer(serviceName)

	return &Tracer{
		provider: tp,
		tracer:   tracer,
		logger:   logger,
	}, nil
}

// StartSpan starts a new span if tracing is enabled. When disabled, returns
// the context unchanged with a no-op finish function. This is the zero-overhead
// path: the compiler inlines the nil check.
func (t *Tracer) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, func()) {
	if t.tracer == nil {
		// Tracing disabled: return original context and a zero-cost
		// no-op finish callback so callers can defer endSpan() unconditionally
		// without a branch on the hot path.
		return ctx, func() {} //nolint:revive // zero-cost no-op when tracing disabled
	}
	ctx, span := t.tracer.Start(ctx, name, opts...)
	return ctx, func() { span.End() }
}

// StartSpanWithTraceContext starts a span from a W3C Trace Context extracted
// from a TLV extension block. The traceID and spanID are used to create the
// parent context. If tracing is disabled, returns ctx unchanged.
func (t *Tracer) StartSpanWithTraceContext(ctx context.Context, name string, traceID []byte, spanID []byte, traceFlags byte, opts ...trace.SpanStartOption) (context.Context, func()) {
	if t.tracer == nil {
		// Tracing disabled: zero-cost no-op finish callback. Callers can
		// defer endSpan() unconditionally on the hot path.
		return ctx, func() {} //nolint:revive // zero-cost no-op when tracing disabled
	}

	if len(traceID) < 16 || len(spanID) < 8 {
		sc := trace.SpanContext{}
		ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		return t.StartSpan(ctx, name, opts...)
	}

	var tid trace.TraceID
	copy(tid[:], traceID[:16])
	var sid trace.SpanID
	copy(sid[:], spanID[:8])
	traceFlagsOpt := trace.TraceFlags(traceFlags)

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: traceFlagsOpt,
		Remote:     true,
	})
	ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
	return t.StartSpan(ctx, name, opts...)
}

// AddSpanAttributes adds attributes to the current span in ctx.
func AddSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attrs...)
	}
}

// Shutdown flushes and shuts down the tracer provider. Safe to call even when
// tracing is disabled (returns nil immediately).
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

// IsEnabled reports whether tracing is active.
func (t *Tracer) IsEnabled() bool {
	return t.tracer != nil
}
