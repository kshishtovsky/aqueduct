package broker

import (
	"testing"
)

func TestMatchWildcard(t *testing.T) {
	tests := []struct {
		pattern string
		topic   string
		want    bool
	}{
		{"sensor/+/temp", "sensor/room1/temp", true},
		{"sensor/+/temp", "sensor/room2/temp", true},
		{"sensor/+/temp", "sensor/room1/humidity", false},
		{"sensor/+/temp", "sensor/room1/floor2/temp", false},
		{"sensor/#", "sensor/room1/temp", true},
		{"sensor/#", "sensor/room1/temp/humidity", true},
		{"sensor/#", "sensor", true},
		{"#", "any/topic/path", true},
		{"+/+", "room1/temp", true},
		{"+/+", "room1/temp/humidity", false},
		{"exact/topic", "exact/topic", true},
		{"exact/topic", "different/topic", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_vs_"+tt.topic, func(t *testing.T) {
			got := MatchWildcard([]byte(tt.pattern), []byte(tt.topic))
			if got != tt.want {
				t.Errorf("MatchWildcard(%q, %q) = %v, want %v", tt.pattern, tt.topic, got, tt.want)
			}
		})
	}
}

func BenchmarkWildcardMatch(b *testing.B) {
	pattern := []byte("sensor/+/temp/#")
	topic := []byte("sensor/room1/temp/sub1/sub2")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = MatchWildcard(pattern, topic)
	}
}
