package transport

import (
	"testing"
	"unsafe"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
)

func TestReadBufPool_GetBuf(t *testing.T) {
	buf := _rp.GetBuf(1024)
	if cap(buf) < 1024 {
		t.Errorf("expected cap >= 1024, got %d", cap(buf))
	}
	if len(buf) != 1024 {
		t.Errorf("expected len 1024, got %d", len(buf))
	}
	_rp.PutBuf(buf)
}

func TestReadBufPool_GetBufGrow(t *testing.T) {
	buf := _rp.GetBuf(2048)
	if cap(buf) < 2048 {
		t.Errorf("expected cap >= 2048, got %d", cap(buf))
	}
	_rp.PutBuf(buf)
}

func TestReadBufPool_PutBufSmall(t *testing.T) {
	// Small buffers should not be returned to pool.
	small := make([]byte, 0, 128)
	_rp.PutBuf(small) // should not panic
}

func TestExtractTopicBytes(t *testing.T) {
	tests := []struct {
		input []byte
		want  string
	}{
		{[]byte("topic:orders"), "orders"},
		{[]byte("topic:"), ""},
		{[]byte("plain"), "plain"},
		{[]byte("topic:prefix:extra"), "prefix:extra"},
	}

	for _, tt := range tests {
		got := extractTopicBytes(tt.input)
		if string(got) != tt.want {
			t.Errorf("extractTopicBytes(%q) = %q, want %q", string(tt.input), string(got), tt.want)
		}
	}
}

func TestParseAckPayload(t *testing.T) {
	consumer, topic, offset, err := parseAckPayload([]byte("topic:orders:consumer:svc-a:offset:42"))
	if err != nil {
		t.Fatalf("parseAckPayload: %v", err)
	}
	if topic != "orders" {
		t.Errorf("expected topic 'orders', got %q", topic)
	}
	if consumer != "svc-a" {
		t.Errorf("expected consumer 'svc-a', got %q", consumer)
	}
	if offset != 42 {
		t.Errorf("expected offset 42, got %d", offset)
	}

	_, _, _, err = parseAckPayload([]byte("invalid"))
	if err == nil {
		t.Error("expected error for invalid ack payload")
	}
}

func TestPayloadLen(t *testing.T) {
	buf := make([]byte, protocol.HeaderSize+10)
	*(*uint32)(unsafe.Pointer(&buf[6])) = 42

	got := protocol.PayloadLen(buf)
	if got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
}
