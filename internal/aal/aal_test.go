package aal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
)

func TestAALOpenClose(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	l, err := Open(logPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := l.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}

	if err := l.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Operations on nil or closed log should handle gracefully
	if err := (*Log)(nil).WriteFrame([]byte("data")); err != nil {
		t.Errorf("nil Log.WriteFrame returned error: %v", err)
	}
	if err := (*Log)(nil).Sync(); err != nil {
		t.Errorf("nil Log.Sync returned error: %v", err)
	}
	if err := (*Log)(nil).Close(); err != nil {
		t.Errorf("nil Log.Close returned error: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("double Close returned error: %v", err)
	}
}

func TestAALWriteAndVerifyFormat(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "aal.log")

	l, err := Open(logPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	framesData := [][]byte{
		[]byte("orders"),
		[]byte("payments"),
		[]byte("notifications"),
	}

	var expectedTotalBytes []byte

	for i, payload := range framesData {
		buf := protocol.SerializeFrame(protocol.CmdPublish, uint32(i+1), payload)
		frameBytes := append([]byte(nil), (*buf)...)
		protocol.ReleaseBuffer(buf)

		expectedTotalBytes = append(expectedTotalBytes, frameBytes...)
		if err := l.WriteFrame(frameBytes); err != nil {
			t.Fatalf("WriteFrame %d failed: %v", i, err)
		}
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read file and compare
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if len(content) != len(expectedTotalBytes) {
		t.Fatalf("file length mismatch: got %d bytes, want %d bytes", len(content), len(expectedTotalBytes))
	}

	if !bytes.Equal(content, expectedTotalBytes) {
		t.Fatal("file content bytes do not match expected binary frame format")
	}

	// Parse frames back to verify protocol compliance
	off := 0
	for i := 0; i < len(framesData); i++ {
		if off >= len(content) {
			t.Fatalf("unexpected end of file at frame %d", i)
		}
		frame, err := protocol.ParseFrame(content[off:])
		if err != nil {
			t.Fatalf("ParseFrame at index %d failed: %v", i, err)
		}
		if frame.Command != protocol.CmdPublish {
			t.Errorf("frame %d: expected CmdPublish, got %v", i, frame.Command)
		}
		if frame.StreamID != uint32(i+1) {
			t.Errorf("frame %d: expected StreamID %d, got %d", i, i+1, frame.StreamID)
		}
		if !bytes.Equal(frame.Payload, framesData[i]) {
			t.Errorf("frame %d: expected payload %q, got %q", i, framesData[i], frame.Payload)
		}
		off += protocol.HeaderSize + int(frame.PayloadLen)
	}
}

func TestAALOpenInvalidPath(t *testing.T) {
	_, err := Open("/invalid_dir/non_existent/aal.log")
	if err == nil {
		t.Error("expected error for invalid path, got nil")
	}
}
