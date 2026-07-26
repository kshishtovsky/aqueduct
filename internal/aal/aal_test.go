package aal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/kshishtovsky/aqueduct/internal/protocol"
)

func TestAALUnencrypted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_raw.log")

	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer l.Close()

	data := []byte("hello raw log")
	if err := l.WriteFrame(data); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}
	_ = l.Sync()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(content[4:], data) {
		t.Errorf("read %q != expected %q", string(content[4:]), string(data))
	}
}

func TestAALEncrypted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_enc.log")
	key := []byte("01234567890123456789012345678901") // 32 bytes

	l, err := OpenEncrypted(path, key)
	if err != nil {
		t.Fatalf("OpenEncrypted failed: %v", err)
	}
	defer l.Close()

	framePayload := []byte("sensitive payload data")
	if err := l.WriteFrame(framePayload); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}
	_ = l.Sync()

	rawFileBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	// Verify file content is encrypted (does NOT match raw payload)
	if bytes.Contains(rawFileBytes, framePayload) {
		t.Error("encrypted file contains raw payload in plain text")
	}

	// Decrypt frame
	decrypted, err := DecryptFrame(rawFileBytes, key)
	if err != nil {
		t.Fatalf("DecryptFrame failed: %v", err)
	}
	if !bytes.Equal(decrypted, framePayload) {
		t.Errorf("decrypted %q != expected %q", string(decrypted), string(framePayload))
	}
}

func TestAALReplayAndRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_replay.log")
	key := []byte("01234567890123456789012345678901")

	l, err := OpenEncrypted(path, key)
	if err != nil {
		t.Fatalf("OpenEncrypted failed: %v", err)
	}

	// Write 100 frames
	for i := 0; i < 100; i++ {
		buf := protocol.SerializeFrame(protocol.CmdPublish, uint32(i), []byte("test-payload"))
		if err := l.WriteFrame(*buf); err != nil {
			t.Fatalf("WriteFrame failed: %v", err)
		}
		protocol.ReleaseBuffer(buf)
	}
	_ = l.Sync()

	// Replay frames
	replayedCount := 0
	readRecords, err := Replay(path, key, func(frameBytes []byte) error {
		frame, parseErr := protocol.ParseFrame(frameBytes)
		if parseErr != nil {
			t.Fatalf("ParseFrame error during replay: %v", parseErr)
		}
		if frame.Command == protocol.CmdPublish && string(frame.Payload) == "test-payload" {
			replayedCount++
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if readRecords != 100 || replayedCount != 100 {
		t.Errorf("expected 100 replayed frames, got %d (readRecords=%d)", replayedCount, readRecords)
	}

	// Test rotation
	if err := l.Rotate(10, key); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	_ = l.Close()
}

func TestAALInvalidKeySize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_badkey.log")
	shortKey := []byte("too_short")

	_, err := OpenEncrypted(path, shortKey)
	if err == nil {
		t.Error("expected error for invalid key size, got nil")
	}

	_, err = DecryptFrame([]byte("123"), shortKey)
	if err == nil {
		t.Error("expected error decrypting with invalid key size, got nil")
	}
}

func TestDecryptFrameShortCiphertext(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	_, err := DecryptFrame([]byte("short"), key)
	if err == nil {
		t.Error("expected ErrShortCiphertext, got nil")
	}
}

func TestNilLogOperations(t *testing.T) {
	var nilLog *Log
	if err := nilLog.WriteFrame([]byte("test")); err != nil {
		t.Errorf("nil log WriteFrame expected nil, got %v", err)
	}
	if err := nilLog.Sync(); err != nil {
		t.Errorf("nil log Sync expected nil, got %v", err)
	}
	if err := nilLog.Close(); err != nil {
		t.Errorf("nil log Close expected nil, got %v", err)
	}
	if err := nilLog.Rotate(100, nil); err != nil {
		t.Errorf("nil log Rotate expected nil, got %v", err)
	}
}

func BenchmarkAALEncryptedWrite(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench_enc.log")
	key := []byte("01234567890123456789012345678901")

	l, err := OpenEncrypted(path, key)
	if err != nil {
		b.Fatalf("OpenEncrypted failed: %v", err)
	}
	defer l.Close()

	frame := []byte("[Header:10B][Payload:128B] benchmarking encrypted AAL write performance")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = l.WriteFrame(frame)
	}
}
