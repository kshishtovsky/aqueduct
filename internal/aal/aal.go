package aal

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

var (
	// ErrInvalidKeySize is returned when an AES-256 key is not exactly 32 bytes.
	ErrInvalidKeySize = errors.New("aal: encryption key must be 32 bytes for AES-256")
	// ErrShortCiphertext is returned when attempting to decrypt a truncated log record.
	ErrShortCiphertext = errors.New("aal: ciphertext too short")
)

// Log represents a zero-allocation Append-Only Log writer.
// All published frames are appended directly to disk in raw or AES-256-GCM encrypted format.
type Log struct {
	mu    sync.Mutex
	file  *os.File
	aead  cipher.AEAD
	pool  sync.Pool
	nonce atomic.Uint64
}

// Open opens or creates an unencrypted append-only log file at the given path.
func Open(path string) (*Log, error) {
	return OpenEncrypted(path, nil)
}

// OpenEncrypted opens or creates an append-only log file with optional AES-256-GCM encryption.
// key must be 32 bytes if provided. If key is nil/empty, encryption is disabled.
func OpenEncrypted(path string, key []byte) (*Log, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("aal: open file: %w", err)
	}

	l := &Log{
		file: file,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, 12+65536)
				return &b
			},
		},
	}

	if len(key) > 0 {
		if len(key) != 32 {
			_ = file.Close()
			return nil, ErrInvalidKeySize
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("aal: new cipher: %w", err)
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("aal: new gcm: %w", err)
		}
		l.aead = gcm
	}

	return l, nil
}

// WriteFrame writes raw or encrypted frame bytes directly to storage media.
// It is safe for concurrent access and performs zero heap allocations.
func (l *Log) WriteFrame(frameBytes []byte) error {
	if l == nil || l.file == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.aead != nil {
		bufPtr := l.pool.Get().(*[]byte)
		buf := *bufPtr

		// Generate 12-byte nonce
		nonceVal := l.nonce.Add(1)
		nonce := buf[:12]
		nonce[0] = byte(nonceVal >> 56)
		nonce[1] = byte(nonceVal >> 48)
		nonce[2] = byte(nonceVal >> 40)
		nonce[3] = byte(nonceVal >> 32)
		nonce[4] = byte(nonceVal >> 24)
		nonce[5] = byte(nonceVal >> 16)
		nonce[6] = byte(nonceVal >> 8)
		nonce[7] = byte(nonceVal)
		nonce[8] = 0x41
		nonce[9] = 0x41
		nonce[10] = 0x4c
		nonce[11] = 0x31

		// Encrypt in-place using pool buffer
		out := l.aead.Seal(buf[12:12], nonce, frameBytes, nil)
		fullLen := 12 + len(out)

		_, err := l.file.Write(buf[:fullLen])
		l.pool.Put(bufPtr)
		return err
	}

	_, err := l.file.Write(frameBytes)
	return err
}

// DecryptFrame decrypts a raw encrypted log record (nonce + ciphertext + tag) using AES-256-GCM.
func DecryptFrame(record []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}
	if len(record) < 12+16 {
		return nil, ErrShortCiphertext
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := record[:12]
	ciphertext := record[12:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}

// Close flushes buffers and closes the log file.
func (l *Log) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// Sync flushes kernel buffers to storage media.
func (l *Log) Sync() error {
	if l == nil || l.file == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Sync()
}
