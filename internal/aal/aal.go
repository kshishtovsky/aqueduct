package aal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
// All published frames are appended directly to disk in length-prefixed raw or AES-256-GCM encrypted format.
type Log struct {
	mu     sync.Mutex
	file   *os.File
	aead   cipher.AEAD
	pool   sync.Pool
	prefix [4]byte
	nonce  atomic.Uint64
}

// Open opens or creates an unencrypted append-only log file at the given path.
func Open(path string) (*Log, error) {
	return OpenEncrypted(path, nil)
}

// OpenEncrypted opens or creates an append-only log file with optional AES-256-GCM encryption.
// key must be 32 bytes if provided. If key is nil/empty, encryption is disabled.
//
// The file is created with mode 0600 (G302) because the log may carry sensitive
// publish payloads and, when encryption is enabled, sits next to the AES key
// in the operator's config.
func OpenEncrypted(path string, key []byte) (*Log, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600) // #nosec G304 G302
	if err != nil {
		return nil, fmt.Errorf("aal: open file: %w", err)
	}

	l := &Log{
		file: file,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, 16+65536)
				return &b
			},
		},
	}

	if len(key) > 0 {
		if len(key) != 32 {
			_ = file.Close()
			return nil, ErrInvalidKeySize
		}
		if _, err := rand.Read(l.prefix[:]); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("aal: generate random nonce prefix: %w", err)
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

// WriteFrame writes length-prefixed raw or encrypted frame bytes directly to storage media.
// It is safe for concurrent access and performs zero heap allocations.
func (l *Log) WriteFrame(frameBytes []byte) error {
	if l == nil || l.file == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	bufPtr := l.pool.Get().(*[]byte)
	buf := *bufPtr

	if l.aead != nil {
		// Generate 12-byte cryptographically unique nonce:
		// [4 bytes random session prefix] + [8 bytes strictly monotonic counter]
		nonceVal := l.nonce.Add(1)
		nonce := buf[4:16]
		copy(nonce[:4], l.prefix[:])
		binary.BigEndian.PutUint64(nonce[4:12], nonceVal)

		// Encrypt in-place using pool buffer starting at offset 16
		out := l.aead.Seal(buf[16:16], nonce, frameBytes, nil)
		// #nosec G115 -- out is bounded by maxBufSize (64KB default) so 12+len(out) < 2^32.
		cipherLen := uint32(12 + len(out))

		// 4-byte length prefix + 12-byte nonce + encrypted ciphertext
		binary.LittleEndian.PutUint32(buf[0:4], cipherLen)
		fullLen := 4 + int(cipherLen)

		_, err := l.file.Write(buf[:fullLen])
		l.pool.Put(bufPtr)
		return err
	}

	// Unencrypted mode: 4-byte length prefix + raw frameBytes
	// #nosec G115 -- frameBytes is bounded by maxBufSize (64KB default) so len(frameBytes) < 2^32.
	frameLen := uint32(len(frameBytes))
	binary.LittleEndian.PutUint32(buf[0:4], frameLen)
	copy(buf[4:4+len(frameBytes)], frameBytes)
	fullLen := 4 + int(frameLen)

	_, err := l.file.Write(buf[:fullLen])
	l.pool.Put(bufPtr)
	return err
}

// Replay reads an AAL file sequentially chunk-by-chunk using a pooled buffer,
// decrypting records if key is provided and executing handler for each frame.
//
// Refactored into setup + per-chunk helpers so each function stays well
// under Sonar's 15-cognitive-complexity threshold.
func Replay(path string, key []byte, handler func(frameBytes []byte) error) (int, error) {
	if path == "" {
		return 0, nil
	}
	// #nosec G304 -- path is operator-controlled (config/yaml/CLI flag).
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("aal: open replay file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil || stat.Size() == 0 {
		return 0, nil
	}

	gcm, err := newReplayCipher(key)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 64*1024)
	off := 0
	count := 0

	for {
		n, rerr := f.Read(buf[off:])
		if n > 0 {
			off += n
		}

		consumed, newCount, handlerErr := decodeReplayChunk(buf[:off], gcm, handler, count)
		count = newCount
		if handlerErr != nil {
			return count, handlerErr
		}
		if consumed > 0 {
			copy(buf, buf[consumed:off])
			off -= consumed
		}

		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return count, rerr
		}
	}

	return count, nil
}

// newReplayCipher returns a fresh AES-256-GCM AEAD for the given key, or nil
// when encryption is disabled.
func newReplayCipher(key []byte) (cipher.AEAD, error) {
	if len(key) == 0 {
		return nil, nil
	}
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aal: replay cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("aal: replay gcm: %w", err)
	}
	return gcm, nil
}

// decodeReplayChunk consumes as many complete records as possible from buf,
// invoking handler for each. Returns (consumed, count, handlerErr).
// handlerErr is non-nil only when the handler returned an error — replay
// must then abort.
func decodeReplayChunk(buf []byte, gcm cipher.AEAD, handler func([]byte) error, count int) (int, int, error) {
	consumed := 0
	for consumed < len(buf) {
		remaining := len(buf) - consumed
		if remaining < 4 {
			break
		}
		recLen := int(binary.LittleEndian.Uint32(buf[consumed : consumed+4]))
		if recLen <= 0 || recLen > 10*1024*1024 {
			// Corrupt record length; scan forward to the next plausible
			// record boundary instead of blind 1-byte advance. This avoids
			// processing garbage data as valid frames when corrupted bytes
			// happen to align with a frame structure.
			consumed = resyncToNextRecord(buf, consumed)
			continue
		}
		if remaining < 4+recLen {
			break
		}

		plain, ok, err := decryptReplayRecord(buf[consumed+4:consumed+4+recLen], gcm)
		if err != nil || !ok {
			consumed += 4 + recLen
			continue
		}
		if err := handler(plain); err != nil {
			return consumed, count, err
		}
		count++
		consumed += 4 + recLen
	}
	return consumed, count, nil
}

// resyncToNextRecord scans forward from pos to find the next plausible AAL
// record boundary. It looks for a 4-byte length prefix followed by data that
// starts with the frame magic byte (0x1F) — the first byte of every valid
// Aqueduct frame. This is much safer than blind 1-byte advance because it
// reduces the chance of processing garbage data as a valid frame.
func resyncToNextRecord(buf []byte, pos int) int {
	// Cap scan distance to avoid quadratic blowup on fully corrupted data.
	maxScan := pos + 1024
	if maxScan > len(buf) {
		maxScan = len(buf)
	}
	for i := pos + 1; i+4 < maxScan; i++ {
		recLen := int(binary.LittleEndian.Uint32(buf[i : i+4]))
		if recLen <= 0 || recLen > 10*1024*1024 {
			continue
		}
		// Check if the record body starts with the frame magic byte.
		if i+4+recLen <= len(buf) && buf[i+4] == 0x1F {
			return i
		}
	}
	// No plausible boundary found; skip the corrupted region entirely.
	return maxScan
}

// decryptReplayRecord decrypts one AAL record. Returns (plaintext, ok, err).
// ok=false means the record was malformed and should be skipped silently;
// err!=nil means a handler-level error that should abort replay.
func decryptReplayRecord(recBytes []byte, gcm cipher.AEAD) ([]byte, bool, error) {
	if gcm == nil {
		return recBytes, true, nil
	}
	if len(recBytes) < 12+16 {
		return nil, false, nil
	}
	plain, err := gcm.Open(nil, recBytes[:12], recBytes[12:], nil)
	if err != nil {
		return nil, false, nil
	}
	return plain, true, nil
}

// Rotate rewrites the AAL file if file size exceeds maxSize.
func (l *Log) Rotate(maxSize int64, key []byte) error {
	if l == nil || l.file == nil || maxSize <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	stat, err := l.file.Stat()
	if err != nil || stat.Size() < maxSize {
		return nil
	}

	origPath := l.file.Name()
	tmpPath := origPath + ".rotate.tmp"
	tmpLog, err := OpenEncrypted(tmpPath, key)
	if err != nil {
		return err
	}

	_, err = Replay(origPath, key, func(frameBytes []byte) error {
		return tmpLog.WriteFrame(frameBytes)
	})
	_ = tmpLog.Close()

	if err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	_ = l.file.Close()
	if err := os.Rename(tmpPath, origPath); err != nil {
		return err
	}

	//nolint:gosec // G304 + G302: path is operator-controlled; 0600 matches OpenEncrypted.
	newFile, err := os.OpenFile(origPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600) // #nosec G304 G302
	if err != nil {
		return err
	}
	l.file = newFile
	return nil
}

// DecryptFrame decrypts a length-prefixed raw encrypted log record using AES-256-GCM.
func DecryptFrame(record []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}
	// Record can be [4-byte len prefix] + [12-byte nonce] + [ciphertext] or [12-byte nonce] + [ciphertext]
	if len(record) >= 4+12+16 {
		recLen := int(binary.LittleEndian.Uint32(record[0:4]))
		if recLen+4 == len(record) {
			record = record[4:]
		}
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
