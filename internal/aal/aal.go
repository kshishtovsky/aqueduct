package aal

import (
	"fmt"
	"os"
	"sync"
)

// Log represents a zero-allocation Append-Only Log writer.
// All published frames are appended directly to disk in raw binary format.
type Log struct {
	mu   sync.Mutex
	file *os.File
}

// Open opens or creates an append-only log file at the given path.
func Open(path string) (*Log, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("aal: open file: %w", err)
	}
	return &Log{file: file}, nil
}

// WriteFrame writes raw binary frame bytes directly to the append-only log file.
// It is safe for concurrent access and performs zero heap allocations.
func (l *Log) WriteFrame(frameBytes []byte) error {
	if l == nil || l.file == nil {
		return nil
	}
	l.mu.Lock()
	_, err := l.file.Write(frameBytes)
	l.mu.Unlock()
	return err
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

// Close syncs and closes the log file.
func (l *Log) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.file.Sync()
	err := l.file.Close()
	l.file = nil
	return err
}
