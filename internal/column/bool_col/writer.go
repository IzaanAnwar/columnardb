package boolcol

import (
	"fmt"
	"os"
)

type Writer struct {
	f      *os.File
	count  int
	buf    byte
	bitPos uint8 // 0..7
	closed bool
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("Create bool column file: %w", err)
	}

	return &Writer{f: f}, nil
}

func (w *Writer) Write(value any) error {
	if w.closed {
		return fmt.Errorf("write on closed bool writer")
	}

	v, ok := value.(bool)
	if !ok {
		return fmt.Errorf("Bool writer expects bool, got %T", value)
	}

	if v {
		w.buf |= 1 << (7 - w.bitPos)
	}

	w.bitPos++
	w.count++

	if w.bitPos == 8 {
		if _, err := w.f.Write([]byte{w.buf}); err != nil {
			return fmt.Errorf("Write bool byte: %w", err)
		}
		w.buf = 0
		w.bitPos = 0
	}

	return nil

}

func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("int64 writer already closed")
	}
	w.closed = true

	// Flush partial value
	if w.bitPos > 0 {
		if _, err := w.f.Write([]byte{w.buf}); err != nil {
			return fmt.Errorf("Flush bool byte: %w", err)
		}
	}

	return w.f.Close()
}

func (w *Writer) RecordCount() int {
	return w.count
}
