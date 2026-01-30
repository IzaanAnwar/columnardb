package int64col

import (
	"encoding/binary"
	"fmt"
	"os"
)

type Writer struct {
	f        *os.File
	count    int
	min      int64
	max      int64
	hasValue bool
	closed   bool
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("Create int64 column file: %w", err)
	}
	return &Writer{f: f}, nil
}

func (w *Writer) Write(value any) error {
	if w.closed {
		return fmt.Errorf("Write on closed int64 writer")
	}

	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("Int64 writer expects int64, got %T: ", value)
	}

	if err := binary.Write(w.f, binary.LittleEndian, v); err != nil {
		return fmt.Errorf("Write int64 value: %w", err)
	}

	if !w.hasValue {
		w.min, w.max = v, v
		w.hasValue = true
	} else {
		if v > w.max {
			w.max = v
		}
		if v < w.min {
			w.min = v
		}
	}
	w.count++
	return nil

}

func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("int64 writer already closed")
	}
	w.closed = true
	return w.f.Close()
}

func (w *Writer) RecordCount() int {
	return w.count
}

// Exposed for metadata assembly (segment-level)
func (w *Writer) Min() int64 { return w.min }
func (w *Writer) Max() int64 { return w.max }
