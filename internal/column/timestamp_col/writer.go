package timestampcol

import (
	int64col "columnar/internal/column/int64_col"
	"fmt"
	"time"
)

type Writer struct {
	inner *int64col.Writer
}

func NewWriter(path string) (*Writer, error) {
	w, err := int64col.NewWriter(path)
	if err != nil {
		return nil, err
	}
	return &Writer{inner: w}, nil
}

func (w *Writer) Write(value any) error {
	switch v := value.(type) {
	case time.Time:
		return w.inner.Write(v.UnixNano())
	case int64:
		// Allow explicit epoch values
		return w.inner.Write(v)
	default:
		return fmt.Errorf("Timestamp writer expects time.Time or int64, got %T", value)
	}
}

func (w *Writer) Close() error {
	return w.inner.Close()
}

func (w *Writer) RecordCount() int {
	return w.inner.RecordCount()
}

// Forward stats access for metadata
func (w *Writer) Min() int64 { return w.inner.Min() }
func (w *Writer) Max() int64 { return w.inner.Max() }
