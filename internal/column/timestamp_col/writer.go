package timestampcol

import (
	int64col "columnar/internal/column/int64_col"
	"fmt"
	"time"
)

type Writer struct {
	inner *int64col.Writer
}

func NewWriter(basePath, colName string) (*Writer, error) {
	w, err := int64col.NewWriter(basePath, colName)
	if err != nil {
		return nil, err
	}
	return &Writer{inner: w}, nil
}

func (w *Writer) Write(value any) error {
	switch v := value.(type) {
	case nil:
		return w.inner.Write(nil)
	case time.Time:
		return w.inner.Write(v.UnixNano())
	case int64:
		return w.inner.Write(v)
	default:
		return fmt.Errorf("Timestamp writer expects time.Time, int64, or nil; got %T", value)
	}
}

func (w *Writer) Close() error {
	return w.inner.Close()
}

func (w *Writer) RecordCount() int { return w.inner.RecordCount() }
func (w *Writer) NullCount() int   { return w.inner.NullCount() }
func (w *Writer) Min() int64       { return w.inner.Min() }
func (w *Writer) Max() int64       { return w.inner.Max() }
