package timestampcol

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriter_WritesUnixMillis(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir, "created_at")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	ts := time.UnixMilli(1700000000123)
	if err := w.Write(ts); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	valuesPath := filepath.Join(dir, "created_at.bin")
	raw, err := os.ReadFile(valuesPath)
	if err != nil {
		t.Fatalf("read values: %v", err)
	}
	if len(raw) != 8 {
		t.Fatalf("values size = %d, want 8", len(raw))
	}

	var got int64
	if err := binary.Read(bytes.NewReader(raw), binary.LittleEndian, &got); err != nil {
		t.Fatalf("binary.Read: %v", err)
	}
	if got != ts.UnixMilli() {
		t.Fatalf("stored = %d, want %d", got, ts.UnixMilli())
	}
}
