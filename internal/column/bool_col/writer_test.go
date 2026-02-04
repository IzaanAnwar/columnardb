package boolcol

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriter_BitPackingAndNulls(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir, "active")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	values := []any{true, false, nil, true}
	for _, v := range values {
		if err := w.Write(v); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if w.RecordCount() != 4 {
		t.Fatalf("RecordCount = %d, want 4", w.RecordCount())
	}
	if w.NullCount() != 1 {
		t.Fatalf("NullCount = %d, want 1", w.NullCount())
	}

	valuesPath := filepath.Join(dir, "active.bin")
	vb, err := os.ReadFile(valuesPath)
	if err != nil {
		t.Fatalf("read values: %v", err)
	}
	if len(vb) != 1 {
		t.Fatalf("values size = %d, want 1", len(vb))
	}
	if vb[0] != 0b10010000 {
		t.Fatalf("values byte = %08b, want 10010000", vb[0])
	}

	nullsPath := filepath.Join(dir, "active.nulls.bin")
	nb, err := os.ReadFile(nullsPath)
	if err != nil {
		t.Fatalf("read nulls: %v", err)
	}
	if len(nb) != 1 {
		t.Fatalf("nulls size = %d, want 1", len(nb))
	}
	if nb[0] != 0b11010000 {
		t.Fatalf("nulls byte = %08b, want 11010000", nb[0])
	}
}
