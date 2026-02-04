package float64col

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestWriter_RejectsNaN(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, "score")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if err := w.Write(math.NaN()); err == nil {
		t.Fatalf("expected error for NaN")
	}
}

func TestWriter_StatsAndNulls(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir, "score")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	values := []any{float64(1.5), nil, float64(-2.25)}
	for _, v := range values {
		if err := w.Write(v); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if w.RecordCount() != 3 {
		t.Fatalf("RecordCount = %d, want 3", w.RecordCount())
	}
	if w.NullCount() != 1 {
		t.Fatalf("NullCount = %d, want 1", w.NullCount())
	}
	if w.Min() != -2.25 || w.Max() != 1.5 {
		t.Fatalf("Min/Max = %f/%f, want -2.25/1.5", w.Min(), w.Max())
	}

	valuesPath := filepath.Join(dir, "score.bin")
	raw, err := os.ReadFile(valuesPath)
	if err != nil {
		t.Fatalf("read values: %v", err)
	}
	if len(raw) != 3*8 {
		t.Fatalf("values size = %d, want %d", len(raw), 3*8)
	}

	reader := bytes.NewReader(raw)
	got := make([]float64, 0, 3)
	for i := 0; i < 3; i++ {
		var v float64
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			t.Fatalf("binary.Read: %v", err)
		}
		got = append(got, v)
	}
	want := []float64{1.5, 0, -2.25}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("values[%d] = %f, want %f", i, got[i], want[i])
		}
	}

	nullsPath := filepath.Join(dir, "score.nulls.bin")
	nb, err := os.ReadFile(nullsPath)
	if err != nil {
		t.Fatalf("read nulls: %v", err)
	}
	if len(nb) != 1 {
		t.Fatalf("nulls size = %d, want 1", len(nb))
	}
	if nb[0] != 0b10100000 {
		t.Fatalf("nulls byte = %08b, want 10100000", nb[0])
	}
}
