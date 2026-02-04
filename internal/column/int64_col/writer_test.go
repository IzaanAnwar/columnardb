package int64col

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestWriter_StatsAndNulls(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir, "age")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	values := []any{int64(10), nil, int64(-3)}
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
	if w.Min() != -3 || w.Max() != 10 {
		t.Fatalf("Min/Max = %d/%d, want -3/10", w.Min(), w.Max())
	}

	valuesPath := filepath.Join(dir, "age.bin")
	raw, err := os.ReadFile(valuesPath)
	if err != nil {
		t.Fatalf("read values: %v", err)
	}
	if len(raw) != 3*8 {
		t.Fatalf("values size = %d, want %d", len(raw), 3*8)
	}

	reader := bytes.NewReader(raw)
	got := make([]int64, 0, 3)
	for i := 0; i < 3; i++ {
		var v int64
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			t.Fatalf("binary.Read: %v", err)
		}
		got = append(got, v)
	}
	want := []int64{10, 0, -3}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("values[%d] = %d, want %d", i, got[i], want[i])
		}
	}

	nullsPath := filepath.Join(dir, "age.nulls.bin")
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
