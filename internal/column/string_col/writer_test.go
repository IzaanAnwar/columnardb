package stringcol

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestWriter_DictionaryAndNulls(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriter(dir, "name")
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	values := []any{nil, "alpha", "beta", "alpha"}
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
	if w.DictionarySize() != 2 {
		t.Fatalf("DictionarySize = %d, want 2", w.DictionarySize())
	}

	idsPath := filepath.Join(dir, "name.ids.bin")
	raw, err := os.ReadFile(idsPath)
	if err != nil {
		t.Fatalf("read ids: %v", err)
	}
	if len(raw) != 4*4 {
		t.Fatalf("ids size = %d, want %d", len(raw), 4*4)
	}

	reader := bytes.NewReader(raw)
	got := make([]uint32, 0, 4)
	for i := 0; i < 4; i++ {
		var v uint32
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			t.Fatalf("binary.Read: %v", err)
		}
		got = append(got, v)
	}
	want := []uint32{0, 1, 2, 1}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ids[%d] = %d, want %d", i, got[i], want[i])
		}
	}

	nullsPath := filepath.Join(dir, "name.nulls.bin")
	nb, err := os.ReadFile(nullsPath)
	if err != nil {
		t.Fatalf("read nulls: %v", err)
	}
	if len(nb) != 1 {
		t.Fatalf("nulls size = %d, want 1", len(nb))
	}
	if nb[0] != 0b01110000 {
		t.Fatalf("nulls byte = %08b, want 01110000", nb[0])
	}

	dictPath := filepath.Join(dir, "name.dict.bin")
	db, err := os.ReadFile(dictPath)
	if err != nil {
		t.Fatalf("read dict: %v", err)
	}
	dictReader := bytes.NewReader(db)
	words := make([]string, 0, 2)
	for dictReader.Len() > 0 {
		var n uint32
		if err := binary.Read(dictReader, binary.LittleEndian, &n); err != nil {
			t.Fatalf("dict length: %v", err)
		}
		buf := make([]byte, n)
		if _, err := dictReader.Read(buf); err != nil {
			t.Fatalf("dict value: %v", err)
		}
		words = append(words, string(buf))
	}
	if len(words) != 2 || words[0] != "alpha" || words[1] != "beta" {
		t.Fatalf("dict = %v, want [alpha beta]", words)
	}
}
