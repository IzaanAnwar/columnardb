package segment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"columnar/internal/metadata"
	"columnar/internal/schema"
)

func TestSegmentWriter_MetadataAndManifest(t *testing.T) {
	root := t.TempDir()
	segmentsDir := filepath.Join(root, "segments")
	if err := os.MkdirAll(segmentsDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	s := &schema.Schema{
		Version: 1,
		Columns: []schema.Column{
			{Name: "id", Type: schema.TypeString, Nullable: false},
			{Name: "age", Type: schema.TypeInt64, Nullable: true},
			{Name: "created_at", Type: schema.TypeTimestamp, Nullable: false},
		},
	}

	w, err := NewSegmentWriter(segmentsDir, 1, s)
	if err != nil {
		t.Fatalf("NewSegmentWriter: %v", err)
	}

	if err := w.WriteRecord(map[string]any{
		"id":         "a",
		"age":        int64(10),
		"created_at": time.UnixMilli(1000),
	}); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	if err := w.WriteRecord(map[string]any{
		"id":         "b",
		"age":        nil,
		"created_at": time.UnixMilli(2000),
	}); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	if err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	metaPath := filepath.Join(segmentsDir, "seg_000001", "metadata.json")
	metaRaw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}

	var meta metadata.SegmentMetadata
	if err := json.Unmarshal(metaRaw, &meta); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}

	if meta.SegmentID != 1 {
		t.Fatalf("SegmentID = %d, want 1", meta.SegmentID)
	}
	if meta.RecordCount != 2 {
		t.Fatalf("RecordCount = %d, want 2", meta.RecordCount)
	}

	colMeta := make(map[string]metadata.ColumnMetadata)
	for _, c := range meta.Columns {
		colMeta[c.Name] = c
	}

	if colMeta["id"].DictionarySize != 2 {
		t.Fatalf("id.DictionarySize = %d, want 2", colMeta["id"].DictionarySize)
	}
	if colMeta["id"].NullCount != 0 {
		t.Fatalf("id.NullCount = %d, want 0", colMeta["id"].NullCount)
	}

	if colMeta["age"].NullCount != 1 {
		t.Fatalf("age.NullCount = %d, want 1", colMeta["age"].NullCount)
	}

	if got := number(colMeta["age"].MinValue); got != 10 {
		t.Fatalf("age.MinValue = %v, want 10", colMeta["age"].MinValue)
	}
	if got := number(colMeta["age"].MaxValue); got != 10 {
		t.Fatalf("age.MaxValue = %v, want 10", colMeta["age"].MaxValue)
	}

	if got := number(colMeta["created_at"].MinValue); got != 1000 {
		t.Fatalf("created_at.MinValue = %v, want 1000", colMeta["created_at"].MinValue)
	}
	if got := number(colMeta["created_at"].MaxValue); got != 2000 {
		t.Fatalf("created_at.MaxValue = %v, want 2000", colMeta["created_at"].MaxValue)
	}

	manifestPath := filepath.Join(root, "manifest.json")
	manifestRaw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}

	if len(manifest.Segments) != 1 {
		t.Fatalf("manifest segments = %d, want 1", len(manifest.Segments))
	}
	if manifest.Segments[0].ID != 1 {
		t.Fatalf("manifest id = %d, want 1", manifest.Segments[0].ID)
	}
	if manifest.Segments[0].Path != "segments/seg_000001" {
		t.Fatalf("manifest path = %q, want %q", manifest.Segments[0].Path, "segments/seg_000001")
	}
	if manifest.Segments[0].RecordCount != 2 {
		t.Fatalf("manifest record_count = %d, want 2", manifest.Segments[0].RecordCount)
	}
}

func TestSegmentWriter_RejectsNilForNonNullable(t *testing.T) {
	root := t.TempDir()
	segmentsDir := filepath.Join(root, "segments")
	if err := os.MkdirAll(segmentsDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	s := &schema.Schema{
		Version: 1,
		Columns: []schema.Column{
			{Name: "id", Type: schema.TypeString, Nullable: false},
		},
	}

	w, err := NewSegmentWriter(segmentsDir, 1, s)
	if err != nil {
		t.Fatalf("NewSegmentWriter: %v", err)
	}

	if err := w.WriteRecord(map[string]any{"id": nil}); err == nil {
		t.Fatalf("expected error for nil in non-nullable column")
	}
}

func number(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}
