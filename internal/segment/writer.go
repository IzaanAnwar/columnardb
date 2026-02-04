// Package segment implements immutable segment writing for columnar data storage.
//
// Segments are the fundamental unit of data organization:
//   - Each segment contains one file per column in columnar layout
//   - Segments are immutable once committed (append-only writes)
//   - Atomic commit pattern ensures crash safety via temp directory rename
//   - Metadata enables efficient segment pruning during queries
//
// The write path follows: temp directory creation → column writes → metadata generation → atomic commit.
package segment

import (
	"columnar/internal/metadata"
	"columnar/internal/schema"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// ColumnWriter defines the interface for writing column data.
// Implementations handle type-specific encoding, statistics collection, and file I/O.
type ColumnWriter interface {
	// Write appends one value (including nulls) and must be called exactly once
	// per column for each logical record.
	Write(value any) error
	Close() error
	// RecordCount must increase by exactly 1 per successful Write() call.
	RecordCount() int
}

// SegmentWriter manages the lifecycle of writing an immutable segment.
// Coordinates multiple column writers and ensures atomic commit semantics.
type SegmentWriter struct {
	schema      *schema.Schema // Schema defining column structure
	segmentID   int            // Unique segment identifier
	basePath    string         // Base directory for segments
	tempDir     string         // Temporary directory for in-progress writes
	finalDir    string         // Final directory after successful commit
	writers     []ColumnWriter // Column writers for each schema column
	recordCount int            // Number of records written to this segment
	committed   bool           // Whether this segment has been committed
}

// NewSegmentWriter creates a new segment writer with atomic commit semantics.
// Creates a temporary directory for in-progress writes and initializes column writers.
//
// basePath: Base directory where segments are stored
// segmentID: Unique identifier for this segment (used for directory naming)
// schema: Schema defining the column structure for this segment
func NewSegmentWriter(basePath string, segmentID int, schema *schema.Schema) (*SegmentWriter, error) {
	finalDir := filepath.Join(basePath, fmt.Sprintf("seg_%06d", segmentID))
	tempDir := finalDir + ".tmp"

	// Create temporary directory - must not exist for atomic rename semantics
	if err := os.Mkdir(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("Failed to create tmp segment dir: %w", err)
	}

	// Initialize column writers for each schema column
	writers := make([]ColumnWriter, len(schema.Columns))
	for i, col := range schema.Columns {
		writer, err := createColumnWriter(tempDir, col)
		if err != nil {
			for j := 0; j < i; j++ {
				if writers[j] != nil {
					_ = writers[j].Close() // close already created writers
				}
			}
			_ = os.RemoveAll(tempDir) // cleanup on failure
			return nil, err
		}
		writers[i] = writer
	}

	return &SegmentWriter{
		schema:      schema,
		segmentID:   segmentID,
		basePath:    basePath,
		tempDir:     tempDir,
		finalDir:    finalDir,
		writers:     writers,
		recordCount: 0,
		committed:   false,
	}, nil
}

// WriteRecord writes one logical record to all columns.
// The record map must contain values for all columns defined in the schema.
// Values are written to column writers in schema order to maintain alignment.
// TODO(v2): Add ordered/batch ingestion APIs; keep map-based ingestion as a thin adapter.
func (w *SegmentWriter) WriteRecord(record map[string]any) error {
	if w.committed {
		return fmt.Errorf("Cannot write to committed segment")
	}

	// Write each column value in schema order
	for i, col := range w.schema.Columns {
		value, ok := record[col.Name]
		if !ok {
			return fmt.Errorf("Missing value for column %q", col.Name)
		}

		if !col.Nullable && isNilValue(value) {
			return fmt.Errorf("Null value for non-nullable column %q", col.Name)
		}

		if err := w.writers[i].Write(value); err != nil {
			return fmt.Errorf("Failed to write column %q: %w", col.Name, err)
		}
	}

	w.recordCount++
	return nil
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Interface, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// Commit atomically finalizes the segment by closing writers and renaming temp directory.
// Validates record count consistency across all columns before committing.
// Once committed, the segment becomes immutable and visible to readers.
func (w *SegmentWriter) Commit() error {
	if w.committed {
		return fmt.Errorf("Segment already committed")
	}

	// Close all column writers and flush any remaining data
	// Best-effort close: attempt all writers even if one fails.
	// Abort cleans up the temp dir after partial closes.
	var closeErr error
	for _, writer := range w.writers {
		if err := writer.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if closeErr != nil {
		w.Abort()
		return closeErr
	}

	// Validate that all columns have identical record counts
	for _, writer := range w.writers {
		if writer.RecordCount() != w.recordCount {
			w.Abort()
			return fmt.Errorf("Record count mismatch between columns")
		}
	}

	if err := w.writeMetadata(); err != nil {
		w.Abort()
		return err
	}

	// Atomic commit: rename temp directory to final directory
	if err := os.Rename(w.tempDir, w.finalDir); err != nil {
		w.Abort()
		return fmt.Errorf("Failed to commit segment: %w", err)
	}

	w.committed = true
	if err := w.updateManifest(); err != nil {
		return fmt.Errorf("segment committed but manifest update failed: %w", err)
	}
	return nil
}

// Abort cleans up an uncommitted segment by removing the temporary directory.
// Safe to call multiple times and on already committed segments.
// Used for error recovery and resource cleanup.
func (w *SegmentWriter) Abort() error {
	_ = os.RemoveAll(w.tempDir)
	return nil
}

func (w *SegmentWriter) writeMetadata() error {
	meta := metadata.SegmentMetadata{
		SegmentID:   w.segmentID,
		RecordCount: w.recordCount,
		Columns:     make([]metadata.ColumnMetadata, len(w.schema.Columns)),
	}

	for i, col := range w.schema.Columns {
		writer := w.writers[i]
		colMeta := metadata.ColumnMetadata{
			Name:        col.Name,
			Type:        string(col.Type),
			RecordCount: writer.RecordCount(),
		}

		if nc, ok := writer.(interface{ NullCount() int }); ok {
			colMeta.NullCount = nc.NullCount()
		}

		switch col.Type {
		case schema.TypeInt64, schema.TypeTimestamp:
			if mm, ok := writer.(interface {
				Min() int64
				Max() int64
			}); ok && colMeta.NullCount < colMeta.RecordCount {
				colMeta.MinValue = mm.Min()
				colMeta.MaxValue = mm.Max()
			}
		case schema.TypeFloat64:
			if mm, ok := writer.(interface {
				Min() float64
				Max() float64
			}); ok && colMeta.NullCount < colMeta.RecordCount {
				colMeta.MinValue = mm.Min()
				colMeta.MaxValue = mm.Max()
			}
		case schema.TypeString:
			if ds, ok := writer.(interface{ DictionarySize() int }); ok {
				colMeta.DictionarySize = ds.DictionarySize()
			}
		}

		meta.Columns[i] = colMeta
	}

	metaPath := filepath.Join(w.tempDir, "metadata.json")
	file, err := os.Create(metaPath)
	if err != nil {
		return fmt.Errorf("create metadata.json: %w", err)
	}

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(meta); err != nil {
		_ = file.Close()
		return fmt.Errorf("write metadata.json: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close metadata.json: %w", err)
	}

	return nil
}

func (w *SegmentWriter) updateManifest() error {
	manifestPath := manifestPathForSegmentsDir(w.basePath)
	relPath, err := filepath.Rel(filepath.Dir(manifestPath), w.finalDir)
	if err != nil {
		relPath = w.finalDir
	}

	item := ManifestItem{
		ID:          w.segmentID,
		Path:        filepath.ToSlash(relPath),
		RecordCount: w.recordCount,
	}
	return appendManifestItem(manifestPath, item)
}

func manifestPathForSegmentsDir(segmentsDir string) string {
	if filepath.Base(segmentsDir) == "segments" {
		return filepath.Join(filepath.Dir(segmentsDir), "manifest.json")
	}
	return filepath.Join(segmentsDir, "manifest.json")
}
