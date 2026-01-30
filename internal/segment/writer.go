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
	"columnar/internal/schema"
	"fmt"
	"os"
	"path/filepath"
)

// ColumnWriter defines the interface for writing column data.
// Implementations handle type-specific encoding, statistics collection, and file I/O.
type ColumnWriter interface {
	Write(value any) error
	Close() error
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
			os.RemoveAll(tempDir) // Clean up on failure
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

		if err := w.writers[i].Write(value); err != nil {
			return fmt.Errorf("Failed to write column %q: %w", col.Name, err)
		}
	}

	w.recordCount++
	return nil
}

// Commit atomically finalizes the segment by closing writers and renaming temp directory.
// Validates record count consistency across all columns before committing.
// Once committed, the segment becomes immutable and visible to readers.
func (w *SegmentWriter) Commit() error {
	if w.committed {
		return fmt.Errorf("Segment already committed")
	}

	// Close all column writers and flush any remaining data
	for _, writer := range w.writers {
		if err := writer.Close(); err != nil {
			w.Abort()
			return err
		}
	}

	// Validate that all columns have identical record counts
	for _, writer := range w.writers {
		if writer.RecordCount() != w.recordCount {
			w.Abort()
			return fmt.Errorf("Record count mismatch between columns")
		}
	}

	// TODO: Write metadata.json before atomic rename

	// Atomic commit: rename temp directory to final directory
	if err := os.Rename(w.tempDir, w.finalDir); err != nil {
		w.Abort()
		return fmt.Errorf("Failed to commit segment: %w", err)
	}

	w.committed = true
	return nil
}

// Abort cleans up an uncommitted segment by removing the temporary directory.
// Safe to call multiple times and on already committed segments.
// Used for error recovery and resource cleanup.
func (w *SegmentWriter) Abort() error {
	_ = os.RemoveAll(w.tempDir)
	return nil
}
