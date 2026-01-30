package segment

import (
	"columnar/internal/schema"
	"fmt"
	"os"
	"path/filepath"
)

type ColumnWriter interface {
	Write(value any) error
	Close() error
	RecordCount() int
}

type SegmentWriter struct {
	schema      *schema.Schema
	segmentID   int
	basePath    string
	tempDir     string
	finalDir    string
	writers     []ColumnWriter
	recordCount int
	committed   bool
}

func NewSegmentWriter(basePath string, segmentID int, schema *schema.Schema) (*SegmentWriter, error) {
	finalDir := filepath.Join(basePath, fmt.Sprintf("seg_%06d", segmentID))
	tempDir := finalDir + ".tmp"

	// Tmp Dir must NOT exist
	if err := os.Mkdir(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("Failed to create tmp segment dir: %w", err)
	}

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

// WriteRecord writes one logical record.
// record MUST contain all columns defined in the schema.
func (w *SegmentWriter) WriteRecord(record map[string]any) error {
	if w.committed {
		return fmt.Errorf("Cannot write to commited segment")
	}

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

func (w *SegmentWriter) Commit() error {
	if w.committed {
		return fmt.Errorf("Segment already committed")
	}

	// Close all column writers
	for _, writer := range w.writers {
		if err := writer.Close(); err != nil {
			w.Abort()
			return err
		}
	}

	// Validate record counts
	for _, writer := range w.writers {
		if writer.RecordCount() != w.recordCount {
			w.Abort()
			return fmt.Errorf("Record count mismatch between columns")
		}
	}
	// TODO (next step) write metadata

	// Atomic Commit
	if err := os.Rename(w.tempDir, w.finalDir); err != nil {
		w.Abort()
		return fmt.Errorf("Failed to commit segment: %w", err)
	}
	w.committed = true
	return nil
}

// Abort deletes the uncommitted segment.
// Safe to call multiple times.
func (w *SegmentWriter) Abort() error {
	_ = os.RemoveAll(w.tempDir)
	return nil
}
