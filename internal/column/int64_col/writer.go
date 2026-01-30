package int64col

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Writer writes int64 values to a column file with null bitmap support.
// Values are stored as 8-byte little-endian integers with a separate null bitmap file.
type Writer struct {
	valuesFile *os.File
	nullsFile  *os.File
	// Null bitmap state: 8 bits per byte, MSB-first
	nullByte byte
	nullBit  uint8 // 0..7

	count     int
	nullCount int
	min       int64
	max       int64
	hasValue  bool
	closed    bool
}

// NewWriter creates a new int64 column writer.
// basePath: directory path where files will be created
// colName: name of the column (used for file naming)
func NewWriter(basePath string, colName string) (*Writer, error) {
	valuesPath := basePath + "/" + colName + ".bin"
	nullsPath := basePath + "/" + colName + ".nulls.bin"

	valuesFile, err := os.OpenFile(valuesPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("create int64 values file: %w", err)
	}

	nullsFile, err := os.OpenFile(nullsPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		valuesFile.Close()
		return nil, fmt.Errorf("create int64 null bitmap file: %w", err)
	}

	return &Writer{
		valuesFile: valuesFile,
		nullsFile:  nullsFile,
	}, nil
}

// writeNullBit writes a bit to the null bitmap.
// isNotNull: true if the value is not null, false if null
// Uses MSB-first bit order: bit 7 is first, bit 0 is last
func (w *Writer) writeNullBit(isNotNull bool) error {
	if isNotNull {
		w.nullByte |= 1 << (7 - w.nullBit)
	}
	w.nullBit++

	if w.nullBit == 8 {
		if _, err := w.nullsFile.Write([]byte{w.nullByte}); err != nil {
			return fmt.Errorf("write null bitmap byte: %w", err)
		}
		w.nullByte = 0
		w.nullBit = 0
	}
	return nil
}

// Write writes one int64 value to the column.
// Accepts int64 or nil (for null values).
func (w *Writer) Write(value any) error {
	if w.closed {
		return fmt.Errorf("Write on closed int64 writer")
	}

	// Handle null values: write 0 as placeholder
	if value == nil {
		w.nullCount++
		if err := w.writeNullBit(false); err != nil {
			return err
		}
		if err := binary.Write(w.valuesFile, binary.LittleEndian, int64(0)); err != nil {
			return fmt.Errorf("write null placeholder: %w", err)
		}
		w.count++
		return nil
	}

	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("Int64 writer expects int64 or nil, got %T: ", value)
	}

	// Mark as not null in bitmap
	if err := w.writeNullBit(true); err != nil {
		return err
	}

	// Write the value in little-endian format
	if err := binary.Write(w.valuesFile, binary.LittleEndian, v); err != nil {
		return fmt.Errorf("write int64 value: %w", err)
	}

	// Update min/max statistics
	if !w.hasValue {
		w.min, w.max = v, v
		w.hasValue = true
	} else {
		if v > w.max {
			w.max = v
		}
		if v < w.min {
			w.min = v
		}
	}
	w.count++
	return nil

}

// Close closes the writer and flushes any remaining bitmap data.
func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("int64 writer already closed")
	}
	w.closed = true

	// Flush remaining null bitmap bits
	if w.nullBit > 0 {
		if _, err := w.nullsFile.Write([]byte{w.nullByte}); err != nil {
			return fmt.Errorf("flush null bitmap: %w", err)
		}
	}

	if err := w.valuesFile.Close(); err != nil {
		return err
	}
	if err := w.nullsFile.Close(); err != nil {
		return err
	}
	return nil
}

// RecordCount returns the number of records written.
func (w *Writer) RecordCount() int { return w.count }

// NullCount returns the number of null values written.
func (w *Writer) NullCount() int { return w.nullCount }

// Min returns the minimum non-null value written.
func (w *Writer) Min() int64 { return w.min }

// Max returns the maximum non-null value written.
func (w *Writer) Max() int64 { return w.max }
