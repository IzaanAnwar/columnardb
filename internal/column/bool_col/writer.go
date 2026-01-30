package boolcol

import (
	"fmt"
	"os"
)

// Writer writes boolean values to a column file with null bitmap support.
// Values are bit-packed (8 bools per byte) with a separate null bitmap file.
type Writer struct {
	valuesFile *os.File
	nullsFile  *os.File
	// Bit-packing state for values
	valueBuf    byte
	valueBitPos uint8 // 0..7
	// Null bitmap state
	nullBuf    byte
	nullBitPos uint8 // 0..7

	count     int
	nullCount int
	closed    bool
}

// NewWriter creates a new boolean column writer.
// basePath: directory path where files will be created
// colName: name of the column (used for file naming)
func NewWriter(basePath string, colName string) (*Writer, error) {
	valuesPath := basePath + "/" + colName + ".bin"
	nullsPath := basePath + "/" + colName + ".nulls.bin"

	valuesFile, err := os.OpenFile(valuesPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("create bool values file: %w", err)
	}

	nullsFile, err := os.OpenFile(nullsPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		valuesFile.Close()
		return nil, fmt.Errorf("create bool null bitmap file: %w", err)
	}

	return &Writer{
		valuesFile: valuesFile,
		nullsFile:  nullsFile,
	}, nil
}

// writeNullBit writes a bit to the null bitmap.
// isNotNull: true if the value is not null, false if null
func (w *Writer) writeNullBit(isNotNull bool) error {
	if isNotNull {
		w.nullBuf |= 1 << (7 - w.nullBitPos)
	}
	w.nullBitPos++

	if w.nullBitPos == 8 {
		if _, err := w.nullsFile.Write([]byte{w.nullBuf}); err != nil {
			return fmt.Errorf("write null bitmap byte: %w", err)
		}
		w.nullBuf = 0
		w.nullBitPos = 0
	}
	return nil
}

// Write writes one boolean value to the column.
// Accepts bool or nil (for null values).
func (w *Writer) Write(value any) error {
	if w.closed {
		return fmt.Errorf("write on closed bool writer")
	}

	// Handle null values
	if value == nil {
		w.nullCount++
		if err := w.writeNullBit(false); err != nil {
			return err
		}
		// Write placeholder value (false) for null
		w.valueBitPos++
		w.count++
		if w.valueBitPos == 8 {
			if _, err := w.valuesFile.Write([]byte{w.valueBuf}); err != nil {
				return fmt.Errorf("write bool values byte: %w", err)
			}
			w.valueBuf = 0
			w.valueBitPos = 0
		}
		return nil
	}

	v, ok := value.(bool)
	if !ok {
		return fmt.Errorf("bool writer expects bool or nil, got %T", value)
	}

	// Mark as not null in bitmap
	if err := w.writeNullBit(true); err != nil {
		return err
	}

	// Pack the boolean value
	if v {
		w.valueBuf |= 1 << (7 - w.valueBitPos)
	}

	w.valueBitPos++
	w.count++

	// Flush byte when full
	if w.valueBitPos == 8 {
		if _, err := w.valuesFile.Write([]byte{w.valueBuf}); err != nil {
			return fmt.Errorf("write bool values byte: %w", err)
		}
		w.valueBuf = 0
		w.valueBitPos = 0
	}

	return nil
}

// Close closes the writer and flushes any remaining data.
func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("bool writer already closed")
	}
	w.closed = true

	// Flush remaining value bits
	if w.valueBitPos > 0 {
		if _, err := w.valuesFile.Write([]byte{w.valueBuf}); err != nil {
			return fmt.Errorf("flush bool values byte: %w", err)
		}
	}

	// Flush remaining null bits
	if w.nullBitPos > 0 {
		if _, err := w.nullsFile.Write([]byte{w.nullBuf}); err != nil {
			return fmt.Errorf("flush null bitmap byte: %w", err)
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
func (w *Writer) RecordCount() int {
	return w.count
}

// NullCount returns the number of null values written.
func (w *Writer) NullCount() int {
	return w.nullCount
}
