package stringcol

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Writer writes string values to column files with dictionary encoding and null bitmap support.
// Creates three files: .ids.bin (dictionary indices), .dict.bin (string dictionary), .nulls.bin (null bitmap).
type Writer struct {
	idsFile   *os.File
	dictFile  *os.File
	nullsFile *os.File

	// Dictionary mapping for compression
	strToID map[string]uint32
	idToStr []string

	// Null bitmap state: 8 bits per byte, MSB-first
	nullByte byte
	nullBit  uint8 // 0..7

	recordCount int
	nullCount   int
	closed      bool
}

// NewWriter creates a new string column writer.
// basePath: directory path where files will be created
// colName: name of the column (used for file naming)
func NewWriter(basePath string, colName string) (*Writer, error) {
	idsPath := basePath + "/" + colName + ".ids.bin"
	dictPath := basePath + "/" + colName + ".dict.bin"
	nullsPath := basePath + "/" + colName + ".nulls.bin"

	idsFile, err := os.OpenFile(idsPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("Create ids file: %w", err)
	}

	dictFile, err := os.OpenFile(dictPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		idsFile.Close()
		return nil, fmt.Errorf("Create dict file: %w", err)
	}

	nullsFile, err := os.OpenFile(nullsPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		idsFile.Close()
		dictFile.Close()
		return nil, fmt.Errorf("Create null bitmap file: %w", err)
	}

	return &Writer{
		idsFile:   idsFile,
		dictFile:  dictFile,
		nullsFile: nullsFile,
		strToID:   make(map[string]uint32),
		idToStr:   make([]string, 0),
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
			return err
		}
		w.nullByte = 0
		w.nullBit = 0
	}

	return nil
}

// Write writes one string value to the column.
// Accepts string or nil (for null values). Uses dictionary encoding for compression.
func (w *Writer) Write(value any) error {
	if w.closed {
		return fmt.Errorf("write on closed string writer")
	}

	// Handle null values: write 0 as dictionary index
	if value == nil {
		w.nullCount++
		if err := w.writeNullBit(false); err != nil {
			return fmt.Errorf("write null bitmap: %w", err)
		}
		if err := binary.Write(w.idsFile, binary.LittleEndian, uint32(0)); err != nil {
			return fmt.Errorf("write null placeholder: %w", err)
		}
		w.recordCount++
		return nil
	}

	// Must be string
	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("string writer expects string or nil, got %T", value)
	}

	// Mark as not null in bitmap
	if err := w.writeNullBit(true); err != nil {
		return fmt.Errorf("write null bitmap: %w", err)
	}

	// Dictionary encoding: get existing ID or assign new one
	id, ok := w.strToID[s]
	if !ok {
		// Reserve 0 for NULL; real IDs start at 1.
		id = uint32(len(w.idToStr) + 1)
		w.strToID[s] = id
		w.idToStr = append(w.idToStr, s)
	}

	// Write dictionary index (4 bytes, little-endian)
	if err := binary.Write(w.idsFile, binary.LittleEndian, id); err != nil {
		return fmt.Errorf("write string id: %w", err)
	}

	w.recordCount++
	return nil
}

// Close closes the writer and flushes all remaining data.
// Writes dictionary to file and flushes partial bitmap bytes.
func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("string writer already closed")
	}
	w.closed = true

	// Flush remaining null bitmap bits
	if w.nullBit > 0 {
		if _, err := w.nullsFile.Write([]byte{w.nullByte}); err != nil {
			return fmt.Errorf("flush null bitmap: %w", err)
		}
	}

	// Write dictionary: length-prefixed strings
	for _, s := range w.idToStr {
		b := []byte(s)
		if err := binary.Write(w.dictFile, binary.LittleEndian, uint32(len(b))); err != nil {
			return fmt.Errorf("write dict length: %w", err)
		}
		if _, err := w.dictFile.Write(b); err != nil {
			return fmt.Errorf("write dict value: %w", err)
		}
	}

	if err := w.idsFile.Close(); err != nil {
		return err
	}
	if err := w.dictFile.Close(); err != nil {
		return err
	}
	if err := w.nullsFile.Close(); err != nil {
		return err
	}

	return nil
}

// RecordCount returns the number of records written.
func (w *Writer) RecordCount() int {
	return w.recordCount
}

// DictionarySize returns the number of unique strings in the dictionary.
func (w *Writer) DictionarySize() int {
	return len(w.idToStr)
}

// NullCount returns the number of null values written.
func (w *Writer) NullCount() int {
	return w.nullCount
}
