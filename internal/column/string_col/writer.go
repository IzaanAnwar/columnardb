package stringcol

import (
	"encoding/binary"
	"fmt"
	"os"
)

type Writer struct {
	idsFile   *os.File
	dictFile  *os.File
	nullsFile *os.File

	// dictionary
	strToID map[string]uint32
	idToStr []string

	// null bitmap state
	nullByte byte
	nullBit  uint8 // 0..7

	recordCount int
	closed      bool
}

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

func (w *Writer) writeNullBit(isNotNull bool) {
	if isNotNull {
		w.nullByte |= 1 << (7 - w.nullBit)
	}

	w.nullBit++

	if w.nullBit == 8 {
		w.nullsFile.Write([]byte{w.nullByte})
		w.nullByte = 0
		w.nullBit = 0
	}
}

func (w *Writer) Write(value any) error {
	if w.closed {
		return fmt.Errorf("write on closed string writer")
	}

	// Handle NULL
	if value == nil {
		w.writeNullBit(false)
		if err := binary.Write(w.idsFile, binary.LittleEndian, uint32(0)); err != nil {
			return fmt.Errorf("write placeholder id: %w", err)
		}
		w.recordCount++
		return nil
	}

	// Must be string
	s, ok := value.(string)
	if !ok {
		return fmt.Errorf("string writer expects string or nil, got %T", value)
	}

	w.writeNullBit(true)

	id, ok := w.strToID[s]
	if !ok {
		id = uint32(len(w.idToStr))
		w.strToID[s] = id
		w.idToStr = append(w.idToStr, s)
	}

	if err := binary.Write(w.idsFile, binary.LittleEndian, id); err != nil {
		return fmt.Errorf("write string id: %w", err)
	}

	w.recordCount++
	return nil
}

func (w *Writer) Close() error {
	if w.closed {
		return fmt.Errorf("string writer already closed")
	}
	w.closed = true

	// Flush remaining null bits
	if w.nullBit > 0 {
		if _, err := w.nullsFile.Write([]byte{w.nullByte}); err != nil {
			return fmt.Errorf("flush null bitmap: %w", err)
		}
	}

	// Write dictionary
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

func (w *Writer) RecordCount() int {
	return w.recordCount
}
