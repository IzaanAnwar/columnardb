package metadata

// ColumnMetadata holds information about one column
type ColumnMetadata struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	RecordCount    int    `json:"record_count"`
	MinValue       any    `json:"min_value,omitempty"`
	MaxValue       any    `json:"max_value,omitempty"`
	DictionarySize int    `json:"dictionary_size,omitempty"`
	NullCount      int    `json:"null_count,omitempty"`
}

// SegmentMetadata holds information about one segment
type SegmentMetadata struct {
	SegmentID   int              `json:"segment_id"`
	RecordCount int              `json:"record_count"`
	Columns     []ColumnMetadata `json:"columns"`
}
