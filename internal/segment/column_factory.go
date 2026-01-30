package segment

import (
	boolcol "columnar/internal/column/bool_col"
	float64col "columnar/internal/column/float64_col"
	int64col "columnar/internal/column/int64_col"
	stringcol "columnar/internal/column/string_col"
	timestampcol "columnar/internal/column/timestamp_col"
	"columnar/internal/schema"
	"fmt"
)

// createColumnWriter creates a type-specific column writer for a single schema column.
// Factory function that instantiates the appropriate writer based on column type.
//
// segmentDir: Directory where column files will be created (segment temp directory)
// col: Schema column definition containing name and type information
func createColumnWriter(
	segmentDir string,
	col schema.Column,
) (ColumnWriter, error) {

	switch col.Type {
	case schema.TypeInt64:
		return int64col.NewWriter(segmentDir, col.Name)

	case schema.TypeFloat64:
		return float64col.NewWriter(segmentDir, col.Name)

	case schema.TypeBool:
		return boolcol.NewWriter(segmentDir, col.Name)

	case schema.TypeString:
		return stringcol.NewWriter(segmentDir, col.Name)

	case schema.TypeTimestamp:
		return timestampcol.NewWriter(segmentDir, col.Name)

	default:
		return nil, fmt.Errorf("unsupported column type: %s", col.Type)
	}
}
