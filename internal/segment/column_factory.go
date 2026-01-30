package segment

import (
	boolcol "columnar/internal/column/bool_col"
	float64col "columnar/internal/column/float64_col"
	int64col "columnar/internal/column/int64_col"
	stringcol "columnar/internal/column/string_col"
	timestampcol "columnar/internal/column/timestamp_col"
	"columnar/internal/schema"
	"fmt"
	"path/filepath"
)

// createColumnWriter creates a column writer for a single column
func createColumnWriter(
	segmentDir string,
	col schema.Column,
) (ColumnWriter, error) {

	switch col.Type {
	case schema.TypeInt64:
		path := filepath.Join(segmentDir, col.Name+".bin")
		return int64col.NewWriter(path)

	case schema.TypeFloat64:
		path := filepath.Join(segmentDir, col.Name+".bin")
		return float64col.NewWriter(path)

	case schema.TypeBool:
		path := filepath.Join(segmentDir, col.Name+".bin")
		return boolcol.NewWriter(path)

	case schema.TypeString:
		path := filepath.Join(segmentDir, col.Name+".bin")
		return stringcol.NewWriter(path)

	case schema.TypeTimestamp:
		return timestampcol.NewWriter(colPath)

	default:
		return nil, fmt.Errorf("unsupported column type: %s", col.Type)
	}
}
