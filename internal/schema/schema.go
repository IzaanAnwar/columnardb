// Package schema defines the data model and validation for columnar schemas.
//
// A schema is append-only and immutable once loaded. It defines:
//   - Column types and nullability
//   - Column ordering (fixed for the lifetime of a datastore)
//   - Version for future compatibility checks
//
// Schema validation ensures structural integrity before any data operations.
package schema

// ColumnType represents the supported data types for columns.
type ColumnType string

const (
	// TypeInt64 represents 64-bit signed integers.
	TypeInt64 ColumnType = "int64"
	// TypeFloat64 represents 64-bit floating point numbers.
	TypeFloat64 ColumnType = "float64"
	// TypeBool represents boolean values.
	TypeBool ColumnType = "bool"
	// TypeString represents UTF-8 strings.
	TypeString ColumnType = "string"
	// TypeTimestamp represents Unix epoch milliseconds (UTC).
	TypeTimestamp ColumnType = "timestamp"
)

// Column defines a single field in the schema.
type Column struct {
	Name     string     `json:"name"`     // Column name (unique within schema)
	Type     ColumnType `json:"type"`     // Data type
	Nullable bool       `json:"nullable"` // Whether null values are allowed
	Index    int        `json:"-"`        // Runtime position index (set by InitializeSchema)
}

// Schema defines the structure of stored data.
type Schema struct {
	Version int      `json:"version"` // Schema version for compatibility
	Columns []Column `json:"columns"` // Ordered list of columns
}
