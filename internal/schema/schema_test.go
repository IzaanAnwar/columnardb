package schema

import (
	"strings"
	"testing"
)

func TestLoadSchema_Valid(t *testing.T) {
	s, err := LoadSchema("../../testdata/valid_schema.json")
	if err != nil {
		t.Fatalf("Expected valid schema, got error: %v", err)
	}

	if s.Version != 1 {
		t.Fatalf("Expected version 1, got %d", s.Version)
	}

	if len(s.Columns) != 5 {
		t.Fatalf("Expected 5 columns, got %d", len(s.Columns))
	}

	// Test that InitializeSchema set indexes correctly
	for i, col := range s.Columns {
		if col.Index != i {
			t.Fatalf("Expected column %d to have index %d, got %d", i, i, col.Index)
		}
	}

	// Test specific column names and types
	if s.Columns[0].Name != "id" || s.Columns[0].Type != TypeString {
		t.Fatalf("Expected first column to be 'id' of type 'string', got '%s' of type '%s'", s.Columns[0].Name, s.Columns[0].Type)
	}

	if s.Columns[1].Name != "age" || s.Columns[1].Type != TypeInt64 {
		t.Fatalf("Expected second column to be 'age' of type 'int64', got '%s' of type '%s'", s.Columns[1].Name, s.Columns[1].Type)
	}
}

func TestLoadSchema_FileNotFound(t *testing.T) {
	_, err := LoadSchema("../../testdata/nonexistent.json")
	if err == nil {
		t.Fatalf("Expected error for missing file")
	}

	// Check that error message is helpful
	if err.Error() == "" {
		t.Fatalf("Expected non-empty error message")
	}
}

func TestLoadSchema_MalformedJSON(t *testing.T) {
	_, err := LoadSchema("../../testdata/malformed_json.json")
	if err == nil {
		t.Fatalf("Expected error for malformed JSON")
	}

	if !strings.Contains(err.Error(), "Failed to parse schema json") {
		t.Fatalf("Expected JSON parsing error, got: %v", err)
	}
}

func TestLoadSchema_InvalidSchema_ZeroVersion(t *testing.T) {
	_, err := LoadSchema("../../testdata/invalid_schema_zero_version.json")
	if err == nil {
		t.Fatalf("Expected error for zero version")
	}

	if !strings.Contains(err.Error(), "version must be > 0") {
		t.Fatalf("Expected version error, got: %v", err)
	}
}

func TestLoadSchema_InvalidSchema_EmptyColumns(t *testing.T) {
	_, err := LoadSchema("../../testdata/invalid_schema_empty_columns.json")
	if err == nil {
		t.Fatalf("Expected error for empty columns")
	}

	if !strings.Contains(err.Error(), "must have at least one column") {
		t.Fatalf("Expected empty columns error, got: %v", err)
	}
}

func TestLoadSchema_InvalidSchema_EmptyName(t *testing.T) {
	_, err := LoadSchema("../../testdata/invalid_schema_empty_name.json")
	if err == nil {
		t.Fatalf("Expected error for empty column name")
	}

	if !strings.Contains(err.Error(), "name cannot be empty") {
		t.Fatalf("Expected empty name error, got: %v", err)
	}
}

func TestLoadSchema_InvalidSchema_DuplicateColumns(t *testing.T) {
	_, err := LoadSchema("../../testdata/invalid_schema_duplicate_columns.json")
	if err == nil {
		t.Fatalf("Expected error for duplicate columns")
	}

	if !strings.Contains(err.Error(), "Duplicate column name") {
		t.Fatalf("Expected duplicate columns error, got: %v", err)
	}
}

func TestLoadSchema_InvalidSchema_UnsupportedType(t *testing.T) {
	_, err := LoadSchema("../../testdata/invalid_schema_unsupported_type.json")
	if err == nil {
		t.Fatalf("Expected error for unsupported type")
	}

	if !strings.Contains(err.Error(), "Unsupported column type") {
		t.Fatalf("Expected unsupported type error, got: %v", err)
	}
}

func TestValidateSchema_DuplicateColumn(t *testing.T) {
	s := &Schema{
		Version: 1,
		Columns: []Column{
			{Name: "id", Type: TypeString, Nullable: false},
			{Name: "id", Type: TypeInt64, Nullable: false},
		},
	}

	if err := ValidateSchema(s); err == nil {
		t.Fatalf("expected error for duplicate columns")
	}
}

func TestValidateSchema_InvalidType(t *testing.T) {
	s := &Schema{
		Version: 1,
		Columns: []Column{
			{Name: "foo", Type: ColumnType("enum"), Nullable: false},
		},
	}

	if err := ValidateSchema(s); err == nil {
		t.Fatalf("expected error for invalid column type")
	}
}

func TestValidateSchema_AllColumnTypes(t *testing.T) {
	s := &Schema{
		Version: 1,
		Columns: []Column{
			{Name: "string_col", Type: TypeString, Nullable: true},
			{Name: "int64_col", Type: TypeInt64, Nullable: false},
			{Name: "float64_col", Type: TypeFloat64, Nullable: false},
			{Name: "bool_col", Type: TypeBool, Nullable: true},
			{Name: "timestamp_col", Type: TypeTimestamp, Nullable: false},
		},
	}

	if err := ValidateSchema(s); err != nil {
		t.Fatalf("Expected all column types to be valid, got error: %v", err)
	}
}

func TestInitializeSchema_SetsIndexes(t *testing.T) {
	s := &Schema{
		Version: 1,
		Columns: []Column{
			{Name: "first", Type: TypeString, Nullable: false},
			{Name: "second", Type: TypeInt64, Nullable: false},
			{Name: "third", Type: TypeBool, Nullable: true},
		},
	}

	// InitializeSchema should not return an error
	InitializeSchema(s)

	// Check that indexes are set correctly
	for i, col := range s.Columns {
		if col.Index != i {
			t.Fatalf("Expected column %d to have index %d, got %d", i, i, col.Index)
		}
	}
}
