package schema

import (
	"testing"
)

func TestValidSchema(t *testing.T) {
	schma := Schema{
		Columns: []Column{
			{
				Name: "user_id",
				Type: TypeInt64,
			},
		},
	}

	if len(schma.Columns) != 1 {
		t.Errorf("Expected 1 column, got '%d'", len(schma.Columns))
	}

	if schma.Columns[0].Name != "user_id" {
		t.Errorf("Expected column name 'user_id', got '%s'", schma.Columns[0].Name)
	}

	if schma.Columns[0].Type != TypeInt64 {
		t.Errorf("Expected type TypeInt64, got '%s'", schma.Columns[0].Type)
	}
}
