package schema

import "fmt"

// ValidateSchema ensures schema meets all structural requirements.
// Returns error for any violation, nil for valid schemas.
func ValidateSchema(s *Schema) error {
	if s.Version <= 0 {
		return fmt.Errorf("Schema version must be > 0")
	}

	if len(s.Columns) == 0 {
		return fmt.Errorf("Schema must have at least one column")
	}

	seen := make(map[string]struct{})

	for _, col := range s.Columns {
		if col.Name == "" {
			return fmt.Errorf("Column name cannot be empty")
		}

		if _, ok := seen[col.Name]; ok {
			return fmt.Errorf("Duplicate column name: %s", col.Name)
		}
		seen[col.Name] = struct{}{}

		switch col.Type {
		case TypeInt64, TypeFloat64, TypeBool, TypeString, TypeTimestamp:
			// Valid type
		default:
			return fmt.Errorf("Unsupported column type: %s", col.Type)
		}

	}

	return nil
}

// InitializeSchema sets derived runtime state for a validated schema.
// Must be called after ValidateSchema passes. Assumes schema is valid.
func InitializeSchema(s *Schema) {
	for i := range s.Columns {
		s.Columns[i].Index = i
	}
}
