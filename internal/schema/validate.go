package schema

import "fmt"

// ValidateSchema ensures the schema meets all database requirements.
// Returns error for invalid schemas, nil for valid ones.
func ValidateSchema(s *Schema) error {
	if s.Version <= 0 {
		return fmt.Errorf("Schema version must be > 0")
	}

	if len(s.Columns) == 0 {
		return fmt.Errorf("Schema must have at least one column")
	}

	seen := make(map[string]struct{})

	for i, col := range s.Columns {

		if col.Name == "" {
			return fmt.Errorf("Column name cannot be empty")
		}

		if _, ok := seen[col.Name]; ok {
			return fmt.Errorf("Duplicate column name: %s", col.Name)
		}

		seen[col.Name] = struct{}{}

		switch col.Type {
		case TypeInt64, TypeBool, TypeString:
			// ok
		default:
			return fmt.Errorf("Unsupported column type: %s", col.Type)
		}

		s.Columns[i].Index = i
	}

	return nil
}
