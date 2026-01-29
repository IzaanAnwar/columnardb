package schema

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadSchema reads, validates, and initializes a schema from JSON file.
// Returns a fully initialized schema ready for use or an error.
func LoadSchema(path string) (*Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read schema file: %w", err)
	}

	var s Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("Failed to parse schema json: %w", err)
	}

	if err := ValidateSchema(&s); err != nil {
		return nil, fmt.Errorf("Invalid schema: %w", err)
	}

	InitializeSchema(&s)

	return &s, nil
}
