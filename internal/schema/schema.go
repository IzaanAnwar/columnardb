package schema

type ColumnType string

const (
	TypeInt64  ColumnType = "int64"
	TypeBool   ColumnType = "bool"
	TypeString ColumnType = "string"
)

type Column struct {
	Name     string     `json:"name"`
	Type     ColumnType `json:"type"`
	Nullable bool       `json:"nullable"`
	Index    int        `json:"-"`
}

type Schema struct {
	Version int      `json:"version"`
	Columns []Column `json:"columns"`
}
