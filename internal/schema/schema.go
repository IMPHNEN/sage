package schema

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/IMPHNEN/sage/internal/dialect"
)

// Schema represents a database schema
type Schema struct {
	Tables []*Table
}

// Table represents a database table
type Table struct {
	Name        string
	Columns     []*Column
	PrimaryKey  *Column
	Indexes     []*Index
	UniqueKeys  []*UniqueKey
	ForeignKeys []*ForeignKey
}

// Column represents a database column
type Column struct {
	Name            string
	Type            string
	Size            int
	Precision       int
	Scale           int
	Nullable        bool
	Unique          bool
	Default         string
	IsAutoIncrement bool
	IsPrimaryKey    bool
}

// Index represents a database index
type Index struct {
	Name    string
	Columns []string
	Unique  bool
}

// UniqueKey represents a unique constraint
type UniqueKey struct {
	Name    string
	Columns []string
}

// ForeignKey represents a foreign key constraint
type ForeignKey struct {
	Name             string
	Columns          []string
	ReferenceTable   string
	ReferenceColumns []string
	OnDelete         string
	OnUpdate         string
}

// NewSchema creates a new schema
func NewSchema() *Schema {
	return &Schema{
		Tables: make([]*Table, 0),
	}
}

// AddTable adds a table to the schema
func (s *Schema) AddTable(table *Table) {
	s.Tables = append(s.Tables, table)
}

// GetTable gets a table by name
func (s *Schema) GetTable(name string) *Table {
	for _, table := range s.Tables {
		if table.Name == name {
			return table
		}
	}
	return nil
}

// NewTable creates a new table
func NewTable(name string) *Table {
	return &Table{
		Name:        name,
		Columns:     make([]*Column, 0),
		Indexes:     make([]*Index, 0),
		UniqueKeys:  make([]*UniqueKey, 0),
		ForeignKeys: make([]*ForeignKey, 0),
	}
}

// AddColumn adds a column to the table
func (t *Table) AddColumn(column *Column) {
	t.Columns = append(t.Columns, column)
	if column.IsPrimaryKey {
		t.PrimaryKey = column
	}
}

// GetColumn gets a column by name
func (t *Table) GetColumn(name string) *Column {
	for _, column := range t.Columns {
		if column.Name == name {
			return column
		}
	}
	return nil
}

// AddIndex adds an index to the table
func (t *Table) AddIndex(index *Index) {
	t.Indexes = append(t.Indexes, index)
}

// AddUniqueKey adds a unique constraint to the table
func (t *Table) AddUniqueKey(uniqueKey *UniqueKey) {
	t.UniqueKeys = append(t.UniqueKeys, uniqueKey)
}

// AddForeignKey adds a foreign key constraint to the table
func (t *Table) AddForeignKey(foreignKey *ForeignKey) {
	t.ForeignKeys = append(t.ForeignKeys, foreignKey)
}

// NewColumn creates a new column
func NewColumn(name string, dataType string) *Column {
	return &Column{
		Name:     name,
		Type:     dataType,
		Nullable: false,
	}
}

// NewIndex creates a new index
func NewIndex(name string, columns []string, unique bool) *Index {
	return &Index{
		Name:    name,
		Columns: columns,
		Unique:  unique,
	}
}

// NewUniqueKey creates a new unique constraint
func NewUniqueKey(name string, columns []string) *UniqueKey {
	return &UniqueKey{
		Name:    name,
		Columns: columns,
	}
}

// NewForeignKey creates a new foreign key constraint
func NewForeignKey(name string, columns []string, referenceTable string, referenceColumns []string) *ForeignKey {
	return &ForeignKey{
		Name:             name,
		Columns:          columns,
		ReferenceTable:   referenceTable,
		ReferenceColumns: referenceColumns,
		OnDelete:         "CASCADE",
		OnUpdate:         "CASCADE",
	}
}

// GenerateCreateTableSQL generates SQL for creating a table
func (t *Table) GenerateCreateTableSQL(d dialect.Dialect) string {
	var columnDefs []string

	for _, column := range t.Columns {
		columnDef := fmt.Sprintf("%s %s", d.Quote(column.Name), column.Type)

		if column.IsAutoIncrement {
			columnDef = fmt.Sprintf("%s %s", d.Quote(column.Name), d.AutoIncrementKeyword())
		}

		if !column.Nullable {
			columnDef += " NOT NULL"
		}

		if column.Default != "" {
			columnDef += " DEFAULT " + column.Default
		}

		columnDefs = append(columnDefs, columnDef)
	}

	// Add primary key constraint
	if t.PrimaryKey != nil {
		primaryKeyDef := fmt.Sprintf("PRIMARY KEY (%s)", d.Quote(t.PrimaryKey.Name))
		columnDefs = append(columnDefs, primaryKeyDef)
	}

	// Add unique constraints
	for _, uniqueKey := range t.UniqueKeys {
		quotedColumns := make([]string, len(uniqueKey.Columns))
		for i, col := range uniqueKey.Columns {
			quotedColumns[i] = d.Quote(col)
		}

		uniqueKeyDef := fmt.Sprintf(
			"CONSTRAINT %s UNIQUE (%s)",
			d.Quote(uniqueKey.Name),
			strings.Join(quotedColumns, ", "),
		)
		columnDefs = append(columnDefs, uniqueKeyDef)
	}

	// Add foreign key constraints
	for _, foreignKey := range t.ForeignKeys {
		quotedColumns := make([]string, len(foreignKey.Columns))
		for i, col := range foreignKey.Columns {
			quotedColumns[i] = d.Quote(col)
		}

		quotedRefColumns := make([]string, len(foreignKey.ReferenceColumns))
		for i, col := range foreignKey.ReferenceColumns {
			quotedRefColumns[i] = d.Quote(col)
		}

		foreignKeyDef := fmt.Sprintf(
			"CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s ON UPDATE %s",
			d.Quote(foreignKey.Name),
			strings.Join(quotedColumns, ", "),
			d.Quote(foreignKey.ReferenceTable),
			strings.Join(quotedRefColumns, ", "),
			foreignKey.OnDelete,
			foreignKey.OnUpdate,
		)
		columnDefs = append(columnDefs, foreignKeyDef)
	}

	return d.CreateTableSQL(t.Name, columnDefs, t.PrimaryKey.Name)
}

// BuildFromStruct builds a schema from a struct
func BuildFromStruct(model interface{}, tableName string) (*Table, error) {
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a struct, got %s", v.Kind())
	}

	t := v.Type()
	table := NewTable(tableName)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get tag options
		tag := field.Tag.Get("db")
		if tag == "-" {
			continue
		}

		tagParts := strings.Split(tag, ",")
		columnName := field.Name
		if len(tagParts) > 0 && tagParts[0] != "" {
			columnName = tagParts[0]
		} else {
			// Convert to snake_case
			columnName = toSnakeCase(field.Name)
		}

		column := NewColumn(columnName, "")

		// Process tag options
		for _, opt := range tagParts[1:] {
			switch opt {
			case "pk":
				column.IsPrimaryKey = true
			case "auto":
				column.IsAutoIncrement = true
			case "nullable":
				column.Nullable = true
			case "unique":
				column.Unique = true
			}

			if strings.HasPrefix(opt, "size:") {
				fmt.Sscanf(strings.TrimPrefix(opt, "size:"), "%d", &column.Size)
			}

			if strings.HasPrefix(opt, "precision:") {
				fmt.Sscanf(strings.TrimPrefix(opt, "precision:"), "%d", &column.Precision)
			}

			if strings.HasPrefix(opt, "scale:") {
				fmt.Sscanf(strings.TrimPrefix(opt, "scale:"), "%d", &column.Scale)
			}

			if strings.HasPrefix(opt, "default:") {
				column.Default = strings.TrimPrefix(opt, "default:")
			}
		}

		table.AddColumn(column)
	}

	return table, nil
}

// toSnakeCase converts a camelCase string to snake_case
func toSnakeCase(s string) string {
	var result strings.Builder

	for i, r := range s {
		if i > 0 && 'A' <= r && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}

	return strings.ToLower(result.String())
}
