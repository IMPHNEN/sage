package dialect

import (
	"reflect"
)

// Dialect defines methods that a SQL dialect must implement
type Dialect interface {
	// Name returns the dialect name
	Name() string

	// Quote quotes an identifier
	Quote(identifier string) string

	// Placeholder returns the parameter placeholder for the given position
	Placeholder(position int) string

	// DataType maps Go types to database types
	DataType(fieldType reflect.Type, size int, precision int, scale int) string

	// AutoIncrementKeyword returns the keyword for auto-increment columns
	AutoIncrementKeyword() string

	// CreateTableSQL generates SQL for table creation
	CreateTableSQL(tableName string, columns []string, primaryKey string) string

	// AddColumnSQL generates SQL for adding a column
	AddColumnSQL(tableName, columnDef string) string

	// DropColumnSQL generates SQL for dropping a column
	DropColumnSQL(tableName, columnName string) string

	// CreateIndexSQL generates SQL for creating an index
	CreateIndexSQL(tableName, indexName string, columns []string, unique bool) string

	// DropIndexSQL generates SQL for dropping an index
	DropIndexSQL(tableName, indexName string) string

	// TruncateTableSQL generates SQL for truncating a table
	TruncateTableSQL(tableName string) string

	// DropTableSQL generates SQL for dropping a table
	DropTableSQL(tableName string) string

	// RenameTableSQL generates SQL for renaming a table
	RenameTableSQL(oldName, newName string) string

	// CurrentDatabaseSQL generates SQL for getting the current database name
	CurrentDatabaseSQL() string

	// ListTablesSQL generates SQL for listing all tables
	ListTablesSQL() string

	// TableExistsSQL generates SQL for checking if a table exists
	TableExistsSQL(tableName string) string
}

// GetDialect returns a dialect by name
func GetDialect(name string) Dialect {
	switch name {
	case "postgres":
		return &PostgresDialect{}
	case "mysql":
		return &MySQLDialect{}
	case "sqlite":
		return &SQLiteDialect{}
	default:
		return nil
	}
}
