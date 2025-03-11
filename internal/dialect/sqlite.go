package dialect

import (
	"fmt"
	"reflect"
	"strings"
)

// SQLiteDialect implements SQL dialect for SQLite
type SQLiteDialect struct{}

// Name returns the dialect name
func (d *SQLiteDialect) Name() string {
	return "sqlite"
}

// Quote quotes an identifier
func (d *SQLiteDialect) Quote(identifier string) string {
	return fmt.Sprintf("\"%s\"", strings.Replace(identifier, "\"", "\"\"", -1))
}

// Placeholder returns the parameter placeholder for the given position
func (d *SQLiteDialect) Placeholder(position int) string {
	return "?"
}

// DataType maps Go types to database types
func (d *SQLiteDialect) DataType(fieldType reflect.Type, size int, precision int, scale int) string {
	switch fieldType.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INTEGER"
	case reflect.Int64, reflect.Uint64:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "REAL"
	case reflect.String:
		return "TEXT"
	case reflect.Struct:
		if fieldType.String() == "time.Time" {
			return "DATETIME"
		}
	case reflect.Slice:
		if fieldType.Elem().Kind() == reflect.Uint8 {
			return "BLOB"
		}
	}

	return "TEXT"
}

// AutoIncrementKeyword returns the keyword for auto-increment columns
func (d *SQLiteDialect) AutoIncrementKeyword() string {
	return "INTEGER PRIMARY KEY AUTOINCREMENT"
}

// CreateTableSQL generates SQL for table creation
func (d *SQLiteDialect) CreateTableSQL(tableName string, columns []string, primaryKey string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		quotedTable,
		strings.Join(columns, ",\n  "),
	)
}

// AddColumnSQL generates SQL for adding a column
func (d *SQLiteDialect) AddColumnSQL(tableName, columnDef string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", quotedTable, columnDef)
}

// DropColumnSQL generates SQL for dropping a column
// Note: SQLite doesn't support DROP COLUMN directly - requires a table rebuild
func (d *SQLiteDialect) DropColumnSQL(tableName, columnName string) string {
	return fmt.Sprintf("-- SQLite doesn't support DROP COLUMN directly\n-- To drop %s from %s, you need to:\n-- 1. Create a new table without the column\n-- 2. Copy data\n-- 3. Drop old table\n-- 4. Rename new table", columnName, tableName)
}

// CreateIndexSQL generates SQL for creating an index
func (d *SQLiteDialect) CreateIndexSQL(tableName, indexName string, columns []string, unique bool) string {
	quotedTable := d.Quote(tableName)
	quotedIndex := d.Quote(indexName)

	uniqueStr := ""
	if unique {
		uniqueStr = "UNIQUE "
	}

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.Quote(col)
	}

	return fmt.Sprintf(
		"CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		uniqueStr,
		quotedIndex,
		quotedTable,
		strings.Join(quotedColumns, ", "),
	)
}

// DropIndexSQL generates SQL for dropping an index
func (d *SQLiteDialect) DropIndexSQL(tableName, indexName string) string {
	quotedIndex := d.Quote(indexName)
	return fmt.Sprintf("DROP INDEX IF EXISTS %s", quotedIndex)
}

// TruncateTableSQL generates SQL for truncating a table
func (d *SQLiteDialect) TruncateTableSQL(tableName string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf("DELETE FROM %s", quotedTable)
}

// DropTableSQL generates SQL for dropping a table
func (d *SQLiteDialect) DropTableSQL(tableName string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTable)
}

// RenameTableSQL generates SQL for renaming a table
func (d *SQLiteDialect) RenameTableSQL(oldName, newName string) string {
	quotedOld := d.Quote(oldName)
	quotedNew := d.Quote(newName)
	return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quotedOld, quotedNew)
}

// CurrentDatabaseSQL generates SQL for getting the current database name
func (d *SQLiteDialect) CurrentDatabaseSQL() string {
	return "PRAGMA database_list"
}

// ListTablesSQL generates SQL for listing all tables
func (d *SQLiteDialect) ListTablesSQL() string {
	return "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
}

// TableExistsSQL generates SQL for checking if a table exists
func (d *SQLiteDialect) TableExistsSQL(tableName string) string {
	return fmt.Sprintf(
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s'",
		tableName,
	)
}
