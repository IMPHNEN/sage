package dialect

import (
	"fmt"
	"reflect"
	"strings"
)

// PostgresDialect implements SQL dialect for PostgreSQL
type PostgresDialect struct{}

// Name returns the dialect name
func (d *PostgresDialect) Name() string {
	return "postgres"
}

// Quote quotes an identifier
func (d *PostgresDialect) Quote(identifier string) string {
	return fmt.Sprintf("\"%s\"", strings.Replace(identifier, "\"", "\"\"", -1))
}

// Placeholder returns the parameter placeholder for the given position
func (d *PostgresDialect) Placeholder(position int) string {
	return fmt.Sprintf("$%d", position)
}

// DataType maps Go types to database types
func (d *PostgresDialect) DataType(fieldType reflect.Type, size int, precision int, scale int) string {
	switch fieldType.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "INTEGER"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INTEGER"
	case reflect.Uint64:
		return "BIGINT"
	case reflect.Float32, reflect.Float64:
		if precision > 0 {
			if scale > 0 {
				return fmt.Sprintf("NUMERIC(%d,%d)", precision, scale)
			}
			return fmt.Sprintf("NUMERIC(%d)", precision)
		}
		return "DOUBLE PRECISION"
	case reflect.String:
		if size > 0 {
			return fmt.Sprintf("VARCHAR(%d)", size)
		}
		return "TEXT"
	case reflect.Struct:
		if fieldType.String() == "time.Time" {
			return "TIMESTAMP WITH TIME ZONE"
		}
	case reflect.Slice:
		if fieldType.Elem().Kind() == reflect.Uint8 {
			return "BYTEA"
		}
	}

	return "TEXT"
}

// AutoIncrementKeyword returns the keyword for auto-increment columns
func (d *PostgresDialect) AutoIncrementKeyword() string {
	return "SERIAL"
}

// CreateTableSQL generates SQL for table creation
func (d *PostgresDialect) CreateTableSQL(tableName string, columns []string, primaryKey string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		quotedTable,
		strings.Join(columns, ",\n  "),
	)
}

// AddColumnSQL generates SQL for adding a column
func (d *PostgresDialect) AddColumnSQL(tableName, columnDef string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", quotedTable, columnDef)
}

// DropColumnSQL generates SQL for dropping a column
func (d *PostgresDialect) DropColumnSQL(tableName, columnName string) string {
	quotedTable := d.Quote(tableName)
	quotedColumn := d.Quote(columnName)
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quotedTable, quotedColumn)
}

// CreateIndexSQL generates SQL for creating an index
func (d *PostgresDialect) CreateIndexSQL(tableName, indexName string, columns []string, unique bool) string {
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
		"CREATE %sINDEX %s ON %s (%s)",
		uniqueStr,
		quotedIndex,
		quotedTable,
		strings.Join(quotedColumns, ", "),
	)
}

// DropIndexSQL generates SQL for dropping an index
func (d *PostgresDialect) DropIndexSQL(tableName, indexName string) string {
	quotedIndex := d.Quote(indexName)
	return fmt.Sprintf("DROP INDEX %s", quotedIndex)
}

// TruncateTableSQL generates SQL for truncating a table
func (d *PostgresDialect) TruncateTableSQL(tableName string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf("TRUNCATE TABLE %s", quotedTable)
}

// DropTableSQL generates SQL for dropping a table
func (d *PostgresDialect) DropTableSQL(tableName string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTable)
}

// RenameTableSQL generates SQL for renaming a table
func (d *PostgresDialect) RenameTableSQL(oldName, newName string) string {
	quotedOld := d.Quote(oldName)
	quotedNew := d.Quote(newName)
	return fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quotedOld, quotedNew)
}

// CurrentDatabaseSQL generates SQL for getting the current database name
func (d *PostgresDialect) CurrentDatabaseSQL() string {
	return "SELECT current_database()"
}

// ListTablesSQL generates SQL for listing all tables
func (d *PostgresDialect) ListTablesSQL() string {
	return "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
}

// TableExistsSQL generates SQL for checking if a table exists
func (d *PostgresDialect) TableExistsSQL(tableName string) string {
	return fmt.Sprintf(
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '%s')",
		tableName,
	)
}
