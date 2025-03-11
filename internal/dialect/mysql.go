package dialect

import (
	"fmt"
	"reflect"
	"strings"
)

// MySQLDialect implements SQL dialect for MySQL
type MySQLDialect struct{}

// Name returns the dialect name
func (d *MySQLDialect) Name() string {
	return "mysql"
}

// Quote quotes an identifier
func (d *MySQLDialect) Quote(identifier string) string {
	return fmt.Sprintf("`%s`", strings.Replace(identifier, "`", "``", -1))
}

// Placeholder returns the parameter placeholder for the given position
func (d *MySQLDialect) Placeholder(position int) string {
	return "?"
}

// DataType maps Go types to database types
func (d *MySQLDialect) DataType(fieldType reflect.Type, size int, precision int, scale int) string {
	switch fieldType.Kind() {
	case reflect.Bool:
		return "TINYINT(1)"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "INT"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INT UNSIGNED"
	case reflect.Uint64:
		return "BIGINT UNSIGNED"
	case reflect.Float32, reflect.Float64:
		if precision > 0 {
			if scale > 0 {
				return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
			}
			return fmt.Sprintf("DECIMAL(%d)", precision)
		}
		return "DOUBLE"
	case reflect.String:
		if size > 0 {
			if size < 65536 {
				return fmt.Sprintf("VARCHAR(%d)", size)
			}
			return "TEXT"
		}
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
func (d *MySQLDialect) AutoIncrementKeyword() string {
	return "AUTO_INCREMENT"
}

// CreateTableSQL generates SQL for table creation
func (d *MySQLDialect) CreateTableSQL(tableName string, columns []string, primaryKey string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
		quotedTable,
		strings.Join(columns, ",\n  "),
	)
}

// AddColumnSQL generates SQL for adding a column
func (d *MySQLDialect) AddColumnSQL(tableName, columnDef string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", quotedTable, columnDef)
}

// DropColumnSQL generates SQL for dropping a column
func (d *MySQLDialect) DropColumnSQL(tableName, columnName string) string {
	quotedTable := d.Quote(tableName)
	quotedColumn := d.Quote(columnName)
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quotedTable, quotedColumn)
}

// CreateIndexSQL generates SQL for creating an index
func (d *MySQLDialect) CreateIndexSQL(tableName, indexName string, columns []string, unique bool) string {
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
func (d *MySQLDialect) DropIndexSQL(tableName, indexName string) string {
	quotedTable := d.Quote(tableName)
	quotedIndex := d.Quote(indexName)
	return fmt.Sprintf("DROP INDEX %s ON %s", quotedIndex, quotedTable)
}

// TruncateTableSQL generates SQL for truncating a table
func (d *MySQLDialect) TruncateTableSQL(tableName string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf("TRUNCATE TABLE %s", quotedTable)
}

// DropTableSQL generates SQL for dropping a table
func (d *MySQLDialect) DropTableSQL(tableName string) string {
	quotedTable := d.Quote(tableName)
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", quotedTable)
}

// RenameTableSQL generates SQL for renaming a table
func (d *MySQLDialect) RenameTableSQL(oldName, newName string) string {
	quotedOld := d.Quote(oldName)
	quotedNew := d.Quote(newName)
	return fmt.Sprintf("RENAME TABLE %s TO %s", quotedOld, quotedNew)
}

// CurrentDatabaseSQL generates SQL for getting the current database name
func (d *MySQLDialect) CurrentDatabaseSQL() string {
	return "SELECT DATABASE()"
}

// ListTablesSQL generates SQL for listing all tables
func (d *MySQLDialect) ListTablesSQL() string {
	return "SHOW TABLES"
}

// TableExistsSQL generates SQL for checking if a table exists
func (d *MySQLDialect) TableExistsSQL(tableName string) string {
	return fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s'",
		tableName,
	)
}
