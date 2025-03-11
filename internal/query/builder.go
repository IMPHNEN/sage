package query

import (
	"fmt"
	"strings"

	"github.com/IMPHNEN/sage/internal/dialect"
)

// Builder builds SQL queries with dialect-specific formatting
type Builder struct {
	dialect    dialect.Dialect
	table      string
	columns    []string
	where      []string
	whereArgs  []interface{}
	orderBy    []string
	limit      int
	offset     int
	joins      []string
	groupBy    []string
	having     []string
	havingArgs []interface{}
	operation  string
	values     map[string]interface{}
	returning  []string
}

// NewBuilder creates a new query builder with the specified dialect
func NewBuilder(dialect dialect.Dialect, table string) *Builder {
	return &Builder{
		dialect:    dialect,
		table:      table,
		columns:    []string{},
		where:      []string{},
		whereArgs:  []interface{}{},
		orderBy:    []string{},
		limit:      0,
		offset:     0,
		joins:      []string{},
		groupBy:    []string{},
		having:     []string{},
		havingArgs: []interface{}{},
		operation:  "",
		values:     make(map[string]interface{}),
		returning:  []string{},
	}
}

// Select sets the columns to select
func (b *Builder) Select(columns ...string) *Builder {
	b.operation = "SELECT"
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		// Handle special case for *
		if col == "*" {
			quotedColumns[i] = col
		} else {
			quotedColumns[i] = b.dialect.Quote(col)
		}
	}
	b.columns = quotedColumns
	return b
}

// Insert prepares an insert operation
func (b *Builder) Insert() *Builder {
	b.operation = "INSERT"
	return b
}

// Update prepares an update operation
func (b *Builder) Update() *Builder {
	b.operation = "UPDATE"
	return b
}

// Delete prepares a delete operation
func (b *Builder) Delete() *Builder {
	b.operation = "DELETE"
	return b
}

// Where adds a WHERE condition
func (b *Builder) Where(condition string, args ...interface{}) *Builder {
	// Replace ? placeholders with dialect-specific placeholders
	if strings.Contains(condition, "?") {
		parts := strings.Split(condition, "?")
		newCondition := parts[0]

		for i := 1; i < len(parts); i++ {
			newCondition += b.dialect.Placeholder(len(b.whereArgs) + i)
			newCondition += parts[i]
		}

		condition = newCondition
	}

	b.where = append(b.where, condition)
	b.whereArgs = append(b.whereArgs, args...)
	return b
}

// OrderBy adds an ORDER BY clause
func (b *Builder) OrderBy(column string, direction string) *Builder {
	quotedCol := b.dialect.Quote(column)
	if strings.ToUpper(direction) == "DESC" {
		b.orderBy = append(b.orderBy, fmt.Sprintf("%s DESC", quotedCol))
	} else {
		b.orderBy = append(b.orderBy, fmt.Sprintf("%s ASC", quotedCol))
	}
	return b
}

// Limit sets the LIMIT clause
func (b *Builder) Limit(limit int) *Builder {
	b.limit = limit
	return b
}

// Offset sets the OFFSET clause
func (b *Builder) Offset(offset int) *Builder {
	b.offset = offset
	return b
}

// Join adds a JOIN clause
func (b *Builder) Join(table string, condition string) *Builder {
	quotedTable := b.dialect.Quote(table)
	join := fmt.Sprintf("JOIN %s ON %s", quotedTable, condition)
	b.joins = append(b.joins, join)
	return b
}

// LeftJoin adds a LEFT JOIN clause
func (b *Builder) LeftJoin(table string, condition string) *Builder {
	quotedTable := b.dialect.Quote(table)
	join := fmt.Sprintf("LEFT JOIN %s ON %s", quotedTable, condition)
	b.joins = append(b.joins, join)
	return b
}

// RightJoin adds a RIGHT JOIN clause
func (b *Builder) RightJoin(table string, condition string) *Builder {
	quotedTable := b.dialect.Quote(table)
	join := fmt.Sprintf("RIGHT JOIN %s ON %s", quotedTable, condition)
	b.joins = append(b.joins, join)
	return b
}

// GroupBy adds a GROUP BY clause
func (b *Builder) GroupBy(columns ...string) *Builder {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = b.dialect.Quote(col)
	}
	b.groupBy = append(b.groupBy, quotedColumns...)
	return b
}

// Having adds a HAVING condition
func (b *Builder) Having(condition string, args ...interface{}) *Builder {
	// Replace ? placeholders with dialect-specific placeholders
	if strings.Contains(condition, "?") {
		parts := strings.Split(condition, "?")
		newCondition := parts[0]

		for i := 1; i < len(parts); i++ {
			newCondition += b.dialect.Placeholder(len(b.havingArgs) + i)
			newCondition += parts[i]
		}

		condition = newCondition
	}

	b.having = append(b.having, condition)
	b.havingArgs = append(b.havingArgs, args...)
	return b
}

// Set adds a column value for INSERT or UPDATE
func (b *Builder) Set(column string, value interface{}) *Builder {
	b.values[column] = value
	return b
}

// Returning adds a RETURNING clause (for PostgreSQL)
func (b *Builder) Returning(columns ...string) *Builder {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = b.dialect.Quote(col)
	}
	b.returning = quotedColumns
	return b
}

// Build constructs the SQL query and parameters
func (b *Builder) Build() (string, []interface{}) {
	var query strings.Builder
	var args []interface{}

	quotedTable := b.dialect.Quote(b.table)

	switch b.operation {
	case "SELECT":
		query.WriteString("SELECT ")
		if len(b.columns) == 0 {
			query.WriteString("*")
		} else {
			query.WriteString(strings.Join(b.columns, ", "))
		}
		query.WriteString(" FROM ")
		query.WriteString(quotedTable)

		// Add joins
		for _, join := range b.joins {
			query.WriteString(" ")
			query.WriteString(join)
		}

		// Add where clause
		if len(b.where) > 0 {
			query.WriteString(" WHERE ")
			query.WriteString(strings.Join(b.where, " AND "))
			args = append(args, b.whereArgs...)
		}

		// Add group by
		if len(b.groupBy) > 0 {
			query.WriteString(" GROUP BY ")
			query.WriteString(strings.Join(b.groupBy, ", "))
		}

		// Add having
		if len(b.having) > 0 {
			query.WriteString(" HAVING ")
			query.WriteString(strings.Join(b.having, " AND "))
			args = append(args, b.havingArgs...)
		}

		// Add order by
		if len(b.orderBy) > 0 {
			query.WriteString(" ORDER BY ")
			query.WriteString(strings.Join(b.orderBy, ", "))
		}

		// Add limit
		if b.limit > 0 {
			query.WriteString(fmt.Sprintf(" LIMIT %d", b.limit))
		}

		// Add offset
		if b.offset > 0 {
			query.WriteString(fmt.Sprintf(" OFFSET %d", b.offset))
		}

	case "INSERT":
		query.WriteString("INSERT INTO ")
		query.WriteString(quotedTable)

		var columns []string
		var placeholders []string
		var values []interface{}

		i := 1
		for column, value := range b.values {
			columns = append(columns, b.dialect.Quote(column))
			placeholders = append(placeholders, b.dialect.Placeholder(i))
			values = append(values, value)
			i++
		}

		query.WriteString(" (")
		query.WriteString(strings.Join(columns, ", "))
		query.WriteString(") VALUES (")
		query.WriteString(strings.Join(placeholders, ", "))
		query.WriteString(")")

		// Add returning clause for PostgreSQL
		if len(b.returning) > 0 && b.dialect.Name() == "postgres" {
			query.WriteString(" RETURNING ")
			query.WriteString(strings.Join(b.returning, ", "))
		}

		args = values

	case "UPDATE":
		query.WriteString("UPDATE ")
		query.WriteString(quotedTable)
		query.WriteString(" SET ")

		var sets []string
		var values []interface{}

		i := 1
		for column, value := range b.values {
			sets = append(sets, fmt.Sprintf("%s = %s", b.dialect.Quote(column), b.dialect.Placeholder(i)))
			values = append(values, value)
			i++
		}

		query.WriteString(strings.Join(sets, ", "))

		// Add where clause
		if len(b.where) > 0 {
			query.WriteString(" WHERE ")
			query.WriteString(strings.Join(b.where, " AND "))
		}

		// Add returning clause for PostgreSQL
		if len(b.returning) > 0 && b.dialect.Name() == "postgres" {
			query.WriteString(" RETURNING ")
			query.WriteString(strings.Join(b.returning, ", "))
		}

		args = append(values, b.whereArgs...)

	case "DELETE":
		query.WriteString("DELETE FROM ")
		query.WriteString(quotedTable)

		// Add where clause
		if len(b.where) > 0 {
			query.WriteString(" WHERE ")
			query.WriteString(strings.Join(b.where, " AND "))
		}

		// Add returning clause for PostgreSQL
		if len(b.returning) > 0 && b.dialect.Name() == "postgres" {
			query.WriteString(" RETURNING ")
			query.WriteString(strings.Join(b.returning, ", "))
		}

		args = b.whereArgs
	}

	return query.String(), args
}
