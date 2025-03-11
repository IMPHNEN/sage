package sage

import (
	"fmt"
	"strings"
)

// QueryBuilder builds SQL queries
type QueryBuilder struct {
	table        string
	columns      []string
	whereClause  []string
	whereArgs    []interface{}
	orderBy      []string
	limit        int
	offset       int
	joins        []string
	groupBy      []string
	havingClause []string
	havingArgs   []interface{}
	operation    string
	values       map[string]interface{}
}

// NewQueryBuilder creates a new query builder for the given table
func NewQueryBuilder(table string) *QueryBuilder {
	return &QueryBuilder{
		table:        table,
		columns:      []string{},
		whereClause:  []string{},
		whereArgs:    []interface{}{},
		orderBy:      []string{},
		limit:        0,
		offset:       0,
		joins:        []string{},
		groupBy:      []string{},
		havingClause: []string{},
		havingArgs:   []interface{}{},
		operation:    "",
		values:       make(map[string]interface{}),
	}
}

// Select sets the columns to select
func (qb *QueryBuilder) Select(columns ...string) *QueryBuilder {
	qb.operation = "SELECT"
	qb.columns = columns
	return qb
}

// Insert prepares an insert operation
func (qb *QueryBuilder) Insert() *QueryBuilder {
	qb.operation = "INSERT"
	return qb
}

// Update prepares an update operation
func (qb *QueryBuilder) Update() *QueryBuilder {
	qb.operation = "UPDATE"
	return qb
}

// Delete prepares a delete operation
func (qb *QueryBuilder) Delete() *QueryBuilder {
	qb.operation = "DELETE"
	return qb
}

// Where adds a WHERE condition
func (qb *QueryBuilder) Where(condition string, args ...interface{}) *QueryBuilder {
	qb.whereClause = append(qb.whereClause, condition)
	qb.whereArgs = append(qb.whereArgs, args...)
	return qb
}

// OrderBy adds an ORDER BY clause
func (qb *QueryBuilder) OrderBy(columns ...string) *QueryBuilder {
	qb.orderBy = append(qb.orderBy, columns...)
	return qb
}

// Limit sets the LIMIT clause
func (qb *QueryBuilder) Limit(limit int) *QueryBuilder {
	qb.limit = limit
	return qb
}

// Offset sets the OFFSET clause
func (qb *QueryBuilder) Offset(offset int) *QueryBuilder {
	qb.offset = offset
	return qb
}

// Join adds a JOIN clause
func (qb *QueryBuilder) Join(join string) *QueryBuilder {
	qb.joins = append(qb.joins, join)
	return qb
}

// GroupBy adds a GROUP BY clause
func (qb *QueryBuilder) GroupBy(columns ...string) *QueryBuilder {
	qb.groupBy = append(qb.groupBy, columns...)
	return qb
}

// Having adds a HAVING condition
func (qb *QueryBuilder) Having(condition string, args ...interface{}) *QueryBuilder {
	qb.havingClause = append(qb.havingClause, condition)
	qb.havingArgs = append(qb.havingArgs, args...)
	return qb
}

// Set adds a column value for INSERT or UPDATE
func (qb *QueryBuilder) Set(column string, value interface{}) *QueryBuilder {
	qb.values[column] = value
	return qb
}

// Build constructs the SQL query
func (qb *QueryBuilder) Build() (string, []interface{}) {
	var query strings.Builder
	var args []interface{}

	switch qb.operation {
	case "SELECT":
		query.WriteString("SELECT ")
		if len(qb.columns) == 0 {
			query.WriteString("*")
		} else {
			query.WriteString(strings.Join(qb.columns, ", "))
		}
		query.WriteString(" FROM ")
		query.WriteString(qb.table)

		// Add joins
		for _, join := range qb.joins {
			query.WriteString(" ")
			query.WriteString(join)
		}

		// Add where clause
		if len(qb.whereClause) > 0 {
			query.WriteString(" WHERE ")
			query.WriteString(strings.Join(qb.whereClause, " AND "))
			args = append(args, qb.whereArgs...)
		}

		// Add group by
		if len(qb.groupBy) > 0 {
			query.WriteString(" GROUP BY ")
			query.WriteString(strings.Join(qb.groupBy, ", "))
		}

		// Add having
		if len(qb.havingClause) > 0 {
			query.WriteString(" HAVING ")
			query.WriteString(strings.Join(qb.havingClause, " AND "))
			args = append(args, qb.havingArgs...)
		}

		// Add order by
		if len(qb.orderBy) > 0 {
			query.WriteString(" ORDER BY ")
			query.WriteString(strings.Join(qb.orderBy, ", "))
		}

		// Add limit
		if qb.limit > 0 {
			query.WriteString(fmt.Sprintf(" LIMIT %d", qb.limit))
		}

		// Add offset
		if qb.offset > 0 {
			query.WriteString(fmt.Sprintf(" OFFSET %d", qb.offset))
		}

	case "INSERT":
		query.WriteString("INSERT INTO ")
		query.WriteString(qb.table)

		var columns []string
		var placeholders []string

		for column := range qb.values {
			columns = append(columns, column)
			placeholders = append(placeholders, "?")
			args = append(args, qb.values[column])
		}

		query.WriteString(" (")
		query.WriteString(strings.Join(columns, ", "))
		query.WriteString(") VALUES (")
		query.WriteString(strings.Join(placeholders, ", "))
		query.WriteString(")")

	case "UPDATE":
		query.WriteString("UPDATE ")
		query.WriteString(qb.table)
		query.WriteString(" SET ")

		var sets []string
		for column, value := range qb.values {
			sets = append(sets, fmt.Sprintf("%s = ?", column))
			args = append(args, value)
		}

		query.WriteString(strings.Join(sets, ", "))

		// Add where clause
		if len(qb.whereClause) > 0 {
			query.WriteString(" WHERE ")
			query.WriteString(strings.Join(qb.whereClause, " AND "))
			args = append(args, qb.whereArgs...)
		}

	case "DELETE":
		query.WriteString("DELETE FROM ")
		query.WriteString(qb.table)

		// Add where clause
		if len(qb.whereClause) > 0 {
			query.WriteString(" WHERE ")
			query.WriteString(strings.Join(qb.whereClause, " AND "))
			args = append(args, qb.whereArgs...)
		}
	}

	return query.String(), args
}
