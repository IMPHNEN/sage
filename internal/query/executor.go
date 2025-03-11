package query

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

// Executor executes SQL queries
type Executor struct {
	db *sql.DB
	tx *sql.Tx
}

// NewExecutor creates a new query executor
func NewExecutor(db *sql.DB) *Executor {
	return &Executor{
		db: db,
	}
}

// NewTxExecutor creates a new query executor with a transaction
func NewTxExecutor(tx *sql.Tx) *Executor {
	return &Executor{
		tx: tx,
	}
}

// execer is an interface that can execute queries
type execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// getExecer returns the appropriate execer (db or tx)
func (e *Executor) getExecer() execer {
	if e.tx != nil {
		return e.tx
	}
	return e.db
}

// Exec executes a query without returning any rows
func (e *Executor) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return e.getExecer().ExecContext(ctx, query, args...)
}

// Query executes a query that returns rows
func (e *Executor) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return e.getExecer().QueryContext(ctx, query, args...)
}

// QueryRow executes a query that returns a single row
func (e *Executor) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return e.getExecer().QueryRowContext(ctx, query, args...)
}

// QueryOne executes a query and scans the result into a struct
func (e *Executor) QueryOne(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.New("destination must be a non-nil pointer")
	}

	elem := v.Elem()
	if elem.Kind() != reflect.Struct {
		return errors.New("destination must be a pointer to a struct")
	}

	row := e.QueryRow(ctx, query, args...)
	return scanStruct(row, dest)
}

// QueryAll executes a query and scans the results into a slice of structs
func (e *Executor) QueryAll(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.New("destination must be a non-nil pointer")
	}

	elem := v.Elem()
	if elem.Kind() != reflect.Slice {
		return errors.New("destination must be a pointer to a slice")
	}

	sliceElemType := elem.Type().Elem()
	isPtr := sliceElemType.Kind() == reflect.Ptr

	// Get the struct type
	structType := sliceElemType
	if isPtr {
		structType = structType.Elem()
	}

	if structType.Kind() != reflect.Struct {
		return errors.New("destination must be a pointer to a slice of structs or struct pointers")
	}

	rows, err := e.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column names from the query
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Create a map of column names to field indices
	fieldMap := make(map[string]int)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		// Get the column name from the db tag or use the field name
		tagValue := field.Tag.Get("db")
		if tagValue == "-" {
			continue
		}

		colName := field.Name
		if tagValue != "" {
			// Extract the column name from the tag
			parts := strings.Split(tagValue, ",")
			if parts[0] != "" {
				colName = parts[0]
			}
		}

		// Convert to lowercase for case-insensitive matching
		colName = strings.ToLower(colName)
		fieldMap[colName] = i
	}

	// Prepare the destination slice
	for rows.Next() {
		// Create a new struct instance
		newElem := reflect.New(structType).Elem()

		// Create a slice of pointers to each field
		fieldPtrs := make([]interface{}, len(columns))
		for i, col := range columns {
			// Convert column name to lowercase for case-insensitive matching
			colLower := strings.ToLower(col)

			if fieldIdx, ok := fieldMap[colLower]; ok {
				// We found a matching field
				fieldPtrs[i] = newElem.Field(fieldIdx).Addr().Interface()
			} else {
				// No matching field, use a placeholder
				var placeholder interface{}
				fieldPtrs[i] = &placeholder
			}
		}

		// Scan the row into the field pointers
		if err := rows.Scan(fieldPtrs...); err != nil {
			return err
		}

		// Add the new element to the slice
		if isPtr {
			elem.Set(reflect.Append(elem, newElem.Addr()))
		} else {
			elem.Set(reflect.Append(elem, newElem))
		}
	}

	return rows.Err()
}

// scanStruct scans a row into a struct
func scanStruct(row *sql.Row, dest interface{}) error {
	v := reflect.ValueOf(dest).Elem()
	t := v.Type()

	// Create a slice of pointers to each field
	numFields := t.NumField()
	fieldPtrs := make([]interface{}, numFields)

	for i := 0; i < numFields; i++ {
		if !v.Field(i).CanAddr() {
			continue
		}
		fieldPtrs[i] = v.Field(i).Addr().Interface()
	}

	// Scan the row into the field pointers
	return row.Scan(fieldPtrs...)
}

// Count executes a query and returns the count
func (e *Executor) Count(ctx context.Context, query string, args ...interface{}) (int64, error) {
	row := e.QueryRow(ctx, query, args...)
	var count int64
	err := row.Scan(&count)
	return count, err
}

// WithTransaction executes a function within a transaction
func (e *Executor) WithTransaction(ctx context.Context, fn func(*Executor) error) error {
	// Start a transaction
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Create a new executor with the transaction
	txExecutor := NewTxExecutor(tx)

	// Execute the function
	err = fn(txExecutor)

	// Commit or rollback the transaction
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
