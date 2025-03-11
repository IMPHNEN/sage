package sage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

var (
	// ErrNotFound indicates that a record was not found
	ErrNotFound = errors.New("record not found")
	// ErrNotAStruct indicates that the provided model is not a struct
	ErrNotAStruct = errors.New("model must be a struct")
	// ErrNoID indicates that no ID field was found for the model
	ErrNoID = errors.New("model does not have an ID field")
)

// Executor is an interface that can execute database operations
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// Create inserts a new record into the database
func (c *Connection) Create(ctx context.Context, model interface{}) error {
	info, err := extractModelInfo(model)
	if err != nil {
		return err
	}

	qb := NewQueryBuilder(info.TableName).Insert()

	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for _, field := range info.Fields {
		// Skip auto-increment primary key fields
		if field.IsKey && field.IsAuto {
			continue
		}

		fieldValue := v.FieldByName(field.Name)
		qb.Set(field.DBName, fieldValue.Interface())
	}

	query, args := qb.Build()
	result, err := c.DB().ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	// If the model has an auto-increment primary key, set it
	if id, err := result.LastInsertId(); err == nil {
		for _, field := range info.Fields {
			if field.IsKey && field.IsAuto {
				idField := v.FieldByName(field.Name)
				if idField.CanSet() {
					idField.Set(reflect.ValueOf(id).Convert(idField.Type()))
				}
				break
			}
		}
	}

	return nil
}

// Find finds a record by its primary key
func (c *Connection) Find(ctx context.Context, model interface{}, id interface{}) error {
	info, err := extractModelInfo(model)
	if err != nil {
		return err
	}

	qb := NewQueryBuilder(info.TableName).Select()
	qb.Where(info.PrimaryKey+" = ?", id)

	query, args := qb.Build()
	return c.scanRow(ctx, model, query, args...)
}

// First finds the first record matching the conditions
func (c *Connection) First(ctx context.Context, model interface{}, conditions string, args ...interface{}) error {
	info, err := extractModelInfo(model)
	if err != nil {
		return err
	}

	qb := NewQueryBuilder(info.TableName).Select()
	if conditions != "" {
		qb.Where(conditions, args...)
	}
	qb.Limit(1)

	query, queryArgs := qb.Build()
	return c.scanRow(ctx, model, query, queryArgs...)
}

// scanRow scans a single row into the model
func (c *Connection) scanRow(ctx context.Context, model interface{}, query string, args ...interface{}) error {
	v := reflect.ValueOf(model)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.New("model must be a non-nil pointer")
	}
	v = v.Elem()

	row := c.DB().QueryRowContext(ctx, query, args...)
	fields, err := scanFields(v)
	if err != nil {
		return err
	}

	err = row.Scan(fields...)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	return err
}

// scanFields creates a slice of pointers to each field in the struct
func scanFields(v reflect.Value) ([]interface{}, error) {
	if v.Kind() != reflect.Struct {
		return nil, ErrNotAStruct
	}

	var fields []interface{}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.CanSet() {
			fields = append(fields, field.Addr().Interface())
		}
	}
	return fields, nil
}

// Update updates a record in the database
func (c *Connection) Update(ctx context.Context, model interface{}) error {
	info, err := extractModelInfo(model)
	if err != nil {
		return err
	}

	qb := NewQueryBuilder(info.TableName).Update()

	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	var idValue interface{}
	for _, field := range info.Fields {
		fieldValue := v.FieldByName(field.Name)

		if field.IsKey {
			idValue = fieldValue.Interface()
			continue
		}

		qb.Set(field.DBName, fieldValue.Interface())
	}

	if idValue == nil {
		return ErrNoID
	}

	qb.Where(info.PrimaryKey+" = ?", idValue)
	query, args := qb.Build()

	result, err := c.DB().ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return ErrNotFound
	}

	return nil
}

// Delete deletes a record from the database
func (c *Connection) Delete(ctx context.Context, model interface{}) error {
	info, err := extractModelInfo(model)
	if err != nil {
		return err
	}

	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	var idValue interface{}
	for _, field := range info.Fields {
		if field.IsKey {
			fieldValue := v.FieldByName(field.Name)
			idValue = fieldValue.Interface()
			break
		}
	}

	if idValue == nil {
		return ErrNoID
	}

	qb := NewQueryBuilder(info.TableName).Delete()
	qb.Where(info.PrimaryKey+" = ?", idValue)

	query, args := qb.Build()

	result, err := c.DB().ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return ErrNotFound
	}

	return nil
}

// All finds all records matching the conditions
func (c *Connection) All(ctx context.Context, models interface{}, conditions string, args ...interface{}) error {
	sliceValue := reflect.ValueOf(models)
	if sliceValue.Kind() != reflect.Ptr || sliceValue.Elem().Kind() != reflect.Slice {
		return errors.New("models must be a pointer to a slice")
	}

	sliceValue = sliceValue.Elem()
	elemType := sliceValue.Type().Elem()

	// Create a new instance of the slice element type
	modelInstance := reflect.New(elemType).Interface()

	info, err := extractModelInfo(modelInstance)
	if err != nil {
		return err
	}

	qb := NewQueryBuilder(info.TableName).Select()
	if conditions != "" {
		qb.Where(conditions, args...)
	}

	query, queryArgs := qb.Build()

	rows, err := c.DB().QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column names from the query
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		// Create a new instance of the model
		modelElem := reflect.New(elemType).Elem()

		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row into the values
		if err := rows.Scan(values...); err != nil {
			return err
		}

		// Map the values to the model fields
		for i, col := range columns {
			// Find the corresponding field in the model
			for _, field := range info.Fields {
				if strings.EqualFold(field.DBName, col) {
					fieldValue := modelElem.FieldByName(field.Name)
					if fieldValue.CanSet() {
						val := reflect.ValueOf(*(values[i].(*interface{})))
						if val.Type().ConvertibleTo(fieldValue.Type()) {
							fieldValue.Set(val.Convert(fieldValue.Type()))
						}
					}
					break
				}
			}
		}

		// Append the model to the slice
		sliceValue.Set(reflect.Append(sliceValue, modelElem))
	}

	return rows.Err()
}
