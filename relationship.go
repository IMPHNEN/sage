package sage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// RelationshipType defines the type of relationship between models
type RelationshipType int

const (
	// HasOne represents a one-to-one relationship (a User has one Profile)
	HasOne RelationshipType = iota
	// BelongsTo represents a belongs-to relationship (a Profile belongs to a User)
	BelongsTo
	// HasMany represents a one-to-many relationship (a User has many Posts)
	HasMany
	// ManyToMany represents a many-to-many relationship (a Post has many Tags)
	ManyToMany
)

// Relationship defines a relationship between models
type Relationship struct {
	Type           RelationshipType
	Model          interface{}
	ForeignKey     string
	ReferenceKey   string
	JoinTable      string
	JoinForeignKey string
	JoinRefKey     string
	Preload        bool
}

// RelationshipOptions defines options for a relationship
type RelationshipOptions struct {
	ForeignKey     string
	ReferenceKey   string
	JoinTable      string
	JoinForeignKey string
	JoinRefKey     string
	Preload        bool
}

// validateRelationship validates a relationship
func validateRelationship(sourceType reflect.Type, rel *Relationship) error {
	if rel.Model == nil {
		return errors.New("model is required for relationship")
	}

	relType := reflect.TypeOf(rel.Model)
	if relType.Kind() == reflect.Ptr {
		relType = relType.Elem()
	}

	// Validate relationship type
	switch rel.Type {
	case HasOne, BelongsTo, HasMany:
		if rel.ForeignKey == "" {
			return errors.New("foreign key is required for HasOne, BelongsTo, and HasMany relationships")
		}
		if rel.ReferenceKey == "" {
			return errors.New("reference key is required for HasOne, BelongsTo, and HasMany relationships")
		}
	case ManyToMany:
		if rel.JoinTable == "" {
			return errors.New("join table is required for ManyToMany relationship")
		}
		if rel.JoinForeignKey == "" {
			return errors.New("join foreign key is required for ManyToMany relationship")
		}
		if rel.JoinRefKey == "" {
			return errors.New("join reference key is required for ManyToMany relationship")
		}
	default:
		return fmt.Errorf("invalid relationship type: %d", rel.Type)
	}

	return nil
}

// preloadHasOne preloads a HasOne relationship
func (c *Connection) preloadHasOne(ctx context.Context, source interface{}, field string, rel *Relationship) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	if sourceValue.Kind() != reflect.Struct {
		return errors.New("source must be a struct or pointer to struct")
	}

	// Get the primary key value from the source
	sourceInfo, err := extractModelInfo(source)
	if err != nil {
		return err
	}

	pkField := sourceValue.FieldByName(toFieldName(sourceInfo.PrimaryKey))
	if !pkField.IsValid() {
		return fmt.Errorf("primary key field %s not found in source model", sourceInfo.PrimaryKey)
	}

	// Create a new instance of the related model
	relType := reflect.TypeOf(rel.Model)
	if relType.Kind() == reflect.Ptr {
		relType = relType.Elem()
	}

	// Build the query to find the related model
	relModel := reflect.New(relType).Interface()
	relInfo, err := extractModelInfo(relModel)
	if err != nil {
		return err
	}

	builder := NewQueryBuilder(relInfo.TableName).
		Select().
		Where(fmt.Sprintf("%s = ?", rel.ForeignKey), pkField.Interface())

	query, args := builder.Build()

	// Execute the query
	rows, err := c.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		// No related model found
		return nil
	}

	// Get column names from the query
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Create a new instance of the related model
	relValue := reflect.New(relType).Elem()

	// Create a slice of interface{} to hold the values
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	// Scan the row into the values
	if err := rows.Scan(values...); err != nil {
		return err
	}

	// Map the values to the related model fields
	for i, col := range columns {
		// Find the corresponding field in the model
		for _, field := range relInfo.Fields {
			if strings.EqualFold(field.DBName, col) {
				fieldValue := relValue.FieldByName(field.Name)
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

	// Set the related model to the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.CanSet() {
		return fmt.Errorf("field %s is not settable", field)
	}

	fieldValue.Set(relValue.Addr())

	return rows.Err()
}

// preloadBelongsTo preloads a BelongsTo relationship
func (c *Connection) preloadBelongsTo(ctx context.Context, source interface{}, field string, rel *Relationship) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	if sourceValue.Kind() != reflect.Struct {
		return errors.New("source must be a struct or pointer to struct")
	}

	// Get the foreign key value from the source
	fkField := sourceValue.FieldByName(toFieldName(rel.ForeignKey))
	if !fkField.IsValid() {
		return fmt.Errorf("foreign key field %s not found in source model", rel.ForeignKey)
	}

	// Create a new instance of the related model
	relType := reflect.TypeOf(rel.Model)
	if relType.Kind() == reflect.Ptr {
		relType = relType.Elem()
	}

	// Build the query to find the related model
	relModel := reflect.New(relType).Interface()
	relInfo, err := extractModelInfo(relModel)
	if err != nil {
		return err
	}

	builder := NewQueryBuilder(relInfo.TableName).
		Select().
		Where(fmt.Sprintf("%s = ?", rel.ReferenceKey), fkField.Interface())

	query, args := builder.Build()

	// Execute the query
	rows, err := c.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		// No related model found
		return nil
	}

	// Get column names from the query
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Create a new instance of the related model
	relValue := reflect.New(relType).Elem()

	// Create a slice of interface{} to hold the values
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	// Scan the row into the values
	if err := rows.Scan(values...); err != nil {
		return err
	}

	// Map the values to the related model fields
	for i, col := range columns {
		// Find the corresponding field in the model
		for _, field := range relInfo.Fields {
			if strings.EqualFold(field.DBName, col) {
				fieldValue := relValue.FieldByName(field.Name)
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

	// Set the related model to the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.CanSet() {
		return fmt.Errorf("field %s is not settable", field)
	}

	fieldValue.Set(relValue.Addr())

	return rows.Err()
}

// preloadHasMany preloads a HasMany relationship
func (c *Connection) preloadHasMany(ctx context.Context, source interface{}, field string, rel *Relationship) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	if sourceValue.Kind() != reflect.Struct {
		return errors.New("source must be a struct or pointer to struct")
	}

	// Get the primary key value from the source
	sourceInfo, err := extractModelInfo(source)
	if err != nil {
		return err
	}

	pkField := sourceValue.FieldByName(toFieldName(sourceInfo.PrimaryKey))
	if !pkField.IsValid() {
		return fmt.Errorf("primary key field %s not found in source model", sourceInfo.PrimaryKey)
	}

	// Create a new instance of the related model
	relType := reflect.TypeOf(rel.Model)
	if relType.Kind() == reflect.Ptr {
		relType = relType.Elem()
	}

	// Build the query to find the related models
	relModel := reflect.New(relType).Interface()
	relInfo, err := extractModelInfo(relModel)
	if err != nil {
		return err
	}

	builder := NewQueryBuilder(relInfo.TableName).
		Select().
		Where(fmt.Sprintf("%s = ?", rel.ForeignKey), pkField.Interface())

	query, args := builder.Build()

	// Execute the query
	rows, err := c.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column names from the query
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Get the field that holds the slice of related models
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.CanSet() {
		return fmt.Errorf("field %s is not settable", field)
	}

	// Create a new slice to hold the related models
	sliceType := fieldValue.Type()
	sliceElemType := sliceType.Elem()
	isPtr := sliceElemType.Kind() == reflect.Ptr

	if isPtr {
		sliceElemType = sliceElemType.Elem()
	}

	// Create a new slice for the related models
	newSlice := reflect.MakeSlice(sliceType, 0, 0)

	// Iterate over the rows and create related models
	for rows.Next() {
		// Create a new instance of the related model
		relValue := reflect.New(relType).Elem()

		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row into the values
		if err := rows.Scan(values...); err != nil {
			return err
		}

		// Map the values to the related model fields
		for i, col := range columns {
			// Find the corresponding field in the model
			for _, field := range relInfo.Fields {
				if strings.EqualFold(field.DBName, col) {
					fValue := relValue.FieldByName(field.Name)
					if fValue.CanSet() {
						val := reflect.ValueOf(*(values[i].(*interface{})))
						if val.Type().ConvertibleTo(fValue.Type()) {
							fValue.Set(val.Convert(fValue.Type()))
						}
					}
					break
				}
			}
		}

		// Add the related model to the slice
		if isPtr {
			newSlice = reflect.Append(newSlice, relValue.Addr())
		} else {
			newSlice = reflect.Append(newSlice, relValue)
		}
	}

	// Set the new slice to the field
	fieldValue.Set(newSlice)

	return rows.Err()
}

// preloadManyToMany preloads a ManyToMany relationship
func (c *Connection) preloadManyToMany(ctx context.Context, source interface{}, field string, rel *Relationship) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	if sourceValue.Kind() != reflect.Struct {
		return errors.New("source must be a struct or pointer to struct")
	}

	// Get the primary key value from the source
	sourceInfo, err := extractModelInfo(source)
	if err != nil {
		return err
	}

	pkField := sourceValue.FieldByName(toFieldName(sourceInfo.PrimaryKey))
	if !pkField.IsValid() {
		return fmt.Errorf("primary key field %s not found in source model", sourceInfo.PrimaryKey)
	}

	// Create a new instance of the related model
	relType := reflect.TypeOf(rel.Model)
	if relType.Kind() == reflect.Ptr {
		relType = relType.Elem()
	}

	// Build the query to find the related models
	relModel := reflect.New(relType).Interface()
	relInfo, err := extractModelInfo(relModel)
	if err != nil {
		return err
	}

	// Build a query to fetch related models through the join table
	query := fmt.Sprintf(
		"SELECT r.* FROM %s r INNER JOIN %s j ON r.%s = j.%s WHERE j.%s = ?",
		relInfo.TableName,
		rel.JoinTable,
		rel.ReferenceKey,
		rel.JoinRefKey,
		rel.JoinForeignKey,
	)

	// Execute the query
	rows, err := c.DB().QueryContext(ctx, query, pkField.Interface())
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column names from the query
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Get the field that holds the slice of related models
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.CanSet() {
		return fmt.Errorf("field %s is not settable", field)
	}

	// Create a new slice to hold the related models
	sliceType := fieldValue.Type()
	sliceElemType := sliceType.Elem()
	isPtr := sliceElemType.Kind() == reflect.Ptr

	if isPtr {
		sliceElemType = sliceElemType.Elem()
	}

	// Create a new slice for the related models
	newSlice := reflect.MakeSlice(sliceType, 0, 0)

	// Iterate over the rows and create related models
	for rows.Next() {
		// Create a new instance of the related model
		relValue := reflect.New(relType).Elem()

		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row into the values
		if err := rows.Scan(values...); err != nil {
			return err
		}

		// Map the values to the related model fields
		for i, col := range columns {
			// Find the corresponding field in the model
			for _, field := range relInfo.Fields {
				if strings.EqualFold(field.DBName, col) {
					fValue := relValue.FieldByName(field.Name)
					if fValue.CanSet() {
						val := reflect.ValueOf(*(values[i].(*interface{})))
						if val.Type().ConvertibleTo(fValue.Type()) {
							fValue.Set(val.Convert(fValue.Type()))
						}
					}
					break
				}
			}
		}

		// Add the related model to the slice
		if isPtr {
			newSlice = reflect.Append(newSlice, relValue.Addr())
		} else {
			newSlice = reflect.Append(newSlice, relValue)
		}
	}

	// Set the new slice to the field
	fieldValue.Set(newSlice)

	return rows.Err()
}

// Preload preloads the given relationships for the model
func (c *Connection) Preload(ctx context.Context, source interface{}, relationships map[string]*Relationship) error {
	// Validate source
	if source == nil {
		return errors.New("source cannot be nil")
	}

	sourceType := reflect.TypeOf(source)
	sourceValue := reflect.ValueOf(source)

	// Dereference if pointer
	if sourceType.Kind() == reflect.Ptr {
		sourceType = sourceType.Elem()
		sourceValue = sourceValue.Elem()
	}

	// Check if we have a slice of models
	isSlice := sourceType.Kind() == reflect.Slice

	if isSlice {
		// Get the type of the slice elements
		elemType := sourceType.Elem()

		// Dereference if pointer to struct
		if elemType.Kind() == reflect.Ptr {
			elemType = elemType.Elem()
		}

		if elemType.Kind() != reflect.Struct {
			return errors.New("source slice must contain structs or pointers to structs")
		}

		// Iterate over the slice elements
		for i := 0; i < sourceValue.Len(); i++ {
			elem := sourceValue.Index(i)
			if elem.Kind() == reflect.Ptr {
				if err := c.Preload(ctx, elem.Interface(), relationships); err != nil {
					return err
				}
			} else {
				// We need a pointer to the element
				ptr := reflect.New(elemType)
				ptr.Elem().Set(elem)
				if err := c.Preload(ctx, ptr.Interface(), relationships); err != nil {
					return err
				}
			}
		}

		return nil
	}

	// Process each relationship
	for field, rel := range relationships {
		if err := validateRelationship(sourceType, rel); err != nil {
			return err
		}

		// Check if the field exists
		fieldValue := sourceValue.FieldByName(field)
		if !fieldValue.IsValid() {
			return fmt.Errorf("field %s does not exist in model", field)
		}

		// Preload the relationship based on its type
		switch rel.Type {
		case HasOne:
			if err := c.preloadHasOne(ctx, source, field, rel); err != nil {
				return err
			}
		case BelongsTo:
			if err := c.preloadBelongsTo(ctx, source, field, rel); err != nil {
				return err
			}
		case HasMany:
			if err := c.preloadHasMany(ctx, source, field, rel); err != nil {
				return err
			}
		case ManyToMany:
			if err := c.preloadManyToMany(ctx, source, field, rel); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported relationship type: %d", rel.Type)
		}
	}

	return nil
}

// toFieldName converts a snake_case database column name to a CamelCase field name
func toFieldName(columnName string) string {
	parts := strings.Split(columnName, "_")
	for i := range parts {
		parts[i] = strings.Title(parts[i])
	}
	return strings.Join(parts, "")
}

// Associate associates a ManyToMany relationship between source and target
func (c *Connection) Associate(ctx context.Context, source interface{}, field string, target interface{}, rel *Relationship) error {
	if rel.Type != ManyToMany {
		return errors.New("associate can only be used with ManyToMany relationships")
	}

	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() == reflect.Ptr {
		targetValue = targetValue.Elem()
	}

	// Get the primary key values
	sourceInfo, err := extractModelInfo(source)
	if err != nil {
		return err
	}

	targetInfo, err := extractModelInfo(target)
	if err != nil {
		return err
	}

	sourcePkField := sourceValue.FieldByName(toFieldName(sourceInfo.PrimaryKey))
	if !sourcePkField.IsValid() {
		return fmt.Errorf("primary key field %s not found in source model", sourceInfo.PrimaryKey)
	}

	targetPkField := targetValue.FieldByName(toFieldName(targetInfo.PrimaryKey))
	if !targetPkField.IsValid() {
		return fmt.Errorf("primary key field %s not found in target model", targetInfo.PrimaryKey)
	}

	// Insert a record in the join table
	query := fmt.Sprintf(
		"INSERT INTO %s (%s, %s) VALUES (?, ?)",
		rel.JoinTable,
		rel.JoinForeignKey,
		rel.JoinRefKey,
	)

	_, err = c.DB().ExecContext(
		ctx,
		query,
		sourcePkField.Interface(),
		targetPkField.Interface(),
	)

	return err
}

// Dissociate removes a ManyToMany relationship between source and target
func (c *Connection) Dissociate(ctx context.Context, source interface{}, field string, target interface{}, rel *Relationship) error {
	if rel.Type != ManyToMany {
		return errors.New("dissociate can only be used with ManyToMany relationships")
	}

	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() == reflect.Ptr {
		targetValue = targetValue.Elem()
	}

	// Get the primary key values
	sourceInfo, err := extractModelInfo(source)
	if err != nil {
		return err
	}

	targetInfo, err := extractModelInfo(target)
	if err != nil {
		return err
	}

	sourcePkField := sourceValue.FieldByName(toFieldName(sourceInfo.PrimaryKey))
	if !sourcePkField.IsValid() {
		return fmt.Errorf("primary key field %s not found in source model", sourceInfo.PrimaryKey)
	}

	targetPkField := targetValue.FieldByName(toFieldName(targetInfo.PrimaryKey))
	if !targetPkField.IsValid() {
		return fmt.Errorf("primary key field %s not found in target model", targetInfo.PrimaryKey)
	}

	// Delete the record from the join table
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = ? AND %s = ?",
		rel.JoinTable,
		rel.JoinForeignKey,
		rel.JoinRefKey,
	)

	_, err = c.DB().ExecContext(
		ctx,
		query,
		sourcePkField.Interface(),
		targetPkField.Interface(),
	)

	return err
}
