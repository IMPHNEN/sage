package sage

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// NestedOption configures how nested models are handled
type NestedOption struct {
	AutoSave        bool // Save related models when the parent model is saved
	AutoDelete      bool // Delete related models when the parent model is deleted
	SkipValidation  bool // Skip validation of related models
	NullifyOnDelete bool // Set foreign keys to NULL instead of deleting relationships
	Preload         bool // Automatically preload related models
}

// DefaultNestedOption returns the default options for nested models
func DefaultNestedOption() NestedOption {
	return NestedOption{
		AutoSave:        true,
		AutoDelete:      false,
		SkipValidation:  false,
		NullifyOnDelete: false,
		Preload:         false,
	}
}

// nestedCreateHasOne creates a HasOne related model
func (c *Connection) nestedCreateHasOne(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	// Get the related model from the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.IsValid() {
		return fmt.Errorf("field %s does not exist in model", field)
	}

	// Skip if field is nil
	if fieldValue.IsNil() {
		return nil
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

	// Set the foreign key in the related model
	relValue := fieldValue.Elem()
	fkField := relValue.FieldByName(toFieldName(rel.ForeignKey))
	if fkField.IsValid() && fkField.CanSet() {
		fkField.Set(pkField)
	}

	// Create the related model
	return c.Create(ctx, fieldValue.Interface())
}

// nestedCreateBelongsTo creates a BelongsTo related model
func (c *Connection) nestedCreateBelongsTo(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	// Get the related model from the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.IsValid() {
		return fmt.Errorf("field %s does not exist in model", field)
	}

	// Skip if field is nil
	if fieldValue.IsNil() {
		return nil
	}

	// Create the related model first
	if err := c.Create(ctx, fieldValue.Interface()); err != nil {
		return err
	}

	// Get the primary key from the related model
	relValue := fieldValue.Elem()
	relInfo, err := extractModelInfo(fieldValue.Interface())
	if err != nil {
		return err
	}

	pkField := relValue.FieldByName(toFieldName(relInfo.PrimaryKey))
	if !pkField.IsValid() {
		return fmt.Errorf("primary key field %s not found in related model", relInfo.PrimaryKey)
	}

	// Set the foreign key in the source model
	fkField := sourceValue.FieldByName(toFieldName(rel.ForeignKey))
	if fkField.IsValid() && fkField.CanSet() {
		fkField.Set(pkField)
	}

	// Update the source model to save the foreign key
	return c.Update(ctx, source)
}

// nestedCreateHasMany creates HasMany related models
func (c *Connection) nestedCreateHasMany(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	// Get the related models from the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.IsValid() {
		return fmt.Errorf("field %s does not exist in model", field)
	}

	// Skip if field is nil or empty slice
	if fieldValue.IsNil() || fieldValue.Len() == 0 {
		return nil
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

	// Create each related model
	for i := 0; i < fieldValue.Len(); i++ {
		relModel := fieldValue.Index(i)

		// Skip if nil
		if relModel.IsNil() {
			continue
		}

		// Set the foreign key in the related model
		relValue := relModel.Elem()
		fkField := relValue.FieldByName(toFieldName(rel.ForeignKey))
		if fkField.IsValid() && fkField.CanSet() {
			fkField.Set(pkField)
		}

		// Create the related model
		if err := c.Create(ctx, relModel.Interface()); err != nil {
			return err
		}
	}

	return nil
}

// nestedCreateManyToMany creates ManyToMany relationships
func (c *Connection) nestedCreateManyToMany(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	// Get the related models from the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.IsValid() {
		return fmt.Errorf("field %s does not exist in model", field)
	}

	// Skip if field is nil or empty slice
	if fieldValue.IsNil() || fieldValue.Len() == 0 {
		return nil
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

	// Create each related model first
	for i := 0; i < fieldValue.Len(); i++ {
		relModel := fieldValue.Index(i)

		// Skip if nil
		if relModel.IsNil() {
			continue
		}

		// Check if the model already exists
		relValue := relModel.Elem()
		relInfo, err := extractModelInfo(relModel.Interface())
		if err != nil {
			return err
		}

		pkRelField := relValue.FieldByName(toFieldName(relInfo.PrimaryKey))
		if !pkRelField.IsValid() {
			return fmt.Errorf("primary key field %s not found in related model", relInfo.PrimaryKey)
		}

		// If the model doesn't have a primary key, create it
		if isZeroValue(pkRelField) {
			if err := c.Create(ctx, relModel.Interface()); err != nil {
				return err
			}
		}

		// Create the association in the join table
		if err := c.Associate(ctx, source, field, relModel.Interface(), rel); err != nil {
			return err
		}
	}

	return nil
}

// isZeroValue checks if a value is the zero value for its type
func isZeroValue(v reflect.Value) bool {
	return reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
}

// nestedUpdateHasOne updates a HasOne related model
func (c *Connection) nestedUpdateHasOne(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	// Get the related model from the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.IsValid() {
		return fmt.Errorf("field %s does not exist in model", field)
	}

	// Skip if field is nil
	if fieldValue.IsNil() {
		return nil
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

	// Set the foreign key in the related model
	relValue := fieldValue.Elem()
	fkField := relValue.FieldByName(toFieldName(rel.ForeignKey))
	if fkField.IsValid() && fkField.CanSet() {
		fkField.Set(pkField)
	}

	// Get the related model's primary key
	relInfo, err := extractModelInfo(fieldValue.Interface())
	if err != nil {
		return err
	}

	pkRelField := relValue.FieldByName(toFieldName(relInfo.PrimaryKey))
	if !pkRelField.IsValid() {
		return fmt.Errorf("primary key field %s not found in related model", relInfo.PrimaryKey)
	}

	// If the model doesn't have a primary key, create it
	if isZeroValue(pkRelField) {
		return c.Create(ctx, fieldValue.Interface())
	}

	// Otherwise, update it
	return c.Update(ctx, fieldValue.Interface())
}

// nestedUpdateBelongsTo updates a BelongsTo related model
func (c *Connection) nestedUpdateBelongsTo(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	// Get the related model from the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.IsValid() {
		return fmt.Errorf("field %s does not exist in model", field)
	}

	// Skip if field is nil
	if fieldValue.IsNil() {
		return nil
	}

	// Get the related model's primary key
	relValue := fieldValue.Elem()
	relInfo, err := extractModelInfo(fieldValue.Interface())
	if err != nil {
		return err
	}

	pkRelField := relValue.FieldByName(toFieldName(relInfo.PrimaryKey))
	if !pkRelField.IsValid() {
		return fmt.Errorf("primary key field %s not found in related model", relInfo.PrimaryKey)
	}

	// If the model doesn't have a primary key, create it
	if isZeroValue(pkRelField) {
		if err := c.Create(ctx, fieldValue.Interface()); err != nil {
			return err
		}
	} else {
		// Otherwise, update it
		if err := c.Update(ctx, fieldValue.Interface()); err != nil {
			return err
		}
	}

	// Set the foreign key in the source model
	fkField := sourceValue.FieldByName(toFieldName(rel.ForeignKey))
	if fkField.IsValid() && fkField.CanSet() {
		fkField.Set(pkRelField)
	}

	return nil
}

// nestedUpdateHasMany updates HasMany related models
func (c *Connection) nestedUpdateHasMany(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	// Get the related models from the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.IsValid() {
		return fmt.Errorf("field %s does not exist in model", field)
	}

	// Skip if field is nil or empty slice
	if fieldValue.IsNil() || fieldValue.Len() == 0 {
		return nil
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

	// Update each related model
	for i := 0; i < fieldValue.Len(); i++ {
		relModel := fieldValue.Index(i)

		// Skip if nil
		if relModel.IsNil() {
			continue
		}

		// Set the foreign key in the related model
		relValue := relModel.Elem()
		fkField := relValue.FieldByName(toFieldName(rel.ForeignKey))
		if fkField.IsValid() && fkField.CanSet() {
			fkField.Set(pkField)
		}

		// Get the related model's primary key
		relInfo, err := extractModelInfo(relModel.Interface())
		if err != nil {
			return err
		}

		pkRelField := relValue.FieldByName(toFieldName(relInfo.PrimaryKey))
		if !pkRelField.IsValid() {
			return fmt.Errorf("primary key field %s not found in related model", relInfo.PrimaryKey)
		}

		// If the model doesn't have a primary key, create it
		if isZeroValue(pkRelField) {
			if err := c.Create(ctx, relModel.Interface()); err != nil {
				return err
			}
		} else {
			// Otherwise, update it
			if err := c.Update(ctx, relModel.Interface()); err != nil {
				return err
			}
		}
	}

	return nil
}

// nestedUpdateManyToMany updates ManyToMany relationships
func (c *Connection) nestedUpdateManyToMany(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	// Get the related models from the field
	fieldValue := sourceValue.FieldByName(field)
	if !fieldValue.IsValid() {
		return fmt.Errorf("field %s does not exist in model", field)
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

	// If the field is nil or empty, clear all associations
	if fieldValue.IsNil() || fieldValue.Len() == 0 {
		// Delete all associations in the join table
		query := fmt.Sprintf(
			"DELETE FROM %s WHERE %s = ?",
			rel.JoinTable,
			rel.JoinForeignKey,
		)

		_, err := c.DB().ExecContext(ctx, query, pkField.Interface())
		return err
	}

	// Get current associations
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = ?",
		rel.JoinRefKey,
		rel.JoinTable,
		rel.JoinForeignKey,
	)

	rows, err := c.DB().QueryContext(ctx, query, pkField.Interface())
	if err != nil {
		return err
	}
	defer rows.Close()

	currentIDs := make(map[interface{}]bool)
	for rows.Next() {
		var id interface{}
		if err := rows.Scan(&id); err != nil {
			return err
		}
		currentIDs[id] = true
	}

	// Process each related model
	newIDs := make(map[interface{}]bool)
	for i := 0; i < fieldValue.Len(); i++ {
		relModel := fieldValue.Index(i)

		// Skip if nil
		if relModel.IsNil() {
			continue
		}

		// Check if the model already exists
		relValue := relModel.Elem()
		relInfo, err := extractModelInfo(relModel.Interface())
		if err != nil {
			return err
		}

		pkRelField := relValue.FieldByName(toFieldName(relInfo.PrimaryKey))
		if !pkRelField.IsValid() {
			return fmt.Errorf("primary key field %s not found in related model", relInfo.PrimaryKey)
		}

		// If the model doesn't have a primary key, create it
		if isZeroValue(pkRelField) {
			if err := c.Create(ctx, relModel.Interface()); err != nil {
				return err
			}
		} else {
			// Otherwise, update it
			if err := c.Update(ctx, relModel.Interface()); err != nil {
				return err
			}
		}

		// Mark as seen
		relID := pkRelField.Interface()
		newIDs[relID] = true

		// If not already associated, create the association
		if !currentIDs[relID] {
			if err := c.Associate(ctx, source, field, relModel.Interface(), rel); err != nil {
				return err
			}
		}
	}

	// Remove associations that are no longer present
	if opts.AutoDelete {
		for id := range currentIDs {
			if !newIDs[id] {
				query := fmt.Sprintf(
					"DELETE FROM %s WHERE %s = ? AND %s = ?",
					rel.JoinTable,
					rel.JoinForeignKey,
					rel.JoinRefKey,
				)

				_, err := c.DB().ExecContext(ctx, query, pkField.Interface(), id)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// nestedDeleteHasOne deletes a HasOne related model
func (c *Connection) nestedDeleteHasOne(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	if !opts.AutoDelete {
		return nil
	}

	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
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

	// Find the related model
	relType := reflect.TypeOf(rel.Model)
	if relType.Kind() == reflect.Ptr {
		relType = relType.Elem()
	}
	relModel := reflect.New(relType).Interface()

	// Build the query to find the related model
	relInfo, err := extractModelInfo(relModel)
	if err != nil {
		return err
	}

	if opts.NullifyOnDelete {
		// Update the foreign key to NULL
		query := fmt.Sprintf(
			"UPDATE %s SET %s = NULL WHERE %s = ?",
			relInfo.TableName,
			rel.ForeignKey,
			rel.ForeignKey,
		)

		_, err := c.DB().ExecContext(ctx, query, pkField.Interface())
		return err
	}

	// Delete the related model
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = ?",
		relInfo.TableName,
		rel.ForeignKey,
	)

	_, err = c.DB().ExecContext(ctx, query, pkField.Interface())
	return err
}

// nestedDeleteHasMany deletes HasMany related models
func (c *Connection) nestedDeleteHasMany(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	if !opts.AutoDelete {
		return nil
	}

	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
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

	// Find the related model type
	relType := reflect.TypeOf(rel.Model)
	if relType.Kind() == reflect.Ptr {
		relType = relType.Elem()
	}
	relModel := reflect.New(relType).Interface()

	// Build the query to find the related models
	relInfo, err := extractModelInfo(relModel)
	if err != nil {
		return err
	}

	if opts.NullifyOnDelete {
		// Update the foreign keys to NULL
		query := fmt.Sprintf(
			"UPDATE %s SET %s = NULL WHERE %s = ?",
			relInfo.TableName,
			rel.ForeignKey,
			rel.ForeignKey,
		)

		_, err := c.DB().ExecContext(ctx, query, pkField.Interface())
		return err
	}

	// Delete the related models
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = ?",
		relInfo.TableName,
		rel.ForeignKey,
	)

	_, err = c.DB().ExecContext(ctx, query, pkField.Interface())
	return err
}

// nestedDeleteManyToMany deletes ManyToMany relationships
func (c *Connection) nestedDeleteManyToMany(ctx context.Context, source interface{}, field string, rel *Relationship, opts NestedOption) error {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
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

	// Delete the associations in the join table
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = ?",
		rel.JoinTable,
		rel.JoinForeignKey,
	)

	_, err = c.DB().ExecContext(ctx, query, pkField.Interface())
	if err != nil {
		return err
	}

	// If autoDelete is enabled, also delete the related models
	if opts.AutoDelete {
		// Get the IDs of the related models
		query := fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s = ?",
			rel.JoinRefKey,
			rel.JoinTable,
			rel.JoinForeignKey,
		)

		rows, err := c.DB().QueryContext(ctx, query, pkField.Interface())
		if err != nil {
			return err
		}
		defer rows.Close()

		var ids []interface{}
		for rows.Next() {
			var id interface{}
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}

		if len(ids) > 0 {
			relType := reflect.TypeOf(rel.Model)
			if relType.Kind() == reflect.Ptr {
				relType = relType.Elem()
			}
			relModel := reflect.New(relType).Interface()

			// Build the query to delete the related models
			relInfo, err := extractModelInfo(relModel)
			if err != nil {
				return err
			}

			// Build a placeholders string for the IN clause
			placeholders := make([]string, len(ids))
			for i := range placeholders {
				placeholders[i] = "?"
			}

			query := fmt.Sprintf(
				"DELETE FROM %s WHERE %s IN (%s)",
				relInfo.TableName,
				relInfo.PrimaryKey,
				strings.Join(placeholders, ", "),
			)

			// Convert ids to []interface{} for ExecContext
			args := make([]interface{}, len(ids))
			for i, id := range ids {
				args[i] = id
			}

			_, err = c.DB().ExecContext(ctx, query, args...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// CreateNested creates a model with its nested relationships
func (c *Connection) CreateNested(ctx context.Context, model interface{}, relationships map[string]*Relationship, opts NestedOption) error {
	// First create the model itself
	if err := c.Create(ctx, model); err != nil {
		return err
	}

	// Process each relationship
	for field, rel := range relationships {
		if !opts.AutoSave {
			continue
		}

		switch rel.Type {
		case HasOne:
			if err := c.nestedCreateHasOne(ctx, model, field, rel, opts); err != nil {
				return err
			}
		case BelongsTo:
			if err := c.nestedCreateBelongsTo(ctx, model, field, rel, opts); err != nil {
				return err
			}
		case HasMany:
			if err := c.nestedCreateHasMany(ctx, model, field, rel, opts); err != nil {
				return err
			}
		case ManyToMany:
			if err := c.nestedCreateManyToMany(ctx, model, field, rel, opts); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported relationship type: %d", rel.Type)
		}
	}

	return nil
}

// UpdateNested updates a model with its nested relationships
func (c *Connection) UpdateNested(ctx context.Context, model interface{}, relationships map[string]*Relationship, opts NestedOption) error {
	// First update the model itself
	if err := c.Update(ctx, model); err != nil {
		return err
	}

	// Process each relationship
	for field, rel := range relationships {
		if !opts.AutoSave {
			continue
		}

		switch rel.Type {
		case HasOne:
			if err := c.nestedUpdateHasOne(ctx, model, field, rel, opts); err != nil {
				return err
			}
		case BelongsTo:
			if err := c.nestedUpdateBelongsTo(ctx, model, field, rel, opts); err != nil {
				return err
			}
		case HasMany:
			if err := c.nestedUpdateHasMany(ctx, model, field, rel, opts); err != nil {
				return err
			}
		case ManyToMany:
			if err := c.nestedUpdateManyToMany(ctx, model, field, rel, opts); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported relationship type: %d", rel.Type)
		}
	}

	return nil
}

// DeleteNested deletes a model and its nested relationships
func (c *Connection) DeleteNested(ctx context.Context, model interface{}, relationships map[string]*Relationship, opts NestedOption) error {
	// Process each relationship first
	for field, rel := range relationships {
		switch rel.Type {
		case HasOne:
			if err := c.nestedDeleteHasOne(ctx, model, field, rel, opts); err != nil {
				return err
			}
		case HasMany:
			if err := c.nestedDeleteHasMany(ctx, model, field, rel, opts); err != nil {
				return err
			}
		case ManyToMany:
			if err := c.nestedDeleteManyToMany(ctx, model, field, rel, opts); err != nil {
				return err
			}
		}
	}

	// Now delete the model itself
	return c.Delete(ctx, model)
}

// FindNested finds a model and preloads its relationships
func (c *Connection) FindNested(ctx context.Context, model interface{}, id interface{}, relationships map[string]*Relationship) error {
	// First find the model
	if err := c.Find(ctx, model, id); err != nil {
		return err
	}

	// Preload relationships
	return c.Preload(ctx, model, relationships)
}

// FirstNested finds the first model matching conditions and preloads its relationships
func (c *Connection) FirstNested(ctx context.Context, model interface{}, relationships map[string]*Relationship, conditions string, args ...interface{}) error {
	// First find the model
	if err := c.First(ctx, model, conditions, args...); err != nil {
		return err
	}

	// Preload relationships
	return c.Preload(ctx, model, relationships)
}

// AllNested finds all models matching conditions and preloads their relationships
func (c *Connection) AllNested(ctx context.Context, models interface{}, relationships map[string]*Relationship, conditions string, args ...interface{}) error {
	// First find the models
	if err := c.All(ctx, models, conditions, args...); err != nil {
		return err
	}

	// Preload relationships
	return c.Preload(ctx, models, relationships)
}
