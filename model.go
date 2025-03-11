package sage

import (
	"reflect"
	"strconv"
	"strings"
)

// Model represents a database model
type Model interface {
	TableName() string
	PrimaryKey() string
}

// ModelInfo contains metadata about a model
type ModelInfo struct {
	TableName  string
	PrimaryKey string
	Fields     []FieldInfo
}

// FieldInfo contains metadata about a model field
type FieldInfo struct {
	Name      string
	DBName    string
	Type      reflect.Type
	IsKey     bool
	IsAuto    bool
	Nullable  bool
	Unique    bool
	Index     bool
	Size      int
	Precision int
	Scale     int
	Tags      map[string]string
}

// extractModelInfo extracts model information from a struct using reflection
func extractModelInfo(model interface{}) (*ModelInfo, error) {
	v := reflect.ValueOf(model)

	// If pointer, get the underlying element
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, ErrNotAStruct
	}

	t := v.Type()

	var tableName string
	var primaryKey string

	// Check if model implements Model interface
	if m, ok := model.(Model); ok {
		tableName = m.TableName()
		primaryKey = m.PrimaryKey()
	} else {
		// Default table name is the struct name in snake_case
		tableName = toSnakeCase(t.Name())

		// Default primary key is "id"
		primaryKey = "id"
	}

	info := &ModelInfo{
		TableName:  tableName,
		PrimaryKey: primaryKey,
		Fields:     make([]FieldInfo, 0, t.NumField()),
	}

	// Process struct fields
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get tag options
		tag := field.Tag.Get("db")
		if tag == "-" {
			continue
		}

		fieldInfo := FieldInfo{
			Name:   field.Name,
			Type:   field.Type,
			Tags:   parseTags(field.Tag),
			DBName: toSnakeCase(field.Name),
		}

		// Process tag options
		tagParts := strings.Split(tag, ",")
		if len(tagParts) > 0 && tagParts[0] != "" {
			fieldInfo.DBName = tagParts[0]
		}

		for _, opt := range tagParts[1:] {
			switch opt {
			case "pk":
				fieldInfo.IsKey = true
				info.PrimaryKey = fieldInfo.DBName
			case "auto":
				fieldInfo.IsAuto = true
			case "nullable":
				fieldInfo.Nullable = true
			case "unique":
				fieldInfo.Unique = true
			case "index":
				fieldInfo.Index = true
			}

			// Handle size, precision, scale
			if strings.HasPrefix(opt, "size:") {
				// Parse size into fieldInfo.Size
				size := strings.TrimPrefix(opt, "size:")
				fieldInfo.Size, _ = strconv.Atoi(size)
			}

			if strings.HasPrefix(opt, "precision:") {
				// Parse precision into fieldInfo.Precision
				precision := strings.TrimPrefix(opt, "precision:")
				fieldInfo.Precision, _ = strconv.Atoi(precision)
			}

			if strings.HasPrefix(opt, "scale:") {
				// Parse scale into fieldInfo.Scale
				scale := strings.TrimPrefix(opt, "scale:")
				fieldInfo.Scale, _ = strconv.Atoi(scale)
			}
		}

		info.Fields = append(info.Fields, fieldInfo)
	}

	return info, nil
}

// parseTags parses struct tags into a map
func parseTags(tag reflect.StructTag) map[string]string {
	result := make(map[string]string)

	for _, tagName := range []string{"db", "json", "validate"} {
		if value, ok := tag.Lookup(tagName); ok {
			parts := strings.Split(value, ",")
			if len(parts) > 0 && parts[0] != "" {
				result[tagName] = parts[0]
			}
		}
	}

	return result
}

// toSnakeCase converts a camelCase string to snake_case
func toSnakeCase(s string) string {
	var result strings.Builder

	for i, r := range s {
		if i > 0 && 'A' <= r && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}

	return strings.ToLower(result.String())
}
