package sage

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidOperation indicates an invalid database operation
	ErrInvalidOperation = errors.New("invalid database operation")

	// ErrInvalidArgument indicates an invalid argument
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrConnectionFailed indicates a connection failure
	ErrConnectionFailed = errors.New("database connection failed")

	// ErrTransactionFailed indicates a transaction failure
	ErrTransactionFailed = errors.New("transaction failed")

	// ErrUnsupportedDriver indicates an unsupported database driver
	ErrUnsupportedDriver = errors.New("unsupported database driver")

	// ErrMigrationFailed indicates a migration failure
	ErrMigrationFailed = errors.New("migration failed")
)

// WrapError wraps an error with additional context
func WrapError(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	message := fmt.Sprintf(format, args...)
	return fmt.Errorf("%s: %w", message, err)
}

// IsNotFoundError checks if an error is ErrNotFound
func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsValidationError checks if an error is a validation error
func IsValidationError(err error) bool {
	var valErr *ValidationError
	return errors.As(err, &valErr)
}

// ValidationError represents validation errors for a model
type ValidationError struct {
	Field   string
	Message string
}

// Error returns the error message
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error on field %s: %s", e.Field, e.Message)
}

// NewValidationError creates a new validation error
func NewValidationError(field, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Message: message,
	}
}

// ValidationErrors represents multiple validation errors
type ValidationErrors struct {
	Errors []*ValidationError
}

// Error returns all error messages joined together
func (e *ValidationErrors) Error() string {
	message := "validation errors: "
	for i, err := range e.Errors {
		if i > 0 {
			message += "; "
		}
		message += err.Error()
	}
	return message
}

// AddError adds a validation error
func (e *ValidationErrors) AddError(field, message string) {
	e.Errors = append(e.Errors, NewValidationError(field, message))
}

// HasErrors checks if there are any validation errors
func (e *ValidationErrors) HasErrors() bool {
	return len(e.Errors) > 0
}

// NewValidationErrors creates a new validation errors container
func NewValidationErrors() *ValidationErrors {
	return &ValidationErrors{
		Errors: make([]*ValidationError, 0),
	}
}
