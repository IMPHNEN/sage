package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Migration represents a database migration
type Migration struct {
	ID          int64
	Name        string
	Description string
	Up          string
	Down        string
	CreatedAt   time.Time
	AppliedAt   *time.Time
}

// MigrationManager manages database migrations
type MigrationManager struct {
	db        *sql.DB
	tableName string
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager(db *sql.DB, tableName string) *MigrationManager {
	if tableName == "" {
		tableName = "migrations"
	}

	return &MigrationManager{
		db:        db,
		tableName: tableName,
	}
}

// CreateMigrationsTable creates the migrations table if it doesn't exist
func (m *MigrationManager) CreateMigrationsTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		name VARCHAR(255) NOT NULL UNIQUE,
		description TEXT,
		up TEXT NOT NULL,
		down TEXT,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		applied_at TIMESTAMP
	)`, m.tableName)

	_, err := m.db.ExecContext(ctx, query)
	return err
}

// AddMigration adds a migration to the migrations table
func (m *MigrationManager) AddMigration(ctx context.Context, migration *Migration) error {
	query := fmt.Sprintf(`INSERT INTO %s (name, description, up, down, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (name) DO UPDATE SET
			description = EXCLUDED.description,
			up = EXCLUDED.up,
			down = EXCLUDED.down,
			created_at = EXCLUDED.created_at
		RETURNING id`, m.tableName)

	row := m.db.QueryRowContext(
		ctx,
		query,
		migration.Name,
		migration.Description,
		migration.Up,
		migration.Down,
		time.Now(),
	)

	return row.Scan(&migration.ID)
}

// GetMigrations gets all migrations from the migrations table
func (m *MigrationManager) GetMigrations(ctx context.Context) ([]*Migration, error) {
	query := fmt.Sprintf(`SELECT id, name, description, up, down, created_at, applied_at
		FROM %s
		ORDER BY id ASC`, m.tableName)

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var migrations []*Migration
	for rows.Next() {
		var migration Migration
		err := rows.Scan(
			&migration.ID,
			&migration.Name,
			&migration.Description,
			&migration.Up,
			&migration.Down,
			&migration.CreatedAt,
			&migration.AppliedAt,
		)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, &migration)
	}

	return migrations, rows.Err()
}

// GetPendingMigrations gets all migrations that haven't been applied
func (m *MigrationManager) GetPendingMigrations(ctx context.Context) ([]*Migration, error) {
	query := fmt.Sprintf(`SELECT id, name, description, up, down, created_at, applied_at
		FROM %s
		WHERE applied_at IS NULL
		ORDER BY id ASC`, m.tableName)

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var migrations []*Migration
	for rows.Next() {
		var migration Migration
		err := rows.Scan(
			&migration.ID,
			&migration.Name,
			&migration.Description,
			&migration.Up,
			&migration.Down,
			&migration.CreatedAt,
			&migration.AppliedAt,
		)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, &migration)
	}

	return migrations, rows.Err()
}

// GetAppliedMigrations gets all migrations that have been applied
func (m *MigrationManager) GetAppliedMigrations(ctx context.Context) ([]*Migration, error) {
	query := fmt.Sprintf(`SELECT id, name, description, up, down, created_at, applied_at
		FROM %s
		WHERE applied_at IS NOT NULL
		ORDER BY id DESC`, m.tableName)

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var migrations []*Migration
	for rows.Next() {
		var migration Migration
		err := rows.Scan(
			&migration.ID,
			&migration.Name,
			&migration.Description,
			&migration.Up,
			&migration.Down,
			&migration.CreatedAt,
			&migration.AppliedAt,
		)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, &migration)
	}

	return migrations, rows.Err()
}

// MarkMigrationAsApplied marks a migration as applied
func (m *MigrationManager) MarkMigrationAsApplied(ctx context.Context, migrationID int64) error {
	query := fmt.Sprintf(`UPDATE %s SET applied_at = $1 WHERE id = $2`, m.tableName)

	result, err := m.db.ExecContext(ctx, query, time.Now(), migrationID)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return errors.New("migration not found")
	}

	return nil
}

// MarkMigrationAsUnapplied marks a migration as unapplied
func (m *MigrationManager) MarkMigrationAsUnapplied(ctx context.Context, migrationID int64) error {
	query := fmt.Sprintf(`UPDATE %s SET applied_at = NULL WHERE id = $1`, m.tableName)

	result, err := m.db.ExecContext(ctx, query, migrationID)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return errors.New("migration not found")
	}

	return nil
}

// MigrateUp applies all pending migrations
func (m *MigrationManager) MigrateUp(ctx context.Context) error {
	// Create migrations table if it doesn't exist
	if err := m.CreateMigrationsTable(ctx); err != nil {
		return err
	}

	// Get pending migrations
	migrations, err := m.GetPendingMigrations(ctx)
	if err != nil {
		return err
	}

	// Begin transaction
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Apply each migration
	for _, migration := range migrations {
		// Execute migration
		_, err := tx.ExecContext(ctx, migration.Up)
		if err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", migration.Name, err)
		}

		// Mark migration as applied
		query := fmt.Sprintf(`UPDATE %s SET applied_at = $1 WHERE id = $2`, m.tableName)
		_, err = tx.ExecContext(ctx, query, time.Now(), migration.ID)
		if err != nil {
			return fmt.Errorf("failed to mark migration %s as applied: %w", migration.Name, err)
		}
	}

	// Commit transaction
	return tx.Commit()
}

// MigrateDown reverts the last migration
func (m *MigrationManager) MigrateDown(ctx context.Context) error {
	// Get the last applied migration
	migrations, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return err
	}

	if len(migrations) == 0 {
		return errors.New("no migrations to revert")
	}

	lastMigration := migrations[0]

	// Begin transaction
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Execute migration
	_, err = tx.ExecContext(ctx, lastMigration.Down)
	if err != nil {
		return fmt.Errorf("failed to revert migration %s: %w", lastMigration.Name, err)
	}

	// Mark migration as unapplied
	query := fmt.Sprintf(`UPDATE %s SET applied_at = NULL WHERE id = $1`, m.tableName)
	_, err = tx.ExecContext(ctx, query, lastMigration.ID)
	if err != nil {
		return fmt.Errorf("failed to mark migration %s as unapplied: %w", lastMigration.Name, err)
	}

	// Commit transaction
	return tx.Commit()
}
