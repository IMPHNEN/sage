package sage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// ConnectionOptions defines options for database connections
type ConnectionOptions struct {
	Driver          string
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Connection represents a database connection
type Connection struct {
	db      *sql.DB
	options ConnectionOptions
	mu      sync.RWMutex
}

// NewConnection creates a new database connection with the given options
func NewConnection(opts ConnectionOptions) (*Connection, error) {
	db, err := sql.Open(opts.Driver, opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure connection pool
	if opts.MaxOpenConns > 0 {
		db.SetMaxOpenConns(opts.MaxOpenConns)
	}
	if opts.MaxIdleConns > 0 {
		db.SetMaxIdleConns(opts.MaxIdleConns)
	}
	if opts.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(opts.ConnMaxLifetime)
	}
	if opts.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(opts.ConnMaxIdleTime)
	}

	// Verify the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Connection{
		db:      db,
		options: opts,
	}, nil
}

// DB returns the underlying sql.DB instance
func (c *Connection) DB() *sql.DB {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.db
}

// Close closes the database connection
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.Close()
}

// Ping verifies the connection to the database is still alive
func (c *Connection) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.db.PingContext(ctx)
}

// BeginTx starts a new transaction
func (c *Connection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Transaction, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tx, err := c.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Transaction{tx: tx}, nil
}
