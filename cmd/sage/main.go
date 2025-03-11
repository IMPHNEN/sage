package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IMPHNEN/sage"
	"github.com/IMPHNEN/sage/internal/schema"
)

func main() {
	// Define command-line flags
	var (
		driver  = flag.String("driver", "", "Database driver (postgres, mysql, sqlite)")
		dsn     = flag.String("dsn", "", "Database connection string")
		command = flag.String("command", "", "Command to execute (migrate, rollback, create, drop)")
		name    = flag.String("name", "", "Migration name (for create)")
		steps   = flag.Int("steps", 1, "Number of migrations to roll back")
		version = flag.Bool("version", false, "Print version information")
	)

	flag.Parse()

	// Print version information
	if *version {
		fmt.Println("Sage ORM CLI v0.1.0")
		os.Exit(0)
	}

	// Validate required flags
	if *driver == "" {
		log.Fatal("Driver is required")
	}

	if *dsn == "" {
		log.Fatal("DSN is required")
	}

	if *command == "" {
		log.Fatal("Command is required")
	}

	// Create database connection
	opts := sage.ConnectionOptions{
		Driver:          *driver,
		DSN:             *dsn,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}

	conn, err := sage.NewConnection(opts)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer conn.Close()

	// Create migration manager
	migrationManager := schema.NewMigrationManager(conn.DB(), "migrations")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Execute command
	switch strings.ToLower(*command) {
	case "migrate":
		if err := migrationManager.CreateMigrationsTable(ctx); err != nil {
			log.Fatalf("Failed to create migrations table: %v", err)
		}

		if err := migrationManager.MigrateUp(ctx); err != nil {
			log.Fatalf("Failed to migrate: %v", err)
		}

		fmt.Println("Migration completed successfully")

	case "rollback":
		if *steps <= 0 {
			log.Fatal("Steps must be greater than 0")
		}

		for i := 0; i < *steps; i++ {
			if err := migrationManager.MigrateDown(ctx); err != nil {
				log.Fatalf("Failed to rollback: %v", err)
			}
		}

		fmt.Printf("Rolled back %d migration(s) successfully\n", *steps)

	case "create":
		if *name == "" {
			log.Fatal("Migration name is required")
		}

		// Create migration file
		timestamp := time.Now().Format("20060102150405")
		fileName := fmt.Sprintf("%s_%s.go", timestamp, *name)
		filePath := fmt.Sprintf("migrations/%s", fileName)

		// Ensure migrations directory exists
		if err := os.MkdirAll("migrations", 0755); err != nil {
			log.Fatalf("Failed to create migrations directory: %v", err)
		}

		// Create migration file content
		content := fmt.Sprintf(`package migrations

import (
	"context"
	"database/sql"
)

// Up%s performs the migration
func Up%s(ctx context.Context, db *sql.DB) error {
	// TODO: Implement migration
	_, err := db.ExecContext(ctx, "")
	return err
}

// Down%s rolls back the migration
func Down%s(ctx context.Context, db *sql.DB) error {
	// TODO: Implement rollback
	_, err := db.ExecContext(ctx, "")
	return err
}
`, strings.Title(*name), strings.Title(*name), strings.Title(*name), strings.Title(*name))

		// Write migration file
		if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
			log.Fatalf("Failed to create migration file: %v", err)
		}

		fmt.Printf("Created migration file: %s\n", filePath)

	case "drop":
		// Prompt for confirmation
		fmt.Print("Are you sure you want to drop all tables? This action cannot be undone. (y/N): ")
		var confirm string
		fmt.Scanln(&confirm)

		if strings.ToLower(confirm) != "y" {
			fmt.Println("Operation cancelled")
			os.Exit(0)
		}

		// Get all tables
		var tableQuery string
		switch *driver {
		case "postgres":
			tableQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
		case "mysql":
			tableQuery = "SHOW TABLES"
		case "sqlite":
			tableQuery = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
		default:
			log.Fatalf("Unsupported driver: %s", *driver)
		}

		rows, err := conn.DB().QueryContext(ctx, tableQuery)
		if err != nil {
			log.Fatalf("Failed to get tables: %v", err)
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				log.Fatalf("Failed to scan table name: %v", err)
			}
			tables = append(tables, tableName)
		}

		// Drop each table
		for _, table := range tables {
			_, err := conn.DB().ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
			if err != nil {
				log.Fatalf("Failed to drop table %s: %v", table, err)
			}
			fmt.Printf("Dropped table: %s\n", table)
		}

		fmt.Println("All tables dropped successfully")

	default:
		log.Fatalf("Unknown command: %s", *command)
	}
}
