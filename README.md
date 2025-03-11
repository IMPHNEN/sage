# Sage ORM

A lightweight object-relational mapping (ORM) library built in Go, designed to provide a simple yet powerful interface for database operations.

## Features

- **Type-safe database queries**: Work with Go structs instead of raw SQL
- **Multiple database support**: Works with PostgreSQL, MySQL, and SQLite
- **Connection pooling**: Efficiently manages database connections
- **Schema migration**: Tools for versioning and migrating database schemas
- **Transaction management**: Simple API for transaction handling
- **Model definition**: Define models using struct tags

## Installation

```bash
go get -u github.com/IMPHNEN/sage
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IMPHNEN/sage"
	"github.com/IMPHNEN/sage/models"
)

func main() {
	// Create a connection
	conn, err := sage.NewConnection(sage.ConnectionOptions{
		Driver: "postgres",
		DSN:    "postgres://user:password@localhost/mydb?sslmode=disable",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx := context.Background()

	// Create a new user
	user := &models.User{
		Username:  "johndoe",
		Email:     "john@example.com",
		FirstName: "John",
		LastName:  "Doe",
		Active:    true,
	}

	if err := conn.Create(ctx, user); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Created user with ID: %d\n", user.ID)

	// Find user by ID
	foundUser := &models.User{}
	if err := conn.Find(ctx, foundUser, user.ID); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found user: %s %s\n", foundUser.FirstName, foundUser.LastName)

	// Update user
	foundUser.Email = "john.doe@example.com"
	if err := conn.Update(ctx, foundUser); err != nil {
		log.Fatal(err)
	}

	// Find all active users
	var users []*models.User
	if err := conn.All(ctx, &users, "active = ?", true); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d active users\n", len(users))

	// Transaction example
	err = conn.WithTransaction(ctx, func(tx *sage.Transaction) error {
		// Create a post
		post := &models.Post{
			UserID:    user.ID,
			Title:     "My First Post",
			Content:   "Hello, world!",
			Published: true,
		}

		if err := post.BeforeCreate(); err != nil {
			return err
		}

		query, args := sage.NewQueryBuilder("posts").
			Insert().
			Set("user_id", post.UserID).
			Set("title", post.Title).
			Set("content", post.Content).
			Set("published", post.Published).
			Set("created_at", post.CreatedAt).
			Set("updated_at", post.UpdatedAt).
			Build()

		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}

		id, err := result.LastInsertId()
		if err != nil {
			return err
		}

		post.ID = id
		fmt.Printf("Created post with ID: %d\n", post.ID)

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}
```

## Model Definition

Models are defined using struct tags:

```go
type User struct {
	ID        int64     `db:"id,pk,auto"`           // Primary key, auto-increment
	Username  string    `db:"username,size:255,unique"` // Unique with max length
	Email     string    `db:"email,size:255,unique"`    // Unique with max length
	Active    bool      `db:"active,default:true"`      // With default value
	CreatedAt time.Time `db:"created_at"`              // Timestamp
}

// Implement Model interface
func (u *User) TableName() string {
	return "users"
}

func (u *User) PrimaryKey() string {
	return "id"
}
```

## Query Building

The query builder provides a fluent API for constructing queries:

```go
// Select query
query, args := sage.NewQueryBuilder("users").
	Select("id", "username", "email").
	Where("active = ?", true).
	OrderBy("created_at", "DESC").
	Limit(10).
	Build()

// Insert query
query, args := sage.NewQueryBuilder("users").
	Insert().
	Set("username", "janedoe").
	Set("email", "jane@example.com").
	Set("active", true).
	Build()

// Update query
query, args := sage.NewQueryBuilder("users").
	Update().
	Set("email", "updated@example.com").
	Where("id = ?", 1).
	Build()

// Delete query
query, args := sage.NewQueryBuilder("users").
	Delete().
	Where("id = ?", 1).
	Build()
```

## Migrations

Sage includes a CLI tool for managing database migrations:

```bash
# Create a new migration
go run sage@latest -driver postgres -dsn "postgres://user:password@localhost/mydb?sslmode=disable" -command create -name create_users_table

# Run all pending migrations
go run sage@latest -driver postgres -dsn "postgres://user:password@localhost/mydb?sslmode=disable" -command migrate

# Rollback the last migration
go run sage@latest -driver postgres -dsn "postgres://user:password@localhost/mydb?sslmode=disable" -command rollback -steps 1
```

## License

MIT License