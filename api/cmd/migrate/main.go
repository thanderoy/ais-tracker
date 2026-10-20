// Command migrate applies and rolls back database migrations using
// golang-migrate as an embedded library, so we ship one binary rather than the
// standalone migrate CLI.
//
// Usage:
//
//	migrate up            apply all pending migrations
//	migrate down [N]      roll back the last N migrations (default 1)
//	migrate force V       set the version to V without running migrations
//	migrate version       print the current version (alias: status)
//
// Configuration comes from the environment:
//
//	DATABASE_URL    postgres connection string (required)
//	MIGRATIONS_DIR  path to the migrations directory (default "migrations")
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) < 1 {
		usage()
		return 2
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fmt.Fprintln(os.Stderr, "migrate: DATABASE_URL is required")
		return 2
	}
	dir := os.Getenv("MIGRATIONS_DIR")
	if dir == "" {
		dir = "migrations"
	}

	m, err := migrate.New("file://"+dir, dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate: init: %v\n", err)
		return 1
	}
	defer func() { _, _ = m.Close() }()

	cmd := args[0]
	switch cmd {
	case "up":
		if err := noChangeOK(m.Up()); err != nil {
			return failf("up", err)
		}
		fmt.Println("migrate: up ok")

	case "down":
		n := 1
		if len(args) > 1 {
			parsed, perr := strconv.Atoi(args[1])
			if perr != nil {
				fmt.Fprintf(os.Stderr, "migrate: down N: %v\n", perr)
				return 2
			}
			n = parsed
		}
		if err := noChangeOK(m.Steps(-n)); err != nil {
			return failf("down", err)
		}
		fmt.Printf("migrate: down %d ok\n", n)

	case "force":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: migrate force V")
			return 2
		}
		v, verr := strconv.Atoi(args[1])
		if verr != nil {
			fmt.Fprintf(os.Stderr, "migrate: force V: %v\n", verr)
			return 2
		}
		if err := m.Force(v); err != nil {
			return failf("force", err)
		}
		fmt.Printf("migrate: forced to %d\n", v)

	case "version", "status":
		if err := printVersion(m); err != nil {
			return failf(cmd, err)
		}

	default:
		usage()
		return 2
	}

	return 0
}

// noChangeOK treats "nothing to migrate" as success, not an error.
func noChangeOK(err error) error {
	if errors.Is(err, migrate.ErrNoChange) {
		return nil
	}
	return err
}

func printVersion(m *migrate.Migrate) error {
	v, dirty, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		fmt.Println("migrate: no migrations applied")
		return nil
	}
	if err != nil {
		return err
	}
	fmt.Printf("migrate: version=%d dirty=%t\n", v, dirty)
	return nil
}

func failf(cmd string, err error) int {
	fmt.Fprintf(os.Stderr, "migrate: %s: %v\n", cmd, err)
	return 1
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: migrate <up|down [N]|force V|version|status>")
}
