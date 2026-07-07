// Package testsupport provides shared helpers for integration tests, chiefly an
// ephemeral, fully migrated Postgres instance. It imports testcontainers and
// golang-migrate; it is only referenced from _test.go files and is never linked
// into the service binaries.
package testsupport

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// image is the Postgres image used for all integration tests. We use the
// TimescaleDB HA image (which also bundles PostGIS and pgvector) so that every
// repository migration applies, including the Phase 2 hypertable and the
// PostGIS/vector objects added later.
const image = "timescale/timescaledb-ha:pg16"

// StartPostgres launches a Postgres container with all required extensions,
// applies every repository migration, and returns the connection DSN plus a
// cleanup func.
func StartPostgres(ctx context.Context) (dsn string, cleanup func(), err error) {
	initScript, err := extensionsScript()
	if err != nil {
		return "", nil, err
	}

	container, err := postgres.Run(ctx, image,
		postgres.WithDatabase("ais"),
		postgres.WithUsername("ais"),
		postgres.WithPassword("ais"),
		postgres.WithInitScripts(initScript),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(180*time.Second),
		),
	)
	if err != nil {
		return "", nil, fmt.Errorf("start container: %w", err)
	}
	cleanup = func() { _ = container.Terminate(context.Background()) }

	dsn, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("connection string: %w", err)
	}

	if err := applyMigrations(dsn); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("apply migrations: %w", err)
	}
	return dsn, cleanup, nil
}

func applyMigrations(dsn string) error {
	dir, err := migrationsDir()
	if err != nil {
		return err
	}
	m, err := migrate.New("file://"+dir, dsn)
	if err != nil {
		return err
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

// migrationsDir resolves <repo>/migrations relative to this source file.
func migrationsDir() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "migrations"), nil
}

// extensionsScript resolves the first-boot extension init SQL, reused from the
// compose stack so tests install the same extension set as production.
func extensionsScript() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "deploy", "postgres", "init", "00-extensions.sql"), nil
}

// repoRoot resolves the repository root relative to this source file.
func repoRoot() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("cannot resolve caller path")
	}
	// thisFile == <repo>/internal/testsupport/postgres.go
	return filepath.Join(filepath.Dir(thisFile), "..", ".."), nil
}
