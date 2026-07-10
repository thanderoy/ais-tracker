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
	"github.com/jackc/pgx/v5"
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
	dsn, cleanup, err = StartRawPostgres(ctx)
	if err != nil {
		return "", nil, err
	}
	if err := applyMigrations(dsn); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("apply migrations: %w", err)
	}
	return dsn, cleanup, nil
}

// StartRawPostgres launches the same container with all required extensions but
// applies no migrations. Tests that drive migrations themselves (the migration
// round-trip test) use this so they start from an empty, extension-ready schema.
func StartRawPostgres(ctx context.Context) (dsn string, cleanup func(), err error) {
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
	return dsn, cleanup, nil
}

// StartLogicalPostgres launches a fully-migrated Postgres configured with
// wal_level=logical, required for logical replication / CDC. The base image
// starts at wal_level=replica, which needs a restart to change, so we set it via
// ALTER SYSTEM and restart the container before applying migrations.
func StartLogicalPostgres(ctx context.Context) (dsn string, cleanup func(), err error) {
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

	if err := setLogicalAndRestart(ctx, container); err != nil {
		cleanup()
		return "", nil, err
	}

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

// setLogicalAndRestart flips wal_level to logical and restarts the container so
// it takes effect, then waits for the server to accept connections again (the
// host port is remapped on restart).
func setLogicalAndRestart(ctx context.Context, container *postgres.PostgresContainer) error {
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect for ALTER SYSTEM: %w", err)
	}
	_, err = conn.Exec(ctx, "ALTER SYSTEM SET wal_level = 'logical'")
	_ = conn.Close(ctx)
	if err != nil {
		return fmt.Errorf("set wal_level: %w", err)
	}

	timeout := 30 * time.Second
	if err := container.Stop(ctx, &timeout); err != nil {
		return fmt.Errorf("stop for restart: %w", err)
	}
	if err := container.Start(ctx); err != nil {
		return fmt.Errorf("restart: %w", err)
	}

	// Poll until the restarted server accepts connections (fresh DSN/port).
	deadline := time.Now().Add(120 * time.Second)
	for {
		newDSN, derr := container.ConnectionString(ctx, "sslmode=disable")
		if derr == nil {
			if c, cerr := pgx.Connect(ctx, newDSN); cerr == nil {
				pingErr := c.Ping(ctx)
				_ = c.Close(ctx)
				if pingErr == nil {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for logical-restart readiness")
		}
		time.Sleep(500 * time.Millisecond)
	}
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
