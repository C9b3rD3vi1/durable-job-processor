package integration_test

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
)

func TestMigrateStoresChecksumAndRejectsChangedFile(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	pool := mustIsolatedPool(t, databaseURL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fsys := fstest.MapFS{
		"9999_integrity_probe.sql": {
			Data: []byte("CREATE TABLE migration_integrity_probe (id integer);"),
		},
	}

	applied, err := postgres.Migrate(ctx, pool, fsys)
	if err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if applied != 1 {
		t.Fatalf("Migrate() applied %d migrations, want 1", applied)
	}

	var checksum string
	if err := pool.QueryRow(ctx,
		`SELECT checksum FROM schema_migrations WHERE version = 9999`,
	).Scan(&checksum); err != nil {
		t.Fatalf("load stored checksum: %v", err)
	}
	if len(checksum) != 64 {
		t.Fatalf("len(checksum) = %d, want 64 hex characters", len(checksum))
	}

	applied, err = postgres.Migrate(ctx, pool, fsys)
	if err != nil {
		t.Fatalf("re-apply Migrate() error = %v", err)
	}
	if applied != 0 {
		t.Fatalf("re-apply Migrate() applied %d migrations, want 0", applied)
	}

	if _, err := pool.Exec(ctx, `UPDATE schema_migrations SET checksum = '' WHERE version = 9999`); err != nil {
		t.Fatalf("clear stored checksum: %v", err)
	}
	if _, err := postgres.Migrate(ctx, pool, fsys); err != nil {
		t.Fatalf("backfill Migrate() error = %v", err)
	}
	var backfilled string
	if err := pool.QueryRow(ctx,
		`SELECT checksum FROM schema_migrations WHERE version = 9999`,
	).Scan(&backfilled); err != nil {
		t.Fatalf("load backfilled checksum: %v", err)
	}
	if backfilled != checksum {
		t.Fatalf("backfilled checksum = %q, want %q", backfilled, checksum)
	}

	fsys["9999_integrity_probe.sql"].Data = []byte("CREATE TABLE migration_integrity_probe (id bigint);")
	_, err = postgres.Migrate(ctx, pool, fsys)
	if err == nil || !strings.Contains(err.Error(), "changed since it was applied") {
		t.Fatalf("Migrate() error = %v, want changed migration rejection", err)
	}
}
