package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationAdvisoryLock serializes migration runs across API/migrate processes.
const migrationAdvisoryLock int64 = 0x44504A50 // "DJP"

type migration struct {
	version int64
	name    string
}

// Migrate applies every unapplied .sql file in fsys in ascending version order.
// It creates schema_migrations automatically, stores a SHA-256 checksum for each
// newly applied file, rejects already-applied files whose contents changed, and
// holds a PostgreSQL advisory lock for the duration so concurrent deploys
// cannot race. Legacy rows without a checksum are accepted and backfilled.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) (int, error) {
	if pool == nil {
		return 0, fmt.Errorf("database pool is required")
	}
	if fsys == nil {
		return 0, fmt.Errorf("migration filesystem is required")
	}

	migrations, err := loadMigrations(fsys)
	if err != nil {
		return 0, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLock); err != nil {
		return 0, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLock)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    bigint PRIMARY KEY,
			name       text NOT NULL,
			checksum   text NOT NULL DEFAULT '',
			applied_at timestamptz NOT NULL DEFAULT now()
		)
	`); err != nil {
		return 0, fmt.Errorf("create schema_migrations: %w", err)
	}
	// Databases created before checksum tracking keep their version history;
	// add the column with an empty sentinel so legacy rows remain valid.
	if _, err := conn.Exec(ctx,
		`ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum text NOT NULL DEFAULT ''`,
	); err != nil {
		return 0, fmt.Errorf("add schema_migrations checksum: %w", err)
	}

	appliedCount := 0
	for _, m := range migrations {
		sqlBytes, err := fs.ReadFile(fsys, m.name)
		if err != nil {
			return appliedCount, fmt.Errorf("read migration %s: %w", m.name, err)
		}
		checksum := migrationChecksum(sqlBytes)

		var storedChecksum string
		err = conn.QueryRow(ctx,
			`SELECT checksum FROM schema_migrations WHERE version = $1`,
			m.version,
		).Scan(&storedChecksum)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return appliedCount, fmt.Errorf("check migration %s: %w", m.name, err)
		}
		if err == nil {
			if storedChecksum != "" && storedChecksum != checksum {
				return appliedCount, fmt.Errorf(
					"migration %s changed since it was applied: stored checksum %q, current %q",
					m.name, storedChecksum, checksum,
				)
			}
			if storedChecksum == "" {
				// Backfill legacy rows so the next run also detects drift.
				if _, err := conn.Exec(ctx,
					`UPDATE schema_migrations SET checksum = $2 WHERE version = $1`,
					m.version, checksum,
				); err != nil {
					return appliedCount, fmt.Errorf("backfill checksum for %s: %w", m.name, err)
				}
			}
			continue
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return appliedCount, fmt.Errorf("begin migration %s: %w", m.name, err)
		}

		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return appliedCount, fmt.Errorf("apply migration %s: %w", m.name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
			m.version, m.name, checksum,
		); err != nil {
			_ = tx.Rollback(ctx)
			return appliedCount, fmt.Errorf("record migration %s: %w", m.name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return appliedCount, fmt.Errorf("commit migration %s: %w", m.name, err)
		}
		appliedCount++
	}

	return appliedCount, nil
}

func migrationChecksum(sql []byte) string {
	sum := sha256.Sum256(sql)
	return hex.EncodeToString(sum[:])
}

func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	migrations := make([]migration, 0, len(entries))
	seen := make(map[int64]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		base := strings.TrimSuffix(entry.Name(), ".sql")
		parts := strings.SplitN(base, "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("migration %q must be named <version>_<description>.sql", entry.Name())
		}
		version, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("migration %q has an invalid version", entry.Name())
		}
		if previous, exists := seen[version]; exists {
			return nil, fmt.Errorf("migrations %q and %q share version %d", previous, entry.Name(), version)
		}
		seen[version] = entry.Name()
		migrations = append(migrations, migration{version: version, name: entry.Name()})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})
	return migrations, nil
}
