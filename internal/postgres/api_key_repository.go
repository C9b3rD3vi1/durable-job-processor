package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// apiKeyColumns is cast to text for UUID columns so the repository can scan
// into plain strings and convert with google/uuid explicitly.
const apiKeyColumns = `
	id::text,
	tenant_id::text,
	name,
	role,
	created_at,
	revoked_at
`

// APIKeyRepository is the PostgreSQL implementation of auth.Repository.
type APIKeyRepository struct {
	pool *pgxpool.Pool
}

// NewAPIKeyRepository creates a repository backed by pool.
func NewAPIKeyRepository(pool *pgxpool.Pool) *APIKeyRepository {
	return &APIKeyRepository{pool: pool}
}

// CreateAPIKey stores a hashed API key and returns its metadata.
func (r *APIKeyRepository) CreateAPIKey(ctx context.Context, tenantID uuid.UUID, name string, role auth.Role, keyHash []byte) (*auth.APIKey, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("API key repository is not configured")
	}
	if tenantID == uuid.Nil {
		return nil, fmt.Errorf("%w: tenant id is required", auth.ErrInvalidArgument)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("%w: key name is required", auth.ErrInvalidArgument)
	}
	if !role.Valid() {
		return nil, fmt.Errorf("%w: role %q is not supported", auth.ErrInvalidArgument, role)
	}
	if len(keyHash) == 0 {
		return nil, fmt.Errorf("%w: key hash is required", auth.ErrInvalidArgument)
	}

	const query = `
		INSERT INTO api_keys (id, tenant_id, name, role, key_hash)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING ` + apiKeyColumns

	key, err := scanAPIKey(r.pool.QueryRow(ctx, query,
		uuid.New().String(),
		tenantID.String(),
		name,
		string(role),
		keyHash,
	))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			if pgErr.ConstraintName == "api_keys_tenant_name_uq" {
				return nil, fmt.Errorf("%w: key name %q already exists for this tenant", auth.ErrAlreadyExists, name)
			}
			return nil, fmt.Errorf("%w: key digest already exists", auth.ErrAlreadyExists)
		}
		return nil, fmt.Errorf("create API key: %w", err)
	}
	return key, nil
}

// FindActiveAPIKeyByHash returns the non-revoked key matching keyHash. A miss
// is reported as ErrInvalidCredential rather than ErrNotFound so callers do not
// learn whether a revoked or unknown key was presented.
func (r *APIKeyRepository) FindActiveAPIKeyByHash(ctx context.Context, keyHash []byte) (*auth.APIKey, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("API key repository is not configured")
	}
	if len(keyHash) == 0 {
		return nil, fmt.Errorf("%w: key hash is required", auth.ErrInvalidArgument)
	}

	const query = `
		SELECT ` + apiKeyColumns + `
		FROM api_keys
		WHERE key_hash = $1
		  AND revoked_at IS NULL`

	key, err := scanAPIKey(r.pool.QueryRow(ctx, query, keyHash))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, auth.ErrInvalidCredential
		}
		return nil, fmt.Errorf("find API key: %w", err)
	}
	return key, nil
}

// RevokeAPIKey marks an active key revoked. Revoking an already revoked key is
// idempotent and returns its metadata.
func (r *APIKeyRepository) RevokeAPIKey(ctx context.Context, tenantID uuid.UUID, name string) (*auth.APIKey, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("API key repository is not configured")
	}
	if tenantID == uuid.Nil {
		return nil, fmt.Errorf("%w: tenant id is required", auth.ErrInvalidArgument)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("%w: key name is required", auth.ErrInvalidArgument)
	}

	const revoke = `
		UPDATE api_keys
		SET revoked_at = now()
		WHERE tenant_id = $1
		  AND name = $2
		  AND revoked_at IS NULL
		RETURNING ` + apiKeyColumns

	key, err := scanAPIKey(r.pool.QueryRow(ctx, revoke, tenantID.String(), name))
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("revoke API key: %w", err)
	}

	// No active row was updated: return an already revoked key, or report a
	// genuinely unknown key.
	const get = `
		SELECT ` + apiKeyColumns + `
		FROM api_keys
		WHERE tenant_id = $1
		  AND name = $2`

	key, err = scanAPIKey(r.pool.QueryRow(ctx, get, tenantID.String(), name))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, auth.ErrNotFound
		}
		return nil, fmt.Errorf("revoke API key: %w", err)
	}
	return key, nil
}

// ListAPIKeys returns all keys for a tenant, newest first.
func (r *APIKeyRepository) ListAPIKeys(ctx context.Context, tenantID uuid.UUID) ([]auth.APIKey, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("API key repository is not configured")
	}
	if tenantID == uuid.Nil {
		return nil, fmt.Errorf("%w: tenant id is required", auth.ErrInvalidArgument)
	}

	const query = `
		SELECT ` + apiKeyColumns + `
		FROM api_keys
		WHERE tenant_id = $1
		ORDER BY created_at DESC, name ASC`

	rows, err := r.pool.Query(ctx, query, tenantID.String())
	if err != nil {
		return nil, fmt.Errorf("list API keys: %w", err)
	}
	defer rows.Close()

	keys := make([]auth.APIKey, 0)
	for rows.Next() {
		key, err := scanAPIKey(rows)
		if err != nil {
			return nil, fmt.Errorf("scan API key: %w", err)
		}
		keys = append(keys, *key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list API keys: %w", err)
	}
	return keys, nil
}

func scanAPIKey(row pgx.Row) (*auth.APIKey, error) {
	var (
		key      auth.APIKey
		id       string
		tenantID string
		role     string
	)

	if err := row.Scan(&id, &tenantID, &key.Name, &role, &key.CreatedAt, &key.RevokedAt); err != nil {
		return nil, err
	}

	parsedID, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("parse API key id: %w", err)
	}
	parsedTenantID, err := uuid.Parse(tenantID)
	if err != nil {
		return nil, fmt.Errorf("parse API key tenant id: %w", err)
	}

	key.ID = parsedID
	key.TenantID = parsedTenantID
	key.Role = auth.Role(role)
	return &key, nil
}
