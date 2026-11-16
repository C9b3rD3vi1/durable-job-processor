// Package auth implements API-key authentication for the HTTP API boundary.
//
// API keys are bearer credentials. Only a SHA-256 digest of each key is stored
// in PostgreSQL; the plaintext key is returned once at creation time. A key
// carries the tenant and role used for authorization, so request handlers never
// trust a caller-supplied tenant identifier.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// TokenPrefix makes generated API keys recognizable to secret scanners.
	TokenPrefix = "djp_"

	tokenBytes = 32
)

// Role is the authorization level granted to an API key.
type Role string

const (
	// RoleClient can submit and read jobs in its tenant.
	RoleClient Role = "client"

	// RoleOperator can additionally replay dead-letter jobs.
	RoleOperator Role = "operator"
)

// Valid reports whether role is a known role.
func (r Role) Valid() bool {
	return r == RoleClient || r == RoleOperator
}

// ParseRole normalizes and validates a role name.
func ParseRole(raw string) (Role, error) {
	role := Role(strings.ToLower(strings.TrimSpace(raw)))
	if !role.Valid() {
		return "", fmt.Errorf("%w: role must be %q or %q", ErrInvalidArgument, RoleClient, RoleOperator)
	}
	return role, nil
}

// Errors returned by the authentication layer. Transport code maps
// ErrInvalidCredential to 401 and treats every other error as internal.
var (
	ErrInvalidArgument   = errors.New("invalid authentication argument")
	ErrInvalidCredential = errors.New("invalid API key")
	ErrNotFound          = errors.New("API key not found")
	ErrAlreadyExists     = errors.New("API key already exists")
)

// Identity is the verified caller derived from an API key.
type Identity struct {
	TenantID uuid.UUID
	KeyID    uuid.UUID
	KeyName  string
	Role     Role
}

// IsOperator reports whether the identity may perform operator actions.
func (i Identity) IsOperator() bool {
	return i.Role == RoleOperator
}

// APIKey is the durable metadata for an API key. The key hash is deliberately
// not exposed outside the repository.
type APIKey struct {
	ID        uuid.UUID
	TenantID  uuid.UUID
	Name      string
	Role      Role
	CreatedAt time.Time
	RevokedAt *time.Time
}

// Active reports whether the key can still authenticate.
func (k APIKey) Active() bool {
	return k.RevokedAt == nil
}

// Repository is the persistence boundary for API keys.
type Repository interface {
	// CreateAPIKey stores keyHash for a tenant and returns the new metadata.
	CreateAPIKey(ctx context.Context, tenantID uuid.UUID, name string, role Role, keyHash []byte) (*APIKey, error)

	// FindActiveAPIKeyByHash returns a non-revoked key or ErrInvalidCredential.
	FindActiveAPIKeyByHash(ctx context.Context, keyHash []byte) (*APIKey, error)

	// RevokeAPIKey marks a key revoked. Revoking an already revoked key is a
	// no-op that returns the existing metadata; an unknown key returns
	// ErrNotFound.
	RevokeAPIKey(ctx context.Context, tenantID uuid.UUID, name string) (*APIKey, error)

	// ListAPIKeys returns every key for a tenant, newest first.
	ListAPIKeys(ctx context.Context, tenantID uuid.UUID) ([]APIKey, error)
}

// TokenAuthenticator resolves a bearer token into a verified identity.
type TokenAuthenticator interface {
	Authenticate(ctx context.Context, token string) (Identity, error)
}

// Authenticator verifies API keys against the repository.
type Authenticator struct {
	repo Repository
}

// NewAuthenticator creates an Authenticator backed by repo.
func NewAuthenticator(repo Repository) *Authenticator {
	return &Authenticator{repo: repo}
}

// Authenticate resolves token to an identity. Unknown, malformed and revoked
// keys all return ErrInvalidCredential so callers cannot probe key existence.
func (a *Authenticator) Authenticate(ctx context.Context, token string) (Identity, error) {
	if a == nil || a.repo == nil {
		return Identity{}, errors.New("authentication is not configured")
	}

	token = strings.TrimSpace(token)
	if !ValidTokenFormat(token) {
		return Identity{}, ErrInvalidCredential
	}

	key, err := a.repo.FindActiveAPIKeyByHash(ctx, HashToken(token))
	if err != nil {
		if errors.Is(err, ErrInvalidCredential) || errors.Is(err, ErrNotFound) {
			return Identity{}, ErrInvalidCredential
		}
		return Identity{}, fmt.Errorf("authenticate API key: %w", err)
	}
	if !key.Role.Valid() {
		return Identity{}, fmt.Errorf("API key %s has invalid role %q", key.ID, key.Role)
	}
	if key.ID == uuid.Nil || key.TenantID == uuid.Nil || strings.TrimSpace(key.Name) == "" {
		return Identity{}, fmt.Errorf("API key %s has incomplete metadata", key.ID)
	}

	return Identity{
		TenantID: key.TenantID,
		KeyID:    key.ID,
		KeyName:  key.Name,
		Role:     key.Role,
	}, nil
}

// GenerateToken returns a new high-entropy bearer token and its storage digest.
// The caller must show token once and retain only the digest.
func GenerateToken() (string, []byte, error) {
	raw := make([]byte, tokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("generate API key: %w", err)
	}

	token := TokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	return token, HashToken(token), nil
}

// HashToken returns the SHA-256 digest stored for token. A dictionary-resistant
// password hash is unnecessary here: tokens have 256 bits of entropy and every
// authenticated request would otherwise pay a deliberate latency cost.
func HashToken(token string) []byte {
	digest := sha256.Sum256([]byte(token))
	return digest[:]
}

// ValidTokenFormat reports whether token has the prefix and length produced by
// GenerateToken. It rejects obviously malformed input before a database lookup.
func ValidTokenFormat(token string) bool {
	if !strings.HasPrefix(token, TokenPrefix) {
		return false
	}
	secret := strings.TrimPrefix(token, TokenPrefix)
	if len(secret) != base64.RawURLEncoding.EncodedLen(tokenBytes) {
		return false
	}
	_, err := base64.RawURLEncoding.DecodeString(secret)
	return err == nil
}
