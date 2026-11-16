package auth

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

type fakeRepository struct {
	key       *APIKey
	findErr   error
	findCalls int
	findHash  []byte

	createErr error
	created   *APIKey

	revokeErr error
	revoked   *APIKey

	listErr error
	listed  []APIKey
}

func (f *fakeRepository) CreateAPIKey(context.Context, uuid.UUID, string, Role, []byte) (*APIKey, error) {
	return f.created, f.createErr
}

func (f *fakeRepository) FindActiveAPIKeyByHash(_ context.Context, hash []byte) (*APIKey, error) {
	f.findCalls++
	f.findHash = hash
	return f.key, f.findErr
}

func (f *fakeRepository) RevokeAPIKey(context.Context, uuid.UUID, string) (*APIKey, error) {
	return f.revoked, f.revokeErr
}

func (f *fakeRepository) ListAPIKeys(context.Context, uuid.UUID) ([]APIKey, error) {
	return f.listed, f.listErr
}

func TestGenerateToken(t *testing.T) {
	token, hash, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}
	if !ValidTokenFormat(token) {
		t.Fatalf("generated token %q is not in a valid format", token)
	}
	if len(hash) != 32 {
		t.Fatalf("hash length = %d, want 32", len(hash))
	}
	if bytes.Equal(hash, []byte(token)) {
		t.Fatal("hash equals the plaintext token")
	}
	if !bytes.Equal(hash, HashToken(token)) {
		t.Fatal("returned hash does not match HashToken(token)")
	}

	second, _, err := GenerateToken()
	if err != nil {
		t.Fatalf("second GenerateToken() error = %v", err)
	}
	if token == second {
		t.Fatal("GenerateToken() returned the same token twice")
	}
}

func TestValidTokenFormat(t *testing.T) {
	for _, token := range []string{
		"",
		"djp_",
		"djp_short",
		"nope_" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"djp_" + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
		"djp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	} {
		if ValidTokenFormat(token) {
			t.Errorf("ValidTokenFormat(%q) = true, want false", token)
		}
	}
}

func TestHashTokenIsStable(t *testing.T) {
	token := "djp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if !bytes.Equal(HashToken(token), HashToken(token)) {
		t.Fatal("HashToken() is not deterministic")
	}
}

func TestParseRole(t *testing.T) {
	for _, test := range []struct {
		raw  string
		want Role
	}{
		{"client", RoleClient},
		{" Operator ", RoleOperator},
	} {
		got, err := ParseRole(test.raw)
		if err != nil {
			t.Errorf("ParseRole(%q) error = %v", test.raw, err)
			continue
		}
		if got != test.want {
			t.Errorf("ParseRole(%q) = %q, want %q", test.raw, got, test.want)
		}
	}
	for _, raw := range []string{"", "admin", "root"} {
		if _, err := ParseRole(raw); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("ParseRole(%q) error = %v, want ErrInvalidArgument", raw, err)
		}
	}
}

func TestAuthenticate(t *testing.T) {
	tenantID := uuid.New()
	keyID := uuid.New()
	repo := &fakeRepository{key: &APIKey{
		ID:       keyID,
		TenantID: tenantID,
		Name:     "operator-key",
		Role:     RoleOperator,
	}}
	authenticator := NewAuthenticator(repo)

	token, _, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	identity, err := authenticator.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if identity.TenantID != tenantID || identity.KeyID != keyID || identity.KeyName != "operator-key" || identity.Role != RoleOperator {
		t.Fatalf("identity = %+v, want tenant %v key %v operator", identity, tenantID, keyID)
	}
	if !identity.IsOperator() {
		t.Error("IsOperator() = false, want true")
	}
	if !bytes.Equal(repo.findHash, HashToken(token)) {
		t.Error("repository received a hash that does not match the token")
	}
}

func TestAuthenticateRejectsMalformedTokenWithoutLookup(t *testing.T) {
	repo := &fakeRepository{}
	authenticator := NewAuthenticator(repo)

	if _, err := authenticator.Authenticate(context.Background(), "not-a-token"); !errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("Authenticate() error = %v, want ErrInvalidCredential", err)
	}
	if repo.findCalls != 0 {
		t.Errorf("repository lookups = %d, want 0 for malformed token", repo.findCalls)
	}
}

func TestAuthenticateMapsMissingKeyToInvalidCredential(t *testing.T) {
	for _, repoErr := range []error{ErrInvalidCredential, ErrNotFound} {
		authenticator := NewAuthenticator(&fakeRepository{findErr: repoErr})
		token, _, err := GenerateToken()
		if err != nil {
			t.Fatalf("GenerateToken() error = %v", err)
		}

		if _, err := authenticator.Authenticate(context.Background(), token); !errors.Is(err, ErrInvalidCredential) {
			t.Errorf("Authenticate() error = %v, want ErrInvalidCredential for repo error %v", err, repoErr)
		}
	}
}

func TestAuthenticateReturnsRepositoryFailure(t *testing.T) {
	repoErr := errors.New("database unavailable")
	authenticator := NewAuthenticator(&fakeRepository{findErr: repoErr})
	token, _, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	_, err = authenticator.Authenticate(context.Background(), token)
	if err == nil {
		t.Fatal("Authenticate() error = nil, want repository failure")
	}
	if errors.Is(err, ErrInvalidCredential) {
		t.Fatalf("Authenticate() error = %v, want an internal error", err)
	}
	if !errors.Is(err, repoErr) {
		t.Errorf("Authenticate() error = %v, want wrapped %v", err, repoErr)
	}
}

func TestAuthenticateRejectsInvalidStoredRole(t *testing.T) {
	authenticator := NewAuthenticator(&fakeRepository{key: &APIKey{
		ID:       uuid.New(),
		TenantID: uuid.New(),
		Name:     "broken",
		Role:     Role("admin"),
	}})
	token, _, err := GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	if _, err := authenticator.Authenticate(context.Background(), token); err == nil {
		t.Fatal("Authenticate() error = nil, want invalid-role error")
	}
}

func TestAuthenticateRejectsIncompleteStoredMetadata(t *testing.T) {
	for name, key := range map[string]*APIKey{
		"missing id":     {TenantID: uuid.New(), Name: "key", Role: RoleClient},
		"missing tenant": {ID: uuid.New(), Name: "key", Role: RoleClient},
		"missing name":   {ID: uuid.New(), TenantID: uuid.New(), Role: RoleClient},
	} {
		t.Run(name, func(t *testing.T) {
			authenticator := NewAuthenticator(&fakeRepository{key: key})
			token, _, err := GenerateToken()
			if err != nil {
				t.Fatalf("GenerateToken() error = %v", err)
			}
			if _, err := authenticator.Authenticate(context.Background(), token); err == nil {
				t.Fatal("Authenticate() error = nil, want invalid-metadata error")
			}
		})
	}
}

func TestAuthenticateWithoutRepository(t *testing.T) {
	var authenticator *Authenticator
	if _, err := authenticator.Authenticate(context.Background(), "djp_irrelevant"); err == nil {
		t.Fatal("Authenticate() error = nil, want configuration error")
	}
}
