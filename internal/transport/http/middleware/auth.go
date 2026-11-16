package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
)

// AuthorizationHeader carries the bearer API key.
const AuthorizationHeader = "Authorization"

type identityContextKey struct{}

// RequireAuth resolves an Authorization: Bearer token into a verified identity.
// A missing, malformed or invalid key is rejected; the tenant is never accepted
// from a caller-supplied header.
func RequireAuth(authenticator auth.TokenAuthenticator, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if authenticator == nil {
				writeError(w, http.StatusServiceUnavailable, "auth_unavailable", "authentication is not configured")
				return
			}

			token, ok := bearerToken(r.Header.Get(AuthorizationHeader))
			if !ok {
				writeError(w, http.StatusUnauthorized, "authentication_required", "Authorization: Bearer <api-key> is required")
				return
			}

			identity, err := authenticator.Authenticate(r.Context(), token)
			if err != nil {
				if errors.Is(err, auth.ErrInvalidCredential) {
					writeError(w, http.StatusUnauthorized, "invalid_api_key", "API key is invalid or revoked")
					return
				}
				if logger != nil {
					logger.Error("authenticate request", "error", err)
				}
				writeError(w, http.StatusInternalServerError, "internal_error", "an internal error occurred")
				return
			}

			ctx := context.WithValue(r.Context(), identityContextKey{}, identity)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireRole authorizes an already authenticated identity. It must run after
// RequireAuth and rejects callers whose key lacks role with 403.
func RequireRole(role auth.Role) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			identity, ok := IdentityFrom(r.Context())
			if !ok {
				writeError(w, http.StatusUnauthorized, "authentication_required", "authentication is required")
				return
			}
			if identity.Role != role {
				writeError(w, http.StatusForbidden, "forbidden", string(role)+" role is required")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// IdentityFrom returns the identity resolved by RequireAuth.
func IdentityFrom(ctx context.Context) (auth.Identity, bool) {
	identity, ok := ctx.Value(identityContextKey{}).(auth.Identity)
	return identity, ok
}

// bearerToken extracts a non-empty token from an Authorization header.
func bearerToken(header string) (string, bool) {
	fields := strings.Fields(header)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		return "", false
	}
	token := strings.TrimSpace(fields[1])
	if token == "" {
		return "", false
	}
	return token, true
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(dto.ErrorResponse{
		Error: dto.ErrorBody{Code: code, Message: message},
	})
}
