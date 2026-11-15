package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/google/uuid"
)

// TenantHeader is the Phase 1 identity boundary. Phase 6 will replace this
// with verified authentication claims and remove caller-supplied tenancy.
const TenantHeader = "X-Tenant-ID"

type tenantContextKey struct{}

// RequireTenant resolves X-Tenant-ID into the request context.
func RequireTenant(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimSpace(r.Header.Get(TenantHeader))
		if raw == "" {
			writeError(w, http.StatusUnauthorized, "tenant_required", TenantHeader+" header is required")
			return
		}
		tenantID, err := uuid.Parse(raw)
		if err != nil || tenantID == uuid.Nil {
			writeError(w, http.StatusBadRequest, "invalid_tenant", TenantHeader+" must be a valid UUID")
			return
		}
		ctx := context.WithValue(r.Context(), tenantContextKey{}, tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// TenantID returns the tenant resolved by RequireTenant.
func TenantID(ctx context.Context) (uuid.UUID, bool) {
	tenantID, ok := ctx.Value(tenantContextKey{}).(uuid.UUID)
	return tenantID, ok
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(dto.ErrorResponse{
		Error: dto.ErrorBody{Code: code, Message: message},
	})
}
