package jobs

import (
	"fmt"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

// Actor is the verified caller for a tenant-scoped job operation. It is built
// from the authenticated API key identity at the HTTP boundary, never from a
// request header or payload field.
type Actor struct {
	TenantID   uuid.UUID
	KeyID      uuid.UUID
	IsOperator bool
}

// validate rejects incomplete identities before a repository call.
func (a Actor) validate() error {
	if a.TenantID == uuid.Nil {
		return fmt.Errorf("%w: tenant_id is required", domain.ErrInvalidArgument)
	}
	if a.KeyID == uuid.Nil {
		return fmt.Errorf("%w: api key id is required", domain.ErrInvalidArgument)
	}
	return nil
}

// canAccess enforces owner-or-operator authorization inside a tenant. Jobs
// without a recorded creator predate attribution and remain tenant-visible.
func (a Actor) canAccess(job *domain.Job) bool {
	if job == nil || job.TenantID != a.TenantID {
		return false
	}
	if a.IsOperator || job.CreatedByKeyID == nil {
		return true
	}
	return *job.CreatedByKeyID == a.KeyID
}
