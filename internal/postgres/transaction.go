package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WithTx runs fn inside a transaction, committing on success and rolling back
// on error or panic. It is the shared transaction boundary for future claim and
// lifecycle operations.
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) (err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			// Use a context that is not tied to the caller's cancellation so a
			// panic or a cancelled request still rolls back the transaction.
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}
	err = tx.Commit(ctx)
	committed = err == nil
	return err
}
