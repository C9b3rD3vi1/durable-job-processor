// Package migrations embeds versioned SQL migrations so the migrate command and
// integration tests can apply the same schema without depending on a local path.
package migrations

import "embed"

// FS contains every versioned SQL migration in this directory.
//
//go:embed *.sql
var FS embed.FS
