// Package migrations embeds the SQL migration files so they can be applied
// both from the migrate command and from the integration tests without
// depending on the current working directory.
package migrations

import "embed"

// FS holds every ordered *.sql migration in this directory.
//
//go:embed *.sql
var FS embed.FS
