package main

import (
	"context"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/pgdrill"
)

// `drill` comes with the read commands; this binary is the one that links
// the PostgreSQL driver, so it is the one that gives drill a postgres://
// target.
func init() {
	cli.SetPostgresDrillTarget(func(ctx context.Context, dsn string) (cli.DrillTarget, error) {
		t, err := pgdrill.Open(ctx, dsn)
		if err != nil {
			return nil, err
		}
		return t, nil
	})
}
