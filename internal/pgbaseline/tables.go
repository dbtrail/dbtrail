package pgbaseline

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
)

// TablesOfNextRun lists, as "schema.table", the tables a snapshot taken now
// would hold: the publication's tables narrowed by filters, by the discovery
// Run itself uses (discoverTables), so the answer and the next snapshot
// cannot disagree about a name or about what is in scope.
//
// It is what a snapshot update asks to learn which tables were added to the
// publication after the snapshot it starts from (#2759). One short-lived
// plain connection and catalog reads; nothing is written, and the caller
// bounds it with ctx.
func TablesOfNextRun(ctx context.Context, queryDSN, publication string, filters event.Filters) ([]string, error) {
	conn, err := pgx.Connect(ctx, queryDSN)
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: connect to list the publication's tables: %w", err)
	}
	defer conn.Close(context.Background())
	// Discovery warns about partitioned tables each time it runs; a snapshot
	// says that once, and this is asked at every update.
	tables, err := discoverTables(ctx, conn, publication, filters, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		out = append(out, t.Schema+"."+t.Table)
	}
	return out, nil
}
