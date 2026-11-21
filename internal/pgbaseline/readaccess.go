package pgbaseline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// checkReadAccess is pgcapture.CheckReadAccess; a test replaces it.
var checkReadAccess = pgcapture.CheckReadAccess

// refuseUnreadable stops a snapshot whose role cannot read a table it would
// copy (#3022), before anything is created: no slot, no snapshot folder, no
// row read. Without it the COPY of the first such table failed with
// "permission denied" (42501), after the slot was made and with the tables
// before it already written.
//
// `server add` asks the same question and refuses there. This is for what
// that cannot see: a server added by an older DBTrail, a table that joined
// the publication since, a permission taken away.
//
// It refuses on the server's own answer only. A check that could not run
// (the catalog read failed) is logged and the snapshot goes ahead: the COPY
// then says what the server thinks, as it did before.
func refuseUnreadable(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters, logger *slog.Logger) error {
	err := checkReadAccess(ctx, conn, publication, filters)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, pgcapture.ErrNoReadAccess):
		return fmt.Errorf("pgbaseline: this snapshot was not started: %w", err)
	case ctx.Err() != nil:
		return fmt.Errorf("pgbaseline: checking read access to the tables to copy: %w", err)
	}
	logger.Warn("pgbaseline: could not check that the role can read the tables to copy; going ahead, and the copy itself will say if it cannot",
		"publication", publication, "error", err)
	return nil
}
