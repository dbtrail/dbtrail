package cli

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"
)

// siblingHintTimeout bounds the two catalogue reads behind the hint. They run
// only after a command already answered, so they must never be what makes it
// slow — and on an index server under load, a hint is worth less than the
// answer arriving.
const siblingHintTimeout = 3 * time.Second

// siblingIndex is one other index database on the same server, with InnoDB's
// row estimate for its events table.
type siblingIndex struct {
	Name string
	Rows int64
}

// HintSiblingIndexes writes one line naming the OTHER index databases on this
// server, when the connected one holds no events at all (#1731).
//
// The shape it exists for: `bintrail-console watch` is given a boot index in
// BINTRAIL_INDEX_DSN, and the control plane provisions a database per source
// (bintrail_idx_<id>) where the events actually land. An operator who exports
// that same env file and runs `query`, `recover`, `reconstruct` or `status`
// reaches the boot database, gets zero of everything, and — during an incident
// — concludes there is no history for the table. The console, looking at the
// per-source database, shows the events. Nothing in the CLI's output pointed
// at the sibling.
//
// Silent unless it has something to say: no line when the index holds events
// (the answer was about real data), when there are no siblings, or when either
// catalogue read fails. A hint that guessed would be worse than none — this is
// read during an incident, and the answer it questions is the operator's own
// command.
func HintSiblingIndexes(ctx context.Context, db *sql.DB, dbName string, w io.Writer) {
	if db == nil || dbName == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, siblingHintTimeout)
	defer cancel()

	if holds, err := indexHoldsEvents(ctx, db, dbName); err != nil || holds {
		return
	}
	siblings, err := siblingIndexes(ctx, db, dbName)
	if err != nil || len(siblings) == 0 {
		return
	}
	fmt.Fprintf(w, "\nNote: %s holds no events. This server also has %s.\n",
		dbName, describeSiblings(siblings))
	fmt.Fprintln(w, "  A daemon that monitors sources from the console writes each source's events into its own")
	fmt.Fprintln(w, "  database, not into the one it was started with. Point --index-dsn (or BINTRAIL_INDEX_DSN)")
	fmt.Fprintln(w, "  at the database for the source you are asking about.")
}

// describeSiblings renders the names with their row estimates, capped so a
// server with many sources still produces one readable line.
func describeSiblings(s []siblingIndex) string {
	const show = 5
	parts := make([]string, 0, show+1)
	for i, sib := range s {
		if i == show {
			parts = append(parts, fmt.Sprintf("and %d more", len(s)-show))
			break
		}
		if sib.Rows > 0 {
			parts = append(parts, fmt.Sprintf("%s (~%d events)", sib.Name, sib.Rows))
			continue
		}
		parts = append(parts, sib.Name)
	}
	return strings.Join(parts, ", ")
}

// indexHoldsEvents answers whether binlog_events has any row at all. One row
// is enough, so this is an index probe rather than a count.
func indexHoldsEvents(ctx context.Context, db *sql.DB, dbName string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, "SELECT 1 FROM `"+dbName+"`.binlog_events LIMIT 1").Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case err == sql.ErrNoRows:
		return false, nil
	default:
		// Includes a database with no binlog_events at all: not an index this
		// hint understands, so it says nothing.
		return false, err
	}
}

// siblingIndexes lists the other databases on this server that carry a
// binlog_events table, FULLEST FIRST. The order is the whole usefulness of the
// hint: a server that has been running a while accumulates index databases —
// old sources, a test index, a restored one — and naming them alphabetically
// buries the one the operator is looking for behind whichever sorts first. The
// row count is InnoDB's estimate from the catalogue: exact enough to separate
// "this one has the data" from "this one is empty too", which is the only
// question being asked, and free compared with COUNT(*).
func siblingIndexes(ctx context.Context, db *sql.DB, dbName string) ([]siblingIndex, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_SCHEMA, COALESCE(TABLE_ROWS, 0)
		FROM information_schema.TABLES
		WHERE TABLE_NAME = 'binlog_events' AND TABLE_SCHEMA <> ?
		ORDER BY COALESCE(TABLE_ROWS, 0) DESC, TABLE_SCHEMA`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []siblingIndex
	for rows.Next() {
		var s siblingIndex
		if err := rows.Scan(&s.Name, &s.Rows); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
