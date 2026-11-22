package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pgCaptureStartRead reads where the capture writing the index started
// reading its source from, telling a failed read apart from nothing recorded
// (query.PGCaptureStartRead, bounded to 5 seconds); a variable so a test
// answers it without an index.
var pgCaptureStartRead = query.PGCaptureStartRead

// pgFileBeforeCapture reports whether a table file whose changes start at
// floor is known to miss changes (#3078): the index records that its capture
// started reading the source at start, after floor, so what was committed
// between the two was never delivered to this index. False when either
// position is not known, which leaves the question to the warning the fold
// has always given (WarnBaselineFirstEventGapForTable).
//
// Equal is not a gap: a snapshot taken before the first capture creates the
// slot, and that capture then starts at the slot's position, the file's floor.
func pgFileBeforeCapture(floor, start uint64, startKnown bool) bool {
	return startKnown && floor != 0 && start > floor
}

// pgRefuseFileBeforeCapture refuses a table whose file is known to miss
// changes (pgFileBeforeCapture). The file's own floor is asked about, also
// when the window is read from a pair beside it: the pair continues the file,
// and the file is what misses the changes.
//
// Unlike the oldest surviving event, the capture's recorded start does not
// move with rotation, so this is a proof and not the doubt the warning
// states. A refused table refuses the update, and the schedule then reads the
// source again, which is the repair: the new snapshot's floor is the slot's
// position, at or after where capture started.
//
// A read of the capture's start that failed is an error of the update, never
// "nothing recorded": letting the table through would rewrite it with a new
// floor, after which a gap it carries could never be seen again. A run told
// to go on over missing changes (AllowGaps) is warned and goes on, in both
// cases.
func pgRefuseFileBeforeCapture(db *sql.DB, cfg FullTableConfig, schema, table string, bmeta baseline.DumpMetadata) error {
	if db == nil || bmeta.LSN == 0 {
		return nil
	}
	start, known, err := pgCaptureStartRead(context.Background(), db)
	if err != nil {
		if cfg.AllowGaps {
			slog.Warn("where capture of the server started could not be read, so whether this table's snapshot starts before it is not known; going on as told to over missing changes",
				"schema", schema, "table", table, "snapshot_from", lsnText(bmeta.LSN), "error", err)
			return nil
		}
		return fmt.Errorf("%s.%s: whether the table's snapshot starts before capture of the server did could not be checked, so it is not brought up to date from it: %w", schema, table, err)
	}
	if !pgFileBeforeCapture(bmeta.LSN, start, known) {
		return nil
	}
	if cfg.AllowGaps {
		slog.Warn("this table's snapshot starts before capture of the server did: the changes committed between the two were never captured and are missing from this reconstruction",
			"schema", schema, "table", table, "snapshot_from", lsnText(bmeta.LSN), "capture_from", lsnText(start))
		return nil
	}
	return fmt.Errorf("%s.%s: the table's snapshot holds its changes from %s on, and capture of the server started reading the source at %s, after it: "+
		"the changes committed between the two were never captured, so the table cannot be brought up to date from this snapshot. Take a new snapshot (`snapshot run`)",
		schema, table, lsnText(bmeta.LSN), lsnText(start))
}
