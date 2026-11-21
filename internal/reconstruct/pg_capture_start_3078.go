package reconstruct

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pgCaptureStartLSN reads where the capture writing the index started
// reading its source from; a variable so a test answers it without an index.
var pgCaptureStartLSN = query.PGCaptureStartLSN

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
// A run told to go on over missing changes (AllowGaps) is warned and goes on.
func pgRefuseFileBeforeCapture(db *sql.DB, cfg FullTableConfig, schema, table string, bmeta baseline.DumpMetadata) error {
	if db == nil || bmeta.LSN == 0 {
		return nil
	}
	start, known := pgCaptureStartLSN(db)
	if !pgFileBeforeCapture(bmeta.LSN, start, known) {
		return nil
	}
	if cfg.AllowGaps {
		slog.Warn("this table's snapshot starts before capture of the server did: the changes committed between the two were never captured and are missing from this reconstruction",
			"schema", schema, "table", table, "snapshot_from", lsnText(bmeta.LSN), "capture_from", lsnText(start))
		return nil
	}
	return fmt.Errorf("%s.%s: the table's snapshot holds its changes from %s on, and capture of the server started reading the source at %s, after it: "+
		"the changes committed between the two were never captured, so the table cannot be brought up to date from this snapshot. Take a new snapshot of the table",
		schema, table, lsnText(bmeta.LSN), lsnText(start))
}
