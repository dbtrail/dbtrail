package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// ErrDestructiveDDL is wrapped into the error CheckDestructiveDDL returns
// when it finds a TRUNCATE/DROP/RENAME/CREATE OR REPLACE on the target table
// inside the reconstruction window (#764).
var ErrDestructiveDDL = errors.New("destructive DDL in reconstruction window")

// DDLWindow is the stretch of the source's history a replay covers, stated
// both ways the replay itself bounds it: by time and by binlog position.
//
// Since and Until are the snapshot's time and the requested instant. Anchor is
// the binlog coordinate the snapshot was taken at, where its row changes
// start; nil when it recorded none (an older snapshot, a PostgreSQL one).
// Cut is the run's positional upper bound (ResolveSnapshotCut), set only by
// a run that bounds its row changes by position; nil everywhere else.
type DDLWindow struct {
	Since, Until time.Time
	Anchor, Cut  *query.BinlogPos
	// Mark is the snapshot's DDL mark (ddl_mark.go), nil when it has none.
	// A row at or below it is not placed by position alone.
	Mark *DDLMark

	// sinceInclusive counts a statement in the same second as Since as
	// inside by time. See FindDestructiveDDL.
	sinceInclusive bool
}

// AnchorOf is the binlog coordinate a snapshot's footer recorded, as
// DDLWindow.Anchor takes it: nil when it recorded none.
func AnchorOf(bmeta baseline.DumpMetadata) *query.BinlogPos {
	if bmeta.BinlogFile == "" || bmeta.BinlogPos <= 0 {
		return nil
	}
	return &query.BinlogPos{File: bmeta.BinlogFile, Pos: uint64(bmeta.BinlogPos)}
}

// destructiveDDL is one schema_changes row of a kind that removes rows without
// writing row changes. AfterSince and AtOrBeforeUntil are decided by the index
// server, like every other lookup on detected_at. Pos is the statement's END.
type destructiveDDL struct {
	ID                          uint64
	Type                        string
	DetectedAt                  time.Time
	File                        string
	Pos                         uint64
	AfterSince, AtOrBeforeUntil bool
}

func (d destructiveDDL) hasPos() bool { return d.File != "" && d.Pos > 0 }

// How a statement was placed inside a window.
const (
	ddlOutside    = iota
	ddlByTime     // its time lies in the window
	ddlByPosition // its time does not, its binlog position does
	ddlUnplaced   // it has no position, so nothing places it before the snapshot
)

// place says whether the statement lies inside the window, and by what.
//
// detected_at is when the statement RAN on the source, not when it was
// indexed. With capture behind, a statement is recorded after a snapshot was
// published, with a time before that snapshot's: by time it belongs to a
// window that closed without it, and no later window (#1912). Its position
// does not have that problem, which is why row changes are selected by it.
//
// So a statement is inside when its time says so, as it always was, and also
// when its position does. The statement is left out only when both agree it
// is outside: a missed statement brings removed rows back for good, a
// statement counted once too often refuses one run.
//
// By position, after the snapshot means past its Anchor. The stored position
// is the statement's end, so one that ends at the Anchor is before it. A row
// with no position cannot be placed before the snapshot and counts as after.
// At or before the target means its time is at or before Until, the bound the
// row changes have in every run, or its position is at or before the Cut in a
// run that has one.
//
// Positions are compared by query.BinlogPos.AtOrBefore, the rule the row
// changes are fetched by. It holds for one source's own sequence of files, in
// position mode and GTID mode alike. A statement recorded under another file
// name than the Anchor's (sameBinlogSequence) comes from another sequence, a
// source this index followed before, and is placed by time alone: read by
// position it would refuse every run from every later snapshot.
//
// What this cannot tell apart is a sequence that started over under the SAME
// name (a new source with the same log_bin name, RESET MASTER): a statement
// from the old numbering that sorts after the Anchor refuses, and a new
// snapshot does not clear it. The refusal says so.
//
// Without an Anchor nothing is placed by position and the window is the time
// window alone: a snapshot that recorded no position also fetches its row
// changes by time alone.
//
// A row at or below the snapshot's Mark was in the index before the snapshot
// was checked or dumped, so it cannot be late: it is left out unless its time
// places it. That is what lets an old statement from a numbering that started
// over under the same name stop refusing once a snapshot with a mark exists.
func (w DDLWindow) place(d destructiveDDL) int {
	how := w.placeByTimeAndPosition(d)
	if how != ddlByTime && how != ddlOutside && w.Mark != nil && d.ID <= w.Mark.ID {
		return ddlOutside
	}
	return how
}

func (w DDLWindow) placeByTimeAndPosition(d destructiveDDL) int {
	if d.AfterSince && d.AtOrBeforeUntil {
		return ddlByTime
	}
	if w.Anchor == nil {
		return ddlOutside
	}
	pos := query.BinlogPos{File: d.File, Pos: d.Pos}
	if d.hasPos() && (!sameBinlogSequence(d.File, w.Anchor.File) || pos.AtOrBefore(*w.Anchor)) {
		return ddlOutside
	}
	if !d.AtOrBeforeUntil && (!d.hasPos() || w.Cut == nil || !pos.AtOrBefore(*w.Cut)) {
		return ddlOutside
	}
	if !d.hasPos() {
		return ddlUnplaced
	}
	return ddlByPosition
}

// sameBinlogSequence reports whether two binlog file names belong to one
// sequence: the same name before the numeric suffix. "binlog.000009" and
// "binlog.000010" do, "mysql-bin.000009" and "binlog.000009" do not. A name
// with no suffix is compared whole.
func sameBinlogSequence(a, b string) bool {
	base := func(f string) string {
		if i := strings.LastIndexByte(f, '.'); i >= 0 {
			return f[:i]
		}
		return f
	}
	return base(a) == base(b)
}

// CheckDestructiveDDL refuses when schema_changes records a TRUNCATE TABLE,
// DROP TABLE, RENAME TABLE or CREATE OR REPLACE TABLE on schema.table inside
// the window a baseline+delta merge replays. See DDLWindow.place for what
// inside means.
//
// TRUNCATE and DROP+re-CREATE emit no row-level binlog events — the parser
// only records them as an audit entry in schema_changes (#700-adjacent DDL
// tracking) — so ReconstructTable's and the shim's _snapshot full-table merge
// have no delta to apply and would silently pass every baseline row straight
// through, resurrecting rows the DDL actually deleted as if they still
// existed at --at (#764). Refusing with a clear, actionable error is the
// chosen fix over auto-truncating-and-replaying-from-the-DDL-point, which
// would be materially more complex and itself error prone (Occam's razor).
//
// RENAME TABLE is included because it moves the table's row-event stream to
// a new name; the baseline for the old name can no longer be trusted to
// represent schema.table's state either.
//
// MariaDB's CREATE OR REPLACE TABLE is included because on an existing table
// it drops the rows with no row events, exactly like DROP then CREATE (#1664).
//
// A missing schema_changes table (a pre-DDL-tracking index, or a caller that
// hasn't run indexer.EnsureSchema) is treated as "nothing to check" rather
// than a hard failure: this is an additive safety net on top of the existing
// reconstruct contract, not a new hard dependency.
func CheckDestructiveDDL(ctx context.Context, db *sql.DB, schema, table string, w DDLWindow) error {
	f, err := findDestructiveDDL(ctx, db, schema, table, w)
	if errors.Is(err, errSchemaChangesMissing) {
		return nil // nothing to check, as this has always answered
	}
	if err != nil || f.how == ddlOutside {
		return err
	}
	return destructiveDDLErr(schema, table, f, w)
}

// destructiveDDLErr is the refusal: what ran, on which table, when, where in
// the binlog, and what to do. It names no command-line flag, because it also
// reaches clients that have none.
func destructiveDDLErr(schema, table string, f ddlFinding, w DDLWindow) error {
	d, how := f.first, f.how
	where := "its binlog position is not recorded"
	if d.hasPos() {
		where = fmt.Sprintf("recorded at %s:%d", d.File, d.Pos)
	}
	where += fmt.Sprintf(", row %d of schema_changes", d.ID)
	// Every row this window counts by position alone, in one statement: an
	// operator removing them one refusal at a time would rerun once per row.
	ids := make([]string, len(f.positional))
	for i, id := range f.positional {
		ids[i] = strconv.FormatUint(id, 10)
	}
	deleteSQL := "DELETE FROM schema_changes WHERE id IN (" + strings.Join(ids, ", ") + ");"
	placed := "between the snapshot this starts from and the requested point in time"
	todo := fmt.Sprintf("Take a new snapshot of the table, which will hold it as it is after the %s, and start from that one",
		strings.ToLower(d.Type))
	switch how {
	case ddlByPosition:
		// The cause is not stated: a statement indexed late and a source
		// whose clock is ahead both land here.
		placed = fmt.Sprintf("inside the replayed changes by its binlog position (the snapshot is at %s:%d), "+
			"though its time is outside them", w.Anchor.File, w.Anchor.Pos)
		todo += ". If instead the statement ran before the snapshot, on a source whose binlog files started over " +
			"since (a failover, RESET MASTER), the two positions do not compare. A snapshot from a full backup " +
			"taken by DBTrail clears that; otherwise remove the rows this window counts by position alone " +
			"from the index database: " + deleteSQL
	case ddlUnplaced:
		placed = "with nothing to place it before the snapshot this starts from, so it is counted as after it"
		todo = "A snapshot from a full backup taken by DBTrail clears this. Otherwise the rows this window " +
			"counts by position alone have to be removed from the index database: " + deleteSQL
	}
	return fmt.Errorf(
		"%w: %s on %s.%s, run at %s (%s), lies %s. "+
			"It wrote no row changes to replay, so the result would keep the rows it removed as if they still existed. %s",
		ErrDestructiveDDL, d.Type, schema, table, d.DetectedAt.UTC().Format(time.RFC3339), where, placed, todo)
}

// errSchemaChangesMissing marks an index that has no schema_changes table at
// all — one that predates DDL tracking, or a caller that has not run
// indexer.EnsureSchema. It is an ANSWER, not a failure, but callers must say
// what they do with it: the baseline paths have always treated it as "nothing
// to check", while the binlog-only fallback says out loud that it could not
// check.
var errSchemaChangesMissing = errors.New("this index has no schema_changes table")

// FindDestructiveDDL is CheckDestructiveDDL's finding without its message, for
// a caller that is not reconstructing and says it in its own words (verify).
// An index with no schema_changes table answers not found, as
// CheckDestructiveDDL does. It looks by time alone.
//
// Unlike CheckDestructiveDDL, since is INCLUSIVE. detected_at and the snapshot
// times are whole seconds, so a statement stamped the same second as the older
// snapshot may have run after that snapshot's anchor, where the replay starts.
// Missing it gives the false mismatch this lookup exists to prevent; counting
// it at worst leaves one table unchecked for one run. snapshotForDDLInWindow
// takes the same second as inside for the same reason.
func FindDestructiveDDL(ctx context.Context, db *sql.DB, schema, table string, since, until time.Time) (ddlType string, detectedAt time.Time, found bool, err error) {
	f, err := findDestructiveDDL(ctx, db, schema, table, DDLWindow{Since: since, Until: until, sinceInclusive: true})
	if errors.Is(err, errSchemaChangesMissing) || (err == nil && f.how == ddlOutside) {
		return "", time.Time{}, false, nil
	}
	if err != nil {
		return "", time.Time{}, false, err
	}
	return f.first.Type, f.first.DetectedAt, true, nil
}

// ddlFinding is what a lookup found: the first statement inside the window,
// oldest first, and how it was placed there (ddlOutside when none is), and the
// ids of every row the window counts by position alone or with no position.
type ddlFinding struct {
	first      destructiveDDL
	how        int
	positional []uint64
}

// findDestructiveDDL places every destructive statement recorded for the
// table. errSchemaChangesMissing when the table does not exist.
//
// The window's Mark is checked against the index only when it would leave a
// row out: a mark whose row is gone or holds something else was read against
// ids that now name other rows (restore-index, the table created again), and
// is not used. Without it, more rows are placed, never fewer.
func findDestructiveDDL(ctx context.Context, db *sql.DB, schema, table string, w DDLWindow) (ddlFinding, error) {
	ddls, err := loadDestructiveDDLs(ctx, db, schema, table, w)
	if err != nil {
		return ddlFinding{how: ddlOutside}, err
	}
	if w.Mark != nil {
		unmarked := w
		unmarked.Mark = nil
		for _, d := range ddls {
			if w.place(d) != unmarked.place(d) {
				ok, err := markStillNamesItsRow(ctx, db, w.Mark)
				if err != nil {
					return ddlFinding{how: ddlOutside}, err
				}
				if !ok {
					slog.Warn("the snapshot's DDL mark names a schema_changes row that is gone or holds another statement, "+
						"so the index's ids were handed out again; statements are placed by binlog position without it",
						"schema", schema, "table", table, "mark_id", w.Mark.ID,
						"mark_row", fmt.Sprintf("%s:%d %s", w.Mark.File, w.Mark.Pos, w.Mark.Type))
					w = unmarked
				}
				break
			}
		}
	}
	f := ddlFinding{how: ddlOutside}
	for _, d := range ddls {
		how := w.place(d)
		if how == ddlOutside {
			continue
		}
		if f.how == ddlOutside {
			f.first, f.how = d, how
		}
		if how != ddlByTime {
			f.positional = append(f.positional, d.ID)
		}
	}
	return f, nil
}

// loadDestructiveDDLs reads every destructive statement recorded for the
// table, oldest first. All of them, not the ones in a time range: a statement
// indexed late carries a time outside the range it belongs to (#1912). The
// table holds one row per table per statement, and idx_schema_table serves
// the read.
func loadDestructiveDDLs(ctx context.Context, db *sql.DB, schema, table string, w DDLWindow) ([]destructiveDDL, error) {
	// schema_name = '' is matched too, and the arm is NOT removable. Since
	// #1435 parseDDL resolves an unqualified statement ("TRUNCATE TABLE
	// orders" after "USE mydb") against the QUERY_EVENT's session default
	// database, so NEW rows carry a real schema — but every row indexed
	// before that fix has schema_name = '' (the original session's default
	// is unrecoverable, so no backfill exists), and deleting this arm as
	// "redundant now" would silently stop destructive-DDL detection for
	// exactly the historical windows people reconstruct after an incident
	// (#764's return path). The '' match can only widen a match (favoring an
	// over-cautious refusal on historical rows), never narrow one.
	sinceOp := ">"
	if w.sinceInclusive {
		sinceOp = ">="
	}
	q := `SELECT id, ddl_type, detected_at, binlog_file, binlog_pos,
			detected_at ` + sinceOp + ` ?, detected_at <= ?
		FROM schema_changes
		WHERE (schema_name = ? OR schema_name = '') AND table_name = ?
		AND ddl_type IN ('TRUNCATE TABLE', 'DROP TABLE', 'RENAME TABLE', 'CREATE OR REPLACE TABLE')
		ORDER BY detected_at ASC, id ASC`

	rows, err := db.QueryContext(ctx, q, w.Since, w.Until, schema, table)
	if err != nil {
		// Graded on the ERROR NUMBER, never on its text. 1146 is "no such
		// table", the index too old to have schema_changes; 1932 is "table
		// doesn't exist IN ENGINE", a missing or corrupt tablespace — the
		// check is BROKEN, not absent. Their messages share the words
		// "doesn't exist", so a string match reads a damaged index as a clean
		// one, and on the binlog-only path this lookup is the whole defense
		// against resurrecting removed rows.
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			return nil, errSchemaChangesMissing
		}
		return nil, fmt.Errorf("check schema_changes for destructive DDL on %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	var out []destructiveDDL
	for rows.Next() {
		var d destructiveDDL
		if err := rows.Scan(&d.ID, &d.Type, &d.DetectedAt, &d.File, &d.Pos, &d.AfterSince, &d.AtOrBeforeUntil); err != nil {
			return nil, fmt.Errorf("check schema_changes for destructive DDL on %s.%s: %w", schema, table, err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("check schema_changes for destructive DDL on %s.%s: %w", schema, table, err)
	}
	return out, nil
}
