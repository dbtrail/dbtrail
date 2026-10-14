package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// explainJoinMemoryLimit caps the DuckDB session that joins the two sides of a
// drill-down. It runs WHILE the reconstruction's own DuckDB session (the
// caller's tuning: the job's budget in the daemon, 4GB on the command line)
// is streaming rows into it, so it must not
// claim another full budget on top: past this cap the rows spill to the
// session's temp_directory instead of growing the process (#2385).
const (
	explainJoinMemoryLimit = "1GB"
	explainJoinMemoryMiB   = 953 // the same in MiB (DuckDB reads 1GB as 10^9 bytes), to compare a job budget against
)

// rowJoin is the full outer join on the primary key behind a drill-down, done
// in DuckDB instead of a Go map (#2385). Both sides are appended as they are
// streamed, so neither is ever held whole in the Go heap: a multi-million-row
// table used to cost gigabytes no memory_limit counted, and on a small host
// that got the process (or the index next to it) killed.
//
// The join runs on fixed-width fingerprints, never on the rows themselves:
// k1,k2 are 128 bits of SHA-256 over the Go-rendered join key, c1,c2 the same
// over the row's canonical cells (encodeCells, which keeps NULL apart from
// empty), d over the deferred-type columns alone. Measured: under a small
// memory_limit DuckDB joins millions of fixed-width rows by spilling, but a
// join that has to carry the cell BLOBs, or a GROUP BY over every key, fails
// with "Out of Memory" instead. The BLOBs are read back only for the rows the
// drill-down prints.
//
// What the old map join did that this one does not: collapse a primary key
// that appears twice in the new baseline into its last row. A baseline of a
// table with a primary key cannot hold one twice; if it ever did, each copy
// is now compared, and shown, on its own.
type rowJoin struct {
	db     *sql.DB
	dir    string
	conn   *sql.Conn
	truth  *duckdb.Appender
	rec    *duckdb.Appender
	tSeq   int64
	rSeq   int64
	closed bool
}

func newRowJoin(ctx context.Context, tuning duckdbutil.Tuning) (*rowJoin, error) {
	limit := explainJoinMemoryLimit
	if t := duckdbutil.DefaultTuningFor(ctx); t != duckdbutil.DefaultTuning() {
		// The job's budget (#2385) is sized for its sessions together; this
		// one runs beside the reconstruction's, so it takes the same share,
		// never more than its own cap.
		if mib, ok := strings.CutSuffix(t.MemoryLimit, "MiB"); ok {
			if n, err := strconv.Atoi(mib); err == nil && n > 0 && n < explainJoinMemoryMiB {
				limit = t.MemoryLimit
			}
		}
	}
	return newRowJoinCapped(ctx, tuning, limit)
}

// newRowJoinCapped is newRowJoin with the session's memory cap given, so a
// test can prove the join spills instead of failing under a tiny one.
func newRowJoinCapped(ctx context.Context, tuning duckdbutil.Tuning, memoryLimit string) (*rowJoin, error) {
	// A database FILE, not an in-memory one: DuckDB cannot evict the blocks of
	// an in-memory table, so under its cap an in-memory join fails with "Out
	// of Memory" instead of spilling. On disk, the cap is a cap.
	removeOldExplainDirs(os.TempDir())
	dir, err := os.MkdirTemp("", explainDirPrefix+"*")
	if err != nil {
		return nil, fmt.Errorf("open drill-down join: %w", err)
	}
	db, err := sql.Open("duckdb", filepath.Join(dir, "join.duckdb"))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("open drill-down join: %w", err)
	}
	// One connection: the appenders and the queries must see the same
	// database, and every SET below lands on it.
	db.SetMaxOpenConns(1)
	j := &rowJoin{db: db, dir: dir}
	threads := tuning.Threads
	if threads <= 0 {
		threads = duckdbutil.DefaultTuning().Threads
	}
	duckdbutil.Tuning{Threads: threads}.Apply(ctx, db)
	// The cap IS this session's guarantee, so unlike the shared sessions'
	// best-effort Apply a cap that does not take is fatal. It travels with
	// its spill directory (Tuning.Apply's contract), here inside dir rather
	// than the shared OS temp dir, so close() removes the spill files too
	// and they never mix with the reconstruction session's.
	for _, stmt := range append(duckdbutil.Tuning{MemoryLimit: memoryLimit}.Statements(),
		"SET temp_directory = '"+strings.ReplaceAll(filepath.Join(dir, "spill"), "'", "''")+"'") {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			j.close()
			return nil, fmt.Errorf("cap drill-down join (%s): %w", stmt, err)
		}
	}
	for _, stmt := range []string{
		"CREATE TABLE truth (seq BIGINT, k1 UBIGINT, k2 UBIGINT, c1 UBIGINT, c2 UBIGINT, d UBIGINT, pk BLOB, cells BLOB)",
		"CREATE TABLE rec (seq BIGINT, k1 UBIGINT, k2 UBIGINT, c1 UBIGINT, c2 UBIGINT, d UBIGINT, pk BLOB, cells BLOB)",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			j.close()
			return nil, fmt.Errorf("prepare drill-down join: %w", err)
		}
	}
	if j.conn, err = db.Conn(ctx); err != nil {
		j.close()
		return nil, fmt.Errorf("prepare drill-down join: %w", err)
	}
	err = j.conn.Raw(func(dc any) error {
		c, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected DuckDB connection type %T", dc)
		}
		var err error
		if j.truth, err = duckdb.NewAppenderFromConn(c, "", "truth"); err != nil {
			return err
		}
		j.rec, err = duckdb.NewAppenderFromConn(c, "", "rec")
		return err
	})
	if err != nil {
		j.close()
		return nil, fmt.Errorf("prepare drill-down join: %w", err)
	}
	return j, nil
}

// explainDirPrefix names the directory one drill-down works in, under the
// OS temp dir: its database file (both sides' rows) and its spill files.
const explainDirPrefix = "bintrail-explain-"

// explainDirMaxAge is past any drill-down still running: a directory older
// than this was left by a process that was killed before close() ran.
const explainDirMaxAge = 24 * time.Hour

// removeOldExplainDirs removes the directories killed drill-downs left in
// dir. The kill #2385 is about (out of memory) is exactly the one that skips
// close(), and what it leaves is a copy of the table's rows, as big as the
// table, on the disk capture writes to.
func removeOldExplainDirs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), explainDirPrefix) {
			continue
		}
		if fi, err := e.Info(); err == nil && time.Since(fi.ModTime()) > explainDirMaxAge {
			_ = os.RemoveAll(filepath.Join(dir, e.Name()))
		}
	}
}

func (j *rowJoin) addTruth(key string, r rowCells, cols []metadata.ColumnMeta) error {
	j.tSeq++
	return appendSide(j.truth, j.tSeq, key, r, cols)
}

func (j *rowJoin) addRecovery(key string, r rowCells, cols []metadata.ColumnMeta) error {
	j.rSeq++
	return appendSide(j.rec, j.rSeq, key, r, cols)
}

func appendSide(a *duckdb.Appender, seq int64, key string, r rowCells, cols []metadata.ColumnMeta) error {
	k1, k2 := fingerprint([]byte(key))
	cells := encodeCells(r.cells, cols, nil)
	c1, c2 := fingerprint(cells)
	d, _ := fingerprint(encodeCells(r.cells, cols, isDeferredType))
	return a.AppendRow(seq, k1, k2, c1, c2, d, []byte(r.pk), cells)
}

func fingerprint(b []byte) (uint64, uint64) {
	s := sha256.Sum256(b)
	return binary.LittleEndian.Uint64(s[:8]), binary.LittleEndian.Uint64(s[8:16])
}

// The recovery rows that differ: absent from the new baseline (extra) or
// present with other cells (changed), in recovery-stream order — the order
// the drill-down has always printed them in.
const (
	joinOn       = "r.k1 = t.k1 AND r.k2 = t.k2"
	recDiffWhere = "t.k1 IS NULL OR r.c1 <> t.c1 OR r.c2 <> t.c2"

	diffCountsQuery = `SELECT
  count(*) FILTER (WHERE t.k1 IS NOT NULL),
  count(*) FILTER (WHERE t.k1 IS NULL),
  coalesce(bool_or(t.k1 IS NOT NULL AND r.d <> t.d), false)
FROM rec r LEFT JOIN truth t ON ` + joinOn + ` WHERE ` + recDiffWhere

	missingCountQuery = `SELECT count(*) FROM truth t ANTI JOIN rec r ON ` + joinOn

	// Top-N: only the seqs travel through the join, never the cells.
	recDiffFirstQuery = `SELECT r.seq FROM rec r LEFT JOIN truth t ON ` + joinOn +
		` WHERE ` + recDiffWhere + ` ORDER BY r.seq LIMIT ?`

	// Rows only the new baseline has, by primary-key label (byte order).
	missingFirstQuery = `SELECT t.pk FROM truth t ANTI JOIN rec r ON ` + joinOn + ` ORDER BY t.pk LIMIT ?`
)

// diff closes both appenders and fills ex: the totals per kind, whether a
// deferred-type column differs anywhere, and the first maxExplainRows
// differing rows with their cells — the same content and order ex.add over
// every row produced when the join was a Go map.
func (j *rowJoin) diff(ctx context.Context, cols []metadata.ColumnMeta, ex *MismatchExplanation) error {
	if err := j.closeAppenders(); err != nil {
		return fmt.Errorf("drill-down join: %w", err)
	}
	var changed, extra, missing int
	if err := j.conn.QueryRowContext(ctx, diffCountsQuery).Scan(&changed, &extra, &ex.deferredSeen); err != nil {
		return fmt.Errorf("drill-down join: %w", err)
	}
	if err := j.conn.QueryRowContext(ctx, missingCountQuery).Scan(&missing); err != nil {
		return fmt.Errorf("drill-down join: %w", err)
	}
	ex.Total = changed + extra + missing
	ex.byKind = map[string]int{}
	for kind, n := range map[string]int{diffChanged: changed, diffExtra: extra, diffMissing: missing} {
		if n > 0 {
			ex.byKind[kind] = n
		}
	}

	seqs, err := queryColumn[int64](ctx, j.conn, recDiffFirstQuery, maxExplainRows)
	if err != nil {
		return err
	}
	if len(seqs) > 0 {
		shown, err := j.recDiffRows(ctx, seqs, cols)
		if err != nil {
			return err
		}
		// A key the new baseline holds twice joins twice; keep the cap.
		ex.Diffs = append(ex.Diffs, shown[:min(len(shown), maxExplainRows)]...)
	}
	if room := maxExplainRows - len(ex.Diffs); room > 0 && missing > 0 {
		pks, err := queryColumn[[]byte](ctx, j.conn, missingFirstQuery, room)
		if err != nil {
			return err
		}
		for _, pk := range pks {
			ex.Diffs = append(ex.Diffs, RowDiff{PK: string(pk), Kind: diffMissing})
		}
	}
	return nil
}

// recDiffRows reads back, for the few recovery rows the drill-down prints,
// both sides' cells and renders the per-column differences.
func (j *rowJoin) recDiffRows(ctx context.Context, seqs []int64, cols []metadata.ColumnMeta) ([]RowDiff, error) {
	in := make([]string, len(seqs))
	for i, s := range seqs {
		in[i] = fmt.Sprint(s)
	}
	rows, err := j.conn.QueryContext(ctx, `SELECT r.seq, r.pk, r.cells, t.cells, t.k1 IS NULL
FROM (SELECT * FROM rec WHERE seq IN (`+strings.Join(in, ",")+`)) r
LEFT JOIN truth t ON `+joinOn+` WHERE `+recDiffWhere+` ORDER BY r.seq`)
	if err != nil {
		return nil, fmt.Errorf("drill-down join: %w", err)
	}
	defer rows.Close()
	var out []RowDiff
	for rows.Next() {
		var (
			seq        int64
			pk         []byte
			rCel, tCel []byte
			isExtra    bool
		)
		if err := rows.Scan(&seq, &pk, &rCel, &tCel, &isExtra); err != nil {
			return nil, fmt.Errorf("drill-down join: %w", err)
		}
		if isExtra {
			out = append(out, RowDiff{PK: string(pk), Kind: diffExtra})
			continue
		}
		rc, err := decodeCells(rCel, len(cols))
		if err != nil {
			return nil, err
		}
		tc, err := decodeCells(tCel, len(cols))
		if err != nil {
			return nil, err
		}
		var cells []CellDiff
		for i, c := range cols {
			if !cellEqual(rc[i], tc[i]) {
				// Both values are shown raw. For a deferred-type column (ENUM/
				// SET/JSON/binary) the recovery value MAY be an event image rather
				// than MySQL's source text — see deferredCaveat. Such cells are NOT
				// blanked: a baseline-vs-baseline drift there is directly
				// comparable and hiding it would mask the drift this command catches.
				cells = append(cells, CellDiff{Column: c.Name, Recovery: displayCell(rc[i]), Baseline: displayCell(tc[i])})
			}
		}
		out = append(out, RowDiff{PK: string(pk), Kind: diffChanged, Cells: cells})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("drill-down join: %w", err)
	}
	return out, nil
}

func queryColumn[T any](ctx context.Context, conn *sql.Conn, q string, args ...any) ([]T, error) {
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("drill-down join: %w", err)
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		var v T
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("drill-down join: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("drill-down join: %w", err)
	}
	return out, nil
}

func (j *rowJoin) closeAppenders() error {
	var errs []error
	for _, a := range []**duckdb.Appender{&j.truth, &j.rec} {
		if *a != nil {
			errs = append(errs, (*a).Close())
			*a = nil
		}
	}
	return errors.Join(errs...)
}

// close releases the session and removes its database file and spill files.
func (j *rowJoin) close() {
	if j.closed {
		return
	}
	j.closed = true
	_ = j.closeAppenders()
	if j.conn != nil {
		_ = j.conn.Close()
	}
	_ = j.db.Close()
	if err := os.RemoveAll(j.dir); err != nil {
		slog.Warn("could not remove the Explain working files", "dir", j.dir, "error", err)
	}
}

// encodeCells serializes one row's canonical cells in column order (only the
// columns keep accepts, when it is set). Each cell is a tag byte (0 = SQL NULL,
// 1 = value) then, for a value, its length and bytes, so the encoding is
// injective and keeps cellEqual's NULL-vs-empty distinction: two rows encode
// equal exactly when every column is cellEqual.
func encodeCells(cells map[string][]byte, cols []metadata.ColumnMeta, keep func(dataType string) bool) []byte {
	var b []byte
	for _, c := range cols {
		if keep != nil && !keep(c.DataType) {
			continue
		}
		v := cells[c.Name]
		if v == nil {
			b = append(b, 0)
			continue
		}
		b = append(b, 1)
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	return b
}

// decodeCells is encodeCells' inverse (for an encoding of every column); a
// value cell decodes to a non-nil slice even when empty, so cellEqual still
// tells it from NULL.
func decodeCells(b []byte, n int) ([][]byte, error) {
	out := make([][]byte, n)
	r := bytes.NewReader(b)
	for i := range n {
		tag, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("drill-down join: truncated row (column %d)", i)
		}
		if tag == 0 {
			continue
		}
		l, err := binary.ReadUvarint(r)
		if err != nil || l > uint64(r.Len()) {
			return nil, fmt.Errorf("drill-down join: corrupt row (column %d)", i)
		}
		v := make([]byte, l)
		_, _ = r.Read(v)
		out[i] = v
	}
	return out, nil
}
