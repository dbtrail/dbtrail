package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/query"
)

// An unchanged large value, carried.
//
// For an UPDATE that does not touch a large value stored out of line (TOAST),
// PostgreSQL does not send that value: the row after the change holds the
// unchanged-TOAST marker in its column (event.UnchangedToastKey). The value
// is the one the row had before the change. Where the earlier row was
// captured whole (REPLICA IDENTITY FULL) capture fills it in from there, and
// no marker reaches the index. Where it was not, the marker stays, and the
// value is the one the newest earlier change of the same row holds for that
// column, or, with no such change in the window, the one in the table's
// copy.
//
// A fold keeps one change per key, the last in the write-ahead log, so the
// value has to be carried into it while the changes are folded: pgCarry.
// Changes do not always arrive in the log's order (foldPage), so the fold
// remembers, for each column it carried or could not, where its value came
// from (pgColSrc), and a change that arrives later and sits between that
// place and the key's last change gives its value in turn. The result is the
// same in every order of arrival.
//
// What the window does not hold stays as the marker, with no source: the
// value is in the table's copy. A snapshot update reads it there
// (pgFillFromBase); a tail refuses. It is never written as NULL.

// pgColSrc is where the value of one column of a key's last change came
// from, for a column that change did not carry itself.
type pgColSrc struct {
	// LSN and ID are the place of the change the value was taken from, or
	// of a DELETE the value cannot be taken past. Both 0: no change of the
	// window says anything about the column.
	LSN, ID uint64
	// Have reports that the last change now holds the value.
	Have bool
	// NotLarge reports that the table's copy holds a NULL or an empty text
	// for the column under the key: not a value PostgreSQL stores out of
	// line, so not what the change left as it was. The copy's row is not
	// the row the change was made to.
	NotLarge bool
	// Gone reports that the change at (LSN, ID) deleted the row, or that
	// the row had no such column then: the value is not in the table's copy
	// either, only in a change after that one.
	Gone bool
}

// before reports whether s is strictly before the change ev in the log.
func (s pgColSrc) before(ev *query.ResultRow) bool {
	lsn, _ := pgRowLSN(ev)
	if s.LSN == lsn {
		return s.ID < ev.EventID
	}
	return s.LSN < lsn
}

func pgSrcAt(ev *query.ResultRow) pgColSrc {
	lsn, _ := pgRowLSN(ev)
	return pgColSrc{LSN: lsn, ID: ev.EventID}
}

var pgUnchangedToast = map[string]any{event.UnchangedToastKey: true}

// pgCarry is run for every change of a PostgreSQL fold, a half of a key
// change included, before the fold files it or drops it for a later one of
// its key. It returns the change to file: ev itself, or, when ev's row holds
// an unchanged large value, a copy whose row is its own (the page's is left
// as it was) with every such value the key's change so far can give.
//
// image is the row of the change the fold holds for a key, to read and to
// change; nil reads the change's own RowAfter. A tail, which packs its rows,
// hands its own.
func (r *foldResult) pgCarry(ev *query.ResultRow, image func(key string, cur *query.ResultRow) (map[string]any, error)) (*query.ResultRow, error) {
	if r.Spill != nil && (r.pgUSeen || len(event.UnresolvedToastColumns(ev.RowAfter)) > 0) {
		return nil, pgToastSpillErr(ev.SchemaName, ev.TableName)
	}
	key := ev.PKValues
	cur, held := r.Changes[key]
	imageOf := func() (map[string]any, error) {
		if cur.EventType == event.EventDelete {
			return nil, nil
		}
		if image != nil {
			return image(key, cur)
		}
		return cur.RowAfter, nil
	}

	if held && laterChange(cur, ev) {
		// ev sits before the key's last change, and the fold will drop it.
		// It may still be the nearest change to hold a value the last
		// change carried from further back, or could not carry.
		srcs := r.pgSrc[key]
		if len(srcs) == 0 {
			return ev, nil
		}
		var img map[string]any
		for c, s := range srcs {
			if !s.before(ev) {
				continue
			}
			at := pgSrcAt(ev)
			var v any = pgUnchangedToast
			if ev.EventType == event.EventDelete {
				at.Gone = true
			} else {
				got, ok := ev.RowAfter[c]
				switch {
				case event.IsUnchangedToastMarker(got):
					// It left the value as it was too: what is known from
					// before it stands.
					continue
				case !ok:
					// The row had no such column then: nothing from before
					// it is this column's value.
					at.Gone = true
				default:
					v, at.Have = got, true
				}
			}
			if img == nil {
				var err error
				if img, err = imageOf(); err != nil {
					return nil, err
				}
				if img == nil {
					return nil, fmt.Errorf("internal: %s.%s: the last change of key %q waits for a value and has no row", ev.SchemaName, ev.TableName, key)
				}
			}
			img[c] = v
			srcs[c] = at
		}
		return ev, nil
	}

	cols := event.UnresolvedToastColumns(ev.RowAfter)
	if len(cols) == 0 {
		// The key's last change carries every value itself.
		delete(r.pgSrc, key)
		return ev, nil
	}
	r.pgUSeen = true
	own := *ev
	own.RowAfter = maps.Clone(ev.RowAfter)
	srcs := make(map[string]pgColSrc, len(cols))
	if held {
		was := r.pgSrc[key]
		img, err := imageOf()
		if err != nil {
			return nil, err
		}
		for _, c := range cols {
			if cur.EventType == event.EventDelete {
				at := pgSrcAt(cur)
				at.Gone = true
				srcs[c] = at
				continue
			}
			v, ok := img[c]
			switch {
			case !ok:
				// The row had no such column at the change before it:
				// nothing from before that change is this column's value.
				at := pgSrcAt(cur)
				at.Gone = true
				srcs[c] = at
			case event.IsUnchangedToastMarker(v):
				srcs[c] = was[c]
			default:
				own.RowAfter[c] = v
				s, carried := was[c]
				if !carried || !s.Have {
					s = pgSrcAt(cur)
					s.Have = true
				}
				srcs[c] = s
			}
		}
	} else {
		for _, c := range cols {
			srcs[c] = pgColSrc{}
		}
	}
	if r.pgSrc == nil {
		r.pgSrc = make(map[string]map[string]pgColSrc)
	}
	r.pgSrc[key] = srcs
	return &own, nil
}

// pgToastResidue is one key whose last change still holds an unchanged large
// value after the fold: no change of the window gave it.
type pgToastResidue struct {
	Key     string
	Columns []string // sorted
	// NotLarge names the columns the table's copy holds as NULL or empty.
	NotLarge []string
	// Gone names the columns whose value is not in the table's copy either:
	// the window holds a DELETE of the row before the change, and no change
	// between the two that gives the value.
	Gone []string
}

// pgResidualToast lists, in key order, the keys of changes whose last change
// still holds an unchanged large value, up to limit of them (0: all), and
// how many there are.
func (r *foldResult) pgResidualToast(limit int) ([]pgToastResidue, int) {
	var out []pgToastResidue
	for key, srcs := range r.pgSrc {
		if _, ok := r.Changes[key]; !ok {
			continue
		}
		var res pgToastResidue
		for c, s := range srcs {
			if s.Have {
				continue
			}
			res.Columns = append(res.Columns, c)
			if s.Gone {
				res.Gone = append(res.Gone, c)
			}
			if s.NotLarge {
				res.NotLarge = append(res.NotLarge, c)
			}
		}
		if len(res.Columns) == 0 {
			continue
		}
		res.Key = key
		sort.Strings(res.Columns)
		sort.Strings(res.Gone)
		sort.Strings(res.NotLarge)
		out = append(out, res)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	n := len(out)
	if limit > 0 && n > limit {
		out = out[:limit]
	}
	return out, n
}

// pgFillFromBase gives every unchanged large value the window did not hold
// the value of its row in basePath, the file the window's changes go over.
// It is right only for a window that holds every change since that file
// (the caller says when it is not: pgBaseNotTheRow): no change of the row in
// it gave the column a value, so the row holds the file's. A value after a
// DELETE (Gone) is not looked for: the file's row is the deleted one. Left
// for the caller to refuse: a key the file has no row for, and a column the
// file holds as NULL or as an empty text (NotLarge), which PostgreSQL never
// leaves unsent, so that row is not the one the change was made to.
//
// Two reads of the file, its key columns and then the named columns of the
// rows found; nothing is read when nothing is left to give. maxBytes (0: no
// limit) is how much text it may bring in. It returns how many values it
// gave.
func (r *foldResult) pgFillFromBase(ctx context.Context, basePath string, pkCols []metadata.ColumnMeta, tuning duckdbutil.Tuning, maxBytes int64) (int, error) {
	var keys []string
	colSet := map[string]bool{}
	for key, srcs := range r.pgSrc {
		if _, ok := r.Changes[key]; !ok {
			continue
		}
		wants := false
		for c, s := range srcs {
			if !s.Have && !s.Gone {
				colSet[c], wants = true, true
			}
		}
		if wants {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return 0, nil
	}
	sort.Strings(keys)
	cols := make([]string, 0, len(colSet))
	for c := range colSet {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	keyCols := make([]string, len(pkCols))
	for i, c := range pkCols {
		keyCols[i] = c.Name
	}
	if err := baselineintegrity.ValidateLocalFileOnce(basePath); err != nil {
		return 0, err
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, fmt.Errorf("open duckdb: %w", err)
	}
	defer duck.Close()
	applyDuckDBTuning(ctx, duck, tuning)
	rows, err := pgdelta.BaseValuesOf(ctx, duck, basePath, keyCols, keys, cols, maxBytes)
	if err != nil {
		return 0, err
	}
	given := 0
	for _, key := range keys {
		row, ok := rows[key]
		if !ok {
			continue
		}
		img, srcs := r.Changes[key].RowAfter, r.pgSrc[key]
		for c, s := range srcs {
			if s.Have || s.Gone {
				continue
			}
			if v := row[c]; v == nil || *v == "" {
				s.NotLarge = true
			} else {
				img[c] = *v
				s.Have = true
				given++
			}
			srcs[c] = s
		}
	}
	return given, nil
}

// pgToastResidueErr is the refusal of a table one of whose changes holds an
// unchanged large value nothing here can give.
func pgToastResidueErr(schema, table string, res pgToastResidue, more int) error {
	target := pgQuoteIdent(schema) + "." + pgQuoteIdent(table)
	why := "no earlier change of the row since the table's copy was written holds it, and the copy has no row under that key"
	if len(res.Gone) > 0 {
		why = "before that change the row was deleted, or had no such column, and no change after that holds it"
	}
	if len(res.NotLarge) > 0 {
		why = "the table's copy holds no large value there for that key (a NULL or an empty text), so its row is not the one the change was made to"
	}
	others := ""
	if more > 0 {
		others = fmt.Sprintf(" (and %d more rows)", more)
	}
	return fmt.Errorf("%s (pk=%s)%s: a change left the large value of column(s) %s as it was, PostgreSQL did not send it "+
		"because the table was not at REPLICA IDENTITY FULL, and %s. Take a new snapshot of the table; "+
		"ALTER TABLE %s REPLICA IDENTITY FULL on the source makes later changes carry such values themselves",
		target, res.Key, others, strings.Join(res.Columns, ", "), why, target)
}

// pgToastSpillErr refuses a fold that moved its changes to disk and met an
// unchanged large value: where each carried value came from is not kept on
// disk, so a nearer one could not replace it.
func pgToastSpillErr(schema, table string) error {
	return fmt.Errorf("%s.%s: its changes since the table's copy was written are too many to hold in memory, and some leave a large value as it was "+
		"without PostgreSQL sending it (the table was not at REPLICA IDENTITY FULL): such a value cannot be followed through changes on disk. "+
		"Take a new snapshot of the table", schema, table)
}
