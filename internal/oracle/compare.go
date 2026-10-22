package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
)

// The three checks, in the order they run.
const (
	CheckOnePerKey = "one row per primary key"
	CheckCount     = "row count equals the source"
	CheckRows      = "row by row"
)

// Context names where a check happens, for its failure message.
type Context struct {
	Seed  uint64
	Step  int // 1-based step whose cut is being checked
	Steps int
	// StepName is the step applied at this cut (snapshot, refresh, ...).
	StepName string
	// Session names the DuckDB session the copy was read under (its time
	// zone), since the run reads it more than one way.
	Session string
	// SnapshotDir is the snapshot directory the copy was read from; its
	// files and sizes go into the message.
	SnapshotDir string
	// AsOf, when set, says the copy answered the table as of an earlier
	// cut than the one checked (the published one, with no tail over it),
	// and the rows it is compared with are that cut's.
	AsOf string
}

// Failure is one check failing: which check, on which key, with both rows.
type Failure struct {
	Context
	Table   string
	Check   string
	Key     string
	Column  string
	Columns []string
	Source  []string // the source's row for Key, nil when it has none
	Copy    []string // the copy's row for Key, nil when it has none
	Detail  string   // the check's sentence
}

func (f *Failure) Error() string {
	var b strings.Builder
	how := f.StepName
	if f.Session != "" {
		how += ", session " + f.Session
	}
	fmt.Fprintf(&b, "oracle: seed %d, step %d of %d (%s), table %s: %s", f.Seed, f.Step, f.Steps, how, f.Table, f.Detail)
	if f.AsOf != "" {
		fmt.Fprintf(&b, "\n  the copy answered as of %s; the rows compared are that cut's", f.AsOf)
	}
	if f.Key != "" {
		fmt.Fprintf(&b, "\n  key %s", f.Key)
	}
	if f.Column != "" {
		fmt.Fprintf(&b, ", column %s", f.Column)
	}
	if f.Source != nil {
		fmt.Fprintf(&b, "\n  source: %s", renderRow(f.Columns, f.Source))
	}
	if f.Copy != nil {
		fmt.Fprintf(&b, "\n  copy:   %s", renderRow(f.Columns, f.Copy))
	}
	if f.SnapshotDir != "" {
		files, err := FilesUnder(f.SnapshotDir)
		if err != nil {
			fmt.Fprintf(&b, "\n  copy read from %s (cannot list: %v)", f.SnapshotDir, err)
		} else {
			fmt.Fprintf(&b, "\n  copy read from %s (%d files):", f.SnapshotDir, len(files))
			for _, fl := range files {
				fmt.Fprintf(&b, "\n    %s %d bytes", fl.Path, fl.Size)
			}
		}
	}
	fmt.Fprintf(&b, "\n  reproduce: %s", Reproduce(f.Seed, f.Steps))
	return b.String()
}

func renderRow(cols, row []string) string {
	parts := make([]string, len(row))
	for i, v := range row {
		name := ""
		if i < len(cols) {
			name = cols[i] + "="
		}
		parts[i] = name + v
	}
	return strings.Join(parts, ", ")
}

// Reproduce is the one-line command that replays seed over steps.
func Reproduce(seed uint64, steps int) string {
	return fmt.Sprintf("BINTRAIL_ORACLE_SEED=%d BINTRAIL_ORACLE_STEPS=%d go test -tags integration -run TestOracle ./internal/oracle/", seed, steps)
}

// FileSize is one file under a snapshot directory, path relative to it.
type FileSize struct {
	Path string
	Size int64
}

// FilesUnder lists every file under dir with its size, sorted by path.
func FilesUnder(dir string) ([]FileSize, error) {
	var out []FileSize
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		out = append(out, FileSize{Path: filepath.ToSlash(rel), Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b FileSize) int { return strings.Compare(a.Path, b.Path) })
	return out, nil
}

// Check runs the three checks in order over the copy's rows against the
// source's, and returns a *Failure for the first that does not hold.
func Check(cx Context, source, copy TableRows) error {
	fail := func(check, detail string) *Failure {
		return &Failure{Context: cx, Table: source.Table, Check: check, Columns: source.Columns, Detail: detail}
	}
	// 1. Every primary key appears once in the copy.
	seen := map[string]int{}
	copyKeys := make([]string, len(copy.Rows))
	for i := range copy.Rows {
		copyKeys[i] = copy.Key(i)
		seen[copyKeys[i]]++
	}
	dups := make([]string, 0)
	for k, n := range seen {
		if n > 1 {
			dups = append(dups, k)
		}
	}
	if len(dups) > 0 {
		slices.Sort(dups)
		f := fail(CheckOnePerKey, fmt.Sprintf("key %s appears %d times in the copy; every primary key must appear once", dups[0], seen[dups[0]]))
		f.Key = dups[0]
		f.Copy = copy.Rows[slices.Index(copyKeys, dups[0])]
		if i := indexOfKey(source, dups[0]); i >= 0 {
			f.Source = source.Rows[i]
		}
		return f
	}
	// 2. The count equals the source's.
	if len(copy.Rows) != len(source.Rows) {
		f := fail(CheckCount, fmt.Sprintf("the copy has %d rows, the source %d", len(copy.Rows), len(source.Rows)))
		return withFirstDifference(f, source, copy, copyKeys)
	}
	// 3. Row by row, by key.
	if f := withFirstDifference(fail(CheckRows, ""), source, copy, copyKeys); f.Key != "" {
		return f
	}
	return nil
}

// withFirstDifference fills f with the first key, in source key order, that
// the copy lacks or renders differently, then with the first key the copy
// has and the source lacks, keeping whatever Detail f already carries ahead
// of the key's sentence. f.Key stays empty when the rows are equal.
func withFirstDifference(f *Failure, source, copy TableRows, copyKeys []string) *Failure {
	lead := f.Detail
	defer func() {
		if lead != "" && f.Key != "" {
			f.Detail = lead + "; " + f.Detail
		}
	}()
	byKey := map[string]int{}
	for i, k := range copyKeys {
		byKey[k] = i
	}
	sourceKeys := make([]string, len(source.Rows))
	for i := range source.Rows {
		sourceKeys[i] = source.Key(i)
	}
	order := slices.Clone(sourceKeys)
	slices.Sort(order)
	for _, k := range order {
		si := slices.Index(sourceKeys, k)
		ci, ok := byKey[k]
		if !ok {
			f.Key, f.Source = k, source.Rows[si]
			f.Detail = fmt.Sprintf("key %s is in the source and not in the copy", k)
			if extra := firstExtra(copyKeys, sourceKeys); extra != "" {
				f.Detail += fmt.Sprintf("; the copy has key %s instead", extra)
				f.Copy = copy.Rows[byKey[extra]]
			}
			return f
		}
		for c, col := range source.Columns {
			if source.Rows[si][c] != cellByName(copy, ci, col) {
				f.Key, f.Column, f.Source, f.Copy = k, col, source.Rows[si], reorder(copy, ci, source.Columns)
				f.Detail = fmt.Sprintf("key %s differs in column %s", k, col)
				return f
			}
		}
	}
	if extra := firstExtra(copyKeys, sourceKeys); extra != "" {
		f.Key, f.Copy = extra, copy.Rows[byKey[extra]]
		f.Detail = fmt.Sprintf("key %s is in the copy and not in the source", extra)
		return f
	}
	if f.Detail == "" {
		f.Detail = "equal"
	}
	return f
}

func firstExtra(copyKeys, sourceKeys []string) string {
	extra := make([]string, 0)
	for _, k := range copyKeys {
		if !slices.Contains(sourceKeys, k) {
			extra = append(extra, k)
		}
	}
	if len(extra) == 0 {
		return ""
	}
	slices.Sort(extra)
	return extra[0]
}

func indexOfKey(r TableRows, key string) int {
	for i := range r.Rows {
		if r.Key(i) == key {
			return i
		}
	}
	return -1
}

func cellByName(r TableRows, row int, col string) string {
	if j := slices.Index(r.Columns, col); j >= 0 && j < len(r.Rows[row]) {
		return r.Rows[row][j]
	}
	return "<no column " + col + ">"
}

// reorder renders the copy's row in the source's column order.
func reorder(r TableRows, row int, columns []string) []string {
	out := make([]string, len(columns))
	for i, c := range columns {
		out[i] = cellByName(r, row, c)
	}
	return out
}

// ReadView reads the copy's state of schema.table through the view the port
// defines, as canonical text in columns order: every cell cast to text,
// NULL as "NULL", the same rendering the source read uses. datetimes names
// the DATETIME columns to read as a wall clock (strftime) instead of cast:
// a view that leaves them TIMESTAMPTZ prints them with a zone suffix, which
// is not the source's text, while a view that already made them wall
// clocks (WallClockDatetimes) needs no list.
func ReadView(ctx context.Context, ddb *sql.DB, schema, table string, columns, pk, datetimes []string) (TableRows, error) {
	parts := make([]string, len(columns))
	for i, c := range columns {
		if slices.Contains(datetimes, c) {
			parts[i] = fmt.Sprintf(`IFNULL(strftime("%s", '%%Y-%%m-%%d %%H:%%M:%%S'), 'NULL')`, c)
			continue
		}
		parts[i] = fmt.Sprintf(`IFNULL(CAST("%s" AS VARCHAR), 'NULL')`, c)
	}
	rows, err := ddb.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM "%s"."%s"`, strings.Join(parts, ", "), schema, table))
	if err != nil {
		return TableRows{}, fmt.Errorf("read the view of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	out := TableRows{Table: table, Columns: columns, PK: pk}
	for rows.Next() {
		cells := make([]string, len(columns))
		ptrs := make([]any, len(columns))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return TableRows{}, fmt.Errorf("read the view of %s.%s: %w", schema, table, err)
		}
		out.Rows = append(out.Rows, cells)
	}
	if err := rows.Err(); err != nil {
		return TableRows{}, fmt.Errorf("read the view of %s.%s: %w", schema, table, err)
	}
	return out, nil
}
