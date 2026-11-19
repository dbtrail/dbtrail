package pgdelta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
)

// KeyOf is a row's key as the index names it (pk_values), from the text of
// its key columns in the key's order: each part escaped, joined by a pipe.
// It is what the capture writes for a PostgreSQL row, whose values are text
// already.
func KeyOf(parts []string) string {
	esc := make([]string, len(parts))
	for i, p := range parts {
		esc[i] = event.EscapePKValue(p)
	}
	return strings.Join(esc, "|")
}

// BasePositions finds, in a base file, the row number of every key that
// want reports true for. keyColumns are the key's columns in the order the
// index writes a key in: their position in the TABLE, not the order the
// primary key declares them (PRIMARY KEY (b, a) on a table (a, b) is the
// key "a|b"). In another order no changed key is found, every old row
// stays, and the new one is added beside it. A key the file does not hold
// is absent from the answer: it is a row the window inserted.
//
// It reads the key columns of every row of the file: PostgreSQL values are
// stored as text, so there is no index of keys to ask.
//
// A key column that is NULL, or a wanted key the file holds twice, is a file
// no pair can be written against, and an error.
func BasePositions(ctx context.Context, db *sql.DB, basePath string, keyColumns []string, want func(key string) bool) (map[string]int64, error) {
	if len(keyColumns) == 0 {
		return nil, errors.New("pgdelta: a table with no key column")
	}
	cols := make([]string, len(keyColumns))
	for i, c := range keyColumns {
		cols[i] = `"` + strings.ReplaceAll(c, `"`, `""`) + `"`
	}
	rows, err := db.QueryContext(ctx, "SELECT file_row_number, "+strings.Join(cols, ", ")+
		" FROM read_parquet("+file(basePath)+", file_row_number=true"+noHive+")")
	if err != nil {
		return nil, fmt.Errorf("read the keys of %s: %w", basePath, err)
	}
	defer rows.Close()
	found := map[string]int64{}
	var pos int64
	parts := make([]sql.NullString, len(keyColumns))
	dest := make([]any, 0, len(parts)+1)
	dest = append(dest, &pos)
	for i := range parts {
		dest = append(dest, &parts[i])
	}
	text := make([]string, len(parts))
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("read the keys of %s: %w", basePath, err)
		}
		for i, p := range parts {
			if !p.Valid {
				return nil, fmt.Errorf("row %d of %s has no value in key column %s", pos, basePath, keyColumns[i])
			}
			text[i] = p.String
		}
		key := KeyOf(text)
		if !want(key) {
			continue
		}
		if at, twice := found[key]; twice {
			return nil, fmt.Errorf("%s holds key %q twice, at rows %d and %d", basePath, key, at, pos)
		}
		found[key] = pos
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the keys of %s: %w", basePath, err)
	}
	return found, nil
}

// BasePositionsOf is BasePositions for a list of keys, found by the engine
// that reads the file: the keys are written to a file of their own and
// joined with the base's key, built there from its key columns the way KeyOf
// builds it. BasePositions hands every row of the file to this process to
// ask about its key, which on a table of tens of millions of rows is most of
// what writing a pair costs.
//
// The two answer alike: keys are compared as their bytes, a key the file
// does not hold is absent, a wanted key the file holds twice is an error,
// and so is a row of the file with a NULL in a key column, wanted or not.
// keys need not be sorted; one named twice is asked once.
func BasePositionsOf(ctx context.Context, db *sql.DB, basePath string, keyColumns []string, keys []string) (map[string]int64, error) {
	if len(keyColumns) == 0 {
		return nil, errors.New("pgdelta: a table with no key column")
	}
	found := map[string]int64{}
	if len(keys) == 0 {
		return found, nil
	}
	quoted := make([]string, len(keyColumns))
	parts := make([]string, len(keyColumns))
	nulls := make([]string, len(keyColumns))
	for i, c := range keyColumns {
		quoted[i] = `"` + strings.ReplaceAll(c, `"`, `""`) + `"`
		// event.EscapePKValue, in the same order: the backslash first.
		parts[i] = `replace(replace(` + quoted[i] + `, '\', '\\'), '|', '\|')`
		nulls[i] = quoted[i] + " IS NULL"
	}
	src := "read_parquet(" + file(basePath) + ", file_row_number=true" + noHive + ")"
	var badRow sql.NullInt64
	err := db.QueryRowContext(ctx, "SELECT min(file_row_number) FROM "+src+" WHERE "+strings.Join(nulls, " OR ")).Scan(&badRow)
	if err != nil {
		return nil, fmt.Errorf("read the keys of %s: %w", basePath, err)
	}
	if badRow.Valid {
		return nil, fmt.Errorf("row %d of %s has no value in a key column (%s)", badRow.Int64, basePath, strings.Join(keyColumns, ", "))
	}

	// The wanted keys, in a file the engine reads. Where none can be written
	// (no temporary folder to write in), the keys are found row by row: it
	// costs time, not the pair.
	wanted, cleanup, err := writeWantedKeys(keys)
	if err != nil {
		set := make(map[string]bool, len(keys))
		for _, k := range keys {
			set[k] = true
		}
		return BasePositions(ctx, db, basePath, keyColumns, func(k string) bool { return set[k] })
	}
	defer cleanup()

	// Both sides as their bytes, the wanted keys made distinct as bytes too:
	// under a session collation that sets case or accents aside, two keys
	// that differ only there are still two.
	rows, err := db.QueryContext(ctx, "SELECT bintrail_b.file_row_number, bintrail_w.bintrail_k"+
		" FROM (SELECT file_row_number, encode("+strings.Join(parts, " || '|' || ")+") AS bintrail_k FROM "+src+") AS bintrail_b"+
		" JOIN (SELECT DISTINCT encode("+KeyColumn+") AS bintrail_k FROM read_parquet("+file(wanted)+noHive+")) AS bintrail_w"+
		" ON bintrail_b.bintrail_k = bintrail_w.bintrail_k")
	if err != nil {
		return nil, fmt.Errorf("find the changed keys in %s: %w", basePath, err)
	}
	defer rows.Close()
	for rows.Next() {
		var pos int64
		var raw []byte
		if err := rows.Scan(&pos, &raw); err != nil {
			return nil, fmt.Errorf("find the changed keys in %s: %w", basePath, err)
		}
		key := string(raw)
		if at, twice := found[key]; twice {
			if at > pos {
				at, pos = pos, at
			}
			return nil, fmt.Errorf("%s holds key %q twice, at rows %d and %d", basePath, key, at, pos)
		}
		found[key] = pos
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("find the changed keys in %s: %w", basePath, err)
	}
	return found, nil
}

// writeWantedKeys writes keys to a Parquet file of one column (KeyColumn) in
// a folder of its own, and returns the file and what removes the folder.
func writeWantedKeys(keys []string) (path string, cleanup func(), err error) {
	dir, err := os.MkdirTemp("", "bintrail-pgkeys-")
	if err != nil {
		return "", nil, err
	}
	cleanup = func() { os.RemoveAll(dir) }
	path = filepath.Join(dir, "keys.parquet")
	w, err := baseline.NewWriter(path, []baseline.Column{{Name: KeyColumn, RawText: true}}, baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100_000})
	if err == nil {
		for _, k := range keys {
			if err = w.WriteRow([]string{k}, []bool{false}); err != nil {
				break
			}
		}
		if cerr := w.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		cleanup()
		return "", nil, err
	}
	return path, cleanup, nil
}

// ReadDead is the row numbers a .pgdead file names, ascending. A file that
// names a row twice, or holds a NULL or a negative number, is an error.
func ReadDead(ctx context.Context, db *sql.DB, deadPath string) ([]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT "`+baseline.TableDeltaPosColumn+`" FROM read_parquet(`+file(deadPath)+noHive+`) ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", deadPath, err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var p sql.NullInt64
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("read %s: %w", deadPath, err)
		}
		if !p.Valid || p.Int64 < 0 || (len(out) > 0 && p.Int64 <= out[len(out)-1]) {
			return nil, fmt.Errorf("%s does not hold ascending row numbers (row %d of it)", deadPath, len(out))
		}
		out = append(out, p.Int64)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", deadPath, err)
	}
	return out, nil
}

// EachNew hands every row of a .pgnew file to yield, in the file's order,
// which is ascending order of the key's bytes: a file in any other order is
// an error, as is a key twice. columns are the table's, in the order each
// Change's Values follow. The Change handed over is yield's until it
// returns: its slices are used again for the next row. An error from yield
// is returned as it is.
func EachNew(ctx context.Context, db *sql.DB, newPath string, columns []string, yield func(Change) error) error {
	sel := make([]string, 0, len(columns)+2)
	sel = append(sel, KeyColumn, OpColumn)
	for _, c := range columns {
		sel = append(sel, `"`+strings.ReplaceAll(c, `"`, `""`)+`"`)
	}
	// The file's own order, asked for: a read on several threads hands the
	// row groups over as they come.
	rows, err := db.QueryContext(ctx, "SELECT "+strings.Join(sel, ", ")+
		" FROM read_parquet("+file(newPath)+", file_row_number=true"+noHive+") ORDER BY file_row_number")
	if err != nil {
		return fmt.Errorf("read %s: %w", newPath, err)
	}
	defer rows.Close()
	var key, op sql.NullString
	vals := make([]sql.NullString, len(columns))
	dest := make([]any, 0, len(columns)+2)
	dest = append(dest, &key, &op)
	for i := range vals {
		dest = append(dest, &vals[i])
	}
	c := Change{Values: make([]string, len(columns)), Nulls: make([]bool, len(columns))}
	prev, first := "", true
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("read %s: %w", newPath, err)
		}
		switch {
		case !key.Valid || !op.Valid:
			return fmt.Errorf("%s holds a row with no key or no mark of what happened to it", newPath)
		case op.String != OpPut && op.String != OpDelete:
			return fmt.Errorf("%s holds a row of key %q marked %q", newPath, key.String, op.String)
		case !first && key.String <= prev:
			return fmt.Errorf("%s is not in the order of its keys (%q after %q)", newPath, key.String, prev)
		}
		prev, first = key.String, false
		c.Key, c.Delete = key.String, op.String == OpDelete
		for i, v := range vals {
			c.Values[i], c.Nulls[i] = v.String, !v.Valid
		}
		if err := yield(c); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read %s: %w", newPath, err)
	}
	return nil
}

// BaseRows is how many rows a base file holds, for BaseID.
func BaseRows(ctx context.Context, db *sql.DB, basePath string) (int64, error) {
	var n int64
	if err := db.QueryRowContext(ctx, "SELECT coalesce(sum(num_rows), 0) FROM parquet_file_metadata("+file(basePath)+")").Scan(&n); err != nil {
		return 0, fmt.Errorf("read the row count of %s: %w", basePath, err)
	}
	return n, nil
}

// Footer reads a file's own metadata, key by key.
func Footer(ctx context.Context, db *sql.DB, name string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT decode(key), decode(value) FROM parquet_kv_metadata("+file(name)+")")
	if err != nil {
		return nil, fmt.Errorf("read the footer of %s: %w", name, err)
	}
	defer rows.Close()
	md := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("read the footer of %s: %w", name, err)
		}
		md[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the footer of %s: %w", name, err)
	}
	return md, nil
}

// CheckChain reads the footers of every file of chain, in the folder (or
// URL prefix) dir, and refuses a chain that is not this base's, whole: a
// file written for another base file; one whose footer names another
// sequence than its name; a pair whose two files come from different
// writes; a pair that does not follow the one before it (another write put
// a pair of that number in its place); a pair written with other columns
// than the base's, whose rows would read as NULL in the columns one side
// lacks. columns is the base file's columns, in any order.
//
// It returns the footer of the last pair's .pgnew file, which carries where
// the next window starts and that pair's run; nil for an empty chain.
func CheckChain(ctx context.Context, db *sql.DB, dir string, chain Chain, base string, columns []string) (map[string]string, error) {
	if base == "" {
		return nil, errors.New("pgdelta: a chain is checked against its base's id")
	}
	wantColumns := columnsValue(columns)
	var last map[string]string
	prev := ""
	for _, p := range chain.Pairs {
		var run string
		for i, name := range []string{p.Dead, p.New} {
			at := join(dir, name)
			md, err := Footer(ctx, db, at)
			if err != nil {
				return nil, err
			}
			switch {
			case md[MetaKeyBase] != base:
				return nil, fmt.Errorf("%w: %s was written for another table file (%q, and this one is %q)", ErrBrokenChain, at, md[MetaKeyBase], base)
			case md[MetaKeySeq] != strconv.Itoa(p.Seq):
				return nil, fmt.Errorf("%w: %s says it is pair %q", ErrBrokenChain, at, md[MetaKeySeq])
			case md[MetaKeyRun] == "":
				return nil, fmt.Errorf("%w: %s does not say which write it is from", ErrBrokenChain, at)
			case i == 1 && md[MetaKeyRun] != run:
				return nil, fmt.Errorf("%w: the two files of pair %0*d of %s are from different writes", ErrBrokenChain, seqWidth, p.Seq, chain.Table)
			case md[MetaKeyPrev] != prev:
				return nil, fmt.Errorf("%w: pair %0*d of %s does not follow the pair before it (it was written after %q, and that pair is %q)",
					ErrBrokenChain, seqWidth, p.Seq, chain.Table, md[MetaKeyPrev], prev)
			case md[MetaKeyColumns] != wantColumns:
				return nil, fmt.Errorf("%w: %s was written with the columns %s, and the table file has %s", ErrBrokenChain, at, md[MetaKeyColumns], wantColumns)
			}
			run, last = md[MetaKeyRun], md
		}
		prev = run
	}
	return last, nil
}

// join is name under dir, a folder or a URL prefix (s3://…), whose double
// slash a path join would fold.
func join(dir, name string) string {
	if strings.Contains(dir, "://") {
		return strings.TrimSuffix(dir, "/") + "/" + name
	}
	return path.Join(dir, name)
}
