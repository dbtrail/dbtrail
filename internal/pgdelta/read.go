package pgdelta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

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
// want reports true for. keyColumns are the key's columns in the key's
// order. A key the file does not hold is absent from the answer: it is a
// row the window inserted.
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
		" FROM read_parquet("+quote(basePath)+", file_row_number=true)")
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

// Footer reads a file's own metadata, key by key.
func Footer(ctx context.Context, db *sql.DB, file string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT decode(key), decode(value) FROM parquet_kv_metadata("+quote(file)+")")
	if err != nil {
		return nil, fmt.Errorf("read the footer of %s: %w", file, err)
	}
	defer rows.Close()
	md := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("read the footer of %s: %w", file, err)
		}
		md[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the footer of %s: %w", file, err)
	}
	return md, nil
}

// CheckChain reads the footers of every file of chain, in the folder (or
// s3:// prefix) dir, and refuses a chain that is not this base's: a file
// written for another base file, one whose footer names another sequence
// than its name, a pair whose two files come from different writes. It
// returns the footer of the last pair's .pgnew file, which carries where
// the next window starts; nil for an empty chain.
func CheckChain(ctx context.Context, db *sql.DB, dir string, chain Chain, base string) (map[string]string, error) {
	if base == "" {
		return nil, errors.New("pgdelta: a chain is checked against its base's id")
	}
	var last map[string]string
	for _, p := range chain.Pairs {
		var run string
		for i, name := range []string{p.Dead, p.New} {
			file := join(dir, name)
			md, err := Footer(ctx, db, file)
			if err != nil {
				return nil, err
			}
			switch {
			case md[MetaKeyBase] != base:
				return nil, fmt.Errorf("%w: %s was written for another table file (%q, and this one is %q)", ErrBrokenChain, file, md[MetaKeyBase], base)
			case md[MetaKeySeq] != strconv.Itoa(p.Seq):
				return nil, fmt.Errorf("%w: %s says it is pair %q", ErrBrokenChain, file, md[MetaKeySeq])
			case md[MetaKeyRun] == "":
				return nil, fmt.Errorf("%w: %s does not say which write it is from", ErrBrokenChain, file)
			case i == 1 && md[MetaKeyRun] != run:
				return nil, fmt.Errorf("%w: the two files of pair %0*d of %s are from different writes", ErrBrokenChain, seqWidth, p.Seq, chain.Table)
			}
			run, last = md[MetaKeyRun], md
		}
	}
	return last, nil
}

// join is name under dir, a folder or an s3:// prefix.
func join(dir, name string) string {
	if strings.HasPrefix(dir, "s3://") {
		return strings.TrimSuffix(dir, "/") + "/" + name
	}
	return path.Join(dir, name)
}
