package pgdelta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// CutValuesOf is BaseValuesOf over a table's published cut: its base file
// and the chain of pairs beside it, as StateSQL reads them. For each of keys
// the cut holds a row for, the text of the named columns in that row: nil
// for a NULL. A key the cut holds no row for is absent from the answer: one
// neither the base nor a pair holds, and one a pair deleted.
//
// The pairs first, the base for what they do not name:
//
//   - a key a .pgnew file holds is answered from there, by its newest pair
//     (the highest sequence), and never from the base, whose row of that key
//     is the row as it was before the pair's window: a row marked deleted
//     there is no row;
//   - every other key is looked for in the base (BasePositionsOf, then the
//     named columns of the rows found). A row a .pgdead file takes away
//     always has its key in the .pgnew file beside it, so a wanted key found
//     at such a row and named by no pair is a chain that is not whole, and
//     an error (ErrBrokenChain), not a value.
//
// dead and rows are the chain's files, one entry per pair; with none it is
// BaseValuesOf. maxBytes (0: no limit) bounds the text of the whole answer;
// past it the answer is ErrBaseValuesTooLarge. The errors of BaseValuesOf
// are this function's too.
func CutValuesOf(ctx context.Context, db *sql.DB, basePath string, dead, rows []string, keyColumns, keys, columns []string, maxBytes int64) (map[string]map[string]*string, error) {
	dead, rows = unique(dead), unique(rows)
	if len(dead) == 0 && len(rows) == 0 {
		return BaseValuesOf(ctx, db, basePath, keyColumns, keys, columns, maxBytes)
	}
	found := map[string]map[string]*string{}
	if len(keyColumns) == 0 {
		return nil, errors.New("pgdelta: a table with no key column")
	}
	if len(keys) == 0 || len(columns) == 0 {
		return found, nil
	}
	in, err := baseColumnsOf(ctx, db, basePath)
	if err != nil {
		return nil, err
	}
	for _, c := range columns {
		if !in[c] {
			return nil, fmt.Errorf("%s has no column %s", basePath, c)
		}
	}

	// What the pairs say of the wanted keys.
	type newest struct {
		seq     string
		deleted bool
		row     map[string]*string
		bytes   int64
	}
	named := map[string]*newest{}
	var held int64
	if len(rows) > 0 {
		wanted, cleanup, err := writeWantedKeys(keys)
		if err != nil {
			return nil, fmt.Errorf("write the keys to read from the table delta of %s: %w", basePath, err)
		}
		defer cleanup()
		sel := make([]string, len(columns))
		for i, c := range columns {
			sel[i] = "encode(bintrail_n." + quoteIdent(c) + ")"
		}
		// Keys as their bytes on both sides, as StateSQL compares them.
		rs, err := db.QueryContext(ctx, "SELECT encode(bintrail_n."+KeyColumn+"), bintrail_n."+OpColumn+", bintrail_n."+SeqColumn+", "+strings.Join(sel, ", ")+
			" FROM read_parquet("+list(rows)+", union_by_name=true"+noHive+") AS bintrail_n"+
			" JOIN (SELECT DISTINCT encode("+KeyColumn+") AS bintrail_wk FROM read_parquet("+file(wanted)+noHive+")) AS bintrail_w"+
			" ON encode(bintrail_n."+KeyColumn+") = bintrail_w.bintrail_wk")
		if err != nil {
			return nil, fmt.Errorf("read the changed keys in the table delta of %s: %w", basePath, err)
		}
		defer rs.Close()
		for rs.Next() {
			var key []byte
			var op, seq sql.NullString
			vals := make([][]byte, len(columns))
			dest := make([]any, 0, len(columns)+3)
			dest = append(dest, &key, &op, &seq)
			for i := range vals {
				dest = append(dest, &vals[i])
			}
			if err := rs.Scan(dest...); err != nil {
				return nil, fmt.Errorf("read the changed keys in the table delta of %s: %w", basePath, err)
			}
			switch {
			case key == nil || !op.Valid || !seq.Valid:
				return nil, fmt.Errorf("%w: the table delta of %s holds a row with no key, no sequence or no mark of what happened to it", ErrBrokenChain, basePath)
			case op.String != OpPut && op.String != OpDelete:
				return nil, fmt.Errorf("%w: the table delta of %s holds a row of key %q marked %q", ErrBrokenChain, basePath, key, op.String)
			}
			k := string(key)
			was := named[k]
			if was != nil {
				if was.seq == seq.String {
					return nil, fmt.Errorf("%w: pair %s of the table delta of %s holds key %q twice", ErrBrokenChain, seq.String, basePath, k)
				}
				// Six digits: the text orders as the number does.
				if was.seq > seq.String {
					continue
				}
				held -= was.bytes
			}
			n := &newest{seq: seq.String, deleted: op.String == OpDelete}
			if !n.deleted {
				n.row = make(map[string]*string, len(columns))
				for i, c := range columns {
					if vals[i] != nil {
						text := string(vals[i])
						n.row[c] = &text
						n.bytes += int64(len(text))
					} else {
						n.row[c] = nil
					}
				}
			}
			held += n.bytes
			if maxBytes > 0 && held > maxBytes {
				return nil, ErrBaseValuesTooLarge
			}
			named[k] = n
		}
		if err := rs.Err(); err != nil {
			return nil, fmt.Errorf("read the changed keys in the table delta of %s: %w", basePath, err)
		}
		rs.Close()
	}

	// The base, for the keys no pair names.
	var rest []string
	for _, k := range keys {
		if n, ok := named[k]; ok {
			if !n.deleted {
				found[k] = n.row
			}
			continue
		}
		rest = append(rest, k)
	}
	if len(rest) == 0 {
		return found, nil
	}
	positions, err := BasePositionsOf(ctx, db, basePath, keyColumns, rest)
	if err != nil {
		return nil, err
	}
	if len(positions) == 0 {
		return found, nil
	}
	if len(dead) > 0 {
		nums := make([]string, 0, len(positions))
		for _, pos := range positions {
			nums = append(nums, fmt.Sprint(pos))
		}
		wanted, cleanup, err := writeWantedKeys(nums)
		if err != nil {
			return nil, fmt.Errorf("write the rows to read from %s: %w", basePath, err)
		}
		defer cleanup()
		var gone sql.NullInt64
		err = db.QueryRowContext(ctx, `SELECT min(bintrail_d."`+baseline.TableDeltaPosColumn+`") FROM read_parquet(`+list(dead)+noHive+`) AS bintrail_d`+
			" JOIN (SELECT CAST("+KeyColumn+" AS BIGINT) AS bintrail_p FROM read_parquet("+file(wanted)+noHive+")) AS bintrail_w"+
			` ON bintrail_d."`+baseline.TableDeltaPosColumn+`" = bintrail_w.bintrail_p`).Scan(&gone)
		if err != nil {
			return nil, fmt.Errorf("read the rows the table delta of %s takes away: %w", basePath, err)
		}
		if gone.Valid {
			return nil, fmt.Errorf("%w: the table delta of %s takes away row %d of it and holds no row of that row's key", ErrBrokenChain, basePath, gone.Int64)
		}
	}
	base, err := baseValuesAt(ctx, db, basePath, positions, columns, textLimit(maxBytes, held))
	if err != nil {
		return nil, err
	}
	for k, row := range base {
		found[k] = row
	}
	return found, nil
}
