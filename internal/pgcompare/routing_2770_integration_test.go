//go:build integration

package pgcompare

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
)

// checkRouting holds the PostgreSQL port's per-statement routing (#2770) to
// what TestIntegrationPGCompare measured: the port lets the copy answer a
// statement only when its screen (pgshim.CopyScreen, with the copy's own
// column types) finds nothing in it the copy is not known to answer the
// same, and a statement the copy then refuses, or answers with a column it
// writes otherwise, goes to the source at run time. So:
//
//   - a line answered DIFFERENTLY is one the screen sends to the source, or
//     one whose answer on the copy has a column the port sends to the source
//     for (a numeric declared without precision);
//   - a line the source FAILS is one the screen sends there, or one the copy
//     fails too (never a copy that answers where PostgreSQL errors);
//   - and many EQUAL lines stay on the copy: a screen that sent everything to
//     the source would pass the first two and serve no purpose.
func checkRouting(t *testing.T, ctx context.Context, src *Source, cp *SnapshotCopy, rep *Report, want []expectation) {
	t.Helper()
	cols, _, err := cp.Query(ctx, "SELECT lower(column_name), data_type, lower(table_name) FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position", 10000)
	if err != nil {
		t.Fatalf("the copy's column types: %v", err)
	}
	types := map[string][]string{}
	// Each table's column types in their order too, as the port has them:
	// a position behind a star is read with them.
	ordered, tables := map[string][]string{}, []string{}
	for _, r := range cols.Rows {
		if r[0] != nil && r[1] != nil && r[2] != nil {
			types[*r[0]] = append(types[*r[0]], strings.ToUpper(*r[1]))
			if _, seen := ordered[*r[2]]; !seen {
				tables = append(tables, *r[2])
			}
			ordered[*r[2]] = append(ordered[*r[2]], strings.ToUpper(*r[1]))
		}
	}
	for _, table := range tables {
		pgshim.SetColumnOrder(types, table, ordered[table])
	}
	// unsized reports whether the copy answers stmt with a column the port
	// sends to the source for.
	unsized := func(stmt string) bool {
		d, _, err := cp.Query(ctx, "DESCRIBE "+stmt, 100)
		if err != nil {
			return false
		}
		for _, r := range d.Rows {
			for _, c := range r {
				if c != nil && strings.ReplaceAll(*c, " ", "") == "DECIMAL(38,18)" {
					return true
				}
			}
		}
		return false
	}
	onCopy := 0
	for i, r := range rep.Results {
		veto := pgshim.CopyScreen(r.Statement, types)
		side := "copy"
		if veto != "" {
			side = "source (" + veto + ")"
		}
		switch r.Verdict {
		case sqlcompare.Equal:
			if veto == "" {
				onCopy++
			}
		case sqlcompare.Different:
			if veto == "" && !unsized(r.Statement) && !mainTextOrder(t, ctx, src, cp, r, types) {
				t.Errorf("answered differently on the copy (%s), and the port lets the copy answer it: %s", r.Kind, r.Statement)
			}
		case sqlcompare.SourceError:
			if veto == "" {
				if _, _, err := cp.Query(ctx, r.Statement, 10); err == nil {
					t.Errorf("the source fails it and the copy answers it, and the port lets the copy answer it: %s", r.Statement)
				}
			}
		}
		if want[i].stay && veto != "" {
			t.Errorf("the port sends to the source (%s) a statement the copy answers the same, which it must leave to the copy: %s", veto, r.Statement)
		}
		t.Logf("%-13s -> %s: %s (%s)", r.Verdict, side, r.Statement, want[i].why)
	}
	// 66 EQUAL lines stay on the copy now. The count only goes down when the
	// screen is made stricter on purpose.
	if onCopy < 66 {
		t.Errorf("%d EQUAL statements are left to the copy, fewer than 66", onCopy)
	}
	t.Logf("%d EQUAL statements are left to the copy", onCopy)
}

// mainTextOrder reports whether r differs from PostgreSQL only by what #3219
// accepted on the main statement's ORDER BY of text: the copy sorts by bytes.
// It holds the claim itself: the same statement on the source, with every
// text column of the fixture sorting by bytes (COLLATE "C", in a transaction
// that is rolled back), must answer exactly what the copy answered, in the
// same order, a LIMIT's rows included. Any other difference fails.
func mainTextOrder(t *testing.T, ctx context.Context, src *Source, cp *SnapshotCopy, r Result, types map[string][]string) bool {
	t.Helper()
	if !strings.Contains(pgshim.CopyScreenSortingLikeSource(r.Statement, types), "text sorted by ORDER BY") {
		return false
	}
	byBytes, err := src.underByteOrder(ctx, r.Statement)
	if err != nil {
		t.Errorf("%s on the source sorting by bytes: %v", r.Statement, err)
		return true
	}
	onCopy, _, err := cp.Query(ctx, r.Statement, 5000)
	if err != nil {
		t.Errorf("%s on the copy: %v", r.Statement, err)
		return true
	}
	verdict, kind, detail := sqlcompare.Compare(byBytes, onCopy, true)
	if verdict != sqlcompare.Equal {
		t.Logf("%s: against PostgreSQL sorting by bytes: %s %s", r.Statement, kind, detail)
		return false
	}
	return true
}

// underByteOrder runs stmt with every text column of the fixture's tables
// collated "C", and takes it all back.
func (s *Source) underByteOrder(ctx context.Context, stmt string) (sqlcompare.Rows, error) {
	if _, err := s.conn.Exec(ctx, "BEGIN"); err != nil {
		return sqlcompare.Rows{}, err
	}
	defer s.conn.Exec(ctx, "ROLLBACK")
	rows, err := s.conn.Query(ctx, `SELECT format('ALTER TABLE %I ALTER COLUMN %I TYPE %s COLLATE "C"', c.relname, a.attname, format_type(a.atttypid, a.atttypmod))
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relkind = 'r' AND c.relname LIKE 'pgcmp%' AND c.relnamespace = 'public'::regnamespace
		  AND a.attnum > 0 AND NOT a.attisdropped AND a.atttypid IN ('text'::regtype, 'varchar'::regtype)`)
	if err != nil {
		return sqlcompare.Rows{}, err
	}
	var alters []string
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			rows.Close()
			return sqlcompare.Rows{}, err
		}
		alters = append(alters, q)
	}
	rows.Close()
	for _, q := range alters {
		if _, err := s.conn.Exec(ctx, q); err != nil {
			return sqlcompare.Rows{}, fmt.Errorf("%s: %w", q, err)
		}
	}
	out, _, err := s.read(ctx, stmt, 5000)
	return out, err
}
