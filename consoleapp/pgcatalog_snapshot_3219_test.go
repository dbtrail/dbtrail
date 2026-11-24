package consoleapp

import (
	"context"
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// snapshotSource is a copy whose snapshot changes on a schedule: stamps[i]
// is what the i-th stamp read says.
type snapshotSource struct {
	stamps []string
	i      int
	err    error
}

func (s *snapshotSource) next() string {
	v := s.stamps[min(s.i, len(s.stamps)-1)]
	s.i++
	return v
}

func (s *snapshotSource) PGCopyStampFresh(context.Context) (string, error) { return s.next(), nil }

func (s *snapshotSource) CopyTablesStamped(context.Context) ([]console.CopyTable, string, error) {
	if s.err != nil {
		return nil, "", s.err
	}
	return []console.CopyTable{{Schema: "public", Table: "t"}}, s.next(), nil
}

func TestPGCatalogOneSnapshot(t *testing.T) {
	cases := []struct {
		name     string
		stamps   []string
		wantRuns int
		wantCode string
	}{
		{"one snapshot", []string{"a", "a"}, 1, ""},
		{"a new one once: read again", []string{"a", "b", "b", "b"}, 2, ""},
		{"a new one twice: an error, never a mix", []string{"a", "b", "b", "c"}, 2, "40001"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := &snapshotSource{stamps: c.stamps}
			runs := 0
			run := func(context.Context, string) (sqlsandbox.Result, error) {
				runs++
				return sqlsandbox.Result{Rows: [][]any{{"public", "t", "code", "VARCHAR"}}}, nil
			}
			_, tables, err := pgCatalogOneSnapshot(t.Context(), src, run)
			if runs != c.wantRuns {
				t.Errorf("columns read %d times, want %d", runs, c.wantRuns)
			}
			if c.wantCode == "" {
				if err != nil || len(tables) != 1 {
					t.Fatalf("err %v, tables %v", err, tables)
				}
				return
			}
			var pe *pgshim.Error
			if !errors.As(err, &pe) || pe.Code != c.wantCode {
				t.Fatalf("err %v, want code %s", err, c.wantCode)
			}
		})
	}

	t.Run("an error of the tables is said", func(t *testing.T) {
		boom := errors.New("listing failed")
		src := &snapshotSource{stamps: []string{"a"}, err: boom}
		_, _, err := pgCatalogOneSnapshot(t.Context(), src, func(context.Context, string) (sqlsandbox.Result, error) {
			return sqlsandbox.Result{}, nil
		})
		if !errors.Is(err, boom) {
			t.Fatalf("err %v", err)
		}
	})
}
