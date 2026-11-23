package consoleapp

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The columns a PostgreSQL source has now travel from the update's one read
// of the source to the fold (#3112).

func pgColsStub(t *testing.T, now []pgbaseline.SourceTable, err error) *int {
	t.Helper()
	calls := 0
	prev := listSourceTablesPG
	t.Cleanup(func() { listSourceTablesPG = prev })
	listSourceTablesPG = func(context.Context, string, string, event.Filters) ([]pgbaseline.SourceTable, error) {
		calls++
		return now, err
	}
	return &calls
}

func pgColsRequest() refreshRequest {
	return refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", SourceDSN: "postgres://u:secret@src/db",
		SourcePostgres: true, SourcePublication: "pub", Schemas: []string{"public"}}
}

func TestCheckNewTables_postgresColumns_3112(t *testing.T) {
	id := baseline.PGColumn{Name: "id", TypeOID: 23, TypeMod: -1, Ordinal: 1}
	t.Run("one read answers the tables and their columns", func(t *testing.T) {
		calls := pgColsStub(t, []pgbaseline.SourceTable{
			{Schema: "public", Table: "orders", OID: 500, Columns: []baseline.PGColumn{id}},
			{Schema: "public", Table: "Orders", OID: 501},
			{Schema: "a.b", Table: "c", OID: 502},
			{Schema: "a", Table: "b.c", OID: 503},
		}, nil)
		c := refusedFixture(t).checkNewTables(pgColsRequest(), []string{"public.orders"})
		if *calls != 1 {
			t.Fatalf("the source was read %d times, want once", *calls)
		}
		if c.unchecked != "" || !slices.Equal(c.all, []string{"a.b.c", "public.Orders"}) {
			t.Fatalf("new tables = %+v", c)
		}
		want := map[reconstruct.PGTableName]reconstruct.PGSourceShape{
			{Schema: "public", Table: "orders"}: {OID: 500, Columns: []baseline.PGColumn{id}},
			{Schema: "public", Table: "Orders"}: {OID: 501},
			{Schema: "a.b", Table: "c"}:         {OID: 502},
			{Schema: "a", Table: "b.c"}:         {OID: 503},
		}
		if !reflect.DeepEqual(c.pgShapes, want) {
			t.Fatalf("shapes = %+v, want %+v", c.pgShapes, want)
		}
	})
	t.Run("a source with no table is an answer, not a source that was not asked", func(t *testing.T) {
		pgColsStub(t, nil, nil)
		if c := refusedFixture(t).checkNewTables(pgColsRequest(), []string{"public.orders"}); c.pgShapes == nil || c.unchecked != "" {
			t.Fatalf("check = %+v, want an empty answer", c)
		}
	})
	t.Run("a source that cannot be asked: nothing to compare, and said", func(t *testing.T) {
		pgColsStub(t, []pgbaseline.SourceTable{{Schema: "public", Table: "orders", OID: 1}},
			errors.New("connect to postgres://u:secret@src/db: connection refused"))
		c := refusedFixture(t).checkNewTables(pgColsRequest(), []string{"public.orders"})
		if c.pgShapes != nil {
			t.Fatalf("a failed read left shapes to compare with: %+v", c.pgShapes)
		}
		if !strings.Contains(c.unchecked, "columns") || !strings.Contains(c.unchecked, "connection refused") || strings.Contains(c.unchecked, "secret") {
			t.Fatalf("unchecked = %q, want it to say columns were not looked for, why, and no password", c.unchecked)
		}
	})
	for what, edit := range map[string]func(*refreshRequest){
		"no publication set":   func(r *refreshRequest) { r.SourcePublication = "" },
		"no source connection": func(r *refreshRequest) { r.SourceDSN = "" },
	} {
		t.Run(what+": the source is not read and nothing is compared", func(t *testing.T) {
			calls := pgColsStub(t, []pgbaseline.SourceTable{{Schema: "public", Table: "orders", OID: 1}}, nil)
			req := pgColsRequest()
			edit(&req)
			if c := refusedFixture(t).checkNewTables(req, []string{"public.orders"}); c.pgShapes != nil || c.unchecked == "" || *calls != 0 {
				t.Fatalf("check = %+v with %d reads", c, *calls)
			}
		})
	}
}

// The fold of an update is handed what the source answered; the fold of a
// source that did not answer, and a MySQL server's, is handed nothing.
func TestExecuteRefresh_handsTheFoldTheSourceColumns_3112(t *testing.T) {
	id := baseline.PGColumn{Name: "id", TypeOID: 23, TypeMod: -1, Ordinal: 1}
	now := []pgbaseline.SourceTable{{Schema: "public", Table: "orders", OID: 500, Columns: []baseline.PGColumn{id}}}
	cases := []struct {
		what    string
		readErr error
		edit    func(*refreshRequest)
		want    map[reconstruct.PGTableName]reconstruct.PGSourceShape
	}{
		{"the source answered", nil, nil, map[reconstruct.PGTableName]reconstruct.PGSourceShape{
			{Schema: "public", Table: "orders"}: {OID: 500, Columns: []baseline.PGColumn{id}}}},
		{"the source did not answer", errors.New("connection refused"), nil, nil},
		{"shapes left on the request by an earlier run are not reused", errors.New("connection refused"), func(r *refreshRequest) {
			r.pgSourceShapes = map[reconstruct.PGTableName]reconstruct.PGSourceShape{{Schema: "public", Table: "orders"}: {OID: 1}}
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.what, func(t *testing.T) {
			pgColsStub(t, now, tc.readErr)
			realList, realFold := newestSnapshotTables, foldTables
			t.Cleanup(func() { newestSnapshotTables, foldTables = realList, realFold })
			newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
				return refreshAt.Add(-time.Hour), []string{"public.orders"}, nil
			}
			var got map[reconstruct.PGTableName]reconstruct.PGSourceShape
			folded := false
			foldTables = func(ctx context.Context, _ reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
				folded, got = true, reconstruct.PGSourceShapesIn(ctx)
				return []*reconstruct.TableReport{{Schema: "public", Table: "orders"}}, nil, nil
			}
			req := pgColsRequest()
			req.BaselineDir = t.TempDir()
			if tc.edit != nil {
				tc.edit(&req)
			}
			if _, _, _, _, _, err := refusedFixture(t).executeRefresh(req, refreshAt); err != nil {
				t.Fatalf("the update failed: %v", err)
			}
			if !folded {
				t.Fatal("the fold did not run")
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("the fold was handed %+v, want %+v", got, tc.want)
			}
		})
	}
}
