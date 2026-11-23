package consoleapp

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
)

// The check for tables an update leaves out, on a PostgreSQL server (#2759):
// its source's tables are its publication's.
func TestCheckNewTables_postgres_2759(t *testing.T) {
	type asked struct {
		dsn, publication string
		filters          event.Filters
	}
	stub := func(t *testing.T, tables []string, err error) *[]asked {
		t.Helper()
		var calls []asked
		prev, prevMySQL := listSourceTablesPG, listSourceTables
		t.Cleanup(func() { listSourceTablesPG, listSourceTables = prev, prevMySQL })
		listSourceTablesPG = func(_ context.Context, dsn, publication string, filters event.Filters) ([]pgbaseline.SourceTable, error) {
			calls = append(calls, asked{dsn, publication, filters})
			var now []pgbaseline.SourceTable
			for _, name := range tables {
				schema, table, _ := strings.Cut(name, ".")
				now = append(now, pgbaseline.SourceTable{Schema: schema, Table: table})
			}
			return now, err
		}
		listSourceTables = func(context.Context, string, config.SSL, []string) ([]string, bool, error) {
			t.Error("a PostgreSQL server's tables were asked for with the MySQL read")
			return nil, false, nil
		}
		return &calls
	}
	pg := func(edit func(*refreshRequest)) refreshRequest {
		req := refreshRequest{ServerID: "a", SourceDSN: "postgres://u:secret@src/db", SourcePostgres: true,
			SourcePublication: "pub", Schemas: []string{"public"}}
		if edit != nil {
			edit(&req)
		}
		return req
	}

	t.Run("a table added to the publication is named", func(t *testing.T) {
		calls := stub(t, []string{"public.orders", "public.audit_log", "public.orders"}, nil)
		c := refusedFixture(t).checkNewTables(pg(nil), []string{"public.orders"})
		if c.unchecked != "" || !slices.Equal(c.all, []string{"public.audit_log"}) {
			t.Fatalf("check = %+v, want public.audit_log alone", c)
		}
		if len(*calls) != 1 || (*calls)[0].dsn != "postgres://u:secret@src/db" || (*calls)[0].publication != "pub" {
			t.Fatalf("asked %+v", *calls)
		}
		if f := (*calls)[0].filters; !f.Matches("public", "x") || f.Matches("other", "x") {
			t.Fatal("the publication's tables were not narrowed to the server's schemas, as a full read narrows them")
		}
	})
	t.Run("nothing new", func(t *testing.T) {
		stub(t, []string{"public.orders"}, nil)
		if c := refusedFixture(t).checkNewTables(pg(nil), []string{"public.orders"}); c.unchecked != "" || len(c.all) != 0 {
			t.Fatalf("check = %+v, want nothing", c)
		}
	})
	t.Run("names are compared as they are: PostgreSQL's are case-sensitive", func(t *testing.T) {
		stub(t, []string{"public.Orders", "public.orders"}, nil)
		c := refusedFixture(t).checkNewTables(pg(nil), []string{"public.orders"})
		if !slices.Equal(c.all, []string{"public.Orders"}) {
			t.Fatalf("new tables = %q, want public.Orders", c.all)
		}
	})
	t.Run("no schema filter asks for every schema", func(t *testing.T) {
		calls := stub(t, []string{"app.t"}, nil)
		refusedFixture(t).checkNewTables(pg(func(r *refreshRequest) { r.Schemas = nil }), nil)
		if f := (*calls)[0].filters; !f.Matches("app", "t") || !f.Matches("public", "t") {
			t.Fatal("with no schemas set, a schema was filtered out")
		}
	})
	t.Run("a source that cannot be asked is said, without its password", func(t *testing.T) {
		stub(t, nil, errors.New("connect to postgres://u:secret@src/db: connection refused"))
		c := refusedFixture(t).checkNewTables(pg(nil), []string{"public.orders"})
		if c.unchecked == "" || len(c.all) != 0 || strings.Contains(c.unchecked, "secret") {
			t.Fatalf("check = %+v, want unchecked with a reason that holds no password", c)
		}
	})
	t.Run("no publication set: said, and the source is not asked", func(t *testing.T) {
		calls := stub(t, []string{"public.x"}, nil)
		c := refusedFixture(t).checkNewTables(pg(func(r *refreshRequest) { r.SourcePublication = "" }), nil)
		if c.unchecked == "" || len(c.all) != 0 || len(*calls) != 0 {
			t.Fatalf("check = %+v with %d reads, want unchecked and none", c, len(*calls))
		}
	})
}
