package consoleapp

import (
	"context"
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

func TestPGColumnCollation_3111(t *testing.T) {
	recorded := &baseline.PGRelation{Collations: true}
	old := &baseline.PGRelation{}
	for _, tc := range []struct {
		rel  *baseline.PGRelation
		col  baseline.PGColumn
		prev pgshim.Collation
		want pgshim.Collation
	}{
		{recorded, baseline.PGColumn{Nondeterministic: true}, pgshim.CollationUnknown, pgshim.CollationNondeterministic},
		{recorded, baseline.PGColumn{}, pgshim.CollationUnknown, pgshim.CollationDeterministic},
		// A snapshot taken before collations were recorded says nothing.
		{old, baseline.PGColumn{}, pgshim.CollationUnknown, pgshim.CollationUnknown},
		{old, baseline.PGColumn{Nondeterministic: true}, pgshim.CollationUnknown, pgshim.CollationUnknown},
		// Of two columns the copy cannot tell apart, the nondeterministic
		// one is kept.
		{recorded, baseline.PGColumn{}, pgshim.CollationNondeterministic, pgshim.CollationNondeterministic},
	} {
		if got := pgColumnCollation(tc.rel.Collations, tc.col.Nondeterministic, tc.prev); got != tc.want {
			t.Errorf("%+v, %+v after %d: %d, want %d", tc.rel, tc.col, tc.prev, got, tc.want)
		}
	}
}

func TestPGFillCollations_3111(t *testing.T) {
	tables := func() []pgshim.CatalogTable {
		return []pgshim.CatalogTable{
			{Schema: "public", Name: "Old", Columns: []pgshim.CatalogColumn{
				{Name: "id", Collation: pgshim.CollationUnknown}, {Name: "T", Collation: pgshim.CollationUnknown},
			}},
			{Schema: "public", Name: "gone", Columns: []pgshim.CatalogColumn{{Name: "t", Collation: pgshim.CollationUnknown}}},
			{Schema: "public", Name: "new", Columns: []pgshim.CatalogColumn{{Name: "t", Collation: pgshim.CollationDeterministic}}},
		}
	}
	var asked [][2]string
	ask := func(_ context.Context, want [][2]string) (map[[3]string]bool, map[[2]string]bool, error) {
		asked = want
		return map[[3]string]bool{{"public", "old", "t"}: true}, map[[2]string]bool{{"public", "old"}: true}, nil
	}
	got := tables()
	pgFillCollations(context.Background(), got, ask)
	if len(asked) != 3 {
		t.Errorf("asked about %v, want every table: the source's answer is today's", asked)
	}
	if c := got[0].Columns; c[0].Collation != pgshim.CollationDeterministic || c[1].Collation != pgshim.CollationNondeterministic {
		t.Errorf("a table the source listed: %+v", c)
	}
	if c := got[1].Columns[0]; c.Collation != pgshim.CollationUnknown {
		t.Errorf("a table the source did not list: %+v, want it still unknown", c)
	}
	// The source failing leaves every unknown column unknown.
	got = tables()
	pgFillCollations(context.Background(), got, func(context.Context, [][2]string) (map[[3]string]bool, map[[2]string]bool, error) {
		return nil, nil, errors.New("refused")
	})
	if got[0].Columns[1].Collation != pgshim.CollationUnknown || got[2].Columns[0].Collation != pgshim.CollationDeterministic {
		t.Errorf("after an error: %+v", got)
	}
	// The source's answer wins over the snapshot's: ALTER ... COLLATE after
	// the snapshot keeps the column's type.
	got = []pgshim.CatalogTable{{Schema: "public", Name: "old", Columns: []pgshim.CatalogColumn{{Name: "t", Collation: pgshim.CollationDeterministic}}}}
	pgFillCollations(context.Background(), got, ask)
	if got[0].Columns[0].Collation != pgshim.CollationNondeterministic {
		t.Errorf("a column the source says is nondeterministic now: %+v", got[0].Columns[0])
	}
}

// A source that cannot be reached is not asked again by every catalog read
// for a while: the failure is the answer meanwhile.
func TestPGSourceCollations_aFailureIsKept_3111(t *testing.T) {
	ask := pgSourceCollations("postgres://nobody@127.0.0.1:1/none?connect_timeout=1&sslmode=disable", "test")
	tables := [][2]string{{"public", "t"}}
	_, _, first := ask(context.Background(), tables)
	if first == nil {
		t.Fatal("a source nothing listens for answered")
	}
	if _, _, again := ask(context.Background(), tables); again != first {
		t.Errorf("asked again at once: %v, want the kept failure %v", again, first)
	}
}
