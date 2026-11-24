package pgshim

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// What the source's plan finds expensive goes to the copy on the simple
// protocol, whatever the screen says of how the copy answers it; the rest
// is decided as before.
func TestHeavyOnCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src, catalog: func() []CatalogTable {
		return []CatalogTable{{Schema: "public", Name: "big", Columns: []CatalogColumn{
			{Name: "n", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1},
			// A char(n) the view does not trim: the screen vetoes comparing it.
			{Name: "c", Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: 9},
		}}}
	}}
	l.copyErr = func(st string) error {
		if containsFold(st, "copyfails") {
			return errors.New("Catalog Error: Scalar Function with name copyfails does not exist")
		}
		return nil
	}
	c := costPortConn(t, ctx, l)
	for _, tc := range []struct {
		name, stmt string
		want       costOutcome
	}{
		{"expensive, with a veto no column decides", "SELECT sum(n) FROM big WHERE now() > '2000-01-01'", costOutcome{"7", "", RouteCopy, ReasonHeavyOnCopy}},
		{"expensive, with a veto of a column's type", "SELECT sum(n) FROM big WHERE c = 'x'", costOutcome{"7", "", RouteCopy, ReasonHeavyOnCopy}},
		{"expensive, with nothing vetoed", "SELECT sum(n) FROM big", costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}},
		{"cheap, with a veto", "SELECT now()", costOutcome{"42", "", RouteSource, ReasonVeto}},
		{"cheap, with a veto of a column's type", "SELECT n FROM t WHERE id = 1", costOutcome{"42", "", RouteSource, ReasonCheapPlan}},
		// What the copy cannot run is still the source's, unseen by the client.
		{"expensive, and the copy fails on it", "SELECT sum(n) FROM big WHERE copyfails(n) > now()", costOutcome{"42", "", RouteSource, ReasonCopyRefused}},
	} {
		got := costSimple(ctx, c, l, tc.stmt)
		if got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}

	// In a transaction block: the source's, as before.
	if _, err := c.Exec(ctx, "BEGIN").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if got := costSimple(ctx, c, l, "SELECT sum(n) FROM big WHERE now() > '2000-01-01'"); got.side != RouteSource {
		t.Errorf("in a block: %+v", got)
	}
	if _, err := c.Exec(ctx, "COMMIT").ReadAll(); err != nil {
		t.Fatal(err)
	}

	// Prepared (the extended protocol): as before, the screen's veto stands.
	if _, err := c.Prepare(ctx, "heavy", "SELECT sum(n) FROM big WHERE now() > '2000-01-01'", nil); err != nil {
		t.Fatal(err)
	}
	r := c.ExecPrepared(ctx, "heavy", nil, nil, []int16{pgproto3.TextFormat}).Read()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if n := l.last(); n.Side != RouteSource || n.Reason != ReasonVeto {
		t.Errorf("prepared: %+v", n)
	}
	if string(r.Rows[0][0]) != "42" {
		t.Errorf("prepared: answered %q", r.Rows[0][0])
	}
}

func containsFold(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if equalFoldASCII(s[i:i+len(sub)], sub) {
			return true
		}
	}
	return false
}

func equalFoldASCII(a, b string) bool {
	for i := range len(a) {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
