package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// routedCopyOpts are the options the routing port writes a statement for
// the copy with (shim copyOpts).
var routedCopyOpts = readrouter.CopyOptions{MySQLClockKeywords: true, MySQLDateFunctions: true, MySQLDateSub: true, MySQLDateAdd: true, MySQLTimestampDiff: true, MySQLAvg: true}

// #2920 through the real worker: a routed AVG of a whole-number column is the
// copy's exact average, typed as MySQL types it (a DECIMAL with 4 decimals,
// 7.0000 over one row of 7), and an AVG of anything else, or one used other
// than as a column of the select list, is refused, from the snapshot's table
// definition.
func TestSQL_realWorkerAvg_2920(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "av", "CREATE TABLE `av` (\n  `id` int NOT NULL,\n  `k` int DEFAULT NULL,\n  `b` bigint DEFAULT NULL,\n"+
		"  `u` bigint unsigned DEFAULT NULL,\n  `dc` decimal(10,2) DEFAULT NULL,\n  `dbl` double DEFAULT NULL,\n  `s` varchar(20) DEFAULT NULL,\n"+
		"  `dt` date DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		[]string{"1", "7", "-9223372036854775808", "18446744073709551615", "1.25", "0.5", "abc", "2026-01-01"})
	ctx := context.Background()
	run := func(stmt string) (sqlOutcome, error) {
		text, why := readrouter.ForCopyOpts(stmt, routedCopyOpts)
		if why != "" {
			t.Fatalf("%s: not written for the copy: %s", stmt, why)
		}
		return f.s.runSQL(ctx, f.s.cm.boot, "u", text, "", 0, sqlsandbox.Session{StrictStar: true, MySQLClock: true, Types: readrouter.ShapeOf(stmt)})
	}
	for _, c := range []struct {
		stmt, want string
	}{
		{"SELECT AVG(k) FROM shop.av", "[DECIMAL(38,4)] [[7.0000]]"},
		{"SELECT AVG(b), AVG(u), AVG(DISTINCT k) FROM shop.av", "[DECIMAL(38,4) DECIMAL(38,4) DECIMAL(38,4)] [[-9223372036854775808.0000 18446744073709551615.0000 7.0000]]"},
		{"SELECT id, AVG(`k`) AS `a` FROM shop.av GROUP BY id ORDER BY id", "[INTEGER DECIMAL(38,4)] [[1 7.0000]]"},
		{"SELECT AVG(k) FROM shop.av WHERE k > 100", "[DECIMAL(38,4)] [[<nil>]]"},
	} {
		out, err := run(c.stmt)
		if err != nil {
			t.Errorf("%s: %v, want the copy's answer", c.stmt, err)
			continue
		}
		var types []string
		for _, col := range out.Result.Columns {
			types = append(types, col.Type)
		}
		if got := fmt.Sprint(types, " ", out.Result.Rows); got != c.want {
			t.Errorf("%s = %s, want %s", c.stmt, got, c.want)
		}
	}
	for _, c := range []struct {
		stmt string
		// mine: no other rule refuses it, so the refusal is AvgVeto's.
		mine bool
	}{
		{"SELECT AVG(dc) FROM shop.av", true},
		{"SELECT AVG(dbl) FROM shop.av", true},
		{"SELECT AVG(s) FROM shop.av", false},
		{"SELECT AVG(dt) FROM shop.av", false},
		{"SELECT AVG(LENGTH(s)) FROM shop.av", false},
		{"SELECT AVG(k) * 3 FROM shop.av", true},
		{"SELECT id, AVG(k) a FROM shop.av GROUP BY id ORDER BY a", true},
		{"SELECT id, AVG(k) FROM shop.av GROUP BY id HAVING AVG(k) > 1", true},
	} {
		_, err := run(c.stmt)
		var refusal *sqlStarRefusal
		switch {
		case !errors.As(err, &refusal):
			t.Errorf("%s: err = %v, want a refusal", c.stmt, err)
		case c.mine && !strings.Contains(refusal.Message, readrouter.VetoAvg):
			t.Errorf("%s: refusal %q, want AvgVeto's", c.stmt, refusal.Message)
		}
	}
}

// The rule asks about every table's columns in one question: the
// whole-number ones (the DISTINCT-safe columns without the DECIMALs) and the
// rest. A table whose types are not known, and a statement with no reading,
// are refused.
func TestSQLAvgRefusalFor_2920(t *testing.T) {
	in := views.Input{Baselines: []views.BaselineTable{
		{Schema: "shop", Table: "a", Path: "/c/shop/a.parquet", SchemaKnown: true, Columns: []string{"k", "dc", "s"},
			DistinctSafe: []string{"k", "dc"}, DistinctOther: []string{"s"}, Decimals: []views.DecimalColumn{{Name: "DC", Precision: 10, Scale: 2}}},
		{Schema: "shop", Table: "b", Path: "/c/shop/b.parquet", SchemaKnown: true, Columns: []string{"bo", "dbl"},
			DistinctSafe: []string{"bo"}, DistinctOther: []string{"dbl"}},
	}}
	asked := &avgAsked{}
	if got := sqlAvgRefusalFor(in, "SELECT _bintrail_avg(k) FROM a, b", asked); got != "" {
		t.Errorf("a reading that vetoes nothing: %q", got)
	}
	if asked.calls != 1 || strings.Join(asked.whole, ",") != "k,bo" || strings.Join(asked.other, ",") != "dc,s,dbl" {
		t.Errorf("asked %d times, whole %q other %q; want once, whole k,bo and other dc,s,dbl", asked.calls, asked.whole, asked.other)
	}
	asked = &avgAsked{why: "x"}
	if got := sqlAvgRefusalFor(in, "SELECT AVG(k) FROM a", asked); !strings.HasSuffix(got, "x") {
		t.Errorf("refusal %q, want the reason", got)
	}
	asked = &avgAsked{why: "x"}
	if got := sqlAvgRefusalFor(in, "SELECT k, average FROM a", asked); got != "" || asked.calls != 0 {
		t.Errorf("no AVG: %q after %d questions, want none asked", got, asked.calls)
	}
	if got := sqlAvgRefusalFor(in, "SELECT avg(k) FROM a", nil); got == "" {
		t.Error("no reading of the statement with an AVG: answered, want a refusal")
	}
	if got := sqlAvgRefusalFor(in, "SELECT k FROM a", nil); got != "" {
		t.Errorf("no reading of the statement and no AVG: %q, want none", got)
	}
	unknown := views.Input{Baselines: []views.BaselineTable{{Schema: "shop", Table: "c", Path: "/c/shop/c.parquet"}}}
	if got := sqlAvgRefusalFor(unknown, "SELECT _bintrail_avg(k) FROM c", readrouter.ShapeOf("SELECT AVG(k) FROM c")); !strings.Contains(got, "shop.c") {
		t.Errorf("a table whose types are not known: %q, want a refusal naming it", got)
	}
}

// avgAsked records what AvgVeto is asked; every other question vetoes
// nothing.
type avgAsked struct {
	askedTypes
	calls        int
	whole, other []string
	why          string
}

func (a *avgAsked) AvgVeto(whole, other []string) string {
	a.calls++
	a.whole, a.other = whole, other
	return a.why
}
