package consoleapp

import (
	"context"
	"fmt"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// A table the last full read left out because its name cannot be stored
// (#2006) is not a new table: counting it as one would start a full read for
// it at every update, and each would leave it out again.
func TestCheckNewTables_knownLeftOutIsNotNew_2006(t *testing.T) {
	prev := listSourceTables
	t.Cleanup(func() { listSourceTables = prev })
	listSourceTables = func(context.Context, string, config.SSL, []string) ([]string, bool, error) {
		return []string{"demo.plain", "demo.order/items", "demo.fresh"}, false, nil
	}
	sup := refusedFixture(t)
	if err := sup.history.Append(console.BaselineRunRecord{ServerID: "a", Kind: console.BaselineRunDump,
		StartedAt: "2026-10-02T10:00:00Z", FinishedAt: "2026-10-02T10:01:00Z", Tables: 1,
		LeftOutTables: []console.RefusedTable{{Name: "demo.order/items", Verdict: console.VerdictLeftOut, Reason: "slash"}},
		LeftOutKeys:   []string{"demo.order/items"}}); err != nil {
		t.Fatal(err)
	}
	c := sup.checkNewTables(refreshRequest{ServerID: "a", SourceDSN: "src:pw@tcp(h:3306)/"}, []string{"demo.plain"})
	if len(c.all) != 1 || c.all[0] != "demo.fresh" {
		t.Fatalf("new tables = %q, want only demo.fresh", c.all)
	}
}

// Measured with mydumper 1.0.3-1: --database a,b writes each schema's
// CREATE DATABASE, --regex writes none, so a renamed schema can be read back
// only from the first. A name holding a comma cannot ride in the list.
func TestBuildConsoleMydumperArgs_schemaList_2006(t *testing.T) {
	args := buildConsoleMydumperArgs("h", 3306, "u", []string{"ventas_año", "demo"}, "/out", baseline.LockModeNoLock, true, nil, true)
	if v := valueAfter(args, "--database"); v != "ventas_año,demo" || has(args, "--regex") {
		t.Errorf("args = %v, want --database ventas_año,demo", args)
	}
	args = buildConsoleMydumperArgs("h", 3306, "u", []string{"a,b", "demo"}, "/out", baseline.LockModeNoLock, true, nil, true)
	if !has(args, "--regex") || has(args, "--database") {
		t.Errorf("a schema with a comma must keep --regex: %v", args)
	}
	args = buildConsoleMydumperArgs("h", 3306, "u", []string{"a", "b"}, "/out", baseline.LockModeNoLock, true, nil, false)
	if !has(args, "--regex") {
		t.Errorf("an older or unknown mydumper keeps --regex: %v", args)
	}
}

// The console image sets no locale, and mydumper 1.0.3-1 then refuses a
// non-ASCII schema name on its command line: a UTF-8 locale is added when
// the daemon has none, and an operator's own is kept.
func TestMydumperEnv_locale_2006(t *testing.T) {
	has := func(env []string, kv string) bool {
		for _, e := range env {
			if e == kv {
				return true
			}
		}
		return false
	}
	for _, c := range []struct {
		name string
		base []string
		want string // "" = nothing added
	}{
		{"no locale", []string{"PATH=/bin"}, "LC_CTYPE=C.UTF-8"},
		{"LANG=C", []string{"LANG=C"}, "LC_CTYPE=C.UTF-8"},
		{"LANG empty", []string{"LANG="}, "LC_CTYPE=C.UTF-8"},
		{"operator UTF-8", []string{"LANG=es_AR.UTF-8"}, ""},
		{"utf8 spelling", []string{"LC_CTYPE=C.utf8"}, ""},
		{"LC_ALL=POSIX overrides LC_CTYPE", []string{"LC_ALL=POSIX", "LANG=es_AR.UTF-8"}, "LC_ALL=C.UTF-8"},
		{"LC_ALL UTF-8 wins over LANG=C", []string{"LC_ALL=en_US.UTF-8", "LANG=C"}, ""},
	} {
		env := mydumperEnv(c.base, "pw")
		if !has(env, "MYSQL_PWD=pw") {
			t.Errorf("%s: no password: %q", c.name, env)
		}
		added := len(env) > len(c.base)+1
		if c.want == "" && added || c.want != "" && !has(env, c.want) {
			t.Errorf("%s: env = %q, want %q added", c.name, env, c.want)
		}
	}
}

// The review's three ways a left-out table still looked new (#2006): a
// renamed schema with no CREATE DATABASE (the table is known, the schema
// not), more left-out tables than the page lists, and the key list is what
// the check reads, not the capped display list.
func TestCheckNewTables_leftOutKeysMatchTheSource_2006(t *testing.T) {
	prev := listSourceTables
	t.Cleanup(func() { listSourceTables = prev })
	var source []string
	for i := 0; i < 25; i++ {
		source = append(source, fmt.Sprintf("demo.t%02d/x", i))
	}
	source = append(source, "demo.plain", "ventas_año.orders", "ventas_año.items", "demo.fresh", "shop.orders")
	listSourceTables = func(context.Context, string, config.SSL, []string) ([]string, bool, error) { return source, false, nil }

	var left []console.LeftOut
	for i := 0; i < 25; i++ {
		left = append(left, console.LeftOut{Table: fmt.Sprintf("demo.t%02d/x", i), Reason: "slash", Schema: "demo", Name: fmt.Sprintf("t%02d/x", i)})
	}
	left = append(left, console.LeftOut{Table: "orders", Reason: "schema unknown", Name: "orders"},
		console.LeftOut{Table: "items", Reason: "schema unknown", Name: "items"},
		console.LeftOut{Table: "unknown name (dump file x)", Reason: "unreadable"})
	shown, omitted := console.LeftOutTablesOf(left)
	sup := refusedFixture(t)
	if err := sup.history.Append(console.BaselineRunRecord{ServerID: "a", Kind: console.BaselineRunDump,
		StartedAt: "2026-10-02T10:00:00Z", FinishedAt: "2026-10-02T10:01:00Z", Tables: 2,
		LeftOutTables: shown, LeftOutTablesOmitted: omitted, LeftOutKeys: console.LeftOutKeys(left)}); err != nil {
		t.Fatal(err)
	}
	// shop holds a table in the snapshot, so its orders is a real new table.
	c := sup.checkNewTables(refreshRequest{ServerID: "a", SourceDSN: "src:pw@tcp(h:3306)/"}, []string{"demo.plain", "shop.users"})
	if fmt.Sprint(c.all) != "[demo.fresh shop.orders]" {
		t.Fatalf("new tables = %q, want only demo.fresh and shop.orders", c.all)
	}
}
