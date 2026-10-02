package consoleapp

import (
	"context"
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
		LeftOutTables: []console.RefusedTable{{Name: "demo.order/items", Verdict: console.VerdictLeftOut, Reason: "slash"}}}); err != nil {
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
	if env := mydumperEnv([]string{"PATH=/bin"}, "pw"); !has(env, "LC_ALL=C.UTF-8") || !has(env, "MYSQL_PWD=pw") {
		t.Errorf("env = %q", env)
	}
	if env := mydumperEnv([]string{"PATH=/bin", "LANG=es_AR.UTF-8"}, ""); has(env, "LC_ALL=C.UTF-8") {
		t.Errorf("an operator's locale was overridden: %q", env)
	}
	if env := mydumperEnv([]string{"PATH=/bin", "LANG="}, ""); !has(env, "LC_ALL=C.UTF-8") {
		t.Errorf("an empty LANG is no locale: %q", env)
	}
}
