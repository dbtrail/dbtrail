package consoleapp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/pgshim"
)

type fakePGModes map[string][2]string

func (f fakePGModes) SQLModeOf(id string) (string, string) {
	m, ok := f[id]
	if !ok {
		return "fast", "default"
	}
	return m[0], m[1]
}

func (f fakePGModes) PostgresServerIDs() []string {
	var ids []string
	for id := range f {
		ids = append(ids, id)
	}
	return ids
}

// The PostgreSQL router's status says each server's mode, every PostgreSQL
// server whether or not a statement reached it, in the MySQL router's
// fields, so the web interface reads both the same way.
func TestPGRouterStatusSaysEachServersMode(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	r := newPGRouteStats(time.Now(), false)
	r.modes = fakePGModes{"fresh1": {"fresh", "default"}, "fast1": {"fast", "saved"}}
	r.note("fresh1", pgshim.RouteNote{Statement: "SELECT 1", Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	r.note("other", pgshim.RouteNote{Statement: "SELECT 1", Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	d := r.doc("127.0.0.1:5433")
	if !d.PerServerMode || !d.FreshByDefault || d.Fresh == nil || *d.Fresh || d.SQLModeForced != "" {
		t.Fatalf("document fields: %+v", d)
	}
	for id, want := range map[string][2]string{"fresh1": {"fresh", "default"}, "fast1": {"fast", "saved"}, "other": {"fast", "default"}} {
		n, ok := d.Servers[id]
		if !ok || n.SQLMode != want[0] || n.SQLModeFrom != want[1] {
			t.Errorf("%s: listed=%v mode=%q from=%q, want %v", id, ok, n.SQLMode, n.SQLModeFrom, want)
		}
	}
	if d.Servers["fresh1"].Copy != 1 {
		t.Errorf("the counts of a listed server were lost: %+v", d.Servers["fresh1"])
	}
	if n := d.Servers["fast1"]; n.Reasons == nil || n.Answers == nil {
		t.Errorf("a server no statement reached has null maps: %+v", n)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"per_server_mode":true`, `"fresh_by_default":true`, `"fresh":false`, `"sql_mode":"fresh"`, `"sql_mode_from":"saved"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the JSON lacks %s: %s", want, b)
		}
	}

	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	if d = r.doc(""); d.Fresh == nil || !*d.Fresh || d.SQLModeForced != "fresh" {
		t.Errorf("every server fresh by the setting: %+v", d)
	}
	t.Setenv("BINTRAIL_SQL_TAILS", "off")
	if d = r.doc(""); d.Fresh == nil || *d.Fresh || d.SQLModeForced != "fast" {
		t.Errorf("every server fast by the setting: %+v", d)
	}
}

// Without the console's modes (the tests that build only the tally), the
// document is as before: no mode fields, no server no statement reached.
func TestPGRouterStatusWithoutModes(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	d := r.doc("")
	if d.PerServerMode || d.Fresh != nil || len(d.Servers) != 0 {
		t.Fatalf("%+v", d)
	}
}
