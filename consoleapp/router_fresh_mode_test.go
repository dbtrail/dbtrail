package consoleapp

import (
	"os"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/internal/console"
)

// Fresh mode is the default (each table's newest captured changes laid over
// its snapshot); fast mode answers from the snapshot alone. BINTRAIL_SQL_TAILS
// decides for every server when it says on or off. The router says who
// decides at startup and in its status document, and the compose file passes
// SQL_FRESH through.

func TestRouterFreshnessLine(t *testing.T) {
	for _, c := range []struct {
		env, want, not string
	}{
		{"", "Router: each server answers in its own mode, fresh unless Fast was saved for it. Fresh:", "is not on or off"},
		{"onn", `Router: each server answers in its own mode, fresh unless Fast was saved for it (BINTRAIL_SQL_TAILS="onn" is not on or off, and decides nothing).`, "for every server (BINTRAIL"},
		{"off", "Router: fast mode for every server (BINTRAIL_SQL_TAILS=off): answers come from each table's last snapshot", "fresh mode for every server"},
		{" Fast ", "Router: fast mode for every server (BINTRAIL_SQL_TAILS=Fast)", "fresh mode for every server"},
		{"on", "Router: fresh mode for every server (BINTRAIL_SQL_TAILS=on): each table's newest captured changes", "fast mode for every server"},
		{" Fresh ", "Router: fresh mode for every server (BINTRAIL_SQL_TAILS=Fresh)", "fast mode for every server"},
	} {
		t.Setenv("BINTRAIL_SQL_TAILS", c.env)
		got := routerFreshnessLine(console.SQLModeSetting())
		if !strings.Contains(got, c.want) || strings.Contains(got, c.not) || !strings.HasSuffix(got, ".\n") {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: %q, want %q", c.env, got, c.want)
		}
	}
}

func TestRouterStatus_saysTheMode(t *testing.T) {
	for _, c := range []struct {
		env    string
		fresh  bool
		forced string
	}{{"", false, ""}, {"on", true, "fresh"}, {"off", false, "fast"}, {"onn", false, ""}} {
		t.Setenv("BINTRAIL_SQL_TAILS", c.env)
		doc := routerStatus(routerSettings{}, console.RouterNumbers{}, &routerCopySnapshots{})
		if doc.Fresh == nil || *doc.Fresh != c.fresh || doc.SQLModeForced != c.forced {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: fresh %v forced %q, want %v %q", c.env, doc.Fresh, doc.SQLModeForced, c.fresh, c.forced)
		}
		if !doc.FreshByDefault || !doc.PerServerMode {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: fresh_by_default %v per_server_mode %v, want both", c.env, doc.FreshByDefault, doc.PerServerMode)
		}
	}
}

func TestCompose_passesSQLFreshToTheRouter(t *testing.T) {
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]struct {
			Environment map[string]any `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if got := doc.Services["router"].Environment["BINTRAIL_SQL_TAILS"]; got != "${SQL_FRESH:-}" {
		t.Errorf("router BINTRAIL_SQL_TAILS = %v, want ${SQL_FRESH:-}", got)
	}
}
