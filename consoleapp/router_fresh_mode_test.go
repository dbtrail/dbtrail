package consoleapp

import (
	"os"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/internal/console"
)

// Fast mode is the default (answers from each table's last snapshot); the
// fresh mode lays the newest captured changes over it. The router says which
// at startup and in its status document, and the compose file passes
// SQL_FRESH through.

func TestRouterFreshnessLine(t *testing.T) {
	for _, c := range []struct {
		env, want, not string
	}{
		{"", "Router: fast mode: answers come from each table's last snapshot", "is not on or off"},
		{"off", "Router: fast mode: answers come from", "is not on or off"},
		{"onn", `Router: fast mode (BINTRAIL_SQL_TAILS="onn" is not on or off, and is read as off)`, "fresh mode"},
		{"on", "Router: fresh mode (BINTRAIL_SQL_TAILS=on): each table's newest captured changes", "Router: fast mode"},
		{" Fresh ", "Router: fresh mode (BINTRAIL_SQL_TAILS=Fresh)", "Router: fast mode"},
	} {
		t.Setenv("BINTRAIL_SQL_TAILS", c.env)
		got := routerFreshnessLine(console.TailsSetting())
		if !strings.Contains(got, c.want) || strings.Contains(got, c.not) || !strings.HasSuffix(got, ".\n") {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: %q, want %q", c.env, got, c.want)
		}
	}
}

func TestRouterStatus_saysTheMode(t *testing.T) {
	for env, want := range map[string]bool{"": false, "on": true} {
		t.Setenv("BINTRAIL_SQL_TAILS", env)
		doc := routerStatus(routerSettings{}, console.RouterNumbers{}, &routerCopySnapshots{})
		if doc.Fresh == nil || *doc.Fresh != want {
			t.Errorf("BINTRAIL_SQL_TAILS=%q: fresh %v, want %v", env, doc.Fresh, want)
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
