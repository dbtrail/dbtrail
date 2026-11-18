package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #2671: where the router's own mode setting comes from, when the option
// file or an env file sets it, is named on the card and on the router's
// numbers; when the environment sets it (the compose .env's SQL_FRESH), the
// sentences are the ones they were.
func TestSQLModeForcedPlace_2671(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const model = vm.runInContext("sqlModeModel", ctx);
const where = vm.runInContext("routerModeWhere", ctx);
const cur = { id: "a", name: "shop", sql_fresh: false, sql_fresh_saved: true };
const rt = (forced, from) => ({ configured: true, status: { fresh: forced === "fresh", per_server_mode: true, fresh_by_default: true, sql_mode_forced: forced, sql_mode_forced_from: from, servers: { a: { sql_mode: forced, sql_mode_from: "environment" } } } });
const out = {
  freshFile: model(cur, rt("fresh", "line 4 of /etc/dbtrail/dbtrail.cnf (sql-tails)"), true).state,
  fastEnvFile: model(cur, rt("fast", "the env file /srv/dbtrail/.bintrail.env (BINTRAIL_SQL_TAILS)"), true).state,
  freshEnv: model(cur, rt("fresh", "the environment variable BINTRAIL_SQL_TAILS"), true).state,
  freshOld: model(cur, rt("fresh", undefined), true).state,
  whereFile: where({ sql_mode_forced_from: "line 4 of /etc/dbtrail/dbtrail.cnf (sql-tails)" }),
  whereEnv: where({ sql_mode_forced_from: "the environment variable BINTRAIL_SQL_TAILS" }),
  whereNone: where({}),
};
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "place.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]string
	s := strings.TrimSpace(string(raw))
	if err := json.Unmarshal([]byte(s[strings.LastIndex(s, "\n")+1:]), &got); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	compose := "it is set where the router starts (SQL_FRESH=on in the compose .env, or BINTRAIL_SQL_TAILS=on). For this server alone: Fast, which applies once that setting is removed (then docker compose up -d router)."
	for k, want := range map[string]string{
		"freshFile":   "Fresh mode, for every server: it is set where the router starts, by line 4 of /etc/dbtrail/dbtrail.cnf (sql-tails). For this server alone: Fast, which applies once that line is removed and the router restarted.",
		"fastEnvFile": "Fast mode, for every server: it is set where the router starts, by the env file /srv/dbtrail/.bintrail.env (BINTRAIL_SQL_TAILS). For this server alone: Fast, which applies once that line is removed and the router restarted.",
		"freshEnv":    "Fresh mode, for every server: " + compose,
		"freshOld":    "Fresh mode, for every server: " + compose,
		"whereFile":   " It is set where the router starts, by line 4 of /etc/dbtrail/dbtrail.cnf (sql-tails): remove that line and restart the router to choose each server's mode on its Snapshots page.",
		"whereEnv":    " It is set where the router starts: remove SQL_FRESH from the compose .env and run docker compose up -d router to choose each server's mode on its Snapshots page.",
		"whereNone":   " It is set where the router starts: remove SQL_FRESH from the compose .env and run docker compose up -d router to choose each server's mode on its Snapshots page.",
	} {
		if !strings.HasPrefix(got[k], want) {
			t.Errorf("%s:\n got %q\nwant %q", k, got[k], want)
		}
	}
}

// The SQL memory's lock names where the startup value came from.
func TestSQLMemoryLockNamesWhere_2671(t *testing.T) {
	got := sqlMemoryLockedReason(sqlMemorySnap{startup: true, from: "line 3 of /etc/dbtrail/dbtrail.cnf (console-sql-memory)"})
	want := "It is set where DBTrail starts, by line 3 of /etc/dbtrail/dbtrail.cnf (console-sql-memory), so it is changed there. Remove it there and restart DBTrail to change it here."
	if got != want {
		t.Fatalf("got %q", got)
	}
	// Not known: the sentence it always was.
	if got := sqlMemoryLockedReason(sqlMemorySnap{startup: true}); !strings.Contains(got, "CLI: --sql-memory, or the environment variable BINTRAIL_CONSOLE_SQL_MEMORY") {
		t.Fatalf("got %q", got)
	}
	// Not locked: nothing.
	if got := sqlMemoryLockedReason(sqlMemorySnap{path: "/x"}); got != "" {
		t.Fatalf("got %q", got)
	}
}

// The telemetry card is told where BINTRAIL_TELEMETRY came from when it is
// what decides, and nothing when something else does.
func TestTelemetryDecidedWhere_2671(t *testing.T) {
	t.Setenv("BINTRAIL_TELEMETRY_DIR", t.TempDir())
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv("BINTRAIL_TELEMETRY", "off")
	s := &Server{telemetryFrom: "line 2 of /etc/dbtrail/dbtrail.cnf (telemetry)"}
	if got := s.telemetryState(); got.DecidedBy != "BINTRAIL_TELEMETRY" || got.DecidedWhere != "line 2 of /etc/dbtrail/dbtrail.cnf (telemetry)" {
		t.Fatalf("decided by the variable: %+v", got)
	}
	t.Setenv("BINTRAIL_TELEMETRY", "")
	if got := s.telemetryState(); got.DecidedWhere != "" {
		t.Fatalf("not decided by the variable: %+v", got)
	}
}

// A saved rotation policy says what it is used instead of, and where that is
// set; the dialog says what is in use when nothing is saved.
func TestRotationStartupWords_2671(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const words = vm.runInContext("rotationStartupWords", ctx);
const line = vm.runInContext("rotationOverriddenLine", ctx);
const from = { retain: "line 3 of /etc/dbtrail/dbtrail.cnf (rotate-retain)", add_future: "--rotate-add-future" };
const startup = { retain: "6h", add_future: "5" };
console.log(JSON.stringify({
  words: words({ startup_from: from, startup }),
  override: line({ source: "override", startup_from: from, startup }),
  overrideNothingGiven: line({ source: "override" }),
  notSaved: line({ source: "default", startup_from: from, startup }),
}));
`
	path := filepath.Join(t.TempDir(), "rot.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]string
	s := strings.TrimSpace(string(raw))
	if err := json.Unmarshal([]byte(s[strings.LastIndex(s, "\n")+1:]), &got); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	w := "retention 6h, by line 3 of /etc/dbtrail/dbtrail.cnf (rotate-retain); future partitions 5, by --rotate-add-future"
	if got["words"] != w || got["override"] != w+". The policy saved here is used instead of these." ||
		got["overrideNothingGiven"] != "" || got["notSaved"] != "" {
		t.Fatalf("%+v", got)
	}
}

// The rotation answer carries where each given startup value came from, and
// that value.
func TestRotationWithOrigins_2671(t *testing.T) {
	d := RotationDefaults{Retain: "6h", Interval: "1h", AddFuture: 3, From: map[string]string{"retain": "--rotate-retain"}}
	got := d.withOrigins(rotationDTO{Retain: "7d", Source: "override"})
	if got.Retain != "7d" || got.StartupFrom["retain"] != "--rotate-retain" || got.Startup["retain"] != "6h" || len(got.StartupFrom) != 1 {
		t.Fatalf("%+v", got)
	}
	if got := (RotationDefaults{Retain: "48h"}).withOrigins(rotationDTO{}); got.StartupFrom != nil || got.Startup != nil {
		t.Fatalf("nothing given: %+v", got)
	}
}
