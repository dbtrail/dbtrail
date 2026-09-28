package consoleapp

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #1684, the daemon's write paths: a snapshot location more than one writer
// resolves to is refused to the periodic refresh, the schedule and the
// compaction. The command-line server keeps its own startup folder.

func TestBaselineRefreshTargets_skipsASharedLocation(t *testing.T) {
	entries := []console.ServerEntry{
		{ID: "a", Name: "a", DSN: "dsn-a", BaselineDir: "/startup"},
		{ID: "b", Name: "b", DSN: "dsn-b", BaselineDir: "/startup/"},
		{ID: "c", Name: "c", DSN: "dsn-c", BaselineDir: "/c"},
		{ID: "d", Name: "d", DSN: "dsn-d", BaselineDir: "/boot"},
	}
	got, _, shared := baselineRefreshTargets(entries, "boot-dsn", "/boot")
	ids := map[string]string{}
	for _, r := range got {
		ids[r.ServerID] = r.BaselineDir
	}
	if len(ids) != 2 || ids["default"] != "/boot" || ids["c"] != "/c" {
		t.Fatalf("targets = %+v, want the command-line server and c only", got)
	}
	why := map[string]string{}
	for _, s := range shared {
		why[s.name] = s.why
	}
	if !strings.Contains(why["a"], "with b,") || !strings.Contains(why["b"], "with a,") ||
		!strings.Contains(why["d"], "the command-line server") || !strings.Contains(why["a"], "own folder or prefix") || len(why) != 3 {
		t.Fatalf("skipped = %+v", shared)
	}
	// Without an index for the command-line server nothing is refreshed for
	// it, so d is alone in its folder and is refreshed.
	got, _, shared = baselineRefreshTargets(entries[2:], "", "/boot")
	if len(got) != 2 || len(shared) != 0 {
		t.Fatalf("no command-line refresh: targets = %+v skipped = %+v", got, shared)
	}
}

// The schedule loop's gates carry the refusal, so a slot for a server on a
// shared location records the refusal instead of writing.
func TestBackupSchedulerGates_refuseASharedLocation(t *testing.T) {
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "a", DSN: "dsn-a", SourceDSN: "s", BaselineDir: "/startup"},
		console.ServerEntry{Name: "b", DSN: "dsn-b", SourceDSN: "s", BaselineDir: "/startup"},
	)
	b := &backupScheduler{sup: newBaselineSupervisor(t.Context(), t.TempDir(), baseline.DefaultLockMode), reg: reg}
	for _, e := range reg.List() {
		if err := console.CheckBackupSchedule(e, console.BackupSchedule{Every: "1d", At: "03:00"}, b.gates()); err == nil ||
			!strings.Contains(console.RefusalReason(err), "own folder or prefix") {
			t.Errorf("%s: schedule = %v, want the shared-location refusal", e.Name, err)
		}
	}
}

// The compaction merges into the folder: refused there too, with the
// refusal logged, never the "busy" line.
func TestTriggerCompact_refusesASharedLocation(t *testing.T) {
	sup, req, cs, _ := compactRig(t, compactMinPairs)
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "s", DSN: "dsn-s", BaselineDir: req.BaselineDir},
		console.ServerEntry{Name: "t", DSN: "dsn-t", BaselineDir: req.BaselineDir},
	)
	sup.reg = reg
	req.ServerID = reg.List()[0].ID
	if err := sup.TriggerCompact(req, nil); !errors.Is(err, console.ErrSharedLocation) {
		t.Fatalf("TriggerCompact = %v, want the shared-location refusal", err)
	}
	sup.maybeCompact(req)
	time.Sleep(50 * time.Millisecond)
	if calls := cs.Calls(); len(calls) != 0 {
		t.Fatalf("merged into a shared folder: %v", calls)
	}
	// Given its own folder, the other server's presence no longer blocks it.
	other := reg.List()[1]
	other.BaselineDir = t.TempDir()
	if err := reg.Update(other); err != nil {
		t.Fatal(err)
	}
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, req.ServerID); st.State != "succeeded" {
		t.Fatalf("alone in its folder: %+v", st)
	}
}

// loadConsoleRegistry counts the command-line server as a writer of the
// startup folder only when this process refreshes it.
func TestLoadConsoleRegistry_countsTheCommandLineWriter(t *testing.T) {
	dir := t.TempDir()
	body := "version: 1\nservers:\n  - id: aaaaaaaaaaaaaaaa\n    name: A\n    index_dsn: u:p@tcp(h:3306)/a\n"
	for _, tc := range []struct {
		cliRefreshes bool
		refused      bool
	}{{false, false}, {true, true}} {
		path := filepath.Join(t.TempDir(), "servers.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		reg, err := loadConsoleRegistry(path, tc.cliRefreshes, dir, "")
		if err != nil {
			t.Fatal(err)
		}
		e := reg.List()[0]
		if e.BaselineDir != dir {
			t.Fatalf("not migrated: %+v", e)
		}
		if err := reg.WriteRefusal(e); (err != nil) != tc.refused {
			t.Errorf("command-line refresh %v: refusal = %v, want refused=%v", tc.cliRefreshes, err, tc.refused)
		}
	}
}
