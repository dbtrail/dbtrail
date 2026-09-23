package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The settings row's "how far back you can go" line uses keep_in_force, and
// the Snapshots listing's retention line uses local_retention.keep_newest
// (#1681). They must be the same number for every kind of server, or the
// page says two things about one folder. Both through their real handlers.
func TestKeepInForce_isTheListingsNumber(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	add := func(e ServerEntry) {
		t.Helper()
		e.DSN = "u:p@tcp(h:3306)/" + e.Name
		if e.BaselineDir != "" {
			if err := os.MkdirAll(e.BaselineDir, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := reg.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	add(ServerEntry{Name: "solo", BaselineDir: filepath.Join(state, "solo"), LocalKeepNewest: 3})
	add(ServerEntry{Name: "shared1", BaselineDir: filepath.Join(state, "shared"), LocalKeepNewest: 2})
	add(ServerEntry{Name: "shared2", BaselineDir: filepath.Join(state, "shared"), LocalKeepNewest: 4})
	add(ServerEntry{Name: "withs3", BaselineDir: filepath.Join(state, "s3"), BaselineS3: "s3://b/p/", LocalKeepNewest: 3})
	add(ServerEntry{Name: "all", BaselineDir: filepath.Join(state, "all")})
	if err := reg.SetBackupSetting(BackupSettingBaselineRetain, ptr("2d")); err != nil {
		t.Fatal(err)
	}

	for _, loop := range []bool{true, false} {
		srv := retentionServer(t, path, loop)
		rows := backupSettingsGet(t, srv).Servers
		if len(rows) != 5 {
			t.Fatalf("%d rows", len(rows))
		}
		sawCount := false
		for _, row := range rows {
			listing := 0
			if row.BaselineS3 != "" {
				// The listing would also read the bucket, a network call a
				// unit test must not make; the same function answers for it.
				if r := srv.localRetentionOf(row.ID); r != nil {
					listing = r.KeepNewest
				}
				if row.KeepInForce != listing || listing != 0 {
					t.Errorf("loop=%v %s: with a destination keep_in_force = %d, retention = %d; want both 0", loop, row.Name, row.KeepInForce, listing)
				}
				continue
			}
			raw, body := retentionFields(t, srv, row.ID)
			if lr, ok := raw["local_retention"]; ok {
				var v localRetentionDTO
				if err := json.Unmarshal(lr, &v); err != nil {
					t.Fatalf("%s: %v in %s", row.Name, err, body)
				}
				listing = v.KeepNewest
			}
			if row.KeepInForce != listing {
				t.Errorf("loop=%v %s: settings keep_in_force = %d, listing local_retention.keep_newest = %d", loop, row.Name, row.KeepInForce, listing)
			}
			if listing > 0 {
				sawCount = true
				// The age retention rides along only where a count is in force.
				if row.PruneRetainMinutes != 2*24*60 {
					t.Errorf("%s: prune_retain_minutes = %d, want %d", row.Name, row.PruneRetainMinutes, 2*24*60)
				}
			} else if row.PruneRetainMinutes != 0 {
				t.Errorf("%s: prune_retain_minutes = %d with no count in force", row.Name, row.PruneRetainMinutes)
			}
		}
		// The loop's own premise: one server counts where a loop runs, none where it does not.
		if sawCount != loop {
			t.Errorf("loop=%v: a count in force = %v", loop, sawCount)
		}
	}
}

func ptr(s string) *string { return &s }

// How often a server gets a snapshot on its own is the SHORTER of its
// schedule and the daemon-wide refresh loop, each only where it runs for that
// server: a daily schedule under an hourly refresh leaves hours, not days,
// and the reach line must not promise days.
func TestSnapshotEveryMinutes_isTheShorterLoopThatRuns(t *testing.T) {
	state := t.TempDir()
	reg, err := LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	add := func(name string, sched *BackupSchedule, source string) string {
		t.Helper()
		dir := filepath.Join(state, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		e, err := reg.Add(ServerEntry{Name: name, DSN: "u:p@tcp(h:3306)/" + name, SourceDSN: source,
			BaselineDir: dir, LocalKeepNewest: 3, BackupSchedule: sched})
		if err != nil {
			t.Fatal(err)
		}
		return e.ID
	}
	src := "src:pw@tcp(127.0.0.1:3306)/"
	daily := add("daily", &BackupSchedule{Every: "1d", At: "03:00"}, src)
	fiveMin := add("fivemin", &BackupSchedule{Every: "5m"}, src)
	refused := add("refused", &BackupSchedule{Every: "soon"}, src) // unreadable: the schedule cannot run
	bare := add("bare", nil, "")

	for _, tc := range []struct {
		name     string
		refresh  string
		sched    bool
		readOnly bool
		want     map[string]int
	}{
		{"schedules only", "", true, false, map[string]int{daily: 1440, fiveMin: 5, refused: 0, bare: 0}},
		{"hourly refresh too", "1h", true, false, map[string]int{daily: 60, fiveMin: 5, refused: 60, bare: 60}},
		{"no schedule loop", "", false, false, map[string]int{daily: 0, fiveMin: 0, refused: 0, bare: 0}},
		// A loop that parses the schedule but cannot run it here (no
		// monitor = a read-only console) takes no snapshots either.
		{"schedules refused here", "", true, true, map[string]int{daily: 0, fiveMin: 0, refused: 0, bare: 0}},
	} {
		cfg := Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, LocalPruneLoop: true,
			BackupSettingsDefaults: BackupSettingsDefaults{RefreshEvery: tc.refresh}}
		if !tc.readOnly {
			cfg.MonitorCtrl = &stubMonitorCtrl{}
		}
		if tc.sched {
			cfg.BackupSchedules = &stubScheduleReporter{full: true}
		}
		srv, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range backupSettingsGet(t, srv).Servers {
			if row.KeepInForce != 3 {
				t.Fatalf("%s/%s: test premise: keep_in_force = %d", tc.name, row.Name, row.KeepInForce)
			}
			if got := row.SnapshotEveryMinutes; got != tc.want[row.ID] {
				t.Errorf("%s/%s: snapshot_every_minutes = %d, want %d", tc.name, row.Name, got, tc.want[row.ID])
			}
		}
		// The refresh loop skips a server with no local folder
		// (baselineRefreshTargets), so it gives that server no cadence.
		if got := srv.snapshotEveryMinutes(ServerEntry{Name: "s3only", DSN: "u:p@tcp(h:3306)/x", BaselineS3: "s3://b/p/"}); got != 0 {
			t.Errorf("%s: a server with no folder gets a refresh cadence of %d", tc.name, got)
		}
	}
}
