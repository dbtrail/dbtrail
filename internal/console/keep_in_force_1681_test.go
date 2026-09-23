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
			raw, body := retentionFields(t, srv, row.ID)
			listing := 0
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
