package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2675 point 5: two servers on one S3 prefix are not pruned by age either,
// and the settings row says so: no age in force, the reason set, and the
// record of what S3 lacks (which no prune refreshes for that folder any
// more) not read.
func TestBackupSettings_aSharedPrefixHasNoAgeInForce(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	add := func(e ServerEntry) {
		t.Helper()
		e.DSN = "u:p@tcp(h:3306)/" + e.Name
		if err := os.MkdirAll(e.BaselineDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	one, two, own := filepath.Join(state, "one"), filepath.Join(state, "two"), filepath.Join(state, "own")
	add(ServerEntry{Name: "one", BaselineDir: one, BaselineS3: "s3://b/shared/"})
	add(ServerEntry{Name: "two", BaselineDir: two, BaselineS3: "s3://b/shared"})
	add(ServerEntry{Name: "own", BaselineDir: own, BaselineS3: "s3://b/own/"})
	if err := reg.SetBackupSetting(BackupSettingBaselineRetain, ptr("2d")); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(baseline.NotInDestination{At: time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC), Count: 3})
	for _, d := range []string{one, own} {
		if err := os.WriteFile(filepath.Join(d, baseline.NotInDestinationFile), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	srv := retentionServer(t, path, true)
	rec, body := doServersReq(t, srv, "GET", "/api/backup-settings", "")
	if rec.Code != 200 {
		t.Fatalf("code %d: %s", rec.Code, body)
	}
	var got struct {
		Servers []struct {
			Name                string
			DeleteAfterMinutes  int         `json:"delete_after_minutes"`
			DeleteAfterSharedS3 bool        `json:"delete_after_shared_s3"`
			KeepSharedS3        bool        `json:"keep_shared_s3"`
			NotInS3             *notInS3DTO `json:"not_in_s3"`
		}
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, r := range got.Servers {
		switch r.Name {
		case "one", "two":
			seen++
			if r.DeleteAfterMinutes != 0 || !r.DeleteAfterSharedS3 || !r.KeepSharedS3 || r.NotInS3 != nil {
				t.Errorf("%s, on a shared prefix: %+v", r.Name, r)
			}
		case "own":
			seen++
			if r.DeleteAfterMinutes != 2*24*60 || r.DeleteAfterSharedS3 || r.NotInS3 == nil || r.NotInS3.Count != 3 {
				t.Errorf("own, on a prefix of its own: %+v", r)
			}
		}
	}
	if seen != 3 {
		t.Fatalf("saw %d of 3 servers: %s", seen, body)
	}
}

// AgePruneSharedS3 on its own: the cases the plan relies on.
func TestAgePruneSharedS3(t *testing.T) {
	root := t.TempDir()
	a, b, g := filepath.Join(root, "a"), filepath.Join(root, "b"), filepath.Join(root, "g")
	for _, d := range []string{a, b, g} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(root, "a-link")
	if err := os.Symlink(a, link); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name               string
		entries            []ServerEntry
		dir, s3, gDir, gS3 string
		want               bool
	}{
		{"no destination", []ServerEntry{{ID: "x", BaselineDir: b}}, a, "", "", "", false},
		{"blank destination", []ServerEntry{{ID: "x", BaselineDir: b, BaselineS3: "  "}}, a, "  ", "", "", false},
		{"itself only", []ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: "s3://k/p/"}}, a, "s3://k/p/", "", "", false},
		{"another folder, trailing slash", []ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: "s3://k/p/"}, {ID: "b", BaselineDir: b, BaselineS3: "s3://k/p"}}, a, "s3://k/p/", "", "", true},
		{"another folder, spaces", []ServerEntry{{ID: "b", BaselineDir: b, BaselineS3: " s3://k/p "}}, a, "s3://k/p/", "", "", true},
		{"nested prefix", []ServerEntry{{ID: "b", BaselineDir: b, BaselineS3: "s3://k/p/sub/"}}, a, "s3://k/p/", "", "", false},
		{"parent prefix", []ServerEntry{{ID: "b", BaselineDir: b, BaselineS3: "s3://k/p/"}}, a, "s3://k/p/sub", "", "", false},
		{"other case", []ServerEntry{{ID: "b", BaselineDir: b, BaselineS3: "s3://k/P/"}}, a, "s3://k/p/", "", "", false},
		{"no folder, same prefix", []ServerEntry{{ID: "b", BaselineS3: "s3://k/p/"}}, a, "s3://k/p/", "", "", true},
		{"same folder through a link", []ServerEntry{{ID: "b", BaselineDir: link, BaselineS3: "s3://k/p/"}}, a, "s3://k/p/", "", "", false},
		{"daemon's prefix, daemon's folder elsewhere", nil, a, "s3://k/p/", g, "s3://k/p", true},
		{"daemon's prefix, no daemon folder", nil, a, "s3://k/p/", "", "s3://k/p/", true},
		{"daemon's own pair", []ServerEntry{{ID: "m", BaselineDir: g, BaselineS3: "s3://k/p/"}}, g, "s3://k/p/", g, "s3://k/p/", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := AgePruneSharedS3(c.entries, c.dir, c.s3, c.gDir, c.gS3); got != c.want {
				t.Errorf("AgePruneSharedS3 = %v, want %v", got, c.want)
			}
		})
	}
}
