package console

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// The edge cases of #1684, written before the migration: each server that
// read its snapshots through the process-wide --baseline-dir/--baseline-s3
// gets that value written into its own entry, once, and nothing else moves.

// writeRegistryFile writes body as the registry file and returns its path.
func writeRegistryFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func loadReg(t *testing.T, path string) *Registry {
	t.Helper()
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func mustGet(t *testing.T, r *Registry, id string) ServerEntry {
	t.Helper()
	e, ok := r.Get(id)
	if !ok {
		t.Fatalf("server %s is gone", id)
	}
	return e
}

// oldResolution is what the read paths resolved before #1684, kept here as
// the reference the migration must reproduce: all or nothing, untrimmed.
func oldResolution(e ServerEntry, dir, s3 string) (string, string) {
	if e.BaselineDir == "" && e.BaselineS3 == "" {
		return dir, s3
	}
	return e.BaselineDir, e.BaselineS3
}

// Every entry shape against every shape of the process default: after the
// migration each server's OWN values are what the fallback resolved to.
// Edge case 2 is the row "s3 only" with a default folder: the folder is NOT
// written into it, because today it reads its own bucket and nothing else.
func TestLocationMigration_preservesTodaysResolution(t *testing.T) {
	entries := []ServerEntry{
		{ID: "0000000000000001", Name: "none"},
		{ID: "0000000000000002", Name: "dir", BaselineDir: "/own/dir"},
		{ID: "0000000000000003", Name: "s3", BaselineS3: "s3://own/p/"},
		{ID: "0000000000000004", Name: "both", BaselineDir: "/own/d", BaselineS3: "s3://own/q/"},
		{ID: "0000000000000005", Name: "spaces", BaselineDir: "  "},
	}
	defaults := []struct{ dir, s3 string }{
		{"", ""},
		{"/proc/dir", ""},
		{"", "s3://proc/b/"},
		{"/proc/dir", "s3://proc/b/"},
		{" /proc/padded ", ""},
	}
	for _, d := range defaults {
		t.Run("dir="+d.dir+",s3="+d.s3, func(t *testing.T) {
			clearStores(t)
			r := loadReg(t, "")
			r.file.Servers = slices.Clone(entries)
			rep := r.MigrateProcessBaselineLocation(d.dir, d.s3)
			var want []string
			for _, e := range entries {
				got := mustGet(t, r, e.ID)
				wd, ws := oldResolution(e, d.dir, d.s3)
				if got.BaselineDir != wd || got.BaselineS3 != ws {
					t.Errorf("%s: own = (%q, %q), resolved before = (%q, %q)", e.Name, got.BaselineDir, got.BaselineS3, wd, ws)
				}
				if e.BaselineDir == "" && e.BaselineS3 == "" && (d.dir != "" || d.s3 != "") {
					want = append(want, e.Name)
				}
			}
			if !slices.Equal(rep.Migrated, want) {
				t.Errorf("migrated = %v, want %v", rep.Migrated, want)
			}
			if rep.NotSaved != "" {
				t.Errorf("an in-memory registry reports %q", rep.NotSaved)
			}
		})
	}
}

// Edge case 1: two servers reading the same default both get it.
// Edge case 5: a second start changes nothing, byte for byte.
func TestLocationMigration_twoServersThenSecondStart(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, `version: 1
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
  - id: aaaaaaaaaaaaaaa2
    name: two
    index_dsn: u:p@tcp(h:3306)/two
`)
	dir := t.TempDir()
	r := loadReg(t, path)
	rep := r.MigrateProcessBaselineLocation(dir, "s3://proc/b/")
	if !slices.Equal(rep.Migrated, []string{"one", "two"}) || rep.NotSaved != "" {
		t.Fatalf("report = %+v", rep)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r2 := loadReg(t, path)
	for _, id := range []string{"aaaaaaaaaaaaaaa1", "aaaaaaaaaaaaaaa2"} {
		if e := mustGet(t, r2, id); e.BaselineDir != dir || e.BaselineS3 != "s3://proc/b/" {
			t.Errorf("%s after reload: (%q, %q)", id, e.BaselineDir, e.BaselineS3)
		}
	}
	st, _ := os.Stat(path)
	time.Sleep(20 * time.Millisecond)
	rep2 := r2.MigrateProcessBaselineLocation(dir, "s3://proc/b/")
	if len(rep2.Migrated) != 0 || !rep2.Done {
		t.Errorf("second start report = %+v, want nothing migrated and done", rep2)
	}
	second, _ := os.ReadFile(path)
	if string(first) != string(second) {
		t.Errorf("the second start rewrote the file:\n%s\n---\n%s", first, second)
	}
	if st2, _ := os.Stat(path); !st2.ModTime().Equal(st.ModTime()) {
		t.Error("the second start saved the file again")
	}
}

// Idempotency against the operator: a location cleared (or changed) after
// the migration survives a restart with the same flags.
func TestLocationMigration_neverOverwritesWhatTheOperatorSetSince(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, `version: 1
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
  - id: aaaaaaaaaaaaaaa2
    name: two
    index_dsn: u:p@tcp(h:3306)/two
`)
	r := loadReg(t, path)
	r.MigrateProcessBaselineLocation("/proc/dir", "")
	one := mustGet(t, r, "aaaaaaaaaaaaaaa1")
	one.BaselineDir = ""
	one.BaselineS3 = ""
	if err := r.Update(one); err != nil {
		t.Fatal(err)
	}
	two := mustGet(t, r, "aaaaaaaaaaaaaaa2")
	two.BaselineDir = "/elsewhere"
	if err := r.Update(two); err != nil {
		t.Fatal(err)
	}
	r2 := loadReg(t, path)
	rep := r2.MigrateProcessBaselineLocation("/proc/dir", "")
	if len(rep.Migrated) != 0 {
		t.Errorf("migrated again: %v", rep.Migrated)
	}
	if e := mustGet(t, r2, "aaaaaaaaaaaaaaa1"); e.BaselineDir != "" || e.BaselineS3 != "" {
		t.Errorf("a cleared location came back: (%q, %q)", e.BaselineDir, e.BaselineS3)
	}
	if e := mustGet(t, r2, "aaaaaaaaaaaaaaa2"); e.BaselineDir != "/elsewhere" {
		t.Errorf("a changed location was overwritten: %q", e.BaselineDir)
	}
}

// A start with no process location writes no file and records nothing: a
// start that merely lacked the flag (serve run without --baseline-dir, a
// missing variable) must not cancel the migration of a later start that has
// it, even when an edit in between saved the file.
func TestLocationMigration_aStartWithoutTheFlagRecordsNothing(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, `version: 1
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
`)
	before, _ := os.ReadFile(path)
	r := loadReg(t, path)
	rep := r.MigrateProcessBaselineLocation("", "")
	if len(rep.Migrated) != 0 || rep.NotSaved != "" || rep.Done {
		t.Fatalf("report = %+v", rep)
	}
	if after, _ := os.ReadFile(path); string(after) != string(before) {
		t.Fatalf("a start with nothing to migrate rewrote the file:\n%s", after)
	}
	// An unrelated edit saves the file.
	if _, err := r.Add(ServerEntry{Name: "two", DSN: "u:p@tcp(h:3306)/two"}); err != nil {
		t.Fatal(err)
	}
	if saved, _ := os.ReadFile(path); strings.Contains(string(saved), "baseline_location_migrated") {
		t.Fatalf("the flagless start recorded the migration:\n%s", saved)
	}
	r2 := loadReg(t, path)
	if rep := r2.MigrateProcessBaselineLocation("/proc/dir", ""); !slices.Equal(rep.Migrated, []string{"one", "two"}) {
		t.Errorf("the start with the flag migrated %v, want both", rep.Migrated)
	}
	if e := mustGet(t, r2, "aaaaaaaaaaaaaaa1"); e.BaselineDir != "/proc/dir" {
		t.Errorf("one = %q", e.BaselineDir)
	}
}

// No registry file and nothing to migrate: no file is created.
func TestLocationMigration_noFileIsCreated(t *testing.T) {
	clearStores(t)
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	for _, d := range [][2]string{{"", ""}, {"/proc/dir", ""}} {
		r := loadReg(t, path)
		r.MigrateProcessBaselineLocation(d[0], d[1])
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("defaults %v: a registry file was created with no server to migrate: %v", d, err)
		}
	}
}

// Edge case 3a: a registry written by a newer version is not migrated on
// disk and says so; this process keeps reading what it read yesterday.
func TestLocationMigration_newerVersionIsNotWritten(t *testing.T) {
	clearStores(t)
	body := `version: 99
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
`
	path := writeRegistryFile(t, body)
	r := loadReg(t, path)
	rep := r.MigrateProcessBaselineLocation("/proc/dir", "")
	if !strings.Contains(rep.NotSaved, "newer") {
		t.Errorf("NotSaved = %q, want it to say the file is from a newer version", rep.NotSaved)
	}
	if !slices.Equal(rep.Migrated, []string{"one"}) {
		t.Errorf("migrated = %v", rep.Migrated)
	}
	if got, _ := os.ReadFile(path); string(got) != body {
		t.Errorf("a newer-version file was rewritten:\n%s", got)
	}
	if e := mustGet(t, r, "aaaaaaaaaaaaaaa1"); e.BaselineDir != "/proc/dir" {
		t.Errorf("this process lost the location it reads: %q", e.BaselineDir)
	}
	if got := r.LocationMigration(); got.NotSaved != rep.NotSaved {
		t.Errorf("the stored report = %+v", got)
	}
}

// Edge case 3b: a file that cannot be written. Same answer: said, not
// saved, and the reads of this process keep working.
func TestLocationMigration_unwritableFileSaysSo(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes through a read-only directory")
	}
	clearStores(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "console-servers.yaml")
	body := `version: 1
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	r := loadReg(t, path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	rep := r.MigrateProcessBaselineLocation("/proc/dir", "s3://proc/b/")
	if rep.NotSaved == "" {
		t.Fatal("a failed save was not reported")
	}
	if got, _ := os.ReadFile(path); string(got) != body {
		t.Errorf("the file changed:\n%s", got)
	}
	if e := mustGet(t, r, "aaaaaaaaaaaaaaa1"); e.BaselineDir != "/proc/dir" || e.BaselineS3 != "s3://proc/b/" {
		t.Errorf("this process lost the location it reads: (%q, %q)", e.BaselineDir, e.BaselineS3)
	}
	// Once the file can be written again, the next save carries it.
	_ = os.Chmod(dir, 0o700)
	if err := r.SetRotation(RotationConfig{Retain: "7d", Interval: "1h"}); err != nil {
		t.Fatal(err)
	}
	if got := r.LocationMigration(); got.NotSaved != "" {
		t.Errorf("after a save that holds it, the page is still told it was not saved: %q", got.NotSaved)
	}
	r2 := loadReg(t, path)
	if e := mustGet(t, r2, "aaaaaaaaaaaaaaa1"); e.BaselineDir != "/proc/dir" {
		t.Errorf("the later save did not carry the migration: %q", e.BaselineDir)
	}
	if rep := r2.MigrateProcessBaselineLocation("/proc/dir", "s3://proc/b/"); len(rep.Migrated) != 0 {
		t.Errorf("migrated twice: %v", rep.Migrated)
	}
}

// Edge case 6: a default folder that does not exist today is written as it
// is (that is what the server reads today, and a mount can come back), is
// never created, and is named in the report.
func TestLocationMigration_missingFolderIsWrittenNotCreated(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, `version: 1
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
`)
	gone := filepath.Join(t.TempDir(), "unmounted", "baselines")
	r := loadReg(t, path)
	rep := r.MigrateProcessBaselineLocation(gone, "")
	if e := mustGet(t, r, "aaaaaaaaaaaaaaa1"); e.BaselineDir != gone {
		t.Errorf("dir = %q, want %q", e.BaselineDir, gone)
	}
	if _, err := os.Stat(gone); !os.IsNotExist(err) {
		t.Errorf("the migration created the folder: %v", err)
	}
	if !slices.Equal(rep.MissingDir, []string{"one"}) {
		t.Errorf("missing = %v, want [one]", rep.MissingDir)
	}

	// A file where the folder should be is named the same way.
	file := filepath.Join(t.TempDir(), "not-a-folder")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	path2 := writeRegistryFile(t, "version: 1\nservers:\n  - id: aaaaaaaaaaaaaaa1\n    name: one\n    index_dsn: u:p@tcp(h:3306)/one\n")
	if rep := loadReg(t, path2).MigrateProcessBaselineLocation(file, ""); !slices.Equal(rep.MissingDir, []string{"one"}) {
		t.Errorf("not a directory: missing = %v, want [one]", rep.MissingDir)
	}
}

// Everything else on the entry stays: the fields this build knows and the
// ones a newer one wrote.
func TestLocationMigration_keepsTheRestOfTheEntry(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, `version: 1
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
    no_archive: true
    local_keep_newest: 3
    backup_schedule:
      every: 1d
      at: "03:00"
    future_field: keep-me
`)
	r := loadReg(t, path)
	r.MigrateProcessBaselineLocation("/proc/dir", "")
	r2 := loadReg(t, path)
	e := mustGet(t, r2, "aaaaaaaaaaaaaaa1")
	// The keep count is reset on a server given a folder (see
	// TestLocationMigration_resetsTheKeepCount); everything else stays.
	if !e.NoArchive || e.LocalKeepNewest != 0 || e.BackupSchedule == nil || e.BackupSchedule.Every != "1d" || e.Extra["future_field"] != "keep-me" || e.DSN != "u:p@tcp(h:3306)/one" {
		t.Errorf("the entry lost fields: %+v", e)
	}
}

// Edge case 4: the command-line entry lives in no file. The migration never
// adds it, and its bundle reads the flags exactly as before.
func TestLocationMigration_neverTouchesTheCommandLineEntry(t *testing.T) {
	clearStores(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "console-servers.yaml")
	r := loadReg(t, path)
	r.MigrateProcessBaselineLocation("/proc/dir", "s3://proc/b/")
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: r,
		BaselineDir: "/proc/dir", BaselineS3: "s3://proc/b/"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the registry file was written for a command-line entry: %v", err)
	}
	if r.Len() != 0 {
		t.Errorf("the registry holds %d entries", r.Len())
	}
	boot, _ := srv.cm.bootInfo()
	if boot == nil || boot.baselineSrc != "/proc/dir" || boot.baselineFallbackSrc != "s3://proc/b/" {
		t.Errorf("the command-line bundle changed: %+v", boot)
	}
}

// A server with its own S3 store and no Snapshots location, migrated onto
// the daemon's bucket: that bucket keeps being read with the process-wide
// endpoint, as it was before, and the server's own bucket keeps its store.
func TestLocationMigration_daemonBucketKeepsItsRouting(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, `version: 1
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
    archive_s3: s3://own-archive/a/
    s3_endpoint: http://minio:9000
`)
	r := loadReg(t, path)
	r.SetProcessS3Location(DaemonBaselineS3Label, "s3://daemon-bkt/b/")
	r.MigrateProcessBaselineLocation("", "s3://daemon-bkt/b/")
	if e := mustGet(t, r, "aaaaaaaaaaaaaaa1"); e.BaselineS3 != "s3://daemon-bkt/b/" {
		t.Fatalf("BaselineS3 = %q", e.BaselineS3)
	}
	if st, ok := storage.BucketStoreFor("daemon-bkt"); ok {
		t.Errorf("the daemon bucket is now routed to %+v; it was read with the ambient endpoint", st)
	}
	if _, ok := storage.BucketStoreFor("own-archive"); !ok {
		t.Error("the server's own bucket lost its store")
	}
	// An edit that does not touch its S3 settings still saves; one that
	// does meets the existing #1575 refusal, which names the fix.
	e := mustGet(t, r, "aaaaaaaaaaaaaaa1")
	e.Name = "renamed"
	if err := r.Update(e); err != nil {
		t.Errorf("a rename of the migrated server was refused: %v", err)
	}
	e.S3Region = "eu-west-1"
	if err := r.Update(e); err == nil || !strings.Contains(err.Error(), "use another bucket") {
		t.Errorf("a store edit over the daemon bucket: %v", err)
	}
}
