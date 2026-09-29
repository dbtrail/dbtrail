package console

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	yaml "go.yaml.in/yaml/v2"
)

// A server remembers the snapshot locations it used before (#1684): a
// location change keeps the old place readable instead of hiding everything
// in it. These are the registry half of the edge cases the issue lists.

// fixedLocationClock pins locationNow for one test.
func fixedLocationClock(t *testing.T, at time.Time) {
	t.Helper()
	prev := locationNow
	locationNow = func() time.Time { return at }
	t.Cleanup(func() { locationNow = prev })
}

func locationsOf(e ServerEntry) []string {
	var out []string
	for _, p := range e.PreviousBaselineLocations {
		out = append(out, p.Location)
	}
	return out
}

func addLocationServer(t *testing.T, r *Registry, dir, s3 string) ServerEntry {
	t.Helper()
	e, err := r.Add(ServerEntry{Name: "srv-" + filepath.Base(dir) + filepath.Base(s3), DSN: "u:p@tcp(h:3306)/idx", BaselineDir: dir, BaselineS3: s3})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func moveLocation(t *testing.T, r *Registry, id, dir, s3 string) ServerEntry {
	t.Helper()
	e, ok := r.Get(id)
	if !ok {
		t.Fatalf("no server %s", id)
	}
	e.BaselineDir, e.BaselineS3 = dir, s3
	if err := r.Update(e); err != nil {
		t.Fatal(err)
	}
	e, _ = r.Get(id)
	return e
}

// A change records the place left, stamped with when; an unrelated edit
// records nothing.
func TestPreviousLocations_moveRecordsTheOldPlace(t *testing.T) {
	at := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	fixedLocationClock(t, at)
	r, _ := LoadRegistry("")
	e := addLocationServer(t, r, "/data/a", "s3://bkt/a/")
	e = moveLocation(t, r, e.ID, "/data/b", "s3://bkt/a/")
	want := []PreviousLocation{{Location: "/data/a", LeftAt: "2026-09-29T10:00:00Z"}}
	if !slices.Equal(e.PreviousBaselineLocations, want) {
		t.Fatalf("previous = %+v, want %+v", e.PreviousBaselineLocations, want)
	}
	until, ok := e.PreviousBaselineLocations[0].Until()
	if !ok || !until.Equal(at) {
		t.Fatalf("Until = %v %v, want %v", until, ok, at)
	}
	// The bucket moves later: it goes first, the folder stays.
	fixedLocationClock(t, at.Add(time.Hour))
	e = moveLocation(t, r, e.ID, "/data/b", "s3://bkt/b/")
	if got := locationsOf(e); !slices.Equal(got, []string{"s3://bkt/a/", "/data/a"}) {
		t.Fatalf("previous = %v, want the bucket then the folder", got)
	}
	// Clearing the folder leaves it readable too.
	e = moveLocation(t, r, e.ID, "", "s3://bkt/b/")
	if got := locationsOf(e); !slices.Equal(got, []string{"/data/b", "s3://bkt/a/", "/data/a"}) {
		t.Fatalf("previous = %v, want the cleared folder first", got)
	}
	// Renaming the server records nothing.
	e.Name = "renamed"
	if err := r.Update(e); err != nil {
		t.Fatal(err)
	}
	e, _ = r.Get(e.ID)
	if len(e.PreviousBaselineLocations) != 3 {
		t.Fatalf("a rename changed the list: %+v", e.PreviousBaselineLocations)
	}
}

// Edge case 1: the same location saved twice in a row does not grow the list,
// spelled the same or not.
func TestPreviousLocations_sameLocationSavedTwice(t *testing.T) {
	fixedLocationClock(t, time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	r, _ := LoadRegistry("")
	dir := t.TempDir()
	e := addLocationServer(t, r, dir, "s3://bkt/p/")
	for _, spell := range []struct{ dir, s3 string }{
		{dir, "s3://bkt/p/"},
		{dir + "/", "s3://bkt/p"},
		{dir + "//", "s3://bkt/p//"},
		{filepath.Join(dir, "."), "S3://BKT/p/"},
	} {
		e = moveLocation(t, r, e.ID, spell.dir, spell.s3)
		if len(e.PreviousBaselineLocations) != 0 {
			t.Fatalf("saving %q %q again recorded %+v", spell.dir, spell.s3, e.PreviousBaselineLocations)
		}
	}
}

// Edge case 2: moving back to a previous location makes it current and takes
// it off the list; the place left goes on it.
func TestPreviousLocations_moveBack(t *testing.T) {
	fixedLocationClock(t, time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	r, _ := LoadRegistry("")
	e := addLocationServer(t, r, "/data/a", "")
	e = moveLocation(t, r, e.ID, "/data/b", "")
	e = moveLocation(t, r, e.ID, "/data/c", "")
	e = moveLocation(t, r, e.ID, "/data/a/", "")
	if got := locationsOf(e); !slices.Equal(got, []string{"/data/c", "/data/b"}) {
		t.Fatalf("previous = %v, want /data/c then /data/b and never the current /data/a", got)
	}
	// A previous folder taken as the bucket's place is not current as a
	// folder: a folder and a bucket are never one location.
	e = moveLocation(t, r, e.ID, "/data/a", "s3://bkt/x/")
	if got := locationsOf(e); !slices.Equal(got, []string{"/data/c", "/data/b"}) {
		t.Fatalf("previous = %v after adding a bucket", got)
	}
	// Moving the bucket back to a place used before as a bucket.
	e = moveLocation(t, r, e.ID, "/data/a", "s3://bkt/y/")
	e = moveLocation(t, r, e.ID, "/data/a", "s3://bkt/x")
	if got := locationsOf(e); !slices.Equal(got, []string{"s3://bkt/y/", "/data/c", "/data/b"}) {
		t.Fatalf("previous = %v, want the bucket left and never the current one", got)
	}
}

// Edge case 6: trailing slashes, upper and lower case, and the two spellings
// of a prefix are one location. The choices, pinned:
//   - S3: the scheme and the bucket are compared without case (a bucket name
//     has no upper case), the prefix WITH case, because S3 keys are case
//     sensitive and s3://b/Snap and s3://b/snap hold different objects;
//     merging them would hide one of the two histories.
//   - A folder: the same folder on disk is one location, so upper and lower
//     case are one exactly where the filesystem says so (macOS by default),
//     and two on a filesystem that tells them apart.
//   - A folder and a bucket are never one location.
func TestSameSnapshotLocation(t *testing.T) {
	cases := []struct {
		a, b string
		same bool
	}{
		{"s3://bkt/prefix", "s3://bkt/prefix/", true},
		{"s3://bkt/prefix//", "s3://bkt/prefix", true},
		{"S3://BKT/prefix/", "s3://bkt/prefix", true},
		{"s3://bkt/Prefix/", "s3://bkt/prefix/", false},
		{"s3://bkt/prefix/sub", "s3://bkt/prefix", false},
		{"s3://bkt/prefix", "s3://other/prefix", false},
		{"/data/snap", "/data/snap/", true},
		{"/data/snap", "/data/./snap//", true},
		{"/data/snap", "/data/snap2", false},
		{"/bkt/prefix", "s3://bkt/prefix", false},
		{"", "", false},
		{"/data/snap", "", false},
	}
	for _, tc := range cases {
		if got := SameSnapshotLocation(tc.a, tc.b); got != tc.same {
			t.Errorf("SameSnapshotLocation(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.same)
		}
		if got := SameSnapshotLocation(tc.b, tc.a); got != tc.same {
			t.Errorf("SameSnapshotLocation(%q, %q) = %v, want %v", tc.b, tc.a, got, tc.same)
		}
	}
	// Case in a folder name follows the filesystem.
	root := t.TempDir()
	upper := filepath.Join(root, "Snap")
	if err := os.Mkdir(upper, 0o700); err != nil {
		t.Fatal(err)
	}
	lower := filepath.Join(root, "snap")
	_, err := os.Stat(lower)
	caseInsensitive := err == nil
	if got := SameSnapshotLocation(upper, lower); got != caseInsensitive {
		t.Fatalf("SameSnapshotLocation(%q, %q) = %v on a filesystem where the other spelling exists=%v", upper, lower, got, caseInsensitive)
	}
	// A symlink to the folder is the folder.
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(upper, link); err != nil {
		t.Fatal(err)
	}
	if !SameSnapshotLocation(upper, link+"/") {
		t.Fatal("a symlink to the folder is not the folder")
	}
}

// The write guard and the list compare locations with the same rule, or a
// place the guard calls shared would be a place the list calls new.
func TestSameSnapshotLocation_writeGuardAgrees(t *testing.T) {
	a := ServerEntry{ID: "a", Name: "a", BaselineS3: "S3://BKT/p/"}
	b := ServerEntry{ID: "b", Name: "b", BaselineS3: "s3://bkt/p"}
	if got := LocationWriters([]ServerEntry{a, b}, a, CommandLineWriter{}); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("writers = %v, want b: the two spellings are one location", got)
	}
}

// Edge case 7: a registry written by this version and read by an older one
// round-trips the list untouched. The older binary is modeled by its entry
// type: every field it knows, and Extra inline, without the new field.
func TestPreviousLocations_roundTripThroughAnOlderBinary(t *testing.T) {
	fixedLocationClock(t, time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	e := addLocationServer(t, r, "/data/a", "")
	e = moveLocation(t, r, e.ID, "/data/b", "")
	want := e.PreviousBaselineLocations
	if len(want) != 1 {
		t.Fatalf("setup: previous = %+v", want)
	}

	type olderEntry struct {
		ID          string         `yaml:"id"`
		Name        string         `yaml:"name"`
		DSN         string         `yaml:"index_dsn"`
		BaselineDir string         `yaml:"baseline_dir,omitempty"`
		Extra       map[string]any `yaml:",inline"`
	}
	type olderFile struct {
		Version int            `yaml:"version"`
		Servers []olderEntry   `yaml:"servers"`
		Extra   map[string]any `yaml:",inline"`
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var old olderFile
	if err := yaml.Unmarshal(raw, &old); err != nil {
		t.Fatal(err)
	}
	if _, ok := old.Servers[0].Extra["previous_baseline_locations"]; !ok {
		t.Fatalf("the older binary does not carry the list in Extra: %+v", old.Servers[0].Extra)
	}
	// The older binary edits something it knows and saves.
	old.Servers[0].Name = "edited-by-older"
	out, err := yaml.Marshal(&old)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
	r2, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := r2.Get(e.ID)
	if got.Name != "edited-by-older" || !slices.Equal(got.PreviousBaselineLocations, want) {
		t.Fatalf("after the older binary's save: name %q previous %+v, want %+v", got.Name, got.PreviousBaselineLocations, want)
	}
}

// The list belongs to the registry: a caller's value is never taken, on an
// add or on an update.
func TestPreviousLocations_callerCannotSetTheList(t *testing.T) {
	fixedLocationClock(t, time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	r, _ := LoadRegistry("")
	forged := []PreviousLocation{{Location: "/elsewhere", LeftAt: "2030-01-01T00:00:00Z"}}
	e, err := r.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/idx", BaselineDir: "/data/a", PreviousBaselineLocations: forged})
	if err != nil {
		t.Fatal(err)
	}
	if len(e.PreviousBaselineLocations) != 0 {
		t.Fatalf("Add took the caller's list: %+v", e.PreviousBaselineLocations)
	}
	e.PreviousBaselineLocations = forged
	if err := r.Update(e); err != nil {
		t.Fatal(err)
	}
	e, _ = r.Get(e.ID)
	if len(e.PreviousBaselineLocations) != 0 {
		t.Fatalf("Update took the caller's list: %+v", e.PreviousBaselineLocations)
	}
}

// A failed save leaves the list as it was: the rollback copy must not share
// the new list's backing array.
func TestPreviousLocations_failedSaveRollsBack(t *testing.T) {
	fixedLocationClock(t, time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "console-servers.yaml")
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	e := addLocationServer(t, r, "/data/a", "")
	e = moveLocation(t, r, e.ID, "/data/b", "")
	before := slices.Clone(e.PreviousBaselineLocations)
	// Make the next save fail: the file's directory becomes read-only.
	if err := os.Chmod(filepath.Dir(path), 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Dir(path), 0o700) })
	e.BaselineDir = "/data/c"
	if err := r.Update(e); err == nil {
		t.Skip("the save did not fail here (running as root?)")
	}
	got, _ := r.Get(e.ID)
	if got.BaselineDir != "/data/b" || !slices.Equal(got.PreviousBaselineLocations, before) {
		t.Fatalf("after a failed save: dir %q previous %+v, want /data/b and %+v", got.BaselineDir, got.PreviousBaselineLocations, before)
	}
	if _, err := r.ForgetPreviousLocation(e.ID, "/data/a"); err == nil {
		t.Fatal("forgetting saved nothing and did not say so")
	}
	got, _ = r.Get(e.ID)
	if !slices.Equal(got.PreviousBaselineLocations, before) {
		t.Fatalf("a failed forget changed the list: %+v", got.PreviousBaselineLocations)
	}
}

// Forgetting a previous location stops consulting it and deletes nothing.
func TestPreviousLocations_forget(t *testing.T) {
	fixedLocationClock(t, time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	r, _ := LoadRegistry("")
	e := addLocationServer(t, r, "/data/a", "")
	e = moveLocation(t, r, e.ID, "/data/b", "")
	e = moveLocation(t, r, e.ID, "/data/c", "")
	got, err := r.ForgetPreviousLocation(e.ID, "/data/a/")
	if err != nil {
		t.Fatal(err)
	}
	if l := locationsOf(got); !slices.Equal(l, []string{"/data/b"}) {
		t.Fatalf("after forgetting /data/a: %v", l)
	}
	if _, err := r.ForgetPreviousLocation(e.ID, "/data/c"); !errors.Is(err, ErrUnknownPreviousLocation) {
		t.Fatalf("forgetting the CURRENT location: %v, want ErrUnknownPreviousLocation", err)
	}
	if _, err := r.ForgetPreviousLocation("nope", "/data/b"); !errors.Is(err, ErrUnknownServer) {
		t.Fatalf("forgetting on an unknown server: %v", err)
	}
	if _, err := r.ForgetPreviousLocation(e.ID, "  "); !errors.Is(err, ErrUnknownPreviousLocation) {
		t.Fatalf("forgetting an empty location: %v", err)
	}
}

// A previous location takes no part in any write rule: it is never a writer
// of the place (edge case 5 from the writing side: another server that took
// the place over is not refused because of it), never a retention target.
func TestPreviousLocations_neverAWriterNorPruned(t *testing.T) {
	fixedLocationClock(t, time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	r, _ := LoadRegistry("")
	a := addLocationServer(t, r, "/data/a", "")
	a = moveLocation(t, r, a.ID, "/data/a2", "")
	a.LocalKeepNewest = 3
	if err := r.Update(a); err != nil {
		t.Fatal(err)
	}
	b, err := r.Add(ServerEntry{Name: "b", DSN: "u:p@tcp(h:3306)/idx2", BaselineDir: "/data/a", LocalKeepNewest: 5})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WriteRefusal(b); err != nil {
		t.Fatalf("the server that took the place over is refused: %v", err)
	}
	a, _ = r.Get(a.ID)
	if err := r.WriteRefusal(a); err != nil {
		t.Fatalf("the server that left is refused: %v", err)
	}
	targets := LocalKeepTargets(r.List())
	if n := targets[canonicalDir("/data/a")]; n != 5 {
		t.Fatalf("retention on /data/a = %d, want b's 5 alone", n)
	}
	if _, ok := targets[canonicalDir("/data/a2")]; !ok {
		t.Fatalf("retention lost the current folder: %v", targets)
	}
}
