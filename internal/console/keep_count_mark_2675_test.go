package console

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
)

// markLogs captures slog for one test, safely across goroutines.
type markLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *markLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *markLogs) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.buf.String() }
func (l *markLogs) Len() int       { l.mu.Lock(); defer l.mu.Unlock(); return l.buf.Len() }
func (l *markLogs) Reset()         { l.mu.Lock(); defer l.mu.Unlock(); l.buf.Reset() }

func captureLogs(t *testing.T) *markLogs {
	t.Helper()
	l := &markLogs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(l, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return l
}

// #2675, point 1: downgrade, then upgrade. A version from before #2658
// saves a count of 3 it does not use on a server it adds with an S3
// destination; the registry's record of the #2658 step survives it (it
// keeps keys it does not know), so on the way back up that count used to
// apply and the folder was pruned to 3. Every count beside a destination
// this build saves now carries a mark, and a count without its mark is
// cleared at load.

const markedHeader = `version: 1
local_keep_with_s3:
  at: "2026-11-11T00:00:00Z"
  entries_marked: true
servers:
`

func TestKeepCountMark_aCountAnOlderVersionSavedIsCleared_2675(t *testing.T) {
	cases := []struct {
		name  string
		entry string
		want  int
	}{
		{"added with a destination by an older version: no mark", `
    baseline_s3: s3://bucket/one/
    local_keep_newest: 3
`, 0},
		{"marked by this version", `
    baseline_s3: s3://bucket/one/
    local_keep_newest: 3
    local_keep_saved_with: 3 s3://bucket/one/
`, 3},
		{"an older version changed the count of a marked entry", `
    baseline_s3: s3://bucket/one/
    local_keep_newest: 5
    local_keep_saved_with: 3 s3://bucket/one/
`, 0},
		{"an older version gave a marked entry another destination", `
    baseline_s3: s3://bucket/two/
    local_keep_newest: 3
    local_keep_saved_with: 3 s3://bucket/one/
`, 0},
		{"an older version gave its first destination to a counted server", `
    baseline_s3: s3://bucket/one/
    local_keep_newest: 3
`, 0},
		{"the destination as written, a trailing slash less", `
    baseline_s3: s3://bucket/one
    local_keep_newest: 3
    local_keep_saved_with: 3 s3://bucket/one/
`, 0},
		{"no destination: every version applies the count", `
    local_keep_newest: 3
`, 3},
		{"keep everything beside a destination: nothing to clear", `
    baseline_s3: s3://bucket/one/
`, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearStores(t)
			path := writeRegistryFile(t, markedHeader+`  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
    baseline_dir: /srv/one`+c.entry)
			r := loadReg(t, path)
			if e := mustGet(t, r, "aaaaaaaaaaaaaaa1"); e.LocalKeepNewest != c.want {
				t.Fatalf("count %d, want %d", e.LocalKeepNewest, c.want)
			}
			if got := LocalKeepTargets(r.List())["/srv/one"]; got != c.want {
				t.Fatalf("the folder is pruned to %d, want %d", got, c.want)
			}
			// A console that only follows the file reads it the same way.
			f, err := LoadRegistryFollower(path)
			if err != nil {
				t.Fatal(err)
			}
			if e, _ := f.Get("aaaaaaaaaaaaaaa1"); e.LocalKeepNewest != c.want {
				t.Fatalf("the follower reads count %d, want %d", e.LocalKeepNewest, c.want)
			}
		})
	}
}

// The clear is said once at the daemon's start, with the server's name, and
// written to the file, so the next start finds nothing to do.
func TestKeepCountMark_theClearIsSaidAndSaved_2675(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, markedHeader+`  - id: aaaaaaaaaaaaaaa1
    name: added-on-the-old-version
    index_dsn: u:p@tcp(h:3306)/one
    baseline_dir: /srv/one
    baseline_s3: s3://bucket/one/
    local_keep_newest: 3
`)
	logs := captureLogs(t)
	r := loadReg(t, path)
	r.SaveLocalKeepMigration()
	line := logs.String()
	t.Log(line)
	if !strings.Contains(line, "servers with an S3 destination had a number of local snapshots to keep that never applied to them") ||
		!strings.Contains(line, "added-on-the-old-version") {
		t.Fatalf("the clear was not said with the server's name: %q", line)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "local_keep_newest") || !strings.Contains(string(raw), "entries_marked: true") {
		t.Fatalf("the file after the start:\n%s", raw)
	}
	logs.Reset()
	loadReg(t, path).SaveLocalKeepMigration()
	if logs.Len() != 0 {
		t.Fatalf("the second start said it again: %q", logs.String())
	}
}

// A file last saved by the first version with the record (it applied the
// counts it saved, and drops the record's keys it does not know): its counts
// are marked as they are, once, and keep applying.
func TestKeepCountMark_aFileFromTheFirstVersionKeepsItsCounts_2675(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, `version: 1
local_keep_with_s3:
  at: "2026-11-11T00:00:00Z"
servers:
  - id: aaaaaaaaaaaaaaa1
    name: one
    index_dsn: u:p@tcp(h:3306)/one
    baseline_dir: /srv/one
    baseline_s3: s3://bucket/one/
    local_keep_newest: 3
`)
	r := loadReg(t, path)
	if e := mustGet(t, r, "aaaaaaaaaaaaaaa1"); e.LocalKeepNewest != 3 || e.LocalKeepSavedWith != "3 s3://bucket/one/" {
		t.Fatalf("entry %+v, want the count kept and marked", e)
	}
	r.SaveLocalKeepMigration()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"entries_marked: true", "local_keep_saved_with: 3 s3://bucket/one/", "local_keep_newest: 3"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("the file lacks %q:\n%s", want, raw)
		}
	}
	if e := mustGet(t, loadReg(t, path), "aaaaaaaaaaaaaaa1"); e.LocalKeepNewest != 3 {
		t.Fatalf("read again: count %d", e.LocalKeepNewest)
	}
}

// What this build saves it marks, on every write: a count set from the
// Snapshots page, a destination added later, a server added with both, and
// a save of something else leaves the mark right.
func TestKeepCountMark_everySaveMarksTheCount_2675(t *testing.T) {
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
	mark := func() string {
		e, _ := srv.cm.reg.Get(id)
		return e.LocalKeepSavedWith
	}
	if rec := putBackupSettings(t, srv, id, `{"keep_newest":3}`); rec.Code != 200 {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}
	if mark() != "" {
		t.Fatalf("a count with no destination: mark %q, want none", mark())
	}
	if rec := putBackupSettings(t, srv, id, `{"baseline_s3":"s3://bucket/a/"}`); rec.Code != 200 {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}
	if mark() != "3 s3://bucket/a/" {
		t.Fatalf("a first destination beside a count: mark %q", mark())
	}
	if rec := putBackupSettings(t, srv, id, `{"keep_newest":7}`); rec.Code != 200 {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}
	if mark() != "7 s3://bucket/a/" {
		t.Fatalf("a count set beside a destination: mark %q", mark())
	}
	if rec := putBackupSettings(t, srv, id, `{"no_archive":true}`); rec.Code != 200 {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}
	if mark() != "7 s3://bucket/a/" {
		t.Fatalf("a save of something else: mark %q", mark())
	}
	if rec := putBackupSettings(t, srv, id, `{"keep_newest":0}`); rec.Code != 200 {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}
	if mark() != "" {
		t.Fatalf("keep everything: mark %q, want none", mark())
	}
	added, err := srv.cm.reg.Add(ServerEntry{Name: "two", DSN: "u:p@tcp(h:3306)/two", BaselineDir: t.TempDir(), BaselineS3: "s3://bucket/two/", LocalKeepNewest: 3})
	if err != nil {
		t.Fatal(err)
	}
	if added.LocalKeepSavedWith != "3 s3://bucket/two/" {
		t.Fatalf("a server added with both: mark %q", added.LocalKeepSavedWith)
	}
	// The mark is the registry's: a caller cannot hand one in.
	forged := added
	forged.LocalKeepNewest, forged.LocalKeepSavedWith = 9, "3 s3://bucket/two/"
	if err := srv.cm.reg.Update(forged); err != nil {
		t.Fatal(err)
	}
	if got, _ := srv.cm.reg.Get(added.ID); got.LocalKeepSavedWith != "9 s3://bucket/two/" {
		t.Fatalf("a mark handed in by a caller was kept: %q", got.LocalKeepSavedWith)
	}
}
