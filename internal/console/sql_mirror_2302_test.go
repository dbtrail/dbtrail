package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A server whose snapshots are only in S3, with a local mirror of them
// (#2302): SQL on the copy reads the mirror.

// newSQLMirrorFixture is the SQL fixture turned into an S3-only server whose
// mirror directory is the fixture's snapshot root.
func newSQLMirrorFixture(t *testing.T, runner sqlRunner) *sqlFixture {
	t.Helper()
	f := newSQLFixture(t, runner, false)
	f.s.cm.boot.baselineSrc = "s3://bucket/prefix"
	f.s.cm.boot.sqlMirrorDir = f.root
	return f
}

func TestSQLMirror_statementRunsOverTheMirror(t *testing.T) {
	runner := &fakeSQLRunner{}
	f := newSQLMirrorFixture(t, runner)
	w := postSQL(t, f.s, `{"sql":"SELECT * FROM shop.orders"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s, want 200 from the mirror", w.Code, w.Body.String())
	}
	if runner.calls() != 1 {
		t.Fatalf("the runner ran %d jobs, want 1", runner.calls())
	}
	job := runner.jobs[0]
	if len(job.CopyDirs) == 0 {
		t.Fatal("the job names no copy directory")
	}
	for _, d := range job.CopyDirs {
		if strings.HasPrefix(d, "s3://") || !strings.HasPrefix(d, f.root+string(filepath.Separator)) {
			t.Errorf("copy directory %q is not below the mirror %q", d, f.root)
		}
		if filepath.Clean(d) == filepath.Clean(f.root) {
			t.Errorf("the mirror root itself was handed to the sandbox")
		}
	}
	if !strings.Contains(w.Body.String(), sqlSnapshotAt.Format("2006-01-02T15:04:05Z")) {
		t.Errorf("the answer does not carry the mirrored snapshot's time: %s", w.Body.String())
	}
}

func TestSQLMirror_refusedAsBeforeWithoutAUsableMirror(t *testing.T) {
	for name, prepare := range map[string]func(t *testing.T, f *sqlFixture){
		"no mirror directory configured": func(t *testing.T, f *sqlFixture) { f.s.cm.boot.sqlMirrorDir = "" },
		"mirror directory does not exist": func(t *testing.T, f *sqlFixture) {
			f.s.cm.boot.sqlMirrorDir = filepath.Join(t.TempDir(), "nope")
		},
		"mirror holds nothing yet": func(t *testing.T, f *sqlFixture) { f.s.cm.boot.sqlMirrorDir = t.TempDir() },
		"mirror holds only a snapshot still being written": func(t *testing.T, f *sqlFixture) {
			if err := baseline.WriteIncompleteMarker(filepath.Join(f.root, sqlSnapshotDirName)); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &fakeSQLRunner{busy: true} // a busy runner would answer 429 if a slot were taken first
			f := newSQLMirrorFixture(t, runner)
			prepare(t, f)
			w := postSQL(t, f.s, `{"sql":"SELECT 1"}`)
			if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), sqlCopyNotLocalMessage) {
				t.Errorf("code=%d body=%s, want 409 copy-not-local before any slot", w.Code, w.Body.String())
			}
		})
	}
}

// A server that keeps a local folder never reads a mirror, whatever the
// field holds: the folder is the copy.
func TestSQLMirror_localFolderWins(t *testing.T) {
	runner := &fakeSQLRunner{}
	f := newSQLFixture(t, runner, false)
	other := t.TempDir()
	writeSQLBaselineFixture(t, other) // a mirror that would be usable
	f.s.cm.boot.sqlMirrorDir = other
	if got := f.s.cm.boot.sqlCopySource(); got != f.root {
		t.Errorf("sqlCopySource = %q, want the local folder %q", got, f.root)
	}
}

func TestSQLMirror_capabilityAndViewList(t *testing.T) {
	f := newSQLMirrorFixture(t, &fakeSQLRunner{})
	req := httptest.NewRequest("GET", "/api/sql", nil)
	if !f.s.sqlAvailable(req, f.s.cm.boot) {
		t.Error("sqlAvailable is false for an S3-only server with a mirrored snapshot")
	}
	g := httptest.NewRecorder()
	f.s.handleSQLInfo(g, req)
	if g.Code != http.StatusOK || !strings.Contains(g.Body.String(), "orders") {
		t.Errorf("GET /api/sql: code=%d body=%s, want the mirror's views", g.Code, g.Body.String())
	}
	if strings.Contains(g.Body.String(), f.root) {
		t.Errorf("GET /api/sql names the mirror's path on this host: %s", g.Body.String())
	}

	f.s.cm.boot.sqlMirrorDir = t.TempDir()
	if f.s.sqlAvailable(req, f.s.cm.boot) {
		t.Error("sqlAvailable is true with an empty mirror")
	}
}

// An earlier copy that is not in the mirror is in S3 only. The refusal must
// say that, and must not name a path on this host.
func TestSQLMirror_snapshotNotInTheMirror(t *testing.T) {
	f := newSQLMirrorFixture(t, &fakeSQLRunner{})
	_, err := f.s.buildViewsInput(t.Context(), f.s.cm.boot, viewsRequest{
		PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true, LocalCopy: true,
		SnapshotAt: sqlSnapshotAt.AddDate(0, 0, -1),
	})
	if err == nil {
		t.Fatal("a snapshot the mirror does not hold was accepted")
	}
	if strings.Contains(err.Error(), f.root) || !strings.Contains(err.Error(), "only in S3") {
		t.Errorf("err = %v; want one that says the snapshot is only in S3 and names no local path", err)
	}
}

// What is not SQL on the copy keeps reading the server's own location: a
// views file built without LocalCopy (the download, the dashboards card)
// must never name the mirror, whose paths exist on this host only.
func TestSQLMirror_onlySQLReadsTheMirror(t *testing.T) {
	f := newSQLMirrorFixture(t, &fakeSQLRunner{})
	if got := baselineSourcesOf(f.s.cm.boot); len(got) != 1 || got[0] != "s3://bucket/prefix" {
		t.Errorf("the server's snapshot locations now include something else: %v", got)
	}
	// Cancelled before it starts: the S3 listing this request would make
	// fails at once, and the local listing a wrong build would make instead
	// does not ask the context.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	in, err := f.s.buildViewsInput(ctx, f.s.cm.boot, viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true})
	if err == nil && in.BaselineSource != "s3://bucket/prefix" {
		t.Errorf("a views file that is not for SQL on the copy reads %q", in.BaselineSource)
	}
	// The same request for SQL on the copy does read it.
	in, err = f.s.buildViewsInput(t.Context(), f.s.cm.boot, viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, LocalCopy: true})
	if err != nil || in.BaselineSource != f.root {
		t.Errorf("LocalCopy: source %q, err %v; want the mirror", in.BaselineSource, err)
	}
}

func TestCopyMirrorDir(t *testing.T) {
	state := t.TempDir()
	reg := &Registry{path: filepath.Join(state, "console-servers.yaml")}
	s3only := ServerEntry{ID: "abc123", BaselineS3: "s3://bucket/prefix"}
	dir := reg.CopyMirrorDir(s3only)
	if dir == "" || !strings.HasPrefix(dir, filepath.Join(state, copyMirrorDirName, "abc123")+string(filepath.Separator)) {
		t.Fatalf("CopyMirrorDir = %q, want a folder below %s", dir, filepath.Join(state, copyMirrorDirName, "abc123"))
	}
	// One spelling apart is the same source; another bucket is another folder,
	// so a mirror never holds two sources' snapshots.
	slash := s3only
	slash.BaselineS3 = "s3://bucket/prefix/"
	if got := reg.CopyMirrorDir(slash); got != dir {
		t.Errorf("a trailing slash moved the mirror: %q vs %q", got, dir)
	}
	moved := s3only
	moved.BaselineS3 = "s3://other/prefix"
	if got := reg.CopyMirrorDir(moved); got == dir || got == "" {
		t.Errorf("another bucket shares the mirror folder: %q", got)
	}
	for name, e := range map[string]ServerEntry{
		"keeps a local folder": {ID: "abc123", BaselineS3: "s3://bucket/prefix", BaselineDir: "/var/lib/x"},
		"no bucket":            {ID: "abc123"},
		"no id":                {BaselineS3: "s3://bucket/prefix"},
	} {
		if got := reg.CopyMirrorDir(e); got != "" {
			t.Errorf("%s: CopyMirrorDir = %q, want none", name, got)
		}
	}
	if got := (&Registry{}).CopyMirrorDir(s3only); got != "" {
		t.Errorf("a registry with no file has a mirror folder: %q", got)
	}
	var nilReg *Registry
	if got := nilReg.CopyMirrorDir(s3only); got != "" {
		t.Errorf("a nil registry has a mirror folder: %q", got)
	}
	// Never inside a folder some other part of the daemon walks or uploads.
	for _, taken := range []string{localSnapshotsDirName, "baselines", "baseline-staging"} {
		if strings.HasPrefix(dir, filepath.Join(state, taken)+string(filepath.Separator)) {
			t.Errorf("the mirror is inside %s", taken)
		}
	}
}
