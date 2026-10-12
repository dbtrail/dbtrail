package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/copymirror"
)

// The mirror's state on the Snapshots page (#2302).

type fakeMirrorReporter map[string]CopyMirrorReport

func (f fakeMirrorReporter) CopyMirrorReport(id string) (CopyMirrorReport, bool) {
	r, ok := f[id]
	return r, ok
}

func TestSQLMirrorState(t *testing.T) {
	checked := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	for name, c := range map[string]struct {
		filled   bool // the mirror holds the fixture's snapshot
		fills    bool // this process fills mirrors
		report   *CopyMirrorReport
		state    string
		ready    bool
		bringing bool
		err      string
	}{
		"up to date":                         {filled: true, fills: true, report: &CopyMirrorReport{CheckedAt: checked}, state: "ready", ready: true},
		"filled, before any round":           {filled: true, fills: true, state: "ready", ready: true},
		"first download under way":           {fills: true, report: &CopyMirrorReport{Bringing: "2026-10-11T10-00-00Z", BringingBytes: 5 << 20}, state: "bringing", bringing: true},
		"a newer one under way":              {filled: true, fills: true, report: &CopyMirrorReport{Bringing: "2026-10-11T10-00-00Z"}, state: "bringing", ready: true, bringing: true},
		"failed with nothing to read":        {fills: true, report: &CopyMirrorReport{CheckedAt: checked, Err: "not enough free disk\nsecond line"}, state: "failed", err: "not enough free disk"},
		"failed, still answering":            {filled: true, fills: true, report: &CopyMirrorReport{CheckedAt: checked, Err: "access denied"}, state: "failed", ready: true, err: "access denied"},
		"a round ran and there is nothing":   {fills: true, report: &CopyMirrorReport{CheckedAt: checked}, state: "empty"},
		"no round yet":                       {fills: true, state: "waiting"},
		"a process that fills nothing":       {state: "unfilled"},
		"fills nothing, reads what is there": {filled: true, state: "ready", ready: true},
		// What a round found in another bucket says nothing about this
		// mirror: not its failure, and not that this bucket is empty.
		"a report about another bucket":         {fills: true, report: &CopyMirrorReport{Source: "s3://other/prefix", CheckedAt: checked, Err: "access denied"}, state: "waiting"},
		"a clean report about another bucket":   {fills: true, report: &CopyMirrorReport{Source: "s3://other/prefix", CheckedAt: checked}, state: "waiting"},
		"another bucket's download, ready here": {filled: true, fills: true, report: &CopyMirrorReport{Source: "s3://other/prefix", Bringing: "2026-10-11T10-00-00Z"}, state: "ready", ready: true},
		// A name that is not a snapshot's is not a download under way.
		"a report with a broken name": {fills: true, report: &CopyMirrorReport{CheckedAt: checked, Bringing: "nonsense"}, state: "empty"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newSQLMirrorFixture(t, &fakeSQLRunner{})
			f.s.fillsCopyMirrors = c.fills
			if !c.filled {
				f.s.cm.boot.sqlMirrorDir = t.TempDir()
			}
			if c.report != nil {
				if c.report.Source == "" {
					c.report.Source = "s3://bucket/prefix/" // the server's own, a slash apart
				}
				f.s.copyMirror = fakeMirrorReporter{bootServerID: *c.report}
			} else if c.fills {
				f.s.copyMirror = fakeMirrorReporter{}
			}
			got := f.s.sqlMirrorOf(f.s.cm.boot, bootServerID)
			if got == nil {
				t.Fatal("no mirror state for a server that has a mirror")
			}
			if got.State != c.state || got.Ready != c.ready || (got.Bringing != nil) != c.bringing || got.Error != c.err {
				t.Errorf("got %+v, want state %q ready %v bringing %v error %q", got, c.state, c.ready, c.bringing, c.err)
			}
			if c.ready != (got.Newest != nil) || (c.ready && !got.Newest.Equal(sqlSnapshotAt)) {
				t.Errorf("newest = %v with ready %v, want the mirrored snapshot's time exactly when ready", got.Newest, c.ready)
			}
			// What the page says SQL can do is what the statement path finds.
			if got.Ready != f.s.cm.boot.usesSQLMirror() {
				t.Errorf("ready = %v and the statement path says %v", got.Ready, f.s.cm.boot.usesSQLMirror())
			}
		})
	}
}

func TestSQLMirrorState_onlyForAServerThatHasAMirror(t *testing.T) {
	f := newSQLFixture(t, &fakeSQLRunner{}, false) // a local snapshot folder
	f.s.fillsCopyMirrors = true
	if got := f.s.sqlMirrorOf(f.s.cm.boot, bootServerID); got != nil {
		t.Errorf("a server with a local folder has a mirror state: %+v", got)
	}
	f.s.cm.boot.sqlMirrorDir = t.TempDir() // a stale field must not matter either
	if got := f.s.sqlMirrorOf(f.s.cm.boot, bootServerID); got != nil {
		t.Errorf("a server whose copy is a local folder has a mirror state: %+v", got)
	}
	if got := f.s.sqlMirrorOf(nil, bootServerID); got != nil {
		t.Errorf("no bundle: %+v", got)
	}
}

// What the page calls ready, and the snapshot it names, are what a statement
// reads: a snapshot still being written is not it, a complete directory with
// no table file is not it, and of two that are, the newer is.
func TestSQLMirrorState_readyAndNewestAreTheStatementPaths(t *testing.T) {
	mk := func(t *testing.T, root, name string, incomplete, table bool) {
		t.Helper()
		dir := filepath.Join(root, name, "shop")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if table {
			if err := os.WriteFile(filepath.Join(dir, "orders.parquet"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if incomplete {
			if err := baseline.WriteIncompleteMarker(filepath.Join(root, name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Run("beside the snapshot SQL reads", func(t *testing.T) {
		f := newSQLMirrorFixture(t, &fakeSQLRunner{})
		mk(t, f.root, "2026-05-01T03-00-00Z", true, true)   // newer, still being written
		mk(t, f.root, "2026-05-02T03-00-00Z", false, false) // newer, complete, no table file
		mk(t, f.root, "2026-04-01T03-00-00Z", false, true)  // older, readable
		got := f.s.sqlMirrorOf(f.s.cm.boot, bootServerID)
		if got == nil || !got.Ready || got.Newest == nil || !got.Newest.Equal(sqlSnapshotAt) {
			t.Errorf("got %+v, want ready from the snapshot of %v", got, sqlSnapshotAt)
		}
	})
	t.Run("a mirror with nothing a statement can read", func(t *testing.T) {
		f := newSQLMirrorFixture(t, &fakeSQLRunner{})
		f.s.cm.boot.sqlMirrorDir = t.TempDir()
		mk(t, f.s.cm.boot.sqlMirrorDir, "2026-05-02T03-00-00Z", false, false)
		got := f.s.sqlMirrorOf(f.s.cm.boot, bootServerID)
		if got == nil || got.Ready || got.Newest != nil || got.State == "ready" {
			t.Errorf("got %+v: the page says SQL reads a snapshot no statement can", got)
		}
		if got.Ready != f.s.cm.boot.usesSQLMirror() {
			t.Errorf("ready = %v and the statement path says %v", got.Ready, f.s.cm.boot.usesSQLMirror())
		}
	})
}

// A newer snapshot the bucket listed and the mirror does not hold is said,
// and only then.
func TestSQLMirrorState_behindTheBucket(t *testing.T) {
	f := newSQLMirrorFixture(t, &fakeSQLRunner{})
	report := func(sourceNewest string) *sqlMirrorDTO {
		f.s.copyMirror = fakeMirrorReporter{bootServerID: {Source: "s3://bucket/prefix", CheckedAt: time.Now(), SourceNewest: sourceNewest}}
		return f.s.sqlMirrorOf(f.s.cm.boot, bootServerID)
	}
	if got := report("2026-05-09T03-00-00Z"); got.Behind == nil || !got.Behind.Equal(time.Date(2026, 5, 9, 3, 0, 0, 0, time.UTC)) || got.CheckedAt == nil {
		t.Errorf("a newer snapshot in the bucket: %+v", got)
	}
	for _, same := range []string{sqlSnapshotDirName, "2026-01-01T00-00-00Z", "", "nonsense"} {
		if got := report(same); got.Behind != nil {
			t.Errorf("source newest %q: behind = %v, want none", same, got.Behind)
		}
	}
}

// The failure line reaches the page whole: the no-room sentence says what to
// do in its second half.
func TestSQLMirrorState_theNoRoomSentenceArrivesWhole(t *testing.T) {
	f := newSQLMirrorFixture(t, &fakeSQLRunner{})
	noRoom := (&copymirror.NoSpaceError{Dir: "/var/lib/bintrail/copy-mirror/0123456789abcdef0123456789abcdef/0123456789abcdef",
		Snapshot: "2026-10-11T10-00-00Z", Need: 42 << 30, Free: 3 << 30, Headroom: 1 << 30}).Error()
	f.s.copyMirror = fakeMirrorReporter{bootServerID: {Source: "s3://bucket/prefix", Err: "download: " + noRoom + "\nsecond line"}}
	got := f.s.sqlMirrorOf(f.s.cm.boot, bootServerID)
	for _, want := range []string{"needs 42.0 GiB", "3.0 GiB free", "1.0 GiB must stay free"} {
		if !strings.Contains(got.Error, want) {
			t.Errorf("error %q lacks %q", got.Error, want)
		}
	}
	if strings.Contains(got.Error, "second line") {
		t.Errorf("error carries more than its first line: %q", got.Error)
	}
	f.s.copyMirror = fakeMirrorReporter{bootServerID: {Source: "s3://bucket/prefix", Err: strings.Repeat("x", 5000)}}
	if got := f.s.sqlMirrorOf(f.s.cm.boot, bootServerID); len(got.Error) > sqlMirrorErrorMax+4 {
		t.Errorf("an endless line is sent whole: %d bytes", len(got.Error))
	}
}

// The capability the SQL card asks for: while the mirror is on its way the
// answer is "not yet", which the page must not take for "never".
func TestCapabilities_sqlPendingWhileTheMirrorFills(t *testing.T) {
	caps := func(f *sqlFixture) capabilitiesResponse {
		t.Helper()
		w := httptest.NewRecorder()
		f.s.handleCapabilities(w, httptest.NewRequest("GET", "/api/capabilities", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
		}
		var c capabilitiesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	f := newSQLMirrorFixture(t, &fakeSQLRunner{})
	if f.s.sqlPending(f.s.cm.boot) {
		t.Error("a mirror that already holds a snapshot is pending")
	}
	if c := caps(f); !c.SQL || c.SQLPending {
		t.Errorf("a filled mirror: sql=%v pending=%v, want true and false", c.SQL, c.SQLPending)
	}
	f.s.cm.boot.sqlMirrorDir = t.TempDir()
	if c := caps(f); c.SQL || !c.SQLPending {
		t.Errorf("an empty mirror being filled: sql=%v pending=%v, want false and true", c.SQL, c.SQLPending)
	}
	// Where a copy arriving would change nothing, the page is not told to
	// keep asking: archive access off, or a process that runs no SQL.
	f.s.cm.boot.noArchive = true
	if c := caps(f); c.SQLPending {
		t.Error("pending with archive access off")
	}
	f.s.cm.boot.noArchive = false
	runner := f.s.sqlRunner
	f.s.sqlRunner = nil
	if c := caps(f); c.SQLPending {
		t.Error("pending in a process that runs no SQL")
	}
	f.s.sqlRunner = runner
	f.s.fillsCopyMirrors = false
	if c := caps(f); c.SQL || c.SQLPending {
		t.Errorf("an empty mirror nobody fills: sql=%v pending=%v, want false and false", c.SQL, c.SQLPending)
	}
	local := newSQLFixture(t, &fakeSQLRunner{}, false)
	local.s.fillsCopyMirrors = true
	if c := caps(local); c.SQLPending {
		t.Error("a server with a local folder is pending")
	}
}

// The page's own words for each state, drawn by the page's own function from
// what the server sends for it.
func TestSnapshotSQLMirrorLines_2302(t *testing.T) {
	js := readAsset(t, "app.js")
	if !strings.Contains(functionBody(t, js, "function baselinesPanel("), "if (b) snapshotSQLMirrorLines(b).forEach") {
		t.Fatal("baselinesPanel no longer draws the mirror's state above the list, or not when the list failed")
	}
	// The list's failure carries the state, and the page keeps it.
	if !strings.Contains(js, "err.body = data;") ||
		!strings.Contains(js, `api("/api/baselines").catch((err) => Object.assign(asErr(err), { sql_mirror: (err && err.body && err.body.sql_mirror) || undefined })),`) {
		t.Fatal("the Snapshots page drops what the server sends beside a failed listing")
	}
	// The card asks again while the answer is "not yet".
	if !strings.Contains(js, "if (!(caps && caps.sql_pending)) state.sqlAskedAt = snapAt;") {
		t.Fatal("the SQL card remembers a \"not yet\" as its answer for this snapshot")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	script := strings.Join([]string{
		functionBody(t, js, "function snapshotSQLMirrorLines("),
		functionBody(t, js, "function utcLabel("),
		functionBody(t, js, "function firstLine("),
		functionBody(t, js, "function humanBytes("),
	}, "\n") + `
function el(tag, o) { return { cls: (o && o.class) || "", text: (o && o.text) || "" }; }
const cases = JSON.parse(require("fs").readFileSync(0, "utf8"));
const out = {};
for (const [name, b] of Object.entries(cases)) {
  out[name] = snapshotSQLMirrorLines(b).map((n) => ({ red: /\berr\b/.test(n.cls), text: n.text }));
}
console.log(JSON.stringify(out));
`
	// Each case is the JSON the server produces for that state, not a
	// hand-written look-alike.
	newest, bringing := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC), time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)
	checkedAt := time.Date(2026, 10, 11, 11, 30, 0, 123456789, time.UTC)
	noRoom := (&copymirror.NoSpaceError{Dir: "/var/lib/bintrail/copy-mirror/0123456789abcdef0123456789abcdef/0123456789abcdef",
		Snapshot: "2026-10-11T10-00-00Z", Need: 42 << 30, Free: 3 << 30, Headroom: 1 << 30}).Error()
	wire := func(m *sqlMirrorDTO) any {
		raw, err := json.Marshal(baselinesResponse{SQLMirror: m})
		if err != nil {
			t.Fatal(err)
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	cases := map[string]any{
		"none":           wire(nil),
		"ready":          wire(&sqlMirrorDTO{State: "ready", Ready: true, Newest: &newest}),
		"firstDownload":  wire(&sqlMirrorDTO{State: "bringing", Bringing: &bringing, BringingBytes: 5 << 30}),
		"newerDownload":  wire(&sqlMirrorDTO{State: "bringing", Ready: true, Newest: &newest, Bringing: &bringing, BringingBytes: 3 << 20}),
		"nothingToFetch": wire(&sqlMirrorDTO{State: "bringing", Ready: true, Newest: &newest, Bringing: &bringing}),
		"failedEmpty":    wire(&sqlMirrorDTO{State: "failed", Error: "not enough free disk for the local mirror of the snapshot"}),
		"failedReady":    wire(&sqlMirrorDTO{State: "failed", Ready: true, Newest: &newest, Error: "access denied."}),
		"failedNoReason": wire(&sqlMirrorDTO{State: "failed"}),
		"readyChecked":   wire(&sqlMirrorDTO{State: "ready", Ready: true, Newest: &newest, CheckedAt: &checkedAt}),
		"readyBehind":    wire(&sqlMirrorDTO{State: "ready", Ready: true, Newest: &newest, Behind: &bringing, CheckedAt: &checkedAt}),
		"failedNoRoom":   wire(&sqlMirrorDTO{State: "failed", Ready: true, Newest: &newest, Error: noRoom, CheckedAt: &checkedAt}),
		// What the page holds when the list itself could not be read.
		"listFailed": map[string]any{"error": "list baselines: s3://b/p: access denied",
			"sql_mirror": wire(&sqlMirrorDTO{State: "failed", Ready: true, Newest: &newest, Error: "access denied"}).(map[string]any)["sql_mirror"]},
		"empty":        wire(&sqlMirrorDTO{State: "empty"}),
		"waiting":      wire(&sqlMirrorDTO{State: "waiting"}),
		"unfilled":     wire(&sqlMirrorDTO{State: "unfilled"}),
		"off":          wire(&sqlMirrorDTO{State: "off"}),
		"unknownState": wire(&sqlMirrorDTO{State: "something-newer"}),
	}
	in, _ := json.Marshal(cases)
	path := filepath.Join(t.TempDir(), "mirror.js")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(node, path)
	cmd.Stdin = strings.NewReader(string(in))
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type line struct {
		Red  bool   `json:"red"`
		Text string `json:"text"`
	}
	var got map[string][]line
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	const has, coming = "the snapshot of 2026-10-11 09:00:00 UTC", "the snapshot of 2026-10-11 10:00:00 UTC"
	const lastChecked = " Last checked against the bucket 2026-10-11 11:30:00 UTC."
	want := map[string][]line{
		"none":           {},
		"ready":          {{false, "SQL reads a copy of the newest snapshots kept on this machine. It holds " + has + "."}},
		"firstDownload":  {{false, "SQL is not available yet. Bringing " + coming + " from S3 to this machine (5.0 GB to download)."}},
		"newerDownload":  {{false, "Bringing " + coming + " from S3 to this machine (3.0 MB to download). SQL answers from " + has + " until it arrives."}},
		"nothingToFetch": {{false, "Bringing " + coming + " from S3 to this machine. SQL answers from " + has + " until it arrives."}},
		"failedEmpty":    {{true, "The copy SQL reads could not be brought up to date: not enough free disk for the local mirror of the snapshot. SQL is not available yet. DBTrail tries again by itself."}},
		"failedReady":    {{true, "The copy SQL reads could not be brought up to date: access denied. SQL answers from " + has + ". DBTrail tries again by itself."}},
		"readyChecked":   {{false, "SQL reads a copy of the newest snapshots kept on this machine. It holds " + has + "." + lastChecked}},
		"readyBehind":    {{false, "SQL reads a copy of the newest snapshots kept on this machine. It holds " + has + ". The bucket has a newer one, of 2026-10-11 10:00:00 UTC, not brought yet." + lastChecked}},
		"failedNoRoom":   {{true, "The copy SQL reads could not be brought up to date: not enough free disk for the local mirror of the snapshot: snapshot 2026-10-11T10-00-00Z needs 42.0 GiB downloaded, the disk has 3.0 GiB free, and 1.0 GiB must stay free (mirror folder /var/lib/bintrail/copy-mirror/0123456789abcdef0123456789abcdef/0123456789abcdef). SQL answers from " + has + ". DBTrail tries again by itself." + lastChecked}},
		"listFailed":     {{true, "The copy SQL reads could not be brought up to date: access denied. SQL answers from " + has + ". DBTrail tries again by itself."}},
		"failedNoReason": {{true, "The copy SQL reads could not be brought up to date: no reason recorded. SQL is not available yet. DBTrail tries again by itself."}},
		"empty":          {{false, "SQL has nothing to read yet: the bucket holds no complete snapshot to bring to this machine."}},
		"waiting":        {{false, "SQL is not available yet. DBTrail has not brought a snapshot from S3 to this machine."}},
		"unfilled":       {{false, "SQL is not available here. These snapshots are only in S3, and nothing on this machine brings them to disk: bintrail-console watch does."}},
		"off":            {{false, "SQL is not available on this server: its snapshots are only in S3, and the copy SQL reads on this machine is turned off for it (no_sql_mirror in this server's snapshot settings). Turn it back on, or give the server a Local folder, and SQL becomes available."}},
		"unknownState":   {{false, "SQL is not available yet. DBTrail has not brought a snapshot from S3 to this machine."}},
	}
	for name, w := range want {
		g := got[name]
		if g == nil {
			g = []line{}
		}
		if !reflect.DeepEqual(g, w) {
			t.Errorf("%s:\n got  %+v\n want %+v", name, g, w)
		}
	}
}

// The Snapshots page's own request carries the state, and carries it when
// the bucket cannot be listed: that is when the page has to say whether SQL
// still answers. The request here is cancelled before it starts, so the
// listing fails at once, as it would against a bucket that refuses.
func TestBaselinesAPI_carriesTheSQLMirrorEvenWhenTheBucketCannotBeListed(t *testing.T) {
	f := newSQLMirrorFixture(t, &fakeSQLRunner{})
	f.s.copyMirror = fakeMirrorReporter{bootServerID: {Source: "s3://bucket/prefix", Err: "access denied"}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w := httptest.NewRecorder()
	f.s.handleBaselines(w, httptest.NewRequest("GET", "/api/baselines", nil).WithContext(ctx))
	var resp struct {
		Error     string        `json:"error"`
		SQLMirror *sqlMirrorDTO `json:"sql_mirror"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("code=%d body=%s: %v", w.Code, w.Body.String(), err)
	}
	if w.Code != http.StatusBadGateway || resp.Error == "" {
		t.Fatalf("code=%d body=%s; this test needs the listing to fail", w.Code, w.Body.String())
	}
	if resp.SQLMirror == nil || !resp.SQLMirror.Ready || resp.SQLMirror.State != "failed" || resp.SQLMirror.Error != "access denied" {
		t.Errorf("sql_mirror = %+v, want the state beside the failure", resp.SQLMirror)
	}
	if strings.Contains(w.Body.String(), f.root) {
		t.Errorf("the answer names the mirror's path on this host: %s", w.Body.String())
	}

	// A server with no mirror keeps the plain error, field and all.
	plain := newSQLFixture(t, &fakeSQLRunner{}, false)
	plain.s.cm.boot.baselineSrc = "s3://bucket/prefix"
	w = httptest.NewRecorder()
	plain.s.handleBaselines(w, httptest.NewRequest("GET", "/api/baselines", nil).WithContext(ctx))
	if w.Code != http.StatusBadGateway || strings.Contains(w.Body.String(), "sql_mirror") {
		t.Errorf("no mirror: code=%d body=%s", w.Code, w.Body.String())
	}
}
