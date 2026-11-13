package console

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A snapshot download and the cleanup (#2678). The download pins the ONE
// snapshot it streams, for a time sized by that snapshot; a client that
// stops reading is dropped; the cleanup goes on around it.

// pinWatcher records, at every write of the archive, what the download
// keeps from the cleanup.
type pinWatcher struct {
	*httptest.ResponseRecorder
	root    string
	pinned  map[string]bool
	reading map[string]bool
}

func (h *pinWatcher) Write(b []byte) (int, error) {
	for _, n := range baseline.Pinned(h.root) {
		h.pinned[n] = true
	}
	h.reading[baseline.Reading(h.root)] = true
	return h.ResponseRecorder.Write(b)
}

// olderSnapshot adds a complete snapshot named name holding every table the
// fixture's snapshot holds, so neither is the only copy of a table.
func olderSnapshot(t *testing.T, root, name string) {
	t.Helper()
	if err := os.CopyFS(filepath.Join(root, name), os.DirFS(filepath.Join(root, detailSnapDir))); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

func snapshotNames(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && e.Name()[0] != '.' {
			out = append(out, e.Name())
		}
	}
	slices.Sort(out)
	return out
}

// While the archive streams, the snapshot being read is pinned and the
// folder is NOT stopped: a prune in the middle removes the other snapshots
// past the count and keeps this one. After it, nothing is pinned.
func TestBaselineDownload_pinsOnlyTheSnapshotItStreams(t *testing.T) {
	dir := newDetailFixture(t)
	olderSnapshot(t, dir, "2026-06-09T12-00-00Z")
	olderSnapshot(t, dir, "2026-06-11T12-00-00Z")
	olderSnapshot(t, dir, "2026-06-12T12-00-00Z")
	srv := newBaselineServer(t, dir, true)
	w := &pinWatcher{ResponseRecorder: httptest.NewRecorder(), root: dir, pinned: map[string]bool{}, reading: map[string]bool{}}
	srv.handleBaselineDownload(w, httptest.NewRequest("GET", "/api/baselines/download"+detailQuery(detailSnapAt), nil))
	if w.Code != http.StatusOK || w.Body.Len() == 0 {
		t.Fatalf("download: code %d, %d bytes", w.Code, w.Body.Len())
	}
	if len(w.pinned) != 1 || !w.pinned[detailSnapDir] {
		t.Fatalf("while the archive streamed the pinned snapshots were %v, want only %s", w.pinned, detailSnapDir)
	}
	if len(w.reading) != 1 || !w.reading[""] {
		t.Fatalf("the download stopped the cleanup of the whole folder: %v", w.reading)
	}
	if got := baseline.Pinned(dir); len(got) != 0 {
		t.Fatalf("the download is over and %v is still pinned", got)
	}
	// The same pin, held by hand, against a real prune: this snapshot stays,
	// the others past the count go.
	release := baseline.PinSnapshot(dir, detailSnapDir, "a snapshot download", time.Hour)
	res, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: dir, KeepNewest: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshotNames(t, dir); !slices.Equal(got, []string{detailSnapDir, "2026-06-12T12-00-00Z"}) || res.KeptPinned != 1 {
		t.Fatalf("with the download's snapshot pinned the folder holds %v (KeptPinned %d)", got, res.KeptPinned)
	}
	release()
	if _, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: dir, KeepNewest: 1}); err != nil {
		t.Fatal(err)
	}
	if got := snapshotNames(t, dir); !slices.Equal(got, []string{"2026-06-12T12-00-00Z"}) {
		t.Fatalf("after the download the folder holds %v, want the count", got)
	}
}

// The pin lasts as long as the snapshot's bytes take at the minimum rate,
// plus one stall timeout: sized by what is streamed.
func TestDownloadPinLimit_followsTheSize(t *testing.T) {
	for _, c := range []struct {
		bytes int64
		want  time.Duration
	}{
		{0, downloadStallTimeout},
		{100 << 20, downloadStallTimeout + 100*time.Second},
		{10 << 30, downloadStallTimeout + 10240*time.Second},
	} {
		if got := downloadPinLimit(c.bytes); got != c.want {
			t.Errorf("downloadPinLimit(%d) = %s, want %s", c.bytes, got, c.want)
		}
		t.Logf("a snapshot of %d MiB is kept for a download for at most %s", c.bytes>>20, downloadPinLimit(c.bytes))
	}
}

// bigFixture is the detail fixture with a file too large for the socket
// buffers to swallow, so a client that does not read blocks the server.
func bigFixture(t *testing.T) string {
	t.Helper()
	dir := newDetailFixture(t)
	big := make([]byte, 48<<20)
	if _, err := rand.Read(big); err != nil { // incompressible: gzip must not shrink it away
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, detailSnapDir, "shop", "big.parquet"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func shortStall(t *testing.T, d time.Duration) {
	t.Helper()
	prev := downloadStallTimeout
	downloadStallTimeout = d
	t.Cleanup(func() { downloadStallTimeout = prev })
}

// A client that opens the download and stops reading is dropped after the
// stall timeout, its pin is released, and the next prune removes what is
// past the count. Without the write deadline the handler would wait for as
// long as the client kept the connection.
func TestBaselineDownload_aStalledClientIsDroppedAndItsPinReleased(t *testing.T) {
	shortStall(t, 300*time.Millisecond)
	dir := bigFixture(t)
	olderSnapshot(t, dir, "2026-06-12T12-00-00Z")
	srv := newBaselineServer(t, dir, true)
	ended := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(ended)
		srv.handleBaselineDownload(w, r)
	}))
	defer ts.Close()

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET /api/baselines/download"+detailQuery(detailSnapAt)+" HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	// The client reads nothing. While the server is stuck on it, the
	// snapshot is pinned.
	deadline := time.Now().Add(5 * time.Second)
	for len(baseline.Pinned(dir)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the download never pinned its snapshot")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-ended:
	case <-time.After(15 * time.Second):
		t.Fatal("a client that stopped reading still holds the download after 15 seconds: it was not dropped")
	}
	if got := baseline.Pinned(dir); len(got) != 0 {
		t.Fatalf("the stalled download was dropped and %v is still pinned", got)
	}
	if _, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: dir, KeepNewest: 1}); err != nil {
		t.Fatal(err)
	}
	if got := snapshotNames(t, dir); !slices.Equal(got, []string{"2026-06-12T12-00-00Z"}) {
		t.Fatalf("after the stalled download the folder holds %v, want the count", got)
	}
}

// A client that reads slowly, but keeps reading, is not cut: the whole
// download takes several stall timeouts and arrives complete.
func TestBaselineDownload_aSlowClientThatKeepsReadingIsNotCut(t *testing.T) {
	shortStall(t, 400*time.Millisecond)
	dir := bigFixture(t)
	srv := newBaselineServer(t, dir, true)
	ts := httptest.NewServer(http.HandlerFunc(srv.handleBaselineDownload))
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/api/baselines/download" + detailQuery(detailSnapAt))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	start := time.Now()
	var total int64
	buf := make([]byte, 1<<20)
	for {
		// A mebibyte, then a pause well inside one stall timeout: slow,
		// and never stopped.
		n, err := io.ReadFull(resp.Body, buf)
		total += int64(n)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			t.Fatalf("the download was cut after %d bytes and %s: %v", total, time.Since(start), err)
		}
		time.Sleep(40 * time.Millisecond)
	}
	if took := time.Since(start); took < 2*downloadStallTimeout {
		t.Fatalf("the download took %s, not long enough to prove a slow reader outlives the stall timeout (%s)", took, downloadStallTimeout)
	}
	if total < 48<<20 {
		t.Fatalf("the download delivered %d bytes, less than the snapshot", total)
	}
}

// What the handler does with the cap: one of the newest is let in, an old
// one is let in up to the count and refused past it, a folder nothing
// prunes has no cap, and the refusal says what is true.
func TestPinForDownload(t *testing.T) {
	dir := newDetailFixture(t)
	older := []string{"2026-06-06T12-00-00Z", "2026-06-07T12-00-00Z", "2026-06-08T12-00-00Z"}
	for _, n := range older {
		olderSnapshot(t, dir, n)
	}
	const keep = 1 // the fixture's snapshot is the newest: the one kept
	r0, ok := pinForDownload(dir, detailSnapDir, keep, 0, time.Hour)
	defer r0()
	r0b, ok2 := pinForDownload(dir, detailSnapDir, keep, 0, time.Hour)
	defer r0b()
	if !ok || !ok2 {
		t.Fatal("a download of the newest snapshot was refused")
	}
	r1, ok := pinForDownload(dir, older[0], keep, 0, time.Hour)
	if !ok {
		t.Fatal("the first download of a snapshot past the count was refused, with only the newest being downloaded")
	}
	if _, ok := pinForDownload(dir, older[1], keep, 0, time.Hour); ok {
		t.Fatal("a second snapshot past the count was held for a download against a count of 1")
	}
	r1()
	if r, ok := pinForDownload(dir, older[1], keep, 0, time.Hour); !ok {
		t.Fatal("the place a finished download freed was not given")
	} else {
		r()
	}
	// No count and no age (a console that prunes nothing, a server that
	// keeps all): no cap.
	for _, n := range older {
		r, ok := pinForDownload(dir, n, 0, 0, time.Hour)
		if !ok {
			t.Fatalf("a download was refused on a folder nothing prunes: %s", n)
		}
		defer r()
	}
	if downloadCap(0) != DefaultLocalKeepNewest || downloadCap(5) != 5 {
		t.Fatalf("the cap: %d with no count, %d with 5", downloadCap(0), downloadCap(5))
	}
	for _, n := range []int{1, 3} {
		t.Logf("the refusal with a count of %d: %s", n, downloadsBusyMessage(n, n*downloadCeilingPerCap))
	}
	if got := downloadsBusyMessage(3, 6); !strings.Contains(got, "may hold 6 snapshots at once, and 3 of those that the cleanup would have removed") || !strings.Contains(got, "Try again shortly") {
		t.Fatalf("the refusal: %s", got)
	}
}

// Through the handler: a download over the cap answers 429 with Retry-After.
func TestBaselineDownload_overTheCapIs429(t *testing.T) {
	dir := newDetailFixture(t)
	olderSnapshot(t, dir, "2026-06-12T12-00-00Z") // newer: the fixture's is now past a count of 1
	other := "2026-06-06T12-00-00Z"
	olderSnapshot(t, dir, other)
	srv := newBaselineServer(t, dir, true)
	prev := downloadRule
	t.Cleanup(func() { downloadRule = prev })
	downloadRule = func(*Server, *http.Request) (int, time.Duration) { return 1, 0 }
	// Another download holds the one place past the count (a pin taken
	// under the cap: only those count toward it, #2691).
	held, _ := baseline.PinDownload(dir, other, time.Hour, nil, 1, 2)
	defer held()
	w := httptest.NewRecorder()
	srv.handleBaselineDownload(w, httptest.NewRequest("GET", "/api/baselines/download"+detailQuery(detailSnapAt), nil))
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "60" {
		t.Fatalf("a download over the cap: code %d, Retry-After %q; want 429 and 60", w.Code, w.Header().Get("Retry-After"))
	}
	t.Logf("the refusal as the client reads it: %s", w.Body.String())
}

// The port's lookup is wired: a server with a folder and an S3 destination
// gets the folder-then-bucket lookup on its port target; one with a folder
// alone gets none, and the shim reads the folder itself.
func TestResolveFlashback_handsThePortTheBucketLookup(t *testing.T) {
	clearStores(t)
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(ServerEntry{Name: "both", DSN: "u:p@tcp(127.0.0.1:1)/idx?timeout=2s", BaselineDir: t.TempDir(), BaselineS3: "s3://bkt/both/"}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(ServerEntry{Name: "folder", DSN: "u:p@tcp(127.0.0.1:1)/idx2?timeout=2s", BaselineDir: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, LazyIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.cm.CloseAll)
	both, err := srv.ResolveFlashback(context.Background(), "both")
	if err != nil {
		t.Fatal(err)
	}
	if both.FindBaseline == nil || !both.BaselineBucket || both.BaselineDir == "" {
		t.Fatalf("a server with a folder and S3: lookup %v, bucket %v, dir %q; the port would read the folder alone", both.FindBaseline != nil, both.BaselineBucket, both.BaselineDir)
	}
	folder, err := srv.ResolveFlashback(context.Background(), "folder")
	if err != nil {
		t.Fatal(err)
	}
	if folder.FindBaseline != nil || folder.BaselineBucket {
		t.Fatal("a server with a folder alone got a bucket lookup")
	}
}

// The page says the cleanup waits only for a job that has run for more than
// ten minutes: a cleanup that falls inside a routine update is not a wait an
// operator has to wonder about.
func TestCleanupWaitingOf_onlyAWaitWorthSaying(t *testing.T) {
	write := func(dir string, since time.Duration) {
		now := time.Now().UTC()
		rec := baseline.CleanupWaiting{Since: now.Add(-since), At: now, What: "an update and the merge of its changes", ValidUntil: now.Add(2 * time.Hour)}
		b, _ := json.Marshal(rec)
		if err := os.WriteFile(filepath.Join(dir, baseline.CleanupWaitingFile), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	if cleanupWaitingOf(dir) != nil {
		t.Fatal("a folder with no record says it waits")
	}
	write(dir, 3*time.Minute)
	if got := cleanupWaitingOf(dir); got != nil {
		t.Fatalf("a job three minutes old is said to hold the cleanup: %+v", got)
	}
	write(dir, 25*time.Minute)
	got := cleanupWaitingOf(dir)
	if got == nil || got.What != "an update and the merge of its changes" || got.Since == "" {
		t.Fatalf("a job 25 minutes old is not said: %+v", got)
	}
}

// The rule the download cap is worked out from, per kind of server. The
// daemon's own server is not in the registry, and its folder is pruned by
// age when the daemon has a bucket: its downloads are capped too.
func TestDownloadRuleFor(t *testing.T) {
	week := 7 * 24 * time.Hour
	for _, c := range []struct {
		what             string
		keep             int
		inRegistry       bool
		serverS3, bootS3 string
		wantKeep         int
		wantRetain       time.Duration
	}{
		{"a count", 3, true, "", "", 3, week},
		{"no count, a destination of its own", 0, true, "s3://b/p/", "", 0, week},
		{"no count, no destination: keeps all", 0, true, "", "s3://boot/", 0, 0},
		{"the daemon's own server, with the daemon's bucket", 0, false, "", "s3://boot/", 0, week},
		{"the daemon's own server, no bucket", 0, false, "", "  ", 0, 0},
	} {
		k, r := downloadRuleFor(c.keep, week, c.inRegistry, c.serverS3, c.bootS3)
		if k != c.wantKeep || r != c.wantRetain {
			t.Errorf("%s: rule (%d, %s), want (%d, %s)", c.what, k, r, c.wantKeep, c.wantRetain)
		}
	}
}
