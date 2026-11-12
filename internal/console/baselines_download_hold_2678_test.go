package console

import (
	"context"
	"crypto/rand"
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

// Downloads repeated at will cannot hold back every old snapshot: at most
// as many snapshots PAST the count as the count itself are held for
// downloads at once, and one more is refused. A download of a snapshot the
// cleanup keeps anyway is never refused, whatever else is being downloaded,
// and a folder with no count has no cap.
func TestPinForDownload_capsOnlySnapshotsPastTheCount(t *testing.T) {
	dir := newDetailFixture(t)
	older := []string{"2026-06-06T12-00-00Z", "2026-06-07T12-00-00Z", "2026-06-08T12-00-00Z"}
	for _, n := range older {
		olderSnapshot(t, dir, n)
	}
	const keep = 1 // the fixture's snapshot is the newest: the one kept
	// One person downloads the newest for hours: nobody else is refused it.
	r0, ok := pinForDownload(dir, detailSnapDir, keep, time.Hour)
	defer r0()
	r0b, ok2 := pinForDownload(dir, detailSnapDir, keep, time.Hour)
	defer r0b()
	if !ok || !ok2 {
		t.Fatal("a download of the newest snapshot was refused")
	}
	// One old snapshot may be held for a download; a second is refused.
	r1, ok := pinForDownload(dir, older[0], keep, time.Hour)
	if !ok {
		t.Fatal("the first download of a snapshot past the count was refused, with only the newest being downloaded")
	}
	if _, ok := pinForDownload(dir, older[1], keep, time.Hour); ok {
		t.Fatal("a second snapshot past the count was held for a download against a count of 1")
	}
	if got := baseline.Pinned(dir); !slices.Equal(got, []string{older[0], detailSnapDir}) {
		t.Fatalf("pinned %v", got)
	}
	r1()
	if r, ok := pinForDownload(dir, older[1], keep, time.Hour); !ok {
		t.Fatal("the place a finished download freed was not given")
	} else {
		r()
	}
	// No count (a console that prunes nothing, a server that keeps all).
	for _, n := range older {
		r, ok := pinForDownload(dir, n, 0, time.Hour)
		if !ok {
			t.Fatalf("a download was refused on a folder nothing prunes: %s", n)
		}
		defer r()
	}
	for _, n := range []int{1, 3} {
		t.Logf("the refusal with a count of %d: %s", n, downloadsBusyMessage(n))
	}
	if got := downloadsBusyMessage(1); strings.Contains(got, "1 older snapshots") || !strings.Contains(got, "Try again shortly") {
		t.Fatalf("the refusal: %s", got)
	}
}

// The cap counts different snapshots, per kind of reader.
func TestPinSnapshotCapped(t *testing.T) {
	root := t.TempDir()
	r1, ok1 := baseline.PinSnapshotCapped(root, "2026-06-06T12-00-00Z", "a snapshot download", time.Hour, 2)
	r2, ok2 := baseline.PinSnapshotCapped(root, "2026-06-07T12-00-00Z", "a snapshot download", time.Hour, 2)
	_, ok3 := baseline.PinSnapshotCapped(root, "2026-06-08T12-00-00Z", "a snapshot download", time.Hour, 2)
	again, ok4 := baseline.PinSnapshotCapped(root, "2026-06-06T12-00-00Z", "a snapshot download", time.Hour, 2)
	if !ok1 || !ok2 || ok3 || !ok4 {
		t.Fatalf("two, a third and a second reader of the first: %v %v %v %v; want true true false true", ok1, ok2, ok3, ok4)
	}
	if got := baseline.Pinned(root); len(got) != 2 {
		t.Fatalf("pinned %v, want two snapshots", got)
	}
	r1()
	again()
	r4, ok := baseline.PinSnapshotCapped(root, "2026-06-08T12-00-00Z", "a snapshot download", time.Hour, 2)
	if !ok {
		t.Fatal("a place freed by the downloads that ended was not given")
	}
	r2()
	r4()
}
