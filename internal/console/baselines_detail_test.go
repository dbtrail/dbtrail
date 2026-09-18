package console

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/views"
)

// writeBaselineFile writes <dir>/<parts...> with the given content and mtime.
func writeBaselineFile(t *testing.T, dir string, content string, mtime time.Time, parts ...string) {
	t.Helper()
	p := filepath.Join(append([]string{dir}, parts...)...)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if !mtime.IsZero() {
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
}

const detailSnapDir = "2026-06-10T12-00-00Z"
const detailSnapAt = "2026-06-10 12:00:00"

func detailQuery(at string) string { return "?at=" + url.QueryEscape(at) }

// newDetailFixture builds a complete two-table snapshot with staggered mtimes.
func newDetailFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	base := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	writeBaselineFile(t, dir, "aaaa", base, detailSnapDir, "shop", "orders.parquet")
	writeBaselineFile(t, dir, "bbbbbbbb", base.Add(90*time.Second), detailSnapDir, "shop", "users.parquet")
	writeBaselineFile(t, dir, "", base.Add(2*time.Minute), detailSnapDir, "_SUCCESS")
	writeBaselineFile(t, dir, `{"version":1}`, base.Add(2*time.Minute), detailSnapDir, "_MANIFEST")
	return dir
}

func TestBaselineFiles_localDetail(t *testing.T) {
	dir := newDetailFixture(t)
	srv := newBaselineServer(t, dir, true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Time != detailSnapAt || got.Incomplete {
		t.Fatalf("header = %+v, want time %s and complete", got, detailSnapAt)
	}
	if len(got.Tables) != 2 ||
		got.Tables[0].Schema != "shop" || got.Tables[0].Table != "orders" || got.Tables[0].SizeBytes != 4 ||
		got.Tables[1].Table != "users" || got.Tables[1].SizeBytes != 8 {
		t.Fatalf("tables = %+v, want orders(4) then users(8)", got.Tables)
	}
	// 4 files total: two tables + two markers; 4+8+0+13 bytes.
	if got.Files != 4 || got.TotalBytes != 25 {
		t.Fatalf("files/bytes = %d/%d, want 4/25", got.Files, got.TotalBytes)
	}
	if got.Run != nil {
		t.Fatalf("no history: run must be absent, got %+v", got.Run)
	}

	// With this daemon's record of the run, the detail joins it by snapshot
	// time and carries the exact duration and the reason it was a full
	// backup (#1604).
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Append(BaselineRunRecord{ServerID: bootServerID, Kind: BaselineRunDump, Trigger: BaselineRunTriggerScheduled,
		SnapshotTime: "2026-06-10T12:00:00Z", StartedAt: "2026-06-10T11:58:30Z", FinishedAt: "2026-06-10T12:00:00Z",
		Why: BackupWhyNoLocalDir, WhyCode: "no_local_dir"}); err != nil {
		t.Fatal(err)
	}
	srv.baselineHistory = h
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Run == nil || got.Run.Kind != BaselineRunDump || got.Run.Seconds != 90 || got.Run.Why != BackupWhyNoLocalDir || got.Run.WhyCode != "no_local_dir" {
		t.Fatalf("run = %+v, want the recorded dump, 90 s, and its reason", got.Run)
	}
	// Span runs from the first parquet to the markers: 120s.
	if got.WriteSpanSeconds != 120 || got.WroteFrom != "2026-06-10 12:00:00" || got.WroteTo != "2026-06-10 12:02:00" {
		t.Fatalf("span = %v (%s → %s), want 120s", got.WriteSpanSeconds, got.WroteFrom, got.WroteTo)
	}
}

func TestBaselineFiles_refusals(t *testing.T) {
	dir := newDetailFixture(t)
	srv := newBaselineServer(t, dir, true)

	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery("2026-01-01 00:00:00"), "")
	if rec.Code != 404 {
		t.Fatalf("missing snapshot: code = %d, body = %s, want 404", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files?at=lunes", "")
	if rec.Code != 400 {
		t.Fatalf("bad at: code = %d, body = %s, want 400", rec.Code, body)
	}
	// No baseline source configured at all.
	srvNone := newBaselineServer(t, "", false)
	rec, body = doServersReq(t, srvNone, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 404 {
		t.Fatalf("unconfigured: code = %d, body = %s, want 404", rec.Code, body)
	}
}

func TestBaselineDownload_localTarRoundTrip(t *testing.T) {
	dir := newDetailFixture(t)
	// A stored views file, as the producers publish since #1583 — its paths
	// name wherever the snapshot lives, so the stream must REPLACE it, never
	// copy it: inside an unpacked tarball every one of those paths is wrong.
	storedViews := "-- stored copy naming " + dir + "\n"
	if err := os.WriteFile(filepath.Join(dir, detailSnapDir, views.SnapshotFileName), []byte(storedViews), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := newBaselineServer(t, dir, true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/download"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/gzip" {
		t.Fatalf("content-type = %q", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.Contains(cd, "dbtrail-backup-"+detailSnapDir+".tar.gz") {
		t.Fatalf("content-disposition = %q", cd)
	}
	got := untarAll(t, body)
	want := map[string]string{
		detailSnapDir + "/_MANIFEST":           `{"version":1}`,
		detailSnapDir + "/_SUCCESS":            "",
		detailSnapDir + "/shop/orders.parquet": "aaaa",
		detailSnapDir + "/shop/users.parquet":  "bbbbbbbb",
	}
	if len(got) != len(want)+1 {
		t.Fatalf("entries = %v, want those of %v plus the generated %s", keysOf(got), keysOf(want), views.SnapshotFileName)
	}
	for name, content := range want {
		if got[name] != content {
			t.Fatalf("entry %s = %q, want %q", name, got[name], content)
		}
	}
	// The tarball's own views file (#1583): relative, "./"-prefixed, and
	// never the stored copy or its absolute paths.
	vsql := got[detailSnapDir+"/"+views.SnapshotFileName]
	switch {
	case vsql == "":
		t.Fatalf("no generated %s in the archive; entries = %v", views.SnapshotFileName, keysOf(got))
	case vsql == storedViews:
		t.Fatal("the archive carries the STORED views file, whose paths name the server's disk")
	case !strings.Contains(vsql, "read_parquet('./shop/orders.parquet')"):
		t.Errorf("views file does not read './shop/orders.parquet' relative:\n%s", vsql)
	case strings.Contains(vsql, dir):
		t.Errorf("views file leaks the server-local path %s:\n%s", dir, vsql)
	}
}

func TestBaselineDownload_refusesIncomplete(t *testing.T) {
	rec409 := audittest.Install(t)
	dir := t.TempDir()
	writeBaselineFile(t, dir, "aaaa", time.Time{}, detailSnapDir, "shop", "orders.parquet")
	writeBaselineFile(t, dir, "", time.Time{}, detailSnapDir, "_INCOMPLETE")
	srv := newBaselineServer(t, dir, true)

	rec, body := doServersReq(t, srv, "GET", "/api/baselines/download"+detailQuery(detailSnapAt), "")
	if rec.Code != 409 {
		t.Fatalf("download: code = %d, body = %s, want 409", rec.Code, body)
	}
	// The refusal served no row data, so it must emit nothing: the audit
	// defer is registered only after every refusal has returned, and that
	// placement is the contract — prose is not a guard.
	for _, ev := range rec409.Events() {
		if ev.Action == "baseline.download" {
			t.Fatalf("a 409 refusal must not emit baseline.download, got %+v", ev)
		}
	}
	// The detail stays readable and says so.
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("files: code = %d, body = %s", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Incomplete {
		t.Fatal("files: want incomplete=true for an _INCOMPLETE snapshot")
	}
}

// fakeObjectStore serves an in-memory key space as the S3 seam.
type fakeObjectStore struct {
	objects map[string]string // key → content
	mtime   time.Time
}

func (f *fakeObjectStore) ListInfo(_ context.Context, prefix string) ([]storage.ObjectInfo, error) {
	var out []storage.ObjectInfo
	for k, v := range f.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, storage.ObjectInfo{Key: k, Size: int64(len(v)), LastModified: f.mtime})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (f *fakeObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	v, ok := f.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(v)), nil
}

func TestBaselineDownload_s3RoundTrip(t *testing.T) {
	fake := &fakeObjectStore{
		mtime: time.Date(2026, 6, 10, 12, 1, 0, 0, time.UTC),
		objects: map[string]string{
			detailSnapDir + "/shop/orders.parquet": "aaaa",
			detailSnapDir + "/_SUCCESS":            "",
			// A sibling snapshot that must NOT leak into this one's archive.
			"2026-06-01T00-00-00Z/shop/orders.parquet": "old",
		},
	}
	orig := newBaselineObjectStore
	newBaselineObjectStore = func(_ context.Context, src string) (baselineObjectStore, error) {
		if src != "s3://bkt/baselines" {
			t.Fatalf("store opened for %q", src)
		}
		return fake, nil
	}
	t.Cleanup(func() { newBaselineObjectStore = orig })

	srv := newBaselineServer(t, "s3://bkt/baselines", true)
	// Pre-seed the decimals memo the download's views file consults: there is
	// no bucket here, and letting DuckDB discover that costs this test ten
	// seconds of httpfs retries. A successful empty answer is the "no embedded
	// schema" shape a real S3 read of these fake bytes would settle on anyway.
	srv.rememberBaselineDecimals("s3://bkt/baselines@2026-06-10T12:00:00Z", nil, false)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/download"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	got := untarAll(t, body)
	if len(got) != 3 || got[detailSnapDir+"/shop/orders.parquet"] != "aaaa" {
		t.Fatalf("entries = %v, want exactly the requested snapshot's two files plus the generated %s",
			keysOf(got), views.SnapshotFileName)
	}
	// Relative here too: an S3 snapshot's tarball unpacks onto somebody's
	// laptop, where "s3://bkt/..." spellings would demand credentials the
	// unpacked copy no longer needs.
	if vsql := got[detailSnapDir+"/"+views.SnapshotFileName]; !strings.Contains(vsql, "read_parquet('./shop/orders.parquet')") ||
		strings.Contains(vsql, "s3://bkt") {
		t.Fatalf("views file should read relative paths, never the bucket:\n%s", vsql)
	}

	// And the detail endpoint over the same fake.
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("files: code = %d, body = %s", rec.Code, body)
	}
	var det baselineFilesResponse
	if err := json.Unmarshal(body, &det); err != nil {
		t.Fatal(err)
	}
	if len(det.Tables) != 1 || det.Tables[0].SizeBytes != 4 || det.TotalBytes != 4 || det.Files != 2 {
		t.Fatalf("detail = %+v, want one 4-byte table across 2 files", det)
	}
}

func untarAll(t *testing.T, b []byte) map[string]string {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	tr := tar.NewReader(gz)
	out := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar: %v", err)
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, tr); err != nil {
			t.Fatalf("tar body: %v", err)
		}
		out[hdr.Name] = buf.String()
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close (truncated archive?): %v", err)
	}
	return out
}

func keysOf(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// TestBaselineDetail_profileRefusal: baseline reads bypass RBAC redaction, so
// a session carrying a data profile must be refused BOTH new surfaces — the
// download most of all, since it is a full unredacted copy of every row.
func TestBaselineDetail_profileRefusal(t *testing.T) {
	dir := newDetailFixture(t)
	srv := newBaselineServer(t, dir, true)
	pol := &ext.AccessPolicy{Profile: "sensitive"}
	for _, path := range []string{
		"/api/baselines/files" + detailQuery(detailSnapAt),
		"/api/baselines/download" + detailQuery(detailSnapAt),
	} {
		req := httptest.NewRequest("GET", path, nil)
		req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol))
		w := httptest.NewRecorder()
		if strings.Contains(path, "download") {
			srv.handleBaselineDownload(w, req)
		} else {
			srv.handleBaselineFiles(w, req)
		}
		if w.Code != 403 {
			t.Fatalf("%s: code = %d body = %s, want 403 under a data profile", path, w.Code, w.Body.String())
		}
	}
}

// failingObjectStore serves the listing but dies on the second Get — the
// mid-stream shape the download's abort contract exists for.
type failingObjectStore struct {
	fakeObjectStore
	gets int
}

func (f *failingObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	f.gets++
	if f.gets >= 3 {
		return nil, errors.New("s3: connection reset")
	}
	return f.fakeObjectStore.Get(ctx, key)
}

// TestBaselineDownload_midStreamAbort pins the two halves of the abort
// contract: the handler panics with http.ErrAbortHandler (so the client's
// read FAILS instead of saving a truncated archive as a success), and the
// audit trail still records the aborted egress with the bytes that left.
func TestBaselineDownload_midStreamAbort(t *testing.T) {
	rec := audittest.Install(t)
	fake := &failingObjectStore{fakeObjectStore: fakeObjectStore{
		mtime: time.Date(2026, 6, 10, 12, 1, 0, 0, time.UTC),
		objects: map[string]string{
			// Sorted stream order: _SUCCESS (0 B), then orders (4 B of row
			// data LEAVES), then users — whose Get fails. sent > 0 at the
			// abort, which is exactly the egress the audit must not lose.
			detailSnapDir + "/_SUCCESS":            "",
			detailSnapDir + "/shop/orders.parquet": "aaaa",
			detailSnapDir + "/shop/users.parquet":  "bbbbbbbb",
		},
	}}
	orig := newBaselineObjectStore
	newBaselineObjectStore = func(_ context.Context, _ string) (baselineObjectStore, error) { return fake, nil }
	t.Cleanup(func() { newBaselineObjectStore = orig })

	srv := newBaselineServer(t, "s3://bkt/baselines", true)
	// Same memo seed as the round-trip test, same reason: there is no bucket
	// for the views file's footer read to reach.
	srv.rememberBaselineDecimals("s3://bkt/baselines@2026-06-10T12:00:00Z", nil, false)
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/baselines/download"+detailQuery(detailSnapAt), nil)
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				if r != http.ErrAbortHandler {
					t.Fatalf("panic = %v, want http.ErrAbortHandler", r)
				}
			}
		}()
		srv.handleBaselineDownload(w, req)
	}()
	if !panicked {
		t.Fatal("a mid-stream Get failure must abort the connection, not end the body cleanly")
	}
	var got *ext.AuditEvent
	for _, ev := range rec.Events() {
		if ev.Action == "baseline.download" {
			e := ev
			got = &e
		}
	}
	if got == nil {
		t.Fatal("an aborted download that already sent row data must still be audited")
	}
	// files counts what was HANDED OVER (the marker and orders; users never
	// left), not the snapshot's 3-file inventory.
	if got.Detail["aborted"] != "true" || got.Detail["bytes"] != "4" || got.Detail["files"] != "2" {
		t.Fatalf("audit detail = %v, want aborted=true, 4 bytes, 2 files handed over (never the snapshot inventory)", got.Detail)
	}
}

// TestBaselineDownload_viewsSQLReadsTheChain (#1718): the tarball's views.sql
// marks a table with a numbered chain (the chain body, over the digit glob), a
// table with the v0.83.0 pair (the legacy body), and a table with none (the
// file alone). Only the NAMES matter to the mark, so the delta files are
// stand-ins like the table files of the fixture.
func TestBaselineDownload_viewsSQLReadsTheChain(t *testing.T) {
	dir := newDetailFixture(t)
	base := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	for _, n := range []string{"orders.000000.posdel", "orders.000000.upserts", "orders.000001.posdel", "orders.000001.upserts",
		"users.posdel", "users.upserts"} {
		writeBaselineFile(t, dir, "d", base, detailSnapDir, "shop", n)
	}
	writeBaselineFile(t, dir, "cc", base, detailSnapDir, "shop", "plain.parquet")
	srv := newBaselineServer(t, dir, true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/download"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	got := untarAll(t, body)
	vsql := got[detailSnapDir+"/"+views.SnapshotFileName]
	if vsql == "" {
		t.Fatalf("no generated %s in the archive; entries = %v", views.SnapshotFileName, keysOf(got))
	}
	view := func(name string) string {
		i := strings.Index(vsql, "CREATE OR REPLACE VIEW \""+name+"\"")
		if i < 0 {
			t.Fatalf("no view %s in:\n%s", name, vsql)
		}
		j := strings.Index(vsql[i:], ";")
		return vsql[i : i+j]
	}
	if v := view("state_shop_orders"); !strings.Contains(v, "bintrail_latest") ||
		!strings.Contains(v, "./shop/orders.[0-9][0-9][0-9][0-9][0-9][0-9].upserts") {
		t.Errorf("orders view does not read the chain:\n%s", v)
	}
	if v := view("state_shop_users"); strings.Contains(v, "bintrail_latest") || !strings.Contains(v, "./shop/users.upserts") {
		t.Errorf("users view does not read the legacy pair:\n%s", v)
	}
	if v := view("state_shop_plain"); strings.Contains(v, "upserts") || strings.Contains(v, "file_row_number") {
		t.Errorf("plain view got a delta body:\n%s", v)
	}
	for _, name := range []string{"shop/orders.000001.posdel", "shop/users.posdel"} {
		if _, ok := got[detailSnapDir+"/"+name]; !ok {
			t.Errorf("delta file %s not in the archive; entries = %v", name, keysOf(got))
		}
	}

	// Half a pair: the files still download, the views file is left out.
	if err := os.Remove(filepath.Join(dir, detailSnapDir, "shop", "orders.000001.upserts")); err != nil {
		t.Fatal(err)
	}
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/download"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("half a pair: code = %d, body = %s", rec.Code, body)
	}
	got = untarAll(t, body)
	if _, ok := got[detailSnapDir+"/"+views.SnapshotFileName]; ok {
		t.Fatalf("half a pair still produced a %s; entries = %v", views.SnapshotFileName, keysOf(got))
	}
}
