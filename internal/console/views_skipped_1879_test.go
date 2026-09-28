package console

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/baseline"
)

const (
	viewsFullDir    = "2026-06-01T00-00-00Z" // a full read that skipped views
	viewsFullAt     = "2026-06-01 00:00:00"
	viewsCarriedDir = "2026-06-02T00-00-00Z" // updated from the recorded changes
	viewsCarriedAt  = "2026-06-02 00:00:00"
	viewsOldDir     = "2026-05-01T00-00-00Z" // written before the record existed
	viewsOldAt      = "2026-05-01 00:00:00"
	viewsBadDir     = "2026-05-02T00-00-00Z" // a record that cannot be read
	viewsBadAt      = "2026-05-02 00:00:00"
)

func writeViewsRecord(t *testing.T, root, dir string, carried bool, readAt time.Time, views ...string) {
	t.Helper()
	rec := baseline.NewViewsSkipped(views, readAt)
	rec.Carried = carried
	if err := baseline.WriteViewsSkipped(filepath.Join(root, dir), rec); err != nil {
		t.Fatal(err)
	}
}

// newViewsFixture is a snapshot location with two tables and one view in
// the source: a full read, an update carried from it, a snapshot from
// before the record, and one whose record cannot be read.
func newViewsFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{viewsFullDir, viewsCarriedDir, viewsOldDir, viewsBadDir} {
		writeBaselineFixture(t, root, dir, "shop", "orders.parquet")
		writeBaselineFixture(t, root, dir, "shop", "customers.parquet")
		writeBaselineFixture(t, root, dir, "_SUCCESS")
	}
	read := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	writeViewsRecord(t, root, viewsFullDir, false, read, "shop.big_orders")
	writeViewsRecord(t, root, viewsCarriedDir, true, read, "shop.big_orders")
	if err := os.WriteFile(filepath.Join(root, viewsBadDir, baseline.ViewsSkippedName), []byte(`{"version":9,"count":4}`), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

// rawRows returns the listing's snapshots as raw JSON objects by time, so a
// test can tell a key that is absent from one that is zero.
func rawRows(t *testing.T, srv *Server) map[string]map[string]json.RawMessage {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got struct {
		Snapshots []map[string]json.RawMessage `json:"snapshots"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]json.RawMessage{}
	for _, sn := range got.Snapshots {
		var at string
		if err := json.Unmarshal(sn["time"], &at); err != nil {
			t.Fatal(err)
		}
		out[at] = sn
	}
	return out
}

func viewKeys(row map[string]json.RawMessage) string {
	var keys []string
	for k, v := range row {
		if strings.HasPrefix(k, "views_") {
			keys = append(keys, k+"="+string(v))
		}
	}
	slices.Sort(keys)
	return strings.Join(keys, " ")
}

func TestBaselinesAPI_viewsSkippedOnTheRow(t *testing.T) {
	srv := newBaselineServer(t, newViewsFixture(t), true)
	rows := rawRows(t, srv)
	if len(rows) != 4 {
		t.Fatalf("listed %d snapshots, want 4: a record, readable or not, never costs a row", len(rows))
	}
	for at, want := range map[string]string{
		viewsFullAt:    `views_read_at="2026-06-01 00:00:00" views_skipped=1`,
		viewsCarriedAt: `views_carried=true views_read_at="2026-06-01 00:00:00" views_skipped=1`,
		// Not recorded: no key at all. A zero here would be drawn as a count.
		viewsOldAt: ``,
		viewsBadAt: ``,
	} {
		if got := viewKeys(rows[at]); got != want {
			t.Errorf("snapshot %s: %q, want %q", at, got, want)
		}
		if len(rows[at]["tables"]) == 0 {
			t.Errorf("snapshot %s lost its tables", at)
		}
	}
}

func viewsDetail(t *testing.T, srv *Server, at string) (baselineFilesResponse, string) {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(at), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	return got, string(body)
}

func TestBaselineFiles_viewsSkipped(t *testing.T) {
	srv := newBaselineServer(t, newViewsFixture(t), true)

	full, _ := viewsDetail(t, srv, viewsFullAt)
	v := full.ViewsSkipped
	if v == nil || v.Count != 1 || !slices.Equal(v.Names, []string{"shop.big_orders"}) || v.Omitted != 0 || v.Carried {
		t.Fatalf("full read: %+v", v)
	}
	if v.ReadAt != viewsFullAt {
		t.Errorf("full read: read_at = %q", v.ReadAt)
	}
	// The record is a file of the snapshot and not a table of it.
	if len(full.Tables) != 2 {
		t.Errorf("tables = %+v, want the two tables", full.Tables)
	}

	carried, _ := viewsDetail(t, srv, viewsCarriedAt)
	v = carried.ViewsSkipped
	if v == nil || !v.Carried || v.ReadAt != viewsFullAt || v.Count != 1 {
		t.Fatalf("carried: %+v, want carried and dated %s, not %s", v, viewsFullAt, viewsCarriedAt)
	}

	for _, at := range []string{viewsOldAt, viewsBadAt} {
		got, body := viewsDetail(t, srv, at)
		if got.ViewsSkipped != nil || strings.Contains(body, "views_skipped") {
			t.Errorf("snapshot %s with nothing recorded answers %s", at, body)
		}
	}
}

func TestBaselineFiles_viewsSkippedAreCappedAndCounted(t *testing.T) {
	root := t.TempDir()
	writeBaselineFixture(t, root, viewsFullDir, "shop", "orders.parquet")
	writeBaselineFixture(t, root, viewsFullDir, "_SUCCESS")
	var views []string
	for i := range 300 {
		views = append(views, fmt.Sprintf("shop.v%03d", i))
	}
	writeViewsRecord(t, root, viewsFullDir, false, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), views...)
	srv := newBaselineServer(t, root, true)

	got, _ := viewsDetail(t, srv, viewsFullAt)
	v := got.ViewsSkipped
	if v == nil || v.Count != 300 || len(v.Names) != ViewsSkippedCap || v.Omitted != 300-ViewsSkippedCap {
		t.Fatalf("got %+v, want 300 counted, %d named", v, ViewsSkippedCap)
	}
	if v.Names[0] != "shop.v000" || v.Names[ViewsSkippedCap-1] != fmt.Sprintf("shop.v%03d", ViewsSkippedCap-1) {
		t.Errorf("names = %q", v.Names)
	}
	if rows := rawRows(t, srv); viewKeys(rows[viewsFullAt]) != `views_read_at="2026-06-01 00:00:00" views_skipped=300` {
		t.Errorf("row: %q", viewKeys(rows[viewsFullAt]))
	}
}

// A name is sent as it is, as a JSON string: the page draws it as text.
func TestBaselineFiles_aViewNameIsDataNotMarkup(t *testing.T) {
	root := t.TempDir()
	writeBaselineFixture(t, root, viewsFullDir, "shop", "orders.parquet")
	const hostile = `shop.<img src=x onerror=alert(1)>`
	writeViewsRecord(t, root, viewsFullDir, false, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		hostile, "shop.two\nlines", "shop."+strings.Repeat("n", 400))
	srv := newBaselineServer(t, root, true)
	got, _ := viewsDetail(t, srv, viewsFullAt)
	v := got.ViewsSkipped
	if v == nil || v.Count != 3 || len(v.Names) != 3 {
		t.Fatalf("got %+v", v)
	}
	if !slices.Contains(v.Names, hostile) || !slices.Contains(v.Names, "shop.two lines") {
		t.Errorf("names = %q", v.Names)
	}
	for _, n := range v.Names {
		if len([]rune(n)) > refusedNameCap || strings.ContainsAny(n, "\n\r\t") {
			t.Errorf("name %q is not one bounded line", n)
		}
	}
}

// A snapshot with no record of its own takes the count this daemon kept for
// the full read that wrote it, of THIS server, and names nothing.
func TestViewsSkipped_fromTheRunHistory(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{viewsFullDir, viewsCarriedDir, viewsOldDir} {
		writeBaselineFixture(t, root, dir, "shop", "orders.parquet")
	}
	srv := newBaselineServer(t, root, true)
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	srv.baselineHistory = h
	for _, rec := range []BaselineRunRecord{
		// The selected server's full read, and another server's at the same instant.
		{ServerID: bootServerID, Kind: BaselineRunDump, SnapshotTime: "2026-06-01T00:00:00Z", ViewsSkipped: 3},
		{ServerID: "another", Kind: BaselineRunDump, SnapshotTime: "2026-06-01T00:00:00Z", ViewsSkipped: 7},
		{ServerID: "another", Kind: BaselineRunDump, SnapshotTime: "2026-05-01T00:00:00Z", ViewsSkipped: 9},
		// A full read that counted none, from before or after the count existed.
		{ServerID: bootServerID, Kind: BaselineRunDump, SnapshotTime: "2026-05-01T00:00:00Z"},
		// An update reads no views: a count on its record is not one.
		{ServerID: bootServerID, Kind: BaselineRunRefresh, SnapshotTime: "2026-06-02T00:00:00Z", ViewsSkipped: 5},
	} {
		rec.StartedAt, rec.FinishedAt = rec.SnapshotTime, rec.SnapshotTime
		if err := h.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	rows := rawRows(t, srv)
	for at, want := range map[string]string{viewsFullAt: "views_skipped=3", viewsOldAt: "", viewsCarriedAt: ""} {
		if got := viewKeys(rows[at]); got != want {
			t.Errorf("row %s: %q, want %q", at, got, want)
		}
	}
	got, _ := viewsDetail(t, srv, viewsFullAt)
	if v := got.ViewsSkipped; v == nil || v.Count != 3 || len(v.Names) != 0 || v.Omitted != 3 || v.Carried {
		t.Fatalf("detail: %+v, want 3 counted and none named", v)
	}
	for _, at := range []string{viewsOldAt, viewsCarriedAt} {
		if got, body := viewsDetail(t, srv, at); got.ViewsSkipped != nil {
			t.Errorf("detail %s: %s", at, body)
		}
	}

	// The snapshot's own record wins over the daemon's.
	writeViewsRecord(t, root, viewsFullDir, false, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "shop.a", "shop.b")
	if got := viewKeys(rawRows(t, srv)[viewsFullAt]); got != `views_read_at="2026-06-01 00:00:00" views_skipped=2` {
		t.Errorf("row with its own record: %q", got)
	}
	got, _ = viewsDetail(t, srv, viewsFullAt)
	if v := got.ViewsSkipped; v == nil || v.Count != 2 || !slices.Equal(v.Names, []string{"shop.a", "shop.b"}) {
		t.Fatalf("detail with its own record: %+v", v)
	}
}

// Over S3 the detail reads the record with one request, and only for a
// snapshot that holds one.
func TestBaselineFiles_viewsSkippedFromS3(t *testing.T) {
	rec, err := json.Marshal(baseline.ViewsSkipped{Version: 1, ReadAt: "2026-06-01T00:00:00Z", Carried: true,
		Count: 2, Views: []string{"shop.big_orders", "shop.totals"}})
	if err != nil {
		t.Fatal(err)
	}
	fake := &countingObjectStore{fakeObjectStore: fakeObjectStore{objects: map[string]string{
		detailSnapDir + "/shop/orders.parquet":          "aaaa",
		detailSnapDir + "/_SUCCESS":                     "",
		detailSnapDir + "/" + baseline.ViewsSkippedName: string(rec),
		viewsOldDir + "/shop/orders.parquet":            "aaaa",
		viewsOldDir + "/_SUCCESS":                       "",
	}}}
	orig := newBaselineObjectStore
	newBaselineObjectStore = func(context.Context, string) (baselineObjectStore, error) { return fake, nil }
	t.Cleanup(func() { newBaselineObjectStore = orig })
	srv := newBaselineServer(t, "s3://bkt/baselines", true)

	got, _ := viewsDetail(t, srv, detailSnapAt)
	v := got.ViewsSkipped
	if v == nil || v.Count != 2 || !v.Carried || v.ReadAt != viewsFullAt || !slices.Equal(v.Names, []string{"shop.big_orders", "shop.totals"}) {
		t.Fatalf("got %+v", v)
	}
	if !slices.Equal(fake.gets, []string{detailSnapDir + "/" + baseline.ViewsSkippedName}) {
		t.Errorf("objects read: %q, want the record and nothing else", fake.gets)
	}
	fake.gets = nil
	if got, body := viewsDetail(t, srv, viewsOldAt); got.ViewsSkipped != nil || len(fake.gets) != 0 {
		t.Errorf("a snapshot with no record: %s, objects read %q", body, fake.gets)
	}
}

type countingObjectStore struct {
	fakeObjectStore
	gets []string
}

func (c *countingObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	c.gets = append(c.gets, key)
	return c.fakeObjectStore.Get(ctx, key)
}

// A session with a data profile is refused the listing and the detail, so
// the name of a view in a schema its profile hides never reaches it.
func TestViewsSkipped_noNamesForASessionWithADataProfile(t *testing.T) {
	srv := newBaselineServer(t, newViewsFixture(t), true)
	pol := &ext.AccessPolicy{Profile: "sensitive"}
	for path, handler := range map[string]func(*httptest.ResponseRecorder, *Server){
		"/api/baselines": func(w *httptest.ResponseRecorder, s *Server) {
			req := httptest.NewRequest("GET", "/api/baselines", nil)
			s.handleBaselines(w, req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol)))
		},
		"/api/baselines/files": func(w *httptest.ResponseRecorder, s *Server) {
			req := httptest.NewRequest("GET", "/api/baselines/files"+detailQuery(viewsFullAt), nil)
			s.handleBaselineFiles(w, req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol)))
		},
	} {
		w := httptest.NewRecorder()
		handler(w, srv)
		if w.Code != 403 {
			t.Errorf("%s: code = %d, want 403", path, w.Code)
		}
		if body := w.Body.String(); strings.Contains(body, "big_orders") || strings.Contains(body, "views_skipped") {
			t.Errorf("%s: a session with a data profile was told about the views: %s", path, body)
		}
	}
}

func TestViewsSkippedOf(t *testing.T) {
	if got := viewsSkippedOf(baseline.ViewsSkipped{}); got != nil {
		t.Errorf("no views: %+v, want nothing", got)
	}
	if got := viewsSkippedOf(baseline.ViewsSkipped{Count: -2, Views: nil}); got != nil {
		t.Errorf("a negative count: %+v, want nothing", got)
	}
	got := viewsSkippedOf(baseline.ViewsSkipped{Count: 2, Views: []string{"a.v", " "}, ReadAt: "not a date", Carried: true})
	if got == nil || got.Count != 2 || !slices.Equal(got.Names, []string{"a.v"}) || got.Omitted != 1 || got.ReadAt != "" || !got.Carried {
		t.Errorf("got %+v", got)
	}
}
