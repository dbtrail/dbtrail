package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func bf(ts string, schema, table, path string) reconstruct.BaselineFile {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		panic(err)
	}
	return reconstruct.BaselineFile{SnapshotTime: t, Schema: schema, Table: table, Path: path}
}

// fakeLister answers per source, so a test can put a local directory and a
// bucket side by side without either existing.
func fakeLister(bySource map[string][]reconstruct.BaselineFile, fail map[string]error) baselineLister {
	return func(_ context.Context, src string) ([]reconstruct.BaselineFile, int, error) {
		if err, bad := fail[src]; bad {
			return nil, 0, err
		}
		return bySource[src], 0, nil
	}
}

// TestMergedBaselines_unionsTheLocationsRatherThanFallingBack is the bug in
// #1542, stated as the case that distinguishes the two candidate fixes.
//
// The local directory is NOT empty: retention pruned it down to the newest
// snapshot while the durable S3 copies stayed (#616/#766). A
// "fall back when the primary is empty" rule answers with that one snapshot and
// hides every older one in the bucket, and it passes any test whose local
// directory happens to be empty. So this fixture has one snapshot on disk and
// two older ones only in S3.
func TestMergedBaselines_unionsTheLocationsRatherThanFallingBack(t *testing.T) {
	const dir, s3 = "/data/baselines", "s3://bucket/baselines"
	got := listBaselinesMerged(context.Background(), []string{dir, s3}, fakeLister(
		map[string][]reconstruct.BaselineFile{
			dir: {bf("2026-06-10T12:00:00Z", "shop", "orders", dir+"/2026-06-10T12-00-00Z/shop/orders.parquet")},
			s3: {
				bf("2026-06-10T12:00:00Z", "shop", "orders", s3+"/2026-06-10T12-00-00Z/shop/orders.parquet"),
				bf("2026-06-03T12:00:00Z", "shop", "orders", s3+"/2026-06-03T12-00-00Z/shop/orders.parquet"),
				bf("2026-05-27T12:00:00Z", "shop", "orders", s3+"/2026-05-27T12-00-00Z/shop/orders.parquet"),
			},
		}, nil))

	if len(got.Files) != 3 {
		t.Fatalf("merged %d file(s), want 3 (one on disk, two more only in the bucket): %+v", len(got.Files), got.Files)
	}
	want := []string{"2026-06-10T12:00:00Z", "2026-06-03T12:00:00Z", "2026-05-27T12:00:00Z"}
	for i, w := range want {
		if got.Files[i].SnapshotTime.Format(time.RFC3339) != w {
			t.Errorf("file[%d] = %s, want %s (newest first, across BOTH locations)",
				i, got.Files[i].SnapshotTime.Format(time.RFC3339), w)
		}
	}

	// The snapshot held in both is listed ONCE, and says so.
	newest := got.Files[0].SnapshotTime.UnixNano()
	if k := got.Kinds[newest]; len(k) != 2 || k[0] != "dir" || k[1] != "s3" {
		t.Errorf("newest snapshot kinds = %v, want [dir s3]: it is in both places", k)
	}
	if k := got.Kinds[got.Files[1].SnapshotTime.UnixNano()]; len(k) != 1 || k[0] != "s3" {
		t.Errorf("pruned snapshot kinds = %v, want [s3] only", k)
	}

	// And the surviving row keeps the LOCAL path: the footer read downstream
	// opens it directly, which is the latency this listing avoids over S3.
	if got.Files[0].Path != dir+"/2026-06-10T12-00-00Z/shop/orders.parquet" {
		t.Errorf("deduped file kept %q; the local copy has to win so the footer is read off disk", got.Files[0].Path)
	}

	if got.Listed != 2 || len(got.Sources) != 2 {
		t.Fatalf("listed %d of %d sources, want 2 of 2", got.Listed, len(got.Sources))
	}
	if got.Sources[0].Kind != "dir" || got.Sources[0].Count != 1 {
		t.Errorf("source[0] = %+v, want the local dir with 1 file", got.Sources[0])
	}
	if got.Sources[1].Kind != "s3" || got.Sources[1].Count != 3 {
		t.Errorf("source[1] = %+v, want the bucket with 3 files", got.Sources[1])
	}
}

// A location that fails must not blank a page whose other half works, and must
// not be silent about it either.
func TestMergedBaselines_oneLocationFailsAndSaysSo(t *testing.T) {
	const dir, s3 = "/data/baselines", "s3://bucket/baselines"
	got := listBaselinesMerged(context.Background(), []string{dir, s3}, fakeLister(
		map[string][]reconstruct.BaselineFile{
			dir: {bf("2026-06-10T12:00:00Z", "shop", "orders", dir+"/x.parquet")},
		},
		map[string]error{s3: errors.New("AccessDenied")}))

	if len(got.Files) != 1 {
		t.Fatalf("merged %d file(s), want the 1 readable one", len(got.Files))
	}
	if got.Listed != 1 || len(got.Sources) != 2 {
		t.Fatalf("listed %d of %d, want 1 of 2", got.Listed, len(got.Sources))
	}
	if got.Sources[1].Error == "" {
		t.Error("the failing location is reported with no error; the listing beside it is a SUBSET and nothing would say so")
	}
	if got.Sources[1].Count != 0 {
		t.Errorf("a failing location reported %d file(s); it contributed none", got.Sources[1].Count)
	}
}

func TestBaselineSourcesOf(t *testing.T) {
	if got := baselineSourcesOf(nil); got != nil {
		t.Errorf("nil bundle = %v, want nil", got)
	}
	if got := baselineSourcesOf(&bundle{}); got != nil {
		t.Errorf("unconfigured bundle = %v, want nil", got)
	}
	if got := baselineSourcesOf(&bundle{baselineSrc: "/d"}); len(got) != 1 || got[0] != "/d" {
		t.Errorf("dir only = %v, want [/d]", got)
	}
	got := baselineSourcesOf(&bundle{baselineSrc: "/d", baselineFallbackSrc: "s3://b/p"})
	if len(got) != 2 || got[0] != "/d" || got[1] != "s3://b/p" {
		t.Errorf("dir + bucket = %v, want [/d s3://b/p] in consult order", got)
	}
}

// newBaselineServerWithFallback is newBaselineServer for a server that keeps
// BOTH a local directory and an S3 destination — the shape the endpoint used to
// list only half of.
func newBaselineServerWithFallback(t *testing.T, src, fallback string) *Server {
	t.Helper()
	s := &Server{token: "t", cm: newConnManager(nil, false)}
	s.cm.boot = &bundle{baselineSrc: src, baselineFallbackSrc: fallback, baselineConfigured: true}
	s.mux = s.buildHandler()
	return s
}

// TestBaselinesAPI_listsBothLocations drives the real endpoint over two local
// directories. It cannot exercise the s3 KIND (there is no bucket here), which
// is what the merge unit test above is for; what it covers is that the handler
// consults the fallback at all and reports both locations.
func TestBaselinesAPI_listsBothLocations(t *testing.T) {
	primary, fallback := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, primary, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, fallback, "2026-06-03T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, fallback, "2026-05-27T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, primary, fallback)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Snapshots) != 3 {
		t.Fatalf("snapshots = %d, want 3 across both locations: %+v", len(got.Snapshots), got.Snapshots)
	}
	if got.Snapshots[0].Time != "2026-06-10 12:00:00" || got.Snapshots[2].Time != "2026-05-27 12:00:00" {
		t.Errorf("order = %v, want newest first across locations",
			[]string{got.Snapshots[0].Time, got.Snapshots[1].Time, got.Snapshots[2].Time})
	}
	if len(got.Sources) != 2 {
		t.Fatalf("sources = %+v, want both locations named", got.Sources)
	}
	if got.Incomplete {
		t.Error("both locations were readable; the listing must not claim to be a subset")
	}
	// Source/Kind still name the primary: they were the whole answer when there
	// was one location, and an existing client reading them must not start
	// getting a different location's path.
	if got.Source != primary || got.Kind != "dir" {
		t.Errorf("source/kind = %q/%q, want the primary %q/dir", got.Source, got.Kind, primary)
	}
}

func TestBaselinesAPI_servesWhatItCanWhenOneLocationFails(t *testing.T) {
	primary := t.TempDir()
	writeBaselineFixture(t, primary, "2026-06-10T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, primary, "/definitely/not/a/directory/1542")
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s — an unreadable second location must not blank a page whose first location works", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Snapshots) != 1 {
		t.Fatalf("snapshots = %d, want the 1 readable one", len(got.Snapshots))
	}
	if !got.Incomplete {
		t.Error("the response does not flag itself incomplete; a short list rendered as the whole set is the bug this endpoint had")
	}
	if len(got.Sources) != 2 || got.Sources[1].Error == "" {
		t.Errorf("sources = %+v, want the failing location named with its error", got.Sources)
	}
}

func TestBaselinesAPI_failsOnlyWhenNoLocationAnswers(t *testing.T) {
	srv := newBaselineServerWithFallback(t, "/nope/a/1542", "/nope/b/1542")
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 502 {
		t.Fatalf("code = %d, want 502 when nothing could be read: %s", rec.Code, body)
	}
	for _, want := range []string{"/nope/a/1542", "/nope/b/1542"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("the failure does not name %s; the operator is left guessing which location broke:\n%s", want, body)
		}
	}
}

// TestBaselineFilesAPI_reachesASnapshotOnlyInTheFallback is the half of #1542
// that the listing fix created.
//
// Before the merge this could not be asked: a snapshot held only in the second
// location had no row, so nothing linked to it. Now it has one, and opening the
// primary alone answers "no backup found" for a row the same page just said is
// there — and the Download button, which the frontend builds inside the success
// path, never appears for exactly the snapshots this feature exists to reveal.
func TestBaselineFilesAPI_reachesASnapshotOnlyInTheFallback(t *testing.T) {
	primary, fallback := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, primary, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, fallback, "2026-06-03T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, primary, fallback)
	rec, body := doServersReq(t, srv, "GET",
		"/api/baselines/files?at=2026-06-03T12:00:00Z", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s — the listing shows this snapshot; its detail must not deny it", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Tables) != 1 || got.Tables[0].Table != "orders" {
		t.Errorf("tables = %+v, want the one table of the fallback-only snapshot", got.Tables)
	}

	// And the primary still wins for a snapshot it holds, so the fallback is a
	// fallback and not a replacement.
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files?at=2026-06-10T12:00:00Z", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d for the primary's own snapshot: %s", rec.Code, body)
	}
}

// A location that cannot be read has to reach the log, not only the response
// body. The body is seen by whoever has the tab open and by nobody else — not
// cron, not alerting, not the journal — and a bucket that has been failing since
// a credential expiry is exactly the case that outlives a browser tab.
func TestMergedBaselines_logsAFailedLocation(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	const dir, s3 = "/data/baselines", "s3://bucket/baselines"
	listBaselinesMerged(context.Background(), []string{dir, s3}, fakeLister(
		map[string][]reconstruct.BaselineFile{dir: {bf("2026-06-10T12:00:00Z", "shop", "orders", dir+"/x.parquet")}},
		map[string]error{s3: errors.New("AccessDenied")}))

	out := buf.String()
	if !strings.Contains(out, s3) || !strings.Contains(out, "AccessDenied") {
		t.Errorf("the failing location was not logged with its source and error:\n%s", out)
	}
	if strings.Contains(out, dir) {
		t.Errorf("the location that WORKED was logged as a failure:\n%s", out)
	}
}
