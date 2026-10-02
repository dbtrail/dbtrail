package console

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// #2014: the Overview's "Dashboards for the team" card. On a server whose
// snapshots go to S3 it hands out a views file that reads the bucket, so a
// teammate's DuckDB is always current with nothing downloaded; anywhere else
// it says why there is no such file. These drive the real route.

// dashS3 is a list-only S3 endpoint (the shape of reconstruct's #1740 fake):
// ListObjectsV2 over a fixed key set, everything else 404. fail answers every
// request 403, a bucket this process may not read.
type dashS3 struct {
	mu   sync.Mutex
	keys []string
	fail bool
	srv  *httptest.Server
}

type dashListResult struct {
	XMLName               xml.Name         `xml:"ListBucketResult"`
	Xmlns                 string           `xml:"xmlns,attr"`
	Name                  string           `xml:"Name"`
	Prefix                string           `xml:"Prefix"`
	KeyCount              int              `xml:"KeyCount"`
	MaxKeys               int              `xml:"MaxKeys"`
	Delimiter             string           `xml:"Delimiter,omitempty"`
	EncodingType          string           `xml:"EncodingType,omitempty"`
	IsTruncated           bool             `xml:"IsTruncated"`
	NextContinuationToken string           `xml:"NextContinuationToken,omitempty"`
	Contents              []dashListObject `xml:"Contents"`
	CommonPrefixes        []dashListPrefix `xml:"CommonPrefixes"`
}

type dashListObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type dashListPrefix struct {
	Prefix string `xml:"Prefix"`
}

func newDashS3(t *testing.T, keys ...string) *dashS3 {
	t.Helper()
	f := &dashS3{keys: slices.Sorted(slices.Values(keys))}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/aws-config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/aws-credentials")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "dashtestaccesskey")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "dashtestsecretkey")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, f.srv.URL)
	return f
}

func (f *dashS3) serve(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.mu.Lock()
	keys, fail := f.keys, f.fail
	f.mu.Unlock()
	if fail {
		http.Error(w, "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>", http.StatusForbidden)
		return
	}
	bucket, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if r.Method != http.MethodGet || bucket != "b" || q.Get("list-type") != "2" {
		http.Error(w, "<Error><Code>NoSuchKey</Code><Message>this store only lists</Message></Error>", http.StatusNotFound)
		return
	}
	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	after := q.Get("start-after")
	if tok := q.Get("continuation-token"); tok != "" {
		after = tok
	}
	encode := func(s string) string { return s }
	if q.Get("encoding-type") == "url" {
		encode = func(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "%2F", "/") }
	}
	out := dashListResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucket, Prefix: encode(prefix),
		MaxKeys: 1000, Delimiter: encode(delimiter), EncodingType: q.Get("encoding-type")}
	var lastPrefix string
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) || k <= after {
			continue
		}
		if delimiter != "" && strings.HasSuffix(after, delimiter) && strings.HasPrefix(k, after) {
			continue
		}
		entry, isPrefix := k, false
		if delimiter != "" {
			if i := strings.Index(k[len(prefix):], delimiter); i >= 0 {
				entry, isPrefix = k[:len(prefix)+i+len(delimiter)], true
			}
		}
		if isPrefix {
			if entry == lastPrefix {
				continue
			}
			out.CommonPrefixes = append(out.CommonPrefixes, dashListPrefix{encode(entry)})
			lastPrefix = entry
		} else {
			out.Contents = append(out.Contents, dashListObject{Key: encode(k), LastModified: "2026-10-01T00:00:00.000Z",
				ETag: `"d41d8cd98f00b204e9800998ecf8427e"`, StorageClass: "STANDARD"})
		}
		out.KeyCount++
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(out)
}

// snapKeys is one completed snapshot's objects under prefix.
func snapKeys(prefix, ts string, tables ...string) []string {
	out := []string{prefix + ts + "/_SUCCESS"}
	for _, tb := range tables {
		out = append(out, prefix+ts+"/"+tb+".parquet")
	}
	return out
}

func newDashServer(t *testing.T, src, fallback string) *Server {
	t.Helper()
	s := &Server{token: "t", version: "v0.96.0", cm: newConnManager(nil, false)}
	s.cm.boot = &bundle{baselineSrc: src, baselineFallbackSrc: fallback, baselineConfigured: src != ""}
	s.mux = s.buildHandler()
	return s
}

func getDashboards(t *testing.T, srv *Server) (int, dashboardsDTO, string) {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/dashboards", "")
	var doc dashboardsDTO
	if rec.Code == 200 {
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatalf("%v: %s", err, body)
		}
	}
	return rec.Code, doc, string(body)
}

// The file must carry no credential: not the env keys this process signs
// with, not the console token.
func assertNoCredentials(t *testing.T, raw string) {
	t.Helper()
	for _, bad := range []string{"dashtestaccesskey", "dashtestsecretkey", "Bearer "} {
		if strings.Contains(raw, bad) {
			t.Errorf("the response carries %q", bad)
		}
	}
}

func TestDashboardsAPI_s3OnlyServerGetsAFileThatReadsTheBucket_2014(t *testing.T) {
	const prefix = "dash-2014-only/"
	newDashS3(t, append(snapKeys(prefix, "2026-10-01T06-00-00Z", "demo/prices"),
		snapKeys(prefix, "2026-10-02T06-00-00Z", "demo/prices", "demo/orders")...)...)
	srv := newDashServer(t, "s3://b/"+prefix, "")
	code, doc, raw := getDashboards(t, srv)
	if code != 200 {
		t.Fatalf("code = %d: %s", code, raw)
	}
	if doc.State != "s3" || doc.S3 != "s3://b/"+prefix || doc.Snapshot != "2026-10-02 06:00:00" || doc.Tables != 2 || !doc.Follows {
		t.Fatalf("doc = %+v", doc)
	}
	for _, want := range []string{`glob('s3://b/` + prefix + `*/_SUCCESS')`, `CREATE OR REPLACE VIEW "demo"."prices"`, `CREATE OR REPLACE VIEW "demo"."orders"`, "PROVIDER credential_chain"} {
		if !strings.Contains(doc.ViewsSQL, want) {
			t.Errorf("views_sql lacks %s:\n%s", want, doc.ViewsSQL)
		}
	}
	// State views only: the events view would read the archives, a second
	// location the reader would need access to and a cost on every session.
	if strings.Contains(doc.ViewsSQL, `CREATE OR REPLACE VIEW "events"`) {
		t.Error("the dashboards file defines the events view")
	}
	// The archives were never asked about, so the header claims nothing
	// about them.
	if strings.Contains(doc.ViewsSQL, "none registered in archive_state") || !strings.Contains(doc.ViewsSQL, "not part of this file") {
		t.Errorf("the header misdescribes the archives:\n%s", doc.ViewsSQL[:min(len(doc.ViewsSQL), 1500)])
	}
	assertNoCredentials(t, raw)
}

// Edge: a server with BOTH a local folder and S3. The card points at S3, the
// one a teammate can reach, even when the local copy is newer.
func TestDashboardsAPI_bothLocationsPointsAtS3_2014(t *testing.T) {
	const prefix = "dash-2014-both/"
	newDashS3(t, snapKeys(prefix, "2026-10-01T06-00-00Z", "demo/prices")...)
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-10-02T06-00-00Z", "demo", "prices.parquet")
	srv := newDashServer(t, dir, "s3://b/"+prefix)
	code, doc, raw := getDashboards(t, srv)
	if code != 200 {
		t.Fatalf("code = %d: %s", code, raw)
	}
	if doc.State != "s3" || doc.S3 != "s3://b/"+prefix || doc.Snapshot != "2026-10-01 06:00:00" {
		t.Fatalf("doc = %+v", doc)
	}
	if doc.NewerLocal != "2026-10-02 06:00:00" {
		t.Errorf("newer_local = %q, want the local snapshot the bucket does not have yet", doc.NewerLocal)
	}
	if strings.Contains(doc.ViewsSQL, dir) {
		t.Errorf("the file names this host's folder:\n%s", doc.ViewsSQL)
	}
	if !strings.Contains(doc.ViewsSQL, "s3://b/"+prefix) {
		t.Errorf("the file does not read the bucket:\n%s", doc.ViewsSQL)
	}
}

// Edge: the snapshot bucket is in another region than the archives. The file
// reads only the snapshot bucket, so its region is pinned in the secret; the
// archive bucket, which the file never reads, must not make it ambiguous.
func TestDashboardsAPI_bucketInAnotherRegionIsPinned_2014(t *testing.T) {
	const prefix = "dash-2014-region/"
	newDashS3(t, snapKeys(prefix, "2026-10-02T06-00-00Z", "demo/prices")...)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
		AddRow("aaaa", "", "arch", "events/bintrail_id=aaaa/f.parquet"))
	srv := newDashServer(t, "s3://b/"+prefix, "")
	srv.cm.boot.db = db
	srv.rememberBucketRegion("b", "eu-west-1", true)
	srv.rememberBucketRegion("arch", "us-east-1", true)
	code, doc, raw := getDashboards(t, srv)
	if code != 200 {
		t.Fatalf("code = %d: %s", code, raw)
	}
	if doc.Region != "eu-west-1" {
		t.Errorf("region = %q, want eu-west-1", doc.Region)
	}
	if !strings.Contains(doc.ViewsSQL, "PROVIDER credential_chain, REGION 'eu-west-1'") {
		t.Errorf("the secret does not pin the bucket's region:\n%s", doc.ViewsSQL)
	}
	if strings.Contains(doc.ViewsSQL, "s3://arch/") {
		t.Errorf("the file reads the archive bucket:\n%s", doc.ViewsSQL)
	}
}

// Edge: a prefix with characters a URL or DuckDB's glob reads specially. The
// location comes back as typed. A space, a plus and an equals sign follow
// like any other name; a glob character ([ * ? {) gets no file at all, since
// DuckDB reads it as a wildcard and the file would find nothing (measured
// against an S3-compatible store: even the pinned read of the exact key
// answers "No files found").
func TestDashboardsAPI_prefixWithSpecialCharacters_2014(t *testing.T) {
	const plain = "dash 2014+team=1/"
	newDashS3(t, append(snapKeys(plain, "2026-10-02T06-00-00Z", "demo/prices"),
		snapKeys("dash 2014 [x]/", "2026-10-02T06-00-00Z", "demo/prices")...)...)
	code, doc, raw := getDashboards(t, newDashServer(t, "s3://b/"+plain, ""))
	if code != 200 {
		t.Fatalf("code = %d: %s", code, raw)
	}
	if doc.State != "s3" || doc.S3 != "s3://b/"+plain || doc.Tables != 1 || !doc.Follows {
		t.Fatalf("doc = %+v", doc)
	}
	if !strings.Contains(doc.ViewsSQL, `glob('s3://b/dash 2014+team=1/*/_SUCCESS')`) {
		t.Errorf("the file does not follow the prefix:\n%s", doc.ViewsSQL)
	}
	for _, globby := range []string{"dash 2014 [x]/", "a*b/", "what?/", "{a}/"} {
		code, doc, raw = getDashboards(t, newDashServer(t, "s3://b/"+globby, ""))
		if code != 200 || doc.State != "s3_pattern_chars" || doc.S3 != "s3://b/"+globby || doc.ViewsSQL != "" {
			t.Errorf("%q: code = %d doc = %+v %s", globby, code, doc, raw)
		}
	}
}

// Edge: tables a full read left out of the snapshot (#2006) are named, so a
// reader looking for one knows it is not missing by accident.
func TestDashboardsAPI_listsTablesLeftOutOfTheSnapshot_2014(t *testing.T) {
	const prefix = "dash-2014-leftout/"
	newDashS3(t, snapKeys(prefix, "2026-10-02T06-00-00Z", "demo/prices")...)
	srv := newDashServer(t, "s3://b/"+prefix, "")
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "history.json"))
	if err != nil {
		t.Fatal(err)
	}
	left, omitted := LeftOutTablesOf([]LeftOut{{Table: "demo.order/items",
		Reason: `the name holds a "/", which a snapshot cannot store as a file name`}})
	// The full read that left it out, then an update folded from it: the
	// newest snapshot is the update's, and the table is still not in it.
	for _, rec := range []BaselineRunRecord{
		{ServerID: bootServerID, Kind: BaselineRunDump, SnapshotTime: "2026-10-02T05:00:00Z", StartedAt: "2026-10-02T05:00:00Z", FinishedAt: "2026-10-02T05:01:00Z",
			LeftOutTables: left, LeftOutTablesOmitted: omitted},
		{ServerID: bootServerID, Kind: "refresh", SnapshotTime: "2026-10-02T06:00:00Z", StartedAt: "2026-10-02T06:00:00Z", FinishedAt: "2026-10-02T06:00:10Z"},
	} {
		if err := h.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	srv.baselineHistory = h
	code, doc, raw := getDashboards(t, srv)
	if code != 200 {
		t.Fatalf("code = %d: %s", code, raw)
	}
	if len(doc.LeftOutTables) != 1 || doc.LeftOutTables[0].Name != "demo.order/items" || !strings.Contains(doc.LeftOutTables[0].Reason, `holds a "/"`) {
		t.Fatalf("left_out_tables = %+v", doc.LeftOutTables)
	}
}

// Edge: S3 is set and nothing has finished uploading there. No file, and the
// state says so rather than "no snapshot".
func TestDashboardsAPI_s3SetNothingUploadedYet_2014(t *testing.T) {
	const prefix = "dash-2014-empty/"
	newDashS3(t, prefix+"2026-10-02T06-00-00Z/_INCOMPLETE")
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-10-02T06-00-00Z", "demo", "prices.parquet")
	srv := newDashServer(t, dir, "s3://b/"+prefix)
	code, doc, raw := getDashboards(t, srv)
	if code != 200 {
		t.Fatalf("code = %d: %s", code, raw)
	}
	if doc.State != "s3_empty" || doc.S3 != "s3://b/"+prefix || doc.ViewsSQL != "" {
		t.Fatalf("doc = %+v", doc)
	}
}

// A bucket that cannot be read is NOT "nothing uploaded": the fix is access,
// not waiting.
func TestDashboardsAPI_s3UnreadableIsNotEmpty_2014(t *testing.T) {
	const prefix = "dash-2014-denied/"
	f := newDashS3(t)
	f.fail = true
	srv := newDashServer(t, "s3://b/"+prefix, "")
	code, doc, raw := getDashboards(t, srv)
	if code != 200 {
		t.Fatalf("code = %d: %s", code, raw)
	}
	if doc.State != "s3_unreadable" || doc.Error == "" || doc.ViewsSQL != "" {
		t.Fatalf("doc = %+v", doc)
	}
	assertNoCredentials(t, raw)
}

// Edge: snapshots kept only on this machine. No file a teammate could not
// use; the state says local only.
func TestDashboardsAPI_localOnlyOffersNoFile_2014(t *testing.T) {
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-10-02T06-00-00Z", "demo", "prices.parquet")
	code, doc, raw := getDashboards(t, newDashServer(t, dir, ""))
	if code != 200 {
		t.Fatalf("code = %d: %s", code, raw)
	}
	if doc.State != "local_only" || doc.ViewsSQL != "" || doc.S3 != "" || doc.Snapshot != "2026-10-02 06:00:00" {
		t.Fatalf("doc = %+v", doc)
	}
}

// Edge: a server with no snapshot location and none taken.
func TestDashboardsAPI_noSnapshotYet_2014(t *testing.T) {
	code, doc, raw := getDashboards(t, newDashServer(t, "", ""))
	if code != 200 {
		t.Fatalf("code = %d: %s", code, raw)
	}
	if doc.State != "none" || doc.ViewsSQL != "" {
		t.Fatalf("doc = %+v", doc)
	}
	// A local folder with nothing in it yet: still local only, since adding
	// S3 is the advice either way, and no snapshot time is claimed.
	dir := t.TempDir()
	code, doc, raw = getDashboards(t, newDashServer(t, dir, ""))
	if code != 200 || doc.State != "local_only" || doc.Snapshot != "" {
		t.Fatalf("an empty local folder: code = %d doc = %+v %s", code, doc, raw)
	}
}

// Edge: a session under a data profile gets nothing, the way the views file
// and the SQL card refuse it: the file is a map past the profile's filter,
// and its table names are what the profile may hide.
func TestDashboardsAPI_profileRefuses_2014(t *testing.T) {
	const prefix = "dash-2014-profile/"
	newDashS3(t, snapKeys(prefix, "2026-10-02T06-00-00Z", "secret/payroll")...)
	srv := newDashServer(t, "s3://b/"+prefix, "")
	req := httptest.NewRequest("GET", "/api/dashboards", nil)
	req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{},
		&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
	w := httptest.NewRecorder()
	srv.handleDashboards(w, req)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "profile") {
		t.Fatalf("code = %d body = %s, want 403 naming the profile", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "payroll") || strings.Contains(w.Body.String(), "s3://") {
		t.Errorf("the refusal leaks a table or the location: %s", w.Body.String())
	}
}

// The route takes settings:read, the tier of the views file it hands out.
func TestDashboardsAPI_routePermission_2014(t *testing.T) {
	for _, r := range apiRoutePerms {
		if r.method == "GET" && r.pattern == "/api/dashboards" {
			if r.perm != ext.PermSettingsRead {
				t.Fatalf("GET /api/dashboards needs %s, want settings:read", r.perm)
			}
			return
		}
	}
	t.Fatal("GET /api/dashboards is not in the route permission table")
}
