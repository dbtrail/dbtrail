package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

func newCopyJobsServer(t *testing.T, withHistory bool) (*Server, *BaselineRunHistory, string) {
	t.Helper()
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{}}
	var hist *BaselineRunHistory
	if withHistory {
		hist, err = OpenBaselineHistory(filepath.Join(t.TempDir(), "console-baseline-history.json"))
		if err != nil {
			t.Fatal(err)
		}
		cfg.BaselineHistory = hist
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return srv, hist, addVerifyEntry(t, srv, "", "s3://b/base", "")
}

type copyJobsBody struct {
	Jobs []copyJobDTO `json:"jobs"`
	Cap  int          `json:"cap"`
}

func getCopyJobs(t *testing.T, srv *Server, id string) (int, copyJobsBody, string) {
	t.Helper()
	rec, body := doServersReqHeader(t, srv, "GET", "/api/servers/"+id+"/copy-jobs", "", id)
	var out copyJobsBody
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode: %v: %s", err, body)
		}
	}
	return rec.Code, out, string(body)
}

// A history file that did not open is refused, not served as an empty list:
// an empty list reads as "no job ever ran here".
func TestCopyJobs_refusedWithoutHistory(t *testing.T) {
	srv, _, id := newCopyJobsServer(t, false)
	code, _, body := getCopyJobs(t, srv, id)
	if code != http.StatusForbidden {
		t.Fatalf("got %d: %s", code, body)
	}
}

// With a copy job turned on and no history, the file is what failed; that is
// a different thing to tell the operator than "nothing here runs copies".
func TestCopyJobs_refusalNamesTheFileOnlyWhenCopiesAreOn(t *testing.T) {
	srv, _, id := newCopyJobsServer(t, false)
	_, _, off := getCopyJobs(t, srv, id)
	if !strings.Contains(off, "no copy job runs") || strings.Contains(off, "log") {
		t.Fatalf("copies off: %s", off)
	}
	srv.baselineCtrl = &stubBaselineCtrl{}
	code, _, on := getCopyJobs(t, srv, id)
	if code != http.StatusForbidden || !strings.Contains(on, "could not be opened") {
		t.Fatalf("copies on, no file: %d %s", code, on)
	}
}

// A compaction that keeps failing the same way is one entry whose end moves.
// It is ordered by that end, above the updates that ran in between, and says
// that it repeats so the page does not draw hours as one job's duration.
func TestCopyJobs_repeatingCompactionFailureOrdersByItsEnd(t *testing.T) {
	srv, hist, id := newCopyJobsServer(t, true)
	compact := func(start, end string) {
		t.Helper()
		if _, err := hist.AppendCompact(BaselineRunRecord{ServerID: id, StartedAt: start, FinishedAt: end, Error: "disk full"}); err != nil {
			t.Fatal(err)
		}
	}
	compact("2026-10-09T13:00:00Z", "2026-10-09T13:00:03Z")
	if err := hist.Append(BaselineRunRecord{ServerID: id, Kind: BaselineRunRefresh, StartedAt: "2026-10-09T13:05:00Z", FinishedAt: "2026-10-09T13:05:20Z"}); err != nil {
		t.Fatal(err)
	}
	compact("2026-10-09T14:05:00Z", "2026-10-09T14:05:03Z")
	_, out, body := getCopyJobs(t, srv, id)
	if len(out.Jobs) != 2 {
		t.Fatalf("want 2 entries: %s", body)
	}
	c := out.Jobs[0]
	if c.Kind != BaselineRunCompact || c.Outcome != CopyJobFailed || !c.Repeats ||
		c.StartedAt != "2026-10-09T13:00:00Z" || c.FinishedAt != "2026-10-09T14:05:03Z" {
		t.Fatalf("first entry = %+v", c)
	}
}

func TestCopyJobs_unknownServer(t *testing.T) {
	srv, _, id := newCopyJobsServer(t, true)
	rec, _ := doServersReqHeader(t, srv, "GET", "/api/servers/nope/copy-jobs", "", id)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404", rec.Code)
	}
}

// A server with a history and no job yet answers an empty LIST (never null,
// which the page would have to special-case) and still states the cap.
func TestCopyJobs_emptyIsAList(t *testing.T) {
	srv, _, id := newCopyJobsServer(t, true)
	code, out, body := getCopyJobs(t, srv, id)
	if code != http.StatusOK || !strings.Contains(body, `"jobs":[]`) || out.Cap != BaselineRunHistoryCap {
		t.Fatalf("got %d: %s", code, body)
	}
}

func TestCopyJobs_newestFirstWithOutcomes(t *testing.T) {
	srv, hist, id := newCopyJobsServer(t, true)
	recs := []BaselineRunRecord{
		{ServerID: id, Kind: BaselineRunDump, StartedAt: "2026-10-09T03:00:00Z", FinishedAt: "2026-10-09T03:16:40Z",
			SnapshotTime: "2026-10-09T03:00:05Z", Rows: 1200, Tables: 7, Why: "no earlier copy to update"},
		{ServerID: id, Kind: BaselineRunRefresh, StartedAt: "2026-10-09T04:00:00Z", FinishedAt: "2026-10-09T04:00:25Z",
			SnapshotTime: "2026-10-09T04:00:00Z", Events: 323469, Tables: 7, Carried: 5},
		// Failed AND published: only the upload failed.
		{ServerID: id, Kind: BaselineRunRefresh, StartedAt: "2026-10-09T05:00:00Z", FinishedAt: "2026-10-09T05:00:09Z",
			SnapshotTime: "2026-10-09T05:00:00Z", Error: "upload: access denied", Failure: &SnapshotFailure{Summary: "upload failed"}},
		{ServerID: id, Kind: BaselineRunRestore, StartedAt: "2026-10-09T06:00:00Z", FinishedAt: "2026-10-09T06:00:02Z", Error: "boom"},
	}
	for _, r := range recs {
		if err := hist.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	// The shape the schedule writes: the full-read kind whatever would have
	// run, start and end on the slot; a second identical skip moves the end.
	for _, at := range []string{"2026-10-09T07:00:00Z", "2026-10-09T07:05:00Z"} {
		if _, err := hist.AppendSkip(BaselineRunRecord{ServerID: id, Kind: BaselineRunDump, StartedAt: at, FinishedAt: at,
			SkipReason: "another copy job holds the server"}); err != nil {
			t.Fatal(err)
		}
	}
	// Another server's job must not appear.
	if err := hist.Append(BaselineRunRecord{ServerID: "other", Kind: BaselineRunDump, StartedAt: "2026-10-09T08:00:00Z", FinishedAt: "2026-10-09T08:00:01Z"}); err != nil {
		t.Fatal(err)
	}

	code, out, body := getCopyJobs(t, srv, id)
	if code != http.StatusOK {
		t.Fatalf("got %d: %s", code, body)
	}
	type want struct {
		kind, outcome string
		published     bool
	}
	wants := []want{
		{"", CopyJobSkipped, false},
		{BaselineRunRestore, CopyJobFailed, false},
		{BaselineRunRefresh, CopyJobFailed, true},
		{BaselineRunRefresh, CopyJobOK, true},
		{BaselineRunDump, CopyJobOK, true},
	}
	if len(out.Jobs) != len(wants) {
		t.Fatalf("got %d jobs, want %d: %s", len(out.Jobs), len(wants), body)
	}
	for i, w := range wants {
		j := out.Jobs[i]
		if j.Kind != w.kind || j.Outcome != w.outcome || j.Published != w.published {
			t.Errorf("job %d = %s/%s/published=%v, want %s/%s/%v", i, j.Kind, j.Outcome, j.Published, w.kind, w.outcome, w.published)
		}
	}
	skip, upload, refresh, dump := out.Jobs[0], out.Jobs[2], out.Jobs[3], out.Jobs[4]
	if skip.SkipReason != "another copy job holds the server" || skip.Trigger != BaselineRunTriggerScheduled ||
		skip.StartedAt != "2026-10-09T07:00:00Z" || skip.FinishedAt != "2026-10-09T07:05:00Z" || !skip.Repeats {
		t.Errorf("skip = %+v", skip)
	}
	if upload.Repeats || refresh.Repeats || dump.Repeats {
		t.Errorf("a job that ran once says it repeats: %+v %+v %+v", upload, refresh, dump)
	}
	if upload.Error != "upload: access denied" || upload.Summary != "upload failed" {
		t.Errorf("failed upload = %+v", upload)
	}
	if refresh.Events != 323469 || refresh.Tables != 7 || refresh.Carried != 5 || refresh.Rows != 0 {
		t.Errorf("refresh = %+v", refresh)
	}
	if dump.Rows != 1200 || dump.Why != "no earlier copy to update" || dump.StartedAt != "2026-10-09T03:00:00Z" || dump.FinishedAt != "2026-10-09T03:16:40Z" {
		t.Errorf("dump = %+v", dump)
	}
}

// The record's lists name tables one by one; none of them leaves this route.
func TestCopyJobs_namesNoTables(t *testing.T) {
	srv, hist, id := newCopyJobsServer(t, true)
	if err := hist.Append(BaselineRunRecord{
		ServerID: id, ServerName: "zz_server_name", Kind: BaselineRunRefresh,
		StartedAt: "2026-10-09T04:00:00Z", FinishedAt: "2026-10-09T04:00:25Z", SnapshotTime: "2026-10-09T04:00:00Z",
		RefusedTables: []RefusedTable{{Name: "zz_refused", Verdict: "refused", Reason: "zz_reason"}},
		LeftOutTables: []RefusedTable{{Name: "zz_leftout"}},
		LeftOutKeys:   []string{"zz_key"},
		NewTables:     []string{"zz_new"},
		DiskNote:      "zz_disk", TransportNote: "zz_transport", NewTablesUnchecked: "zz_unchecked",
	}); err != nil {
		t.Fatal(err)
	}
	code, out, body := getCopyJobs(t, srv, id)
	if code != http.StatusOK || len(out.Jobs) != 1 {
		t.Fatalf("got %d: %s", code, body)
	}
	if strings.Contains(body, "zz_") {
		t.Fatalf("the response carries a field it must not: %s", body)
	}
}

// A session that carries a data profile keeps the outcome and the counts and
// is not sent the free text, which can name a table the profile withholds.
func TestCopyJobs_restrictedSessionGetsNoFreeText(t *testing.T) {
	srv, hist, id := newCopyJobsServer(t, true)
	for _, r := range []BaselineRunRecord{
		{ServerID: id, Kind: BaselineRunDump, StartedAt: "2026-10-09T03:00:00Z", FinishedAt: "2026-10-09T03:16:40Z",
			SnapshotTime: "2026-10-09T03:00:05Z", Rows: 1200, Why: "zz_why names shop.cards"},
		{ServerID: id, Kind: BaselineRunRefresh, StartedAt: "2026-10-09T04:00:00Z", FinishedAt: "2026-10-09T04:00:09Z",
			Error: "zz_error in shop.cards", Failure: &SnapshotFailure{Summary: "zz_summary shop.cards"}},
	} {
		if err := hist.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := hist.AppendSkip(BaselineRunRecord{ServerID: id, Kind: BaselineRunRefresh, FinishedAt: "2026-10-09T07:00:00Z", SkipReason: "zz_skip"}); err != nil {
		t.Fatal(err)
	}
	call := func(pol *ext.AccessPolicy) string {
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/servers/"+id+"/copy-jobs", nil)
		req.SetPathValue("id", id)
		if pol != nil {
			req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol))
		}
		rec := httptest.NewRecorder()
		srv.handleCopyJobs(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
		}
		return rec.Body.String()
	}
	open := call(nil)
	for _, want := range []string{"zz_why", "zz_error", "zz_summary", "zz_skip"} {
		if !strings.Contains(open, want) {
			t.Errorf("an unrestricted session lost %s: %s", want, open)
		}
	}
	if strings.Contains(open, "detail_withheld") {
		t.Errorf("an unrestricted session is told detail was withheld: %s", open)
	}
	// A profile the service was started with restricts every session.
	srv.denyTables = []query.SchemaTable{{Schema: "shop", Table: "cards"}}
	if boot := call(nil); strings.Contains(boot, "zz_") || !strings.Contains(boot, `"detail_withheld":true`) {
		t.Fatalf("a startup profile did not withhold the free text: %s", boot)
	}
	srv.denyTables = nil
	closed := call(&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()})
	if strings.Contains(closed, "zz_") {
		t.Fatalf("a profiled session was sent free text: %s", closed)
	}
	for _, want := range []string{`"outcome":"failed"`, `"outcome":"skipped"`, `"rows":1200`, `"detail_withheld":true`} {
		if !strings.Contains(closed, want) {
			t.Errorf("a profiled session lost %s: %s", want, closed)
		}
	}
}
