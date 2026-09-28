package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/doctor"
)

func snapshotExpiryGet(t *testing.T, srv *Server, id string) (int, doctor.SnapshotExpiry) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/backup-settings/servers/"+id+"/expiry", nil)
	req.SetPathValue("id", id)
	srv.handleSnapshotExpiry(rec, req)
	var v doctor.SnapshotExpiry
	if rec.Code == 200 {
		if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
			t.Fatalf("decode %q: %v", rec.Body.String(), err)
		}
	}
	return rec.Code, v
}

// TestSnapshotExpiry_threeStatesReachTheWire: each state leaves the handler
// as itself. The one that matters most is the third: a read that failed is
// "unreadable" on the wire, with its reason, and never "none".
func TestSnapshotExpiry_threeStatesReachTheWire(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "", "")
	answers := map[string]doctor.SnapshotExpiry{
		"s3://b/ruled":  {State: doctor.ExpiryInForce, Bucket: "b", Prefix: "ruled", Days: 30, RuleID: "dbtrail-backups-expire-30d"},
		"s3://b/bare":   {State: doctor.ExpiryNone, Bucket: "b", Prefix: "bare", Conditional: 1},
		"s3://b/denied": {State: doctor.ExpiryUnreadable, Bucket: "b", Prefix: "denied", Reason: doctor.ReasonDenied, Error: "api error AccessDenied"},
	}
	var asked []string
	srv.snapshotExpiry.read = func(_ context.Context, url string) doctor.SnapshotExpiry {
		asked = append(asked, url)
		return answers[url]
	}
	for url, want := range answers {
		e, err := srv.cm.reg.Add(ServerEntry{Name: "s-" + want.Prefix, DSN: "u:p@tcp(h:3306)/idx_" + want.Prefix, BaselineS3: url})
		if err != nil {
			t.Fatal(err)
		}
		code, got := snapshotExpiryGet(t, srv, e.ID)
		if code != 200 || got != want {
			t.Errorf("%s: code %d, got %+v, want %+v", url, code, got, want)
		}
	}
	if len(asked) != len(answers) {
		t.Errorf("asked S3 %d times for %d destinations: %v", len(asked), len(answers), asked)
	}
}

// TestSnapshotExpiry_noS3DestinationDoesNotApply: a server that keeps its
// snapshots in a folder, or inherits the daemon's S3 default, has no bucket
// rule of its own to read, and S3 is not asked.
func TestSnapshotExpiry_noS3DestinationDoesNotApply(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "", "s3://daemon/default")
	srv.snapshotExpiry.read = func(context.Context, string) doctor.SnapshotExpiry {
		t.Error("S3 was asked for a server with no S3 destination of its own")
		return doctor.SnapshotExpiry{}
	}
	local, err := srv.cm.reg.Add(ServerEntry{Name: "local", DSN: "u:p@tcp(h:3306)/idx1", BaselineDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	inherits, err := srv.cm.reg.Add(ServerEntry{Name: "inherits", DSN: "u:p@tcp(h:3306)/idx2"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{local.ID, inherits.ID} {
		code, got := snapshotExpiryGet(t, srv, id)
		if code != 200 || got.State != snapshotExpiryNotApplicable {
			t.Errorf("%s: code %d, state %q, want 200 and %q", id, code, got.State, snapshotExpiryNotApplicable)
		}
	}
	if code, _ := snapshotExpiryGet(t, srv, "nope"); code != 404 {
		t.Errorf("unknown server: code %d, want 404", code)
	}
}

// TestSnapshotExpiry_answersAreReusedForAShortWhile: a page that redraws its
// rows asks S3 once; an answer that was read lasts longer than a failure;
// and the read is bounded and survives the request that started it.
func TestSnapshotExpiry_answersAreReusedForAShortWhile(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "", "")
	e, err := srv.cm.reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx", BaselineS3: "s3://b/backups"})
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	srv.snapshotExpiry.now = func() time.Time { return clock }
	reads := 0
	next := doctor.SnapshotExpiry{State: doctor.ExpiryNone, Bucket: "b", Prefix: "backups"}
	srv.snapshotExpiry.read = func(ctx context.Context, _ string) doctor.SnapshotExpiry {
		reads++
		dl, ok := ctx.Deadline()
		if !ok || time.Until(dl) > snapshotExpiryBudget {
			t.Errorf("the read carries no deadline within %s", snapshotExpiryBudget)
		}
		if ctx.Err() != nil {
			t.Errorf("the read inherited the request's cancellation: %v", ctx.Err())
		}
		return next
	}
	get := func() doctor.SnapshotExpiry {
		rec := httptest.NewRecorder()
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // the tab went away
		req := httptest.NewRequest("GET", "/x", nil).WithContext(ctx)
		req.SetPathValue("id", e.ID)
		srv.handleSnapshotExpiry(rec, req)
		var v doctor.SnapshotExpiry
		if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		return v
	}

	get()
	get()
	if reads != 1 {
		t.Fatalf("two requests inside the window read S3 %d times, want 1", reads)
	}
	clock = clock.Add(snapshotExpiryTTL)
	next = doctor.SnapshotExpiry{State: doctor.ExpiryUnreadable, Reason: doctor.ReasonError, Error: "timeout"}
	if v := get(); reads != 2 || v.State != doctor.ExpiryUnreadable {
		t.Fatalf("after the window: %d reads, state %q; want a second read and its answer", reads, v.State)
	}
	// A failure is asked again sooner, and what it is replaced by is what
	// the page gets: the old failure does not linger.
	clock = clock.Add(snapshotExpiryRetryTTL)
	next = doctor.SnapshotExpiry{State: doctor.ExpiryInForce, Days: 30}
	if v := get(); reads != 3 || v.State != doctor.ExpiryInForce {
		t.Fatalf("after a failure's window: %d reads, state %q; want a third read and in_force", reads, v.State)
	}
}

func TestSnapshotExpiry_routeIsClassified(t *testing.T) {
	if p, ok := permForRoute("GET", "/api/backup-settings/servers/abc/expiry"); !ok || p != ext.PermSettingsRead {
		t.Errorf("permForRoute = (%q,%v), want (%q,true)", p, ok, ext.PermSettingsRead)
	}
}
