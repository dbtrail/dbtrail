package console

import (
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// newFlipServer builds the shape the flip matters for: an archive source AND a
// baseline, so the file has both halves and leaving one out is visible.
func newFlipServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	// One expectation PER request the tests here make. sqlmock consumes them,
	// and a read that runs out does not fail the test -- it comes back as
	// ArchiveDiscoveryFailed, which renders a file with no events view for
	// entirely the wrong reason. That near-miss is why the count is generous
	// and why every assertion below also checks the discovery-failed wording is
	// absent.
	for range 8 {
		mock.ExpectQuery("FROM archive_state").WillReturnRows(
			sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
				AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))
	}

	srv := newViewsServer(t, dir, false)
	srv.cm.boot.db = db
	return srv
}

// The console download is the other producer of this file, and it has its own
// copy of the decision — a route that forgot to set OmitEvents would serve the
// expensive file forever while the CLI served the cheap one, with nothing to
// notice the divergence.

func TestViewsAPI_eventsViewIsOptIn(t *testing.T) {
	srv := newFlipServer(t)

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("default download: status %d: %s", rec.Code, body)
	}
	if strings.Contains(string(body), `CREATE OR REPLACE VIEW "events"`) {
		t.Errorf("the default download still defines the events view, so every reader "+
			"pays one Parquet footer read per archived file:\n%s", body)
	}
	// The impostor: an unreadable archive_state ALSO yields no events view, so
	// the assertion above would pass while proving nothing about the flip.
	//
	// Asserted POSITIVELY, against the source the header names. The negative
	// form this replaced could never fire twice over: the OmitEvents branch
	// returns BEFORE the discovery-failed branch, so that string is unreachable
	// in a default render, and the header spells it in the other order
	// ("could not be read from archive_state") anyway.
	if !strings.Contains(string(body), "s3://bkt/events/bintrail_id=aaaa") {
		t.Fatalf("the header does not name the discovered archive source, so discovery "+
			"failed and the missing events view says nothing about the default:\n%s", body)
	}
	if !strings.Contains(string(body), "-- events: not included in this file.") {
		t.Errorf("the default file does not say the events view was left out:\n%s", body)
	}
	// The state views are what the cheap file is FOR; without them this would
	// pass on an empty response.
	if !regexp.MustCompile(`CREATE OR REPLACE VIEW "[^"]+"."`).MatchString(string(body)) {
		t.Fatalf("the default download defines no state view either:\n%s", body)
	}

	rec, body = doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("include_events=1: status %d: %s", rec.Code, body)
	}
	if !strings.Contains(string(body), `CREATE OR REPLACE VIEW "events"`) {
		t.Errorf("include_events=1 did not add the events view:\n%s", body)
	}
}

// TestViewsAPI_liveLegNeedsTheEventsView: the leg hangs on that view, so asking
// for it alone is a request the route cannot honour. A 200 carrying a file
// without the leg would be the worst answer — the client asked, got success,
// and has to read the SQL to discover it did not happen.
func TestViewsAPI_liveLegNeedsTheEventsView(t *testing.T) {
	srv := newFlipServer(t)

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_live=1", "")
	if rec.Code != 400 {
		t.Fatalf("include_live without include_events: status %d, want 400: %s", rec.Code, body)
	}
	if !strings.Contains(string(body), "include_events=1") {
		t.Errorf("the refusal never names the parameter that fixes it: %s", body)
	}
}

// TestViewsAPI_includeEventsIsStrict: isTrue would read an unrecognized value as
// false, which here means a 200 and a file missing the view the caller asked
// for. Same reasoning as include_live, and it has to be asserted separately
// because they are separate parameters reaching separate call sites.
func TestViewsAPI_includeEventsIsStrict(t *testing.T) {
	srv := newFlipServer(t)
	for _, v := range []string{"yes", "on", "2", "maybe"} {
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events="+v, "")
		if rec.Code != 400 {
			t.Errorf("include_events=%q: status %d, want 400: %s", v, rec.Code, body)
		}
		// The parser is shared with include_live and takes the name as an
		// argument, so the one new way to get this wrong is passing the other
		// parameter's name: the caller who typo'd include_events would be told
		// to fix include_live. Status codes alone cannot see that.
		if !strings.Contains(string(body), "include_events") {
			t.Errorf("the refusal for include_events=%q names a different parameter: %s", v, body)
		}
	}
	for _, v := range []string{"1", "true"} {
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events="+v, "")
		if rec.Code != 200 {
			t.Errorf("include_events=%q: status %d, want 200: %s", v, rec.Code, body)
		}
		if !strings.Contains(string(body), `CREATE OR REPLACE VIEW "events"`) {
			t.Errorf("include_events=%q was accepted but added no events view", v)
		}
	}
}

// TestViewsAPI_refusesAFileWithNoViewInIt is the route's half of the same
// regression. errNoViewSources answers "this server has nothing to describe" —
// archives AND baselines both empty. This is the narrower shape the flip
// created: archives exist, so the card is offered and the capability says yes,
// but with the change log left out and no baseline snapshot there is no view to
// put in the file. It was served as a 200 with a views.sql attachment
// containing zero CREATE statements.
func TestViewsAPI_refusesAFileWithNoViewInIt(t *testing.T) {
	// newViewsServer with an empty baseline source: archives only.
	srv := newViewsServer(t, "", false)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for range 4 {
		mock.ExpectQuery("FROM archive_state").WillReturnRows(
			sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
				AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))
	}
	srv.cm.boot.db = db

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 404 {
		t.Fatalf("a file with no view in it was served: status %d\n%s", rec.Code, body)
	}
	for _, want := range []string{"no view at all", "Include the change log"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("the refusal never mentions %q, so it names neither the problem "+
				"nor the control that fixes it: %s", want, body)
		}
	}

	// Positive control: the same server WITH the change log has a view to
	// serve, so the refusal is about the empty file and not about the server.
	rec, body = doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("include_events=1 on the same server: status %d\n%s", rec.Code, body)
	}
	if !strings.Contains(string(body), `CREATE OR REPLACE VIEW "events"`) {
		t.Errorf("the 200 carries no events view:\n%s", body)
	}
}

// TestViewsAPI_defaultDownloadIsNotRefusedOverAnUnusedS3Setting: the events
// view is the half that reads the archives, so a default file whose only S3
// path would have been an archive reads nothing through httpfs and must not be
// refused over an S3 variable it never consults. The gate that decides this
// runs inside buildViewsInput, which is why the flag is a parameter there and
// not something the handler sets afterwards — set afterwards, the gate ran with
// the zero value and this 502'd while the CLI, which knows first, did not.
func TestViewsAPI_defaultDownloadIsNotRefusedOverAnUnusedS3Setting(t *testing.T) {
	t.Setenv(storage.EnvS3PathStyle, "yes-please")

	dir := t.TempDir()
	writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	srv := newViewsServer(t, dir, false)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for range 4 {
		mock.ExpectQuery("FROM archive_state").WillReturnRows(
			sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
				AddRow("aaaa", nil, "bkt", "events/bintrail_id=aaaa/f.parquet"))
	}
	srv.cm.boot.db = db

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("the default download was refused over an S3 setting its file never "+
			"reads: status %d\n%s", rec.Code, body)
	}
	if strings.Contains(string(body), "INSTALL httpfs") {
		t.Errorf("the default file loads httpfs, so it DOES read S3 and the refusal "+
			"would have been correct:\n%s", body)
	}

	// Positive control: asking for the view that DOES read S3 must still fail
	// loudly, or this test would pass on a gate that never refuses anything.
	rec, body = doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 502 {
		t.Errorf("a file that reads s3:// was served under a broken S3 setting: "+
			"status %d\n%s", rec.Code, body)
	}
}
