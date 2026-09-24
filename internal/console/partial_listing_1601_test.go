package console

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// unreadableDir makes dir unreadable for the test. Root bypasses directory
// permissions, so the test skips there rather than pass on a no-op fixture.
func unreadableDir(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		// Under CI a root runner would turn every #1601 test green by skip
		// with no signal; fail there so the loss of coverage is seen.
		if os.Getenv("CI") != "" {
			t.Fatal("running as root under CI: the mode-000 fixture is a no-op and this coverage would silently vanish")
		}
		t.Skip("root bypasses directory read permissions; the mode-000 fixture is a no-op")
	}
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
}

// The merge carries the skip count per source and in total, so the Backups
// page's "incomplete" flag can see it.
func TestListBaselinesMerged_carriesTheSkipCount(t *testing.T) {
	ts := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	f := reconstruct.BaselineFile{Schema: "shop", Table: "orders", SnapshotTime: ts, Path: "/backups/2026-06-10T12-00-00Z/shop/orders.parquet"}
	lister := func(_ context.Context, src string) ([]reconstruct.BaselineFile, int, error) {
		if src == "/backups" {
			return []reconstruct.BaselineFile{f}, 2, nil
		}
		return nil, 0, errors.New("bucket down")
	}
	got := listBaselinesMerged(context.Background(), []string{"/backups", "s3://bucket/prefix"}, lister)
	if got.Skipped != 2 {
		t.Errorf("Skipped = %d, want 2", got.Skipped)
	}
	if len(got.Sources) != 2 || got.Sources[0].Skipped != 2 || got.Sources[0].Count != 1 {
		t.Errorf("per-source report lost the skip count: %+v", got.Sources)
	}
	if got.Listed != 1 {
		t.Errorf("Listed = %d, want 1: a partial answer still counts as an answer; the skip count is the extra signal", got.Listed)
	}
}

// The Backups page flags the listing incomplete when a location answered in
// part, exactly as it does when one did not answer.
func TestBaselinesAPI_partialListingIsIncomplete(t *testing.T) {
	local := t.TempDir()
	writeBaselineFixture(t, local, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, local, "2026-06-01T00-00-00Z", "shop", "orders.parquet")
	unreadableDir(t, filepath.Join(local, "2026-06-01T00-00-00Z"))

	srv := newBaselineServerWithFallback(t, local, "")
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var resp struct {
		Incomplete bool `json:"incomplete"`
		Sources    []struct {
			Skipped int `json:"skipped"`
		} `json:"sources"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Incomplete {
		t.Error("incomplete = false with an unreadable snapshot directory; the page shows a shorter list as the whole set")
	}
	if len(resp.Sources) == 0 || resp.Sources[0].Skipped != 1 {
		t.Errorf("the source report does not carry the skip count: %+v", resp.Sources)
	}
}

// The generated views.sql says when the OTHER location could only be read in
// part: the snapshot it could not read may be the newer one, and a header
// that says nothing reads as "nothing newer over there".
func TestViewsFile_namesAPartiallyReadableOtherLocation(t *testing.T) {
	local, bucketish := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, local, "2026-06-03T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, bucketish, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	unreadableDir(t, filepath.Join(bucketish, "2026-06-10T12-00-00Z"))

	srv := newBaselineServerWithFallback(t, local, bucketish)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, firstLines(string(body), 8))
	}
	sql := string(body)
	// The specific line, not the phrase: "could not be read" also appears in
	// other header notes, and a generic match let the case go unguarded.
	if !strings.Contains(sql, "whether "+bucketish+" holds a newer snapshot could not be read") {
		t.Errorf("the header does not say the other location could not be fully read:\n%s", firstLines(sql, 25))
	}
	if strings.Contains(sql, "NOTE: a newer snapshot") {
		t.Errorf("the header claims a newer snapshot it could not have listed:\n%s", firstLines(sql, 25))
	}
}

// And when the file's OWN location was read in part, the header says the
// pinned snapshot may not be the newest one there.
func TestViewsFile_namesAPartiallyReadableOwnLocation(t *testing.T) {
	local := t.TempDir()
	writeBaselineFixture(t, local, "2026-06-03T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, local, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	unreadableDir(t, filepath.Join(local, "2026-06-10T12-00-00Z"))

	srv := newBaselineServerWithFallback(t, local, "")
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, firstLines(string(body), 8))
	}
	sql := string(body)
	if !strings.Contains(sql, "2026-06-03T12:00:00Z") {
		t.Fatalf("the readable snapshot is not pinned:\n%s", firstLines(sql, 25))
	}
	if !strings.Contains(sql, "under this location could not be read") {
		t.Errorf("the header does not say the location was read in part:\n%s", firstLines(sql, 25))
	}
}

// A newer snapshot the other location DID list is still named, even when a
// directory beside it could not be read: the found fact is the more useful
// one, and "could not be read" would send the operator looking for a
// snapshot the header already knows.
func TestViewsFile_newerSnapshotReadBesideAnUnreadableOne(t *testing.T) {
	local, bucketish := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, local, "2026-06-03T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, bucketish, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, bucketish, "2026-06-12T12-00-00Z", "shop", "orders.parquet")
	unreadableDir(t, filepath.Join(bucketish, "2026-06-12T12-00-00Z"))

	srv := newBaselineServerWithFallback(t, local, bucketish)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, firstLines(string(body), 8))
	}
	sql := string(body)
	if !strings.Contains(sql, "NOTE: a newer snapshot") || !strings.Contains(sql, "2026-06-10T12:00:00Z") {
		t.Errorf("a newer snapshot that WAS read is not named:\n%s", firstLines(sql, 25))
	}
	if strings.Contains(sql, "holds a newer snapshot could not be read") {
		t.Errorf("the header says the check did not answer although it found a newer snapshot:\n%s", firstLines(sql, 25))
	}
}

// When EVERY snapshot directory under the file's own location is unreadable
// and nothing is archived, the answer is not "no baseline yet" (which sends
// the operator to take a backup they already have) but a refusal naming the
// unreadable count.
func TestViewsFile_allUnreadableIsNotNothingHere(t *testing.T) {
	local := t.TempDir()
	writeBaselineFixture(t, local, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	unreadableDir(t, filepath.Join(local, "2026-06-10T12-00-00Z"))

	srv := newBaselineServerWithFallback(t, local, "")
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code == 404 || rec.Code == 200 {
		t.Fatalf("code = %d, body = %s; an unreadable location must be neither \"nothing here\" nor a file", rec.Code, firstLines(string(body), 8))
	}
	if !strings.Contains(string(body), "could not be read") {
		t.Errorf("the refusal does not name the unreadable directory: %s", body)
	}
}

// When BOTH legs fail, the refusal names both: an operator who fixes the
// directory permission must not meet the archive refusal cold afterwards.
func TestViewsFile_allUnreadableNamesTheArchiveFailureToo(t *testing.T) {
	local := t.TempDir()
	writeBaselineFixture(t, local, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	unreadableDir(t, filepath.Join(local, "2026-06-10T12-00-00Z"))

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnError(errors.New("archive_state: access denied"))

	srv := newBaselineServerWithFallback(t, local, "")
	srv.cm.boot.db = db
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code == 404 || rec.Code == 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, firstLines(string(body), 8))
	}
	if !strings.Contains(string(body), "could not be read") || !strings.Contains(string(body), "access denied") {
		t.Errorf("the refusal must name BOTH the unreadable directory and the archive failure: %s", body)
	}
}
