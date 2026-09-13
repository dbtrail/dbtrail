package console

import (
	"strings"
	"testing"
)

// A generated views.sql names ONE backup location and every state view
// resolves a path under it. That is why #1571's fix here is a warning and not
// a merge: a file carrying both a local directory and an s3:// prefix
// produces views that half of its readers cannot open.
//
// What silence cost was quieter and worse. When the newest snapshot has aged
// out of local retention but still lives in the bucket, the file pins the
// older local one and reads as current: the state views describe a week-old
// table and nothing in the file says so.
func TestViewsFile_namesANewerSnapshotItDoesNotRead(t *testing.T) {
	local, bucketish := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, local, "2026-06-03T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, bucketish, "2026-06-10T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, local, bucketish)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if !strings.Contains(sql, "2026-06-03T12:00:00Z") {
		t.Fatalf("the file no longer pins the location it was asked for:\n%s", firstLines(sql, 20))
	}
	if !strings.Contains(sql, "2026-06-10T12:00:00Z") || !strings.Contains(sql, bucketish) {
		t.Errorf("the file pins the 06-03 snapshot and says nothing about the 06-10 one in the "+
			"other location. A reader cannot tell the state views are describing a week-old "+
			"table:\n%s", firstLines(sql, 20))
	}
	// And the ROUTE, not just the fact. The reader of this file has a checkbox,
	// not a command line, so a note that names a problem without naming the
	// control that fixes it is where they stop (the same rule LiveLegHowTo
	// keeps for the other half of this page).
	if !strings.Contains(sql, `tick "Works on another machine"`) {
		t.Errorf("the note says a newer snapshot exists elsewhere and not how to get it:\n%s", firstLines(sql, 25))
	}

	// The warning must not become a correction: the paths still have to
	// resolve, so nothing from the other location may appear as a view source.
	for _, line := range strings.Split(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		if strings.Contains(line, bucketish) {
			t.Errorf("a view reads from the OTHER location: %q. The file names one root and its "+
				"paths only resolve there; mixing them produces views nobody can open", line)
		}
	}
}

// The mirror case: when the pinned location IS the newest, the file says
// nothing. A note that fires either way is noise, and a reader who learns to
// skip it stops reading the one that matters.
func TestViewsFile_saysNothingWhenItAlreadyPinsTheNewest(t *testing.T) {
	local, bucketish := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, local, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, bucketish, "2026-06-03T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, local, bucketish)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), "NOTE: a newer snapshot") {
		t.Errorf("the file warns about a newer snapshot while already pinning the newest:\n%s",
			firstLines(string(body), 20))
	}
}

// An unreadable second location must not fail the download. The file is
// still correct about the location it does read; the warning is the only
// thing lost, and it degrades to silence rather than to an error.
func TestViewsFile_survivesAnUnreadableSecondLocation(t *testing.T) {
	local := t.TempDir()
	writeBaselineFixture(t, local, "2026-06-10T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, local, "/definitely/not/a/directory/1571")
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, want the file anyway; the location it reads is intact. body = %s",
			rec.Code, firstLines(string(body), 8))
	}
	if !strings.Contains(string(body), "2026-06-10T12:00:00Z") {
		t.Error("the pinned snapshot is missing from a file whose own location was readable")
	}
}

func firstLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}

// With "Works on another machine" already ticked, the newer snapshot is the
// LOCAL one -- and the route to it is to untick the box, which would hand a
// file of local paths to a reader who asked for one that travels. So the note
// states the fact and withholds the route.
//
// Guarded because the !req.PortableBaseline half survived mutation: nothing
// drove portable_baseline together with a newer local snapshot, so dropping it
// would tell a reader who has ALREADY ticked the box to tick it again, with CI
// green.
func TestViewsFile_withTheBoxTickedItStatesTheFactWithoutTheRoute(t *testing.T) {
	local, bucketish := t.TempDir(), t.TempDir()
	// Reversed against the test above: the newer snapshot is the local one, so
	// the file being generated (the portable, s3-rooted one) is the older.
	writeBaselineFixture(t, local, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, bucketish, "2026-06-03T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, local, bucketish)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?portable_baseline=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if !strings.Contains(sql, "2026-06-10T12:00:00Z") {
		t.Fatalf("the note does not name the newer snapshot at all:\n%s", firstLines(sql, 25))
	}
	if strings.Contains(sql, "Works on another machine") {
		t.Errorf("the file tells a reader who ALREADY ticked the box to tick it. The route that "+
			"would reach the newer snapshot here is to UNTICK it, and naming that route would "+
			"undo the one thing this download was asked for:\n%s", firstLines(sql, 25))
	}
}

// A second location that will not answer must SAY SO. Silence is
// indistinguishable from "the other location holds nothing newer", and the two
// lead to opposite actions: one operator stops looking, the other goes and
// checks. The archive half of the same header already says
// "(could not be read from archive_state; ...)" for exactly this reason.
func TestViewsFile_saysWhenTheOtherLocationDidNotAnswer(t *testing.T) {
	local := t.TempDir()
	writeBaselineFixture(t, local, "2026-06-03T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, local, "/definitely/not/a/directory/1571")
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	// Deliberately NOT the bare "could not be read": the archive half of the
	// same header carries that phrase for its own failure, so the loose form
	// passed while this line was absent entirely.
	if !strings.Contains(sql, "holds a newer snapshot could not be read") {
		t.Errorf("the file is indistinguishable from one whose other location was read and held "+
			"nothing newer:\n%s", firstLines(sql, 25))
	}
	if !strings.Contains(sql, "/definitely/not/a/directory/1571") {
		t.Errorf("the disclosure does not name WHICH location went unchecked:\n%s", firstLines(sql, 25))
	}
}

// Both locations holding the SAME snapshot is the steady state on a server
// whose local backups are uploaded, so the boundary matters: the note must
// fire only on a STRICTLY newer snapshot. Relaxing the comparison to
// "not older" would print a note pointing at the snapshot the file already
// reads, which is worse than silence -- it sends the operator to fetch a copy
// of what they have.
func TestViewsFile_saysNothingWhenBothLocationsHoldTheSameSnapshot(t *testing.T) {
	local, bucketish := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, local, "2026-06-03T12-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, bucketish, "2026-06-03T12-00-00Z", "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, local, bucketish)
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if sql := string(body); strings.Contains(sql, "a newer snapshot") {
		t.Errorf("the file claims a newer snapshot exists elsewhere when both locations hold the "+
			"same one:\n%s", firstLines(sql, 25))
	}
}
