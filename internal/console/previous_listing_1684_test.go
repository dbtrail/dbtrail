package console

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The Snapshots list over real folders (#1684): the current folder, a
// previous one holding this server's snapshots, another writer's signed one,
// and one written after the move, plus a previous folder that is gone.
func TestBaselinesAPI_previousLocations(t *testing.T) {
	cur, prev := t.TempDir(), t.TempDir()
	gone := filepath.Join(t.TempDir(), "unmounted")
	writeBaselineFixture(t, cur, "2026-09-28T00-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, prev, "2026-09-20T00-00-00Z", "shop", "orders.parquet") // this server's, unsigned
	writeBaselineFixture(t, prev, "2026-09-21T00-00-00Z", "shop", "orders.parquet") // this server's, signed
	writeBaselineFixture(t, prev, "2026-09-22T00-00-00Z", "shop", "orders.parquet") // another writer's
	writeBaselineFixture(t, prev, "2026-09-27T00-00-00Z", "shop", "orders.parquet") // after the move
	if err := baseline.WriteWriterMarker(filepath.Join(prev, "2026-09-21T00-00-00Z"), "me-1"); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteWriterMarker(filepath.Join(prev, "2026-09-22T00-00-00Z"), "other-2"); err != nil {
		t.Fatal(err)
	}
	prevOwn := bundleOwnWriter
	bundleOwnWriter = func(context.Context, *bundle) (string, error) { return "me-1", nil }
	t.Cleanup(func() { bundleOwnWriter = prevOwn })

	srv := newBaselineServer(t, cur, true)
	left := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	srv.cm.boot.previous = []PreviousLocation{{Location: prev, LeftAt: left}, {Location: gone, LeftAt: left}}
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselinesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	var times []string
	for _, sn := range got.Snapshots {
		times = append(times, sn.Time)
	}
	if want := "2026-09-28 00:00:00,2026-09-21 00:00:00,2026-09-20 00:00:00"; strings.Join(times, ",") != want {
		t.Fatalf("snapshots = %v, want %s", times, want)
	}
	if k := got.Snapshots[1].Kinds; strings.Join(k, ",") != "previous" {
		t.Fatalf("kinds of a previous-location snapshot = %v", k)
	}
	if !got.Incomplete {
		t.Fatal("a previous folder that cannot be read left the listing complete")
	}
	if len(got.Sources) != 3 || !got.Sources[1].Previous || got.Sources[1].Hidden != 2 || !got.Sources[2].Previous || got.Sources[2].Error == "" {
		t.Fatalf("sources = %+v", got.Sources)
	}
	if !strings.Contains(got.Sources[1].HiddenWhy, "other-2") {
		t.Fatalf("hidden_why does not name the other writer: %q", got.Sources[1].HiddenWhy)
	}
	if len(got.SharedWith) != 0 {
		t.Fatalf("a previous location raised the shared-location warning: %+v", got.SharedWith)
	}
	// The detail of a previous-location snapshot opens; another writer's does not.
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files?at=2026-09-21+00:00:00", "")
	if rec.Code != 200 {
		t.Fatalf("detail of this server's previous snapshot: %d %s", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files?at=2026-09-22+00:00:00", "")
	if rec.Code == 200 || !strings.Contains(string(body), "other-2") {
		t.Fatalf("detail of another writer's snapshot: %d %s", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "GET", "/api/baselines/files?at=2026-09-27+00:00:00", "")
	if rec.Code == 200 {
		t.Fatalf("detail of a snapshot written after the move: %d %s", rec.Code, body)
	}
}

// The Snapshots settings row: a move shows the place left, and forgetting it
// takes it off the list and deletes nothing.
func TestBackupSettingsAPI_previousLocations(t *testing.T) {
	fixedLocationClock(t, time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC))
	oldDir, newDir := t.TempDir(), t.TempDir()
	writeBaselineFixture(t, oldDir, "2026-09-20T00-00-00Z", "shop", "orders.parquet")
	reg, _ := LoadRegistry("")
	e, err := reg.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/idx", BaselineDir: oldDir})
	if err != nil {
		t.Fatal(err)
	}
	clearStores(t)
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	// A bundle already open for the server, as after a first selection: the
	// save must hand it the place left, not leave it with the old list.
	srv.cm.bundles[e.ID] = &bundle{}
	rec, body := doServersReq(t, srv, "PUT", "/api/backup-settings/servers/"+e.ID, `{"baseline_dir":"`+newDir+`"}`)
	if rec.Code != 200 {
		t.Fatalf("move: %d %s", rec.Code, body)
	}
	var dto backupSettingsServerDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	if len(dto.PreviousLocations) != 1 || dto.PreviousLocations[0].Location != oldDir || dto.PreviousLocations[0].LeftAt != "2026-09-29T10:00:00Z" {
		t.Fatalf("previous = %+v, want the folder left", dto.PreviousLocations)
	}
	// The selected server's bundle reads the place left: the listing shows it.
	if b := srv.cm.bundles[e.ID]; len(b.previous) != 1 || b.previous[0].Location != oldDir {
		t.Fatalf("the bundle does not read the place left: %+v", b.previous)
	}
	rec, body = doServersReq(t, srv, "PUT", "/api/backup-settings/servers/"+e.ID, `{"forget_previous_location":"`+oldDir+`/","keep_newest":2}`)
	if rec.Code != 400 {
		t.Fatalf("forget with another field: %d %s, want 400", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "PUT", "/api/backup-settings/servers/"+e.ID, `{"forget_previous_location":"`+oldDir+`/"}`)
	if rec.Code != 200 {
		t.Fatalf("forget: %d %s", rec.Code, body)
	}
	dto = backupSettingsServerDTO{}
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	if dto.PreviousLocations == nil || len(dto.PreviousLocations) != 0 {
		t.Fatalf("previous after forget = %#v, want an empty list", dto.PreviousLocations)
	}
	if _, err := os.Stat(filepath.Join(oldDir, "2026-09-20T00-00-00Z", "shop", "orders.parquet")); err != nil {
		t.Fatalf("forgetting deleted a snapshot: %v", err)
	}
	rec, _ = doServersReq(t, srv, "PUT", "/api/backup-settings/servers/"+e.ID, `{"forget_previous_location":"`+oldDir+`"}`)
	if rec.Code != 404 {
		t.Fatalf("forgetting twice: %d, want 404", rec.Code)
	}
}
