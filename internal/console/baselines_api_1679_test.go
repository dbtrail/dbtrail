package console

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1679: the Backups page lists the newest snapshots only, one more than it
// shows, so the listing's cost follows the page and Truncated is still
// known. Before, every location was listed whole (two sweeps of an S3
// prefix per location, per request) and the cap only cut the response.
func TestHandleBaselines_asksForOnePastTheCap(t *testing.T) {
	rep := &stubScheduleReporter{full: true}
	srv, id := newScheduleServer(t, rep)
	e, _ := srv.cm.reg.Get(id)
	srv.cm.bundles[id].baselineSrc = e.BaselineDir
	prev := listBaselinesForPage
	t.Cleanup(func() { listBaselinesForPage = prev })
	var asked []int
	// The listing answers SHORT of the window with more behind it: the
	// shape of a window whose newest snapshots include an incomplete one.
	answer := baselinesMaxSnapshots - 2
	listBaselinesForPage = func(_ context.Context, source string, newest int) ([]reconstruct.BaselineFile, int, bool, error) {
		asked = append(asked, newest)
		var files []reconstruct.BaselineFile
		for i := range answer {
			files = append(files, reconstruct.BaselineFile{SnapshotTime: time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC).Add(-time.Duration(i) * time.Hour),
				Schema: "shop", Table: "orders", Path: fmt.Sprintf("%s/snap%d/shop/orders.parquet", source, i)})
		}
		return files, 0, true, nil
	}
	rec, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	if len(asked) != 1 || asked[0] != baselinesMaxSnapshots+1 {
		t.Fatalf("listing asked for %v snapshots, want one call for %d (the cap plus one)", asked, baselinesMaxSnapshots+1)
	}
	var resp struct {
		Snapshots []json.RawMessage `json:"snapshots"`
		Truncated bool              `json:"truncated"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Snapshots) != answer || !resp.Truncated {
		t.Fatalf("snapshots=%d truncated=%v, want %d and truncated: the listing said older ones exist", len(resp.Snapshots), resp.Truncated, answer)
	}
	// And a listing that says it is everything is not truncated.
	listBaselinesForPage = func(_ context.Context, source string, newest int) ([]reconstruct.BaselineFile, int, bool, error) {
		return []reconstruct.BaselineFile{{SnapshotTime: time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC), Schema: "shop", Table: "orders", Path: source + "/snap/shop/orders.parquet"}}, 0, false, nil
	}
	_, body = doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	var again struct {
		Snapshots []json.RawMessage `json:"snapshots"`
		Truncated bool              `json:"truncated"`
	}
	if err := json.Unmarshal(body, &again); err != nil { // a fresh struct: an omitted false would leave the first decode's true
		t.Fatal(err)
	}
	if len(again.Snapshots) != 1 || again.Truncated {
		t.Fatalf("snapshots=%d truncated=%v, want one and not truncated", len(again.Snapshots), again.Truncated)
	}
}
