//go:build integration

package status_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2664 against a real index: CollectStatus (behind `bintrail status`,
// GET /api/status and the MCP status tool) and CollectCoverageSummary (behind
// GET /api/coverage) read the same stream_state row and the same partitions,
// and must say the same word about an acknowledged loss. The fixture tests set
// StreamStateInfo.Floor by hand; only this one proves the collector fills it.
func TestAcknowledgedLossWindow_statusAndCoverageAgree(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	exec("INSERT INTO stream_state (id, mode, server_id, last_checkpoint) VALUES (1, 'gtid', 7, UTC_TIMESTAMP())")

	floor, err := status.OldestDeltaFromDB(ctx, db, dbName)
	if err != nil || floor.Hour.IsZero() || floor.BelowIsUnknown {
		t.Fatalf("fixture: delta floor = %+v, err %v; want a known floor", floor, err)
	}

	type doc struct {
		Stream struct {
			Continuity struct {
				Status string `json:"status"`
			} `json:"continuity"`
			GapLost         *struct{} `json:"gap_lost"`
			GapAcknowledged *struct{} `json:"gap_acknowledged"`
		} `json:"stream"`
	}
	read := func() (text string, d doc, coverage string) {
		t.Helper()
		data, err := status.CollectStatus(ctx, db, dbName)
		if err != nil {
			t.Fatalf("CollectStatus: %v", err)
		}
		var tb, jb bytes.Buffer
		data.Write(&tb)
		if err := data.WriteJSON(&jb); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(jb.Bytes(), &d); err != nil {
			t.Fatalf("invalid JSON: %v\n%s", err, jb.String())
		}
		sum, err := status.CollectCoverageSummary(ctx, db, dbName, time.Now().UTC())
		if err != nil {
			t.Fatalf("CollectCoverageSummary: %v", err)
		}
		return tb.String(), d, sum.Continuity
	}
	continuityLine := func(text string) string {
		for _, l := range strings.Split(text, "\n") {
			if strings.Contains(l, "Continuity:") {
				return l
			}
		}
		return ""
	}
	before := floor.Hour.Add(-30 * 24 * time.Hour).Format("2006-01-02 15:04:05")
	inside := floor.Hour.Add(10 * time.Minute).Format("2006-01-02 15:04:05")

	for _, c := range []struct {
		name          string
		lostAt, acked string // acked "" = not acknowledged
		want          string
		wantLine      string
	}{
		{"acknowledged, older than the changes still held", before, floor.Hour.Format("2006-01-02 15:04:05"),
			status.ContinuityOK, "no gaps in the captured range"},
		{"acknowledged, inside the changes still held", inside, floor.Hour.Add(20 * time.Minute).Format("2006-01-02 15:04:05"),
			status.ContinuityGapAcknowledged, "no gaps since the loss acknowledged below"},
		{"not acknowledged, older than the changes still held", before, "",
			status.ContinuityGapLost, "GAP LOST at " + before},
		{"not acknowledged, inside the changes still held", inside, "",
			status.ContinuityGapLost, "GAP LOST at " + inside},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.acked == "" {
				exec("UPDATE stream_state SET gap_lost_at = ?, gap_lost_detail = 'binlogs purged', gap_acked_at = NULL WHERE id = 1", c.lostAt)
			} else {
				exec("UPDATE stream_state SET gap_lost_at = ?, gap_lost_detail = 'binlogs purged', gap_acked_at = ? WHERE id = 1", c.lostAt, c.acked)
			}
			text, d, coverage := read()
			t.Logf("status text: %s", continuityLine(text))
			t.Logf("status JSON: %q; coverage summary: %q", d.Stream.Continuity.Status, coverage)
			if d.Stream.Continuity.Status != c.want {
				t.Errorf("status JSON continuity = %q, want %q", d.Stream.Continuity.Status, c.want)
			}
			if coverage != d.Stream.Continuity.Status {
				t.Errorf("the coverage summary says %q and status says %q about one index", coverage, d.Stream.Continuity.Status)
			}
			if !strings.Contains(continuityLine(text), c.wantLine) {
				t.Errorf("status text continuity line = %q, want it to contain %q", continuityLine(text), c.wantLine)
			}
			if got, want := d.Stream.GapAcknowledged != nil, c.want == status.ContinuityGapAcknowledged; got != want {
				t.Errorf("gap_acknowledged present = %v, want %v", got, want)
			}
			if got, want := d.Stream.GapLost != nil, c.want == status.ContinuityGapLost; got != want {
				t.Errorf("gap_lost present = %v, want %v", got, want)
			}
			if banner := strings.Contains(text, "EVENTS PERMANENTLY LOST"); banner != (c.want == status.ContinuityGapLost) {
				t.Errorf("permanent-loss banner printed = %v under %q", banner, c.want)
			}
		})
	}
}
