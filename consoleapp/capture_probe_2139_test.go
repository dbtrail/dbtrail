package consoleapp

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// A rows event with a row the parser could not map (#2139) is written to the
// ledger under its own reason. captureComparable must refuse on it like on any
// other dropped event: the index never received the row.
func TestCaptureComparable_refusesOnRowMapFailure(t *testing.T) {
	skips := parser.NewSkipCounters(nil)
	skips.RecordSkipAttributed(parser.SkipRowMapFailed, parser.SkipAttribution{
		File: "binlog.000007", Pos: 200, Schema: "shop", Table: "notes",
	})
	raw, err := skips.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	st := streamStateFor(uuidA + ":1-10")
	st.CaptureSkips = sql.NullString{String: raw, Valid: true}
	anchor := time.Now().UTC().Add(time.Hour)

	if ok, _ := captureComparable(st, anchor, time.Time{}); ok {
		t.Fatal("comparable with a dropped row and no full read on record")
	}
	if ok, _ := captureComparable(st, anchor, time.Now().UTC().Add(-time.Hour)); ok {
		t.Fatal("comparable with a row dropped after the last full read")
	}
	// A full read that started after the drop read the row from the source.
	if ok, detail := captureComparable(st, anchor, time.Now().UTC().Add(time.Minute)); !ok {
		t.Fatalf("a full read after the drop must make the index comparable again, got %q", detail)
	}
}
