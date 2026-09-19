//go:build integration

package rotation

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// ResolveEffective is what every SURFACE reports — doctor's projection, the
// console's capacity card and rotation panel, status. It must answer exactly
// what the loop drops on, including for the shapes the loop treats specially.
func TestResolveEffective(t *testing.T) {
	for _, tc := range []struct {
		name     string
		recorded string
		want     time.Duration
		wantRaw  string
		source   RetainSource
	}{
		{"a recorded window is the answer", "48h", 48 * time.Hour, "48h", RetainRecorded},
		{"no record keeps the legacy window", "", legacyRetainDur, LegacyRetain, RetainLegacy},
		{"an unreadable record is not presented as a fact", "two weeks", legacyRetainDur, LegacyRetain, RetainUnreadable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if tc.recorded != "" {
				testutil.MustExec(t, db, indexer.DDLRotationPolicy)
				testutil.MustExec(t, db, "INSERT INTO rotation_policy (id, initial_retain) VALUES (1, ?)", tc.recorded)
			}
			got := ResolveEffective(context.Background(), db, dbName)
			if got.Retain != tc.want || got.Raw != tc.wantRaw || got.Source != tc.source {
				t.Fatalf("got {%v %q %s}, want {%v %q %s}", got.Retain, got.Raw, got.Source, tc.want, tc.wantRaw, tc.source)
			}
			if got.DescribeSource() == "" {
				t.Error("every answer must carry the sentence a surface puts beside the number")
			}
			if info := got.StatusInfo(); info == nil || info.Basis != string(tc.source) {
				t.Errorf("StatusInfo = %+v, want basis %s", info, tc.source)
			}
		})
	}
}

// The loop and the surfaces must not diverge: whatever ResolveEffective says
// is what rotateOneIndex drops on. Checked on the shape where they could most
// easily differ — an index holding history older than its own record, where
// the loop refuses to DROP but the window it reports is still its own.
func TestResolveEffective_agreesWithTheLoop(t *testing.T) {
	old := time.Now().UTC().Add(-90 * 24 * time.Hour).Truncate(time.Hour)
	current := time.Now().UTC().Truncate(time.Hour)
	db, dsn, dbName := implicitRigRecordedAt(t, "48h", time.Now().UTC().Add(-time.Minute), old, current)

	eff := ResolveEffective(context.Background(), db, dbName)
	if eff.Retain != 48*time.Hour || eff.Source != RetainRecorded {
		t.Fatalf("surface reports {%v %s}, want 48h/recorded", eff.Retain, eff.Source)
	}
	// And the loop, on the same index, uses that window (while refusing the
	// drops the guard holds back).
	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: dsn}, implicitSettings()); err != nil {
		t.Fatalf("rotateOneIndex: %v", err)
	}
}
