package rotation

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
)

// LegacyRetain and legacyRetainDur must describe the same window: one is what
// the operator reads, the other what the loop drops on.
func TestLegacyRetainSpellingMatchesItsDuration(t *testing.T) {
	d, err := cliutil.ParseRetain(LegacyRetain)
	if err != nil || d != legacyRetainDur {
		t.Fatalf("ParseRetain(%q) = %v, %v; want %v", LegacyRetain, d, err, legacyRetainDur)
	}
}

func TestImplicitRetainFrom(t *testing.T) {
	readErr := errors.New("SELECT command denied")
	for _, tc := range []struct {
		name     string
		value    string
		found    bool
		readErr  error
		want     time.Duration
		wantRaw  string
		recorded bool
		wantErr  bool
	}{
		{name: "recorded window is used", value: "48h", found: true,
			want: 48 * time.Hour, wantRaw: "48h", recorded: true},
		{name: "recorded days", value: "7d", found: true,
			want: 7 * 24 * time.Hour, wantRaw: "7d", recorded: true},
		// An index an older build created: no record, and the window it has
		// been running on all along.
		{name: "no record keeps the legacy window", found: false,
			want: legacyRetainDur, wantRaw: LegacyRetain},
		// Fail toward keeping data: a window we cannot read is not a licence
		// to drop on the (possibly much shorter) current default.
		{name: "read failure keeps the legacy window", readErr: readErr,
			want: legacyRetainDur, wantRaw: LegacyRetain, wantErr: true},
		{name: "unparseable record", value: "two weeks", found: true,
			want: legacyRetainDur, wantRaw: LegacyRetain, wantErr: true},
		{name: "empty record", value: "", found: true,
			want: legacyRetainDur, wantRaw: LegacyRetain, wantErr: true},
		{name: "zero record would disable drops", value: "0h", found: true,
			want: legacyRetainDur, wantRaw: LegacyRetain, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := implicitRetainFrom(tc.value, time.Now(), tc.found, tc.readErr)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got.retain != tc.want || got.raw != tc.wantRaw || got.recorded != tc.recorded {
				t.Errorf("got {%v %q recorded=%v}, want {%v %q recorded=%v}",
					got.retain, got.raw, got.recorded, tc.want, tc.wantRaw, tc.recorded)
			}
		})
	}
}

// An unreadable record must never be mistaken for a recorded one: recorded is
// what exempts an index from the upgrade guard.
func TestImplicitRetainFrom_uncertainIsNeverRecorded(t *testing.T) {
	for _, imp := range []implicitRetain{
		mustImplicit(t, "", false, nil),
		mustImplicit(t, "nonsense", true, nil),
		mustImplicit(t, "48h", true, errors.New("read failed")),
	} {
		if imp.recorded {
			t.Errorf("an uncertain answer came back recorded (%v): it would skip the upgrade guard", imp)
		}
	}
}

func mustImplicit(t *testing.T, value string, found bool, readErr error) implicitRetain {
	t.Helper()
	imp, _ := implicitRetainFrom(value, time.Now(), found, readErr)
	return imp
}

// The notice is news about a policy, so it is said once per index per process —
// not once per hourly cycle.
func TestNoticeKeptRetain_oncePerIndex(t *testing.T) {
	logs := captureSlog(t)
	keptRetainNoticed.Delete("dsn-a")
	keptRetainNoticed.Delete("dsn-b")
	t.Cleanup(func() {
		keptRetainNoticed.Delete("dsn-a")
		keptRetainNoticed.Delete("dsn-b")
	})

	noticeKeptRetain("dsn-a", "bintrail_index", "30d", "48h")
	noticeKeptRetain("dsn-a", "bintrail_index", "30d", "48h")
	noticeKeptRetain("dsn-b", "other_index", "30d", "48h")

	if n := logs.count(slog.LevelWarn, "keeps the retention it was created under"); n != 2 {
		t.Errorf("notice logged %d times, want 2 (once per index, not once per cycle)", n)
	}
}
