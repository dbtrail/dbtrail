package status

import (
	"context"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// #3083: the "aging" verdict, where the snapshot update's rule is known, is
// graded on that update's own line over the live floor: a file the update
// keeps never shows as aging, a file it rewrites does.

func TestGrade_onTheUpdatesLine_3083(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	rules := []RefreshRule{
		{Retain: 48 * time.Hour, Interval: 5 * time.Minute, DropsAfter: 48 * time.Hour},
		{Retain: 6 * time.Hour, Interval: 30 * time.Minute, DropsAfter: 6 * time.Hour},
		{Retain: 48 * time.Hour, Interval: 5 * time.Minute}, // window not known
		{Interval: 5 * time.Minute, DropsAfter: 12 * time.Hour},
		{Retain: 2 * time.Hour, Interval: 5 * time.Minute, DropsAfter: 2 * time.Hour},
	}
	for _, rule := range rules {
		for _, liveAge := range []time.Duration{-time.Hour, 30 * time.Minute, 3*time.Hour + 32*time.Minute, 11 * time.Hour, 47 * time.Hour, 60 * time.Hour} {
			live := now.Add(-liveAge)
			for _, archived := range []time.Duration{0, 10 * time.Hour} {
				f := DeltaFloor{Hour: live.Add(-archived), Live: live, Refresh: &rule}
				line, _ := rule.Line(live, now)
				for start := now.Add(-80 * time.Hour); !start.After(now.Add(2 * time.Hour)); start = start.Add(7 * time.Minute) {
					got := f.Grade(start, now)
					var want BaselineStalenessVerdict
					switch {
					case start.Before(f.Hour):
						want = BaselineBroken
					case start.After(line):
						want = BaselineOK
					default:
						want = BaselineAging
					}
					if got != want {
						t.Fatalf("%+v, live floor %s ago, archives %s, start %s ago: %s, want %s (line %s)",
							rule, liveAge, archived, now.Sub(start), got, want, line.Format(time.RFC3339))
					}
				}
			}
		}
	}
}

// The report's case: 3h32m-old index, 48h retention, a file from 12 minutes
// after the first hour. On the span alone it is aging; the update keeps it.
func TestGrade_aYoungInstallIsNotAging_3083(t *testing.T) {
	floor := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	first := floor.Add(12 * time.Minute)
	at := first.Add(3*time.Hour + 20*time.Minute)
	rule := RefreshRule{Retain: 48 * time.Hour, Interval: 5 * time.Minute, DropsAfter: 48 * time.Hour}
	if v := (DeltaFloor{Hour: floor, Live: floor}).Grade(first, at); v != BaselineAging {
		t.Fatalf("without the rule: %s, want aging (the case no longer shows the warning)", v)
	}
	if v := (DeltaFloor{Hour: floor, Live: floor, Refresh: &rule}).Grade(first, at); v != BaselineOK {
		t.Errorf("with the update's rule: %s, want ok", v)
	}
}

// What the rule does not touch: no rule, a rule with no live floor read, broken,
// and the unattributable demotion.
func TestGrade_whatTheRuleLeavesAlone_3083(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	floor := now.Add(-3 * time.Hour)
	rule := RefreshRule{Retain: 48 * time.Hour, Interval: 5 * time.Minute, DropsAfter: 48 * time.Hour}
	for start := now.Add(-5 * time.Hour); !start.After(now); start = start.Add(13 * time.Minute) {
		want := BaselineStalenessFor(start, floor, now)
		if got := (DeltaFloor{Hour: floor, Live: floor}).Grade(start, now); got != want {
			t.Fatalf("no rule, start %s ago: %s, want %s", now.Sub(start), got, want)
		}
		if got := (DeltaFloor{Hour: floor, Refresh: &rule}).Grade(start, now); got != want {
			t.Fatalf("rule but no live floor, start %s ago: %s, want %s", now.Sub(start), got, want)
		}
	}
	if got := (DeltaFloor{Hour: floor, Live: floor, Refresh: &rule}).Grade(floor.Add(-time.Minute), now); got != BaselineBroken {
		t.Errorf("a start under the floor: %s, want broken", got)
	}
	if got := (DeltaFloor{Hour: floor, Live: floor, BelowIsUnknown: true, Refresh: &rule}).Grade(floor.Add(-time.Minute), now); got != BaselineUnknown {
		t.Errorf("a start under an unattributable floor: %s, want unknown", got)
	}
	if got := (DeltaFloor{Live: floor, Refresh: &rule}).Grade(now, now); got != BaselineUnknown {
		t.Errorf("no floor at all: %s, want unknown", got)
	}
}

// OldestDeltaFromDB keeps the live floor beside the archive-extended one.
func TestOldestDeltaFromDB_keepsTheLiveFloor_3083(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT PARTITION_NAME FROM information_schema.PARTITIONS")).
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow("p_2026080110").AddRow("p_future"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT MIN(partition_name), MAX(partition_name), COUNT(DISTINCT bintrail_id) FROM archive_state")).
		WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow("p_2026080103", "p_2026080109", 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM bintrail_servers")).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
	got, err := OldestDeltaFromDB(context.Background(), db, "binlog_index")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Hour.Equal(time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)) || !got.Live.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("Hour %s, Live %s: want the archive floor at 03:00 and the live one at 10:00", got.Hour, got.Live)
	}
}
