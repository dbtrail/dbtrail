package consoleapp

import (
	"context"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/status"
)

// #3083: the Snapshots page gets the update's rule only for a server an update
// of this daemon actually refreshes, with the interval of that update.

func addServer(t *testing.T, reg *console.Registry, e console.ServerEntry) console.ServerEntry {
	t.Helper()
	got, err := reg.Add(e)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// A young index (3h32m), and a file from 12 minutes after its first hour: on
// the span alone it is aging, on the update's line it is not.
func pageVerdict(rule status.RefreshRule, ok bool) status.BaselineStalenessVerdict {
	floor := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	f := status.DeltaFloor{Hour: floor, Live: floor}
	if ok {
		f.Refresh = &rule
	}
	return f.Grade(floor.Add(12*time.Minute), floor.Add(3*time.Hour+32*time.Minute))
}

func TestRefreshLineSource_onlyForServersAnUpdateRefreshes_3083(t *testing.T) {
	_, reg, sup := newScheduleFixture(t, false)
	sup.followRotation(func() rotation.Settings {
		return rotation.Settings{Enabled: true, Explicit: true, Retain: 48 * time.Hour, RetainRaw: "48h"}
	})
	local := addServer(t, reg, console.ServerEntry{Name: "local", DSN: "idx:pw@tcp(127.0.0.1:3306)/a", BaselineDir: t.TempDir()})
	s3only := addServer(t, reg, console.ServerEntry{Name: "s3only", DSN: "idx:pw@tcp(127.0.0.1:3306)/b", BaselineS3: "s3://bucket/prefix"})
	shared := t.TempDir()
	one := addServer(t, reg, console.ServerEntry{Name: "one", DSN: "idx:pw@tcp(127.0.0.1:3306)/c", BaselineDir: shared})
	two := addServer(t, reg, console.ServerEntry{Name: "two", DSN: "idx:pw@tcp(127.0.0.1:3306)/d", BaselineDir: shared})
	src := newRefreshLineSource(sup, nil, reg, "", "", 5*time.Minute)
	ctx := context.Background()

	rule, ok := src.RefreshRule(ctx, local.ID, nil, "")
	if !ok || rule.Interval != 5*time.Minute || rule.DropsAfter != 48*time.Hour {
		t.Fatalf("a target of the daemon-wide loop: %+v ok=%v, want its 5m interval and the 48h window", rule, ok)
	}
	if v := pageVerdict(rule, ok); v != status.BaselineOK {
		t.Errorf("a target on a young index grades %s, want ok", v)
	}
	for _, e := range []console.ServerEntry{s3only, one, two} {
		rule, ok := src.RefreshRule(ctx, e.ID, nil, "")
		if ok {
			t.Errorf("%s: handed a rule (%+v) although the loop does not refresh it", e.Name, rule)
		}
		if v := pageVerdict(rule, ok); v != status.BaselineAging {
			t.Errorf("%s on a young index grades %s, want aging as before: nothing is going to rewrite it", e.Name, v)
		}
	}
	if _, ok := newRefreshLineSource(sup, nil, reg, "", "", 0).RefreshRule(ctx, local.ID, nil, ""); ok {
		t.Error("with the daemon-wide loop off and no schedule, a rule was handed over")
	}
	if src := newRefreshLineSource(nil, nil, reg, "", "", 5*time.Minute); src != nil {
		t.Error("no supervisor, yet a source that is not a nil interface")
	}
}

func TestRefreshLineSource_theIntervalOfTheUpdatesThatRun_3083(t *testing.T) {
	sched, reg, sup := newScheduleFixture(t, false)
	e := addScheduled(t, reg, true) // an hourly schedule the loop runs as updates
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		sched  *backupScheduler
		global time.Duration
		want   time.Duration
	}{
		{"its own schedule only", sched, 0, time.Hour},
		{"its schedule and a shorter daemon-wide loop", sched, 5 * time.Minute, time.Hour},
		{"its schedule and a longer daemon-wide loop", sched, 2 * time.Hour, 2 * time.Hour},
		{"no schedule loop: the daemon-wide one", nil, 5 * time.Minute, 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule, ok := newRefreshLineSource(sup, tc.sched, reg, "", "", tc.global).RefreshRule(ctx, e.ID, nil, "")
			if !ok || rule.Interval != tc.want {
				t.Errorf("interval %s ok=%v, want %s", rule.Interval, ok, tc.want)
			}
		})
	}
	// A schedule the loop would not run (its folder shared with another
	// server) hands over nothing.
	addServer(t, reg, console.ServerEntry{Name: "other", DSN: "idx:pw@tcp(127.0.0.1:3306)/z", BaselineDir: e.BaselineDir})
	if rule, ok := newRefreshLineSource(sup, sched, reg, "", "", 0).RefreshRule(ctx, e.ID, nil, ""); ok {
		t.Errorf("a schedule that does not run handed over %+v", rule)
	}
}

// The index's own retention is read on the page's connection with a deadline,
// never through a connection of its own, and a failed read leaves the window
// unknown.
func TestRefreshLineSource_readsTheRetentionOnThePagesConnection_3083(t *testing.T) {
	_, reg, sup := newScheduleFixture(t, false)
	sup.followRotation(func() rotation.Settings {
		return rotation.Settings{Enabled: true, Retain: 48 * time.Hour, RetainRaw: "48h"} // no explicit window: the index's own is asked
	})
	opened := stubIndexRetention(t, rotation.Effective{Retain: 6 * time.Hour, Source: rotation.RetainRecorded})
	local := addServer(t, reg, console.ServerEntry{Name: "local", DSN: "idx:pw@tcp(127.0.0.1:3306)/a", BaselineDir: t.TempDir()})
	src := newRefreshLineSource(sup, nil, reg, "", "", 5*time.Minute)
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := regexp.QuoteMeta("SELECT initial_retain, recorded_at FROM `idx`.rotation_policy WHERE id = 1")
	mock.ExpectQuery(q).WillReturnRows(sqlmock.NewRows([]string{"initial_retain", "recorded_at"}).AddRow("12h", time.Now().Add(-time.Hour)))
	rule, ok := src.RefreshRule(ctx, local.ID, db, "idx")
	if !ok || rule.DropsAfter != 12*time.Hour {
		t.Errorf("rule %+v ok=%v, want the 12h the index recorded, read on the page's connection", rule, ok)
	}
	mock.ExpectQuery(q).WillReturnError(context.DeadlineExceeded)
	if rule, ok := src.RefreshRule(ctx, local.ID, db, "idx"); !ok || rule.DropsAfter != 0 {
		t.Errorf("an unreadable retention: %+v ok=%v, want a rule with the window unknown", rule, ok)
	}
	if rule, ok := src.RefreshRule(ctx, local.ID, nil, ""); !ok || rule.DropsAfter != 0 {
		t.Errorf("no page connection: %+v ok=%v, want a rule with the window unknown", rule, ok)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	if n := opened.Load(); n != 0 {
		t.Errorf("opened a connection of its own %d time(s)", n)
	}
}
