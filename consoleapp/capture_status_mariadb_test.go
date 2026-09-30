package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/status"
)

// The Overview's "is capture caught up" line for a MariaDB source. The
// source's @@gtid_binlog_pos is compared with the capture's checkpoint under
// the #1794 rules: up to date is the two positions EQUAL, per domain and
// server, and nothing else. Everything that is not equal is unknown with a
// reason; a MariaDB source is never called behind by this read.

func TestCompareMariaDBPositions(t *testing.T) {
	cases := []struct {
		name, captured, executed string
		verdict                  string
		detail                   string
	}{
		{name: "equal", captured: "0-1-100", executed: "0-1-100", verdict: console.CaptureCaughtUp},
		{name: "equal, several domains in another order", captured: "1-2-7,0-1-100", executed: "0-1-100,1-2-7", verdict: console.CaptureCaughtUp},
		{name: "equal, with line breaks", captured: "0-1-100,\n1-2-7", executed: " 0-1-100 , 1-2-7 ", verdict: console.CaptureCaughtUp},

		{name: "the source ahead in one domain", captured: "0-1-100,1-2-7", executed: "0-1-100,1-2-9", detail: "the source is ahead of the capture's checkpoint"},
		{name: "the source ahead on another server after a failover", captured: "0-1-100", executed: "0-2-140", detail: "the source is ahead of the capture's checkpoint"},
		{name: "a domain the capture has not seen", captured: "0-1-100", executed: "0-1-100,1-2-1", detail: "the source is ahead of the capture's checkpoint"},

		{name: "the capture ahead of the source", captured: "0-1-120", executed: "0-1-100", detail: "the capture's checkpoint and the source's GTID position do not match"},
		{name: "the same sequence written by another server", captured: "0-1-100", executed: "0-2-100", detail: "the capture's checkpoint and the source's GTID position do not match"},
		{name: "a domain the source no longer has", captured: "0-1-100,1-2-7", executed: "0-1-100", detail: "the capture's checkpoint and the source's GTID position do not match"},

		{name: "the source reports nothing", captured: "0-1-100", executed: "", detail: "the source reported no GTID position"},
		{name: "the source reports blanks", captured: "0-1-100", executed: " \n", detail: "the source reported no GTID position"},
		{name: "the source's position is garbage", captured: "0-1-100", executed: "garbage", detail: "the source's GTID position does not parse"},
		{name: "the source's position has an empty entry", captured: "0-1-100", executed: "0-1-100,,1-2-7", detail: "the source's GTID position does not parse"},
		{name: "the capture's set is garbage", captured: "garbage", executed: "0-1-100", detail: "the capture's GTID set does not parse"},
		{name: "the capture's set is a MySQL set", captured: uuidB + ":1-10", executed: "0-1-100", detail: "the capture's GTID set does not parse"},
		{name: "the capture's set names a domain twice", captured: "0-1-100,0-2-90", executed: "0-1-100", detail: "the capture's GTID set does not parse"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v, d := compareMariaDBPositions(c.captured, c.executed)
			if v != c.verdict || !strings.HasPrefix(d, c.detail) {
				t.Fatalf("got (%q, %q), want (%q, %q…)", v, d, c.verdict, c.detail)
			}
			if v == console.CaptureBehind {
				t.Fatal("a MariaDB source was called behind")
			}
			if v == console.CaptureCaughtUp && d != "" {
				t.Fatalf("caught up with a reason %q", d)
			}
		})
	}
}

func TestHeadFromStateMariaDB(t *testing.T) {
	posQ := "^" + regexp.QuoteMeta("SELECT @@GLOBAL.gtid_binlog_pos") + "$"
	checkpoint := captureT0.Add(-5 * time.Second)
	state := func(set string, edit func(*status.StreamStateInfo)) *status.StreamStateInfo {
		s := streamStateFor(set)
		s.LastCheckpoint = checkpoint
		if edit != nil {
			edit(s)
		}
		return s
	}
	cases := []struct {
		name      string
		st        *status.StreamStateInfo
		source    func(m sqlmock.Sqlmock) // nil: the source is never opened
		openFails bool
		verdict   string
		detail    string
		wantErr   bool
	}{
		{name: "equal", st: state("0-1-100", nil),
			source: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(posQ).WillReturnRows(sqlmock.NewRows([]string{"p"}).AddRow("0-1-100"))
			},
			verdict: console.CaptureCaughtUp},
		{name: "the source ahead", st: state("0-1-100", nil),
			source: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(posQ).WillReturnRows(sqlmock.NewRows([]string{"p"}).AddRow("0-1-130"))
			},
			detail: "the source is ahead of the capture's checkpoint"},
		{name: "a MariaDB with nothing in its binlog", st: state("0-1-100", nil),
			source: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(posQ).WillReturnRows(sqlmock.NewRows([]string{"p"}).AddRow(nil))
			},
			detail: "the source reported no GTID position"},
		{name: "the source is not a MariaDB", st: state("0-1-100", nil),
			source: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(posQ).WillReturnError(&mysql.MySQLError{Number: 1193, Message: "Unknown system variable"})
			},
			detail: "the source has no MariaDB GTID position"},
		{name: "the position cannot be read", st: state("0-1-100", nil),
			source: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(posQ).WillReturnError(&mysql.MySQLError{Number: 1227, Message: "Access denied"})
			},
			detail: "the source did not report its GTID position", wantErr: true},
		{name: "the source does not answer", st: state("0-1-100", nil), openFails: true,
			detail: "the source did not answer", wantErr: true},
		{name: "position mode: the source never opened", st: state("0-1-100", func(s *status.StreamStateInfo) { s.Mode = "position" }),
			detail: "the capture runs in binlog-position mode"},
		{name: "no capture on record: the source never opened", st: nil, detail: "the index has no live capture on record"},
		{name: "no GTID checkpoint: the source never opened", st: state("  ", nil), detail: "the capture has no GTID checkpoint"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src, sm, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			if err != nil {
				t.Fatal(err)
			}
			if c.source != nil {
				c.source(sm)
				sm.ExpectClose()
			}
			opened := false
			r, err := headFromStateMariaDB(context.Background(), c.st, func() (*sql.DB, error) {
				opened = true
				if c.openFails {
					return nil, fmt.Errorf("dial tcp 10.0.0.1:3306: connect: connection refused")
				}
				return src, nil
			})
			wantOpened := c.source != nil || c.openFails
			if (err != nil) != c.wantErr || r.verdict != c.verdict || !strings.HasPrefix(r.detail, c.detail) || opened != wantOpened {
				t.Fatalf("got %+v err=%v opened=%v, want verdict %q detail %q… err=%v opened=%v", r, err, opened, c.verdict, c.detail, c.wantErr, wantOpened)
			}
			if c.verdict != "" && (r.captured == "" || r.executed == "" || !r.checkpoint.Equal(checkpoint)) {
				t.Fatalf("a verdict without what it was read from: %+v", r)
			}
			// What the page is told: up to date on equal, else unknown.
			got, _ := captureStatusFrom(r, nil, captureT0)
			want := console.CaptureStateUnknown
			if c.verdict == console.CaptureCaughtUp {
				want = console.CaptureStateUpToDate
			}
			if got.State != want {
				t.Fatalf("the page is told %+v, want %q", got, want)
			}
			if c.source != nil {
				if err := sm.ExpectationsWereMet(); err != nil {
					t.Fatalf("source (closed too): %v", err)
				}
			} else {
				src.Close()
			}
		})
	}
}

// A MariaDB server is asked with the MariaDB read, a MySQL one with the
// MySQL read, and neither with the other's.
func TestCaptureStatus_mariadbIsAskedWithItsOwnRead(t *testing.T) {
	var mysqlReads, mariadbReads int
	c, _ := captureReporter("", func(context.Context, string, string) captureProbeResult {
		mysqlReads++
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	})
	c.readMariaDB = func(context.Context, string, string) captureProbeResult {
		mariadbReads++
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	}
	m := console.ServerEntry{ID: "m", Name: "m", DSN: "u:p@tcp(idx:3306)/m", SourceDSN: "u:p@tcp(src-m:3306)/", Flavor: console.FlavorMariaDB}
	if got := c.CaptureStatus(context.Background(), m); got.State != console.CaptureStateUpToDate || mariadbReads != 1 || mysqlReads != 0 {
		t.Fatalf("MariaDB: got %+v, MariaDB reads %d, MySQL reads %d", got, mariadbReads, mysqlReads)
	}
	if got := c.CaptureStatus(context.Background(), captureEntryA); got.State != console.CaptureStateUpToDate || mariadbReads != 1 || mysqlReads != 1 {
		t.Fatalf("MySQL: got %+v, MariaDB reads %d, MySQL reads %d", got, mariadbReads, mysqlReads)
	}
}

// The daemon's own index has no flavor on its entry: a daemon started with
// --source-flavor mariadb must still ask its source with the MariaDB read.
func TestCaptureStatus_mariadbBootSourceUsesTheDeclaredFlavor(t *testing.T) {
	var mysqlReads, mariadbReads int
	c, _ := captureReporter("u:p@tcp(boot-src:3306)/", func(context.Context, string, string) captureProbeResult {
		mysqlReads++
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	})
	c.withBootFlavor(func() string { return console.FlavorMariaDB })
	c.readMariaDB = func(context.Context, string, string) captureProbeResult {
		mariadbReads++
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	}
	boot := console.ServerEntry{ID: bootCaptureServerID, DSN: "u:p@tcp(idx:3306)/boot"}
	if got := c.CaptureStatus(context.Background(), boot); got.State != console.CaptureStateUpToDate || mariadbReads != 1 || mysqlReads != 0 {
		t.Fatalf("got %+v, MariaDB reads %d, MySQL reads %d", got, mariadbReads, mysqlReads)
	}
}

// An edit that changes only the flavor starts over: the answer of the other
// flavor's read is not served.
func TestCaptureStatus_aFlavorEditStartsOver(t *testing.T) {
	var mysqlReads, mariadbReads int
	c, _ := captureReporter("", func(context.Context, string, string) captureProbeResult {
		mysqlReads++
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	})
	c.readMariaDB = func(context.Context, string, string) captureProbeResult {
		mariadbReads++
		return captureProbeResult{detail: "the source is ahead of the capture's checkpoint"}
	}
	e := captureEntryA
	if got := c.CaptureStatus(context.Background(), e); got.State != console.CaptureStateUpToDate {
		t.Fatalf("MySQL: %+v", got)
	}
	e.Flavor = console.FlavorMariaDB
	if got := c.CaptureStatus(context.Background(), e); got.State != console.CaptureStateUnknown || mariadbReads != 1 {
		t.Fatalf("after the edit: %+v, MariaDB reads %d", got, mariadbReads)
	}
}
