package verify

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/consistency"
)

// On a MariaDB source, live-source verify used to find no @@gtid_executed,
// take the "source GTIDs disabled" branch and ASSUME the index was current,
// so an index that was merely behind read as a false mismatch. The source's
// @@gtid_binlog_pos is now compared per domain with the index's checkpoint:
// the index covers the snapshot when, in every domain the source has, it has
// reached at least the source's sequence number. Anything that does not
// settle that (position mode, no checkpoint, an empty or unparseable set, a
// read that failed) is "cannot tell", never covered.
func TestIndexCovers_MariaDB(t *testing.T) {
	const stateQuery = "SELECT mode, gtid_set FROM stream_state WHERE id = 1"
	row := func(mode, set any) func(m sqlmock.Sqlmock) {
		return func(m sqlmock.Sqlmock) {
			m.ExpectQuery(stateQuery).WillReturnRows(sqlmock.NewRows([]string{"mode", "gtid_set"}).AddRow(mode, set))
		}
	}
	noQuery := func(sqlmock.Sqlmock) {}

	cases := []struct {
		name    string
		src     string
		expect  func(m sqlmock.Sqlmock)
		covered bool
		detail  string // substring of the reason; checked only when not covered
	}{
		// Caught up and ahead.
		{name: "caught up", src: "0-1-100", expect: row("gtid", "0-1-100"), covered: true},
		{name: "index ahead of the snapshot", src: "0-1-100", expect: row("gtid", "0-1-120"), covered: true},

		// Behind.
		{name: "behind in the only domain", src: "0-1-101", expect: row("gtid", "0-1-100"),
			detail: "index is behind the source snapshot"},
		{name: "behind in one of two domains", src: "0-1-100,1-2-51", expect: row("gtid", "0-1-100,1-2-50"),
			detail: "index is behind the source snapshot"},

		// Several domains.
		{name: "several domains caught up, listed in another order", src: "0-1-99,1-2-50", expect: row("gtid", "1-2-50,0-1-100"), covered: true},
		{name: "a source domain the index never saw", src: "0-1-100,1-2-1", expect: row("gtid", "0-1-100"),
			detail: "index is behind the source snapshot"},
		{name: "a domain only the index has", src: "0-1-100", expect: row("gtid", "0-1-100,5-9-3"), covered: true},
		{name: "several domains with line breaks and spaces", src: "0-1-100,\n 1-2-50", expect: row("gtid", " 0-1-100 , 1-2-50\n"), covered: true},

		// Failover: the sequence is domain-wide, the server id is not part
		// of the comparison.
		{name: "failover, index further on another server", src: "0-1-100", expect: row("gtid", "0-2-105"), covered: true},
		{name: "failover, source further on another server", src: "0-2-101", expect: row("gtid", "0-1-100"),
			detail: "index is behind the source snapshot"},
		// A checkpoint written by an older go-mysql after a failover could
		// name a domain twice. Which one is the position is a guess: cannot
		// tell.
		{name: "legacy checkpoint naming a domain twice", src: "0-1-100", expect: row("gtid", "0-1-100,0-2-90"), detail: "index GTID set is unparseable"},

		// Position mode and no checkpoint.
		{name: "position mode", src: "0-1-100", expect: row("position", nil), detail: "binlog-position mode"},
		{name: "position mode with a stale GTID set left behind", src: "0-1-100", expect: row("position", "0-1-100"), detail: "binlog-position mode"},
		{name: "no mode recorded", src: "0-1-100", expect: row(nil, "0-1-100"), detail: "binlog-position mode"},
		// GTID mode with an empty set is a healthy capture that has recorded no
		// transaction yet (a MariaDB capture started on a server that had written
		// nothing). Behind, never the --reset advice, which would record a false
		// loss.
		{name: "GTID mode, no set saved", src: "0-1-100", expect: row("gtid", nil), detail: "index is behind the source snapshot: the capture has recorded no transaction yet"},
		{name: "GTID mode, blank set saved", src: "0-1-100", expect: row("gtid", "  "), detail: "index is behind the source snapshot: the capture has recorded no transaction yet"},
		{name: "no stream state row", src: "0-1-100",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(stateQuery).WillReturnRows(sqlmock.NewRows([]string{"mode", "gtid_set"}))
			},
			detail: "no stream state"},

		// Empty and garbage on the source side: the index is not even read.
		{name: "source position empty", src: "", expect: noQuery, detail: "reported no GTID position"},
		{name: "source position blank", src: " \n", expect: noQuery, detail: "reported no GTID position"},
		{name: "source position garbage", src: "garbage", expect: noQuery, detail: "source GTID position is unparseable"},
		{name: "source position cut short", src: "0-1-", expect: noQuery, detail: "source GTID position is unparseable"},
		{name: "source position with an empty entry", src: "0-1-5,,1-2-3", expect: noQuery, detail: "source GTID position is unparseable"},
		{name: "source position in MySQL format", src: "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-5", expect: noQuery, detail: "source GTID position is unparseable"},

		// Garbage on the index side.
		{name: "index set garbage", src: "0-1-100", expect: row("gtid", "garbage"), detail: "index GTID set is unparseable"},
		{name: "index set with an empty entry", src: "0-1-100", expect: row("gtid", "0-1-100,,1-2-3"), detail: "index GTID set is unparseable"},
		{name: "index set in MySQL format", src: "0-1-100", expect: row("gtid", "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-500"), detail: "index GTID set is unparseable"},

		// The index could not be read.
		{name: "index read denied", src: "0-1-100",
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(stateQuery).WillReturnError(&mysql.MySQLError{Number: 1142, Message: "SELECT command denied to user"})
			},
			detail: "could not read index coverage"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tc.expect(mock)

			covered, detail := indexCovers(context.Background(), db, tc.src, consistency.GTIDFlavorMariaDB)
			if covered != tc.covered {
				t.Fatalf("covered = %v (%q), want %v", covered, detail, tc.covered)
			}
			if tc.covered && detail != "" {
				t.Errorf("covered with a note %q; a checked coverage carries none", detail)
			}
			if !tc.covered && !strings.Contains(detail, tc.detail) {
				t.Errorf("detail = %q, want it to contain %q", detail, tc.detail)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// The assumption stays where it was, and only there: a MySQL source with
// GTIDs off, or a server with neither variable, still proceeds with the
// coverage-unverified note. A MariaDB source never reaches that branch.
func TestIndexCovers_EmptySourceByFlavor(t *testing.T) {
	for _, tc := range []struct {
		flavor  string
		covered bool
	}{
		{consistency.GTIDFlavorMySQL, true},
		{"", true},
		{consistency.GTIDFlavorMariaDB, false},
	} {
		t.Run("flavor="+tc.flavor, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			covered, detail := indexCovers(context.Background(), db, "", tc.flavor)
			if covered != tc.covered {
				t.Fatalf("covered = %v (%q), want %v", covered, detail, tc.covered)
			}
			if tc.covered && !strings.Contains(detail, "assuming the index is current") {
				t.Errorf("detail = %q, want the coverage-unverified note", detail)
			}
			if !tc.covered && strings.Contains(detail, "assuming") {
				t.Errorf("detail = %q assumes on a MariaDB source", detail)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// When the index has no GTID checkpoint to compare with, the advice must not
// be a plain restart at the source's current position: with the index
// behind, that skips events and nothing records it. It names --reset, which
// records the skipped span as permanently lost, and a new full snapshot,
// and says the events in between are not captured.
func TestIndexCovers_noCheckpointAdviceNeverSkipsSilently(t *testing.T) {
	cases := []struct {
		name, flavor, src, mode, set, variable string
	}{
		{"mysql, no GTID set", consistency.GTIDFlavorMySQL, "3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-5", "position", "", "SELECT @@GLOBAL.gtid_executed"},
		{"mariadb, position mode", consistency.GTIDFlavorMariaDB, "0-1-100", "position", "", "SELECT @@gtid_binlog_pos"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if tc.flavor == consistency.GTIDFlavorMariaDB {
				mock.ExpectQuery("SELECT mode, gtid_set FROM stream_state").
					WillReturnRows(sqlmock.NewRows([]string{"mode", "gtid_set"}).AddRow(tc.mode, tc.set))
			} else {
				mock.ExpectQuery("SELECT gtid_set FROM stream_state").
					WillReturnRows(sqlmock.NewRows([]string{"gtid_set"}).AddRow(tc.set))
			}
			covered, detail := indexCovers(context.Background(), db, tc.src, tc.flavor)
			if covered {
				t.Fatalf("covered with no GTID checkpoint: %q", detail)
			}
			for _, want := range []string{
				`--reset --start-gtid "$(mysql -N -e '` + tc.variable + `')"`,
				"permanently lost",
				"are not captured",
				"new full snapshot",
			} {
				if !strings.Contains(detail, want) {
					t.Errorf("detail = %q\nwant it to contain %q", detail, want)
				}
			}
			if strings.Contains(detail, "restart it with --start-gtid") {
				t.Errorf("detail still advises a plain restart at the current position: %q", detail)
			}
		})
	}
}
