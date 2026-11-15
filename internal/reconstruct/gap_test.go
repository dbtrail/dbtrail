package reconstruct

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The one gap message WarnBaselineFirstEventGap can emit since the #1163
// redesign, keyed by a substring unique to it. The pre-#1163 assertive
// per-table message is gone: a per-table first event past the anchor is the
// EXPECTED healthy shape, so it can never carry an assertion by itself; the
// verdict is decided against the index's earliest surviving event and the
// unprovable case is worded as exactly that.
const msgUnproven = "cannot be proven"

// TestDecideBaselineGap pins the #1163 decision matrix: the per-table first
// event alone never proves a gap; the index's earliest surviving event either
// proves coverage (quiet) or leaves it unproven (hedged warning). The
// discriminating regression case is the START-SEEDED one: capture began AFTER
// the baseline (oldest indexed event past the anchor) — the GTID-containment
// design this replaces silenced it, because stream_state.gtid_set is seeded
// with the stream's start set and therefore "contains" the baseline's set in
// that exact scenario.
func TestDecideBaselineGap(t *testing.T) {
	// Coordinates from the #1163 report's healthy repro: baseline at
	// (mysql-bin.000003, 1160964), first per-table event 332 bytes later.
	bmeta := baseline.DumpMetadata{BinlogFile: "mysql-bin.000003", BinlogPos: 1160964}
	firstPast := query.ResultRow{BinlogFile: "mysql-bin.000003", StartPos: 1161296}

	cases := []struct {
		name    string
		flavor  string
		bmeta   baseline.DumpMetadata
		first   query.ResultRow
		start   query.IndexStart
		startOK bool
		want    GapVerdict
	}{
		{
			// The #1163 healthy repro: stream started before the baseline, so
			// the oldest surviving event precedes the anchor — proven, quiet,
			// even though this table's first event sits past the anchor.
			name:    "healthy: oldest indexed event precedes the anchor = quiet",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   firstPast,
			start:   query.IndexStart{BinlogFile: "mysql-bin.000001", StartPos: 4},
			startOK: true,
			want:    GapVerdictNone,
		},
		{
			// THE regression case: capture started after the baseline (#781).
			// Oldest surviving event is past the anchor → coverage unprovable
			// → warn. The replaced GTID-containment design silenced this.
			name:    "capture started after the baseline = unproven, warns",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   firstPast,
			start:   query.IndexStart{BinlogFile: "mysql-bin.000003", StartPos: 1161296},
			startOK: true,
			want:    GapVerdictUnproven,
		},
		{
			name:    "first event at the anchor = quiet regardless of index start",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   query.ResultRow{BinlogFile: "mysql-bin.000003", StartPos: 1160964},
			start:   query.IndexStart{BinlogFile: "mysql-bin.000004", StartPos: 4},
			startOK: true,
			want:    GapVerdictNone,
		},
		{
			name:    "oldest event unavailable (empty table / read failed) = unproven",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   firstPast,
			startOK: false,
			want:    GapVerdictUnproven,
		},
		{
			// #318 shape: a positionless oldest row must never silently read
			// as "at-or-before the anchor" (empty string sorts before every
			// file name lexically — that would be a free false proof).
			name:    "positionless oldest event is not a proof = unproven",
			flavor:  "mysql",
			bmeta:   bmeta,
			first:   firstPast,
			start:   query.IndexStart{BinlogFile: "", StartPos: 0},
			startOK: true,
			want:    GapVerdictUnproven,
		},
		{
			name:    "mariadb: same semantics, oldest precedes anchor = quiet",
			flavor:  "mariadb",
			bmeta:   baseline.DumpMetadata{BinlogFile: "mariadb-bin.000213", BinlogPos: 11149},
			first:   query.ResultRow{BinlogFile: "mariadb-bin.000214", StartPos: 400},
			start:   query.IndexStart{BinlogFile: "mariadb-bin.000210", StartPos: 4},
			startOK: true,
			want:    GapVerdictNone,
		},
		{
			// PostgreSQL compares numeric LSNs in StartPos; a start LSN
			// at-or-below the baseline floor proves coverage.
			name:    "postgres: oldest LSN at the baseline floor = quiet",
			flavor:  "postgres",
			bmeta:   baseline.DumpMetadata{LSN: 5000},
			first:   query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000},
			start:   query.IndexStart{BinlogFile: "0/1388", StartPos: 5000},
			startOK: true,
			want:    GapVerdictNone,
		},
		{
			name:    "postgres: oldest LSN past the floor = unproven",
			flavor:  "postgres",
			bmeta:   baseline.DumpMetadata{LSN: 5000},
			first:   query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000},
			start:   query.IndexStart{BinlogFile: "0/1B00", StartPos: 6912},
			startOK: true,
			want:    GapVerdictUnproven,
		},
		{
			// A zero start LSN is the PG positionless shape — not a proof.
			name:    "postgres: zero oldest LSN is not a proof = unproven",
			flavor:  "postgres",
			bmeta:   baseline.DumpMetadata{LSN: 5000},
			first:   query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000},
			start:   query.IndexStart{StartPos: 0},
			startOK: true,
			want:    GapVerdictUnproven,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DecideBaselineGap(tc.flavor, tc.bmeta, tc.first, tc.start, tc.startOK)
			if got != tc.want {
				t.Errorf("DecideBaselineGap = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestWarnBaselineFirstEventGap exercises the #781 full-table gap warning: the
// baseline↔first-event check ported from the single-row path, with the #1163
// earliest-surviving-event proof. It captures slog output and asserts whether
// the hedged gap message fires, honoring the same flavor-dependent
// anchor/position skip cases as the single-row switch. Not parallel: it swaps
// the process-global default logger (safe only in the sequential test phase).
func TestWarnBaselineFirstEventGap(t *testing.T) {
	bmeta := baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000}
	firstPast := query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000, EventID: 7}
	startBefore := query.IndexStart{BinlogFile: "binlog.000041", StartPos: 4}
	startAfter := query.IndexStart{BinlogFile: "binlog.000042", StartPos: 5000}

	cases := []struct {
		name     string
		flavor   string
		bmeta    baseline.DumpMetadata
		first    query.ResultRow
		start    query.IndexStart
		startOK  bool
		wantWarn bool
	}{
		{
			name:     "proven coverage stays quiet",
			flavor:   "mysql",
			bmeta:    bmeta,
			first:    firstPast,
			start:    startBefore,
			startOK:  true,
			wantWarn: false,
		},
		{
			name:     "unprovable coverage warns hedged",
			flavor:   "mysql",
			bmeta:    bmeta,
			first:    firstPast,
			start:    startAfter,
			startOK:  true,
			wantWarn: true,
		},
		{
			name:     "no index-start evidence warns hedged",
			flavor:   "mysql",
			bmeta:    bmeta,
			first:    firstPast,
			startOK:  false,
			wantWarn: true,
		},
		{
			// Anchor absent → the check is skipped (info), never a gap report.
			name:     "baseline lacks anchor = skipped",
			flavor:   "mysql",
			bmeta:    baseline.DumpMetadata{},
			first:    firstPast,
			startOK:  false,
			wantWarn: false,
		},
		{
			// First event lacks a comparable position (#318) → skipped.
			name:     "first event positionless = skipped",
			flavor:   "mysql",
			bmeta:    bmeta,
			first:    query.ResultRow{EventID: 9},
			start:    startAfter,
			startOK:  true,
			wantWarn: false,
		},
		{
			name:     "postgres proven by LSN floor stays quiet",
			flavor:   "postgres",
			bmeta:    baseline.DumpMetadata{LSN: 5000},
			first:    query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000, EventID: 3},
			start:    query.IndexStart{BinlogFile: "0/0FA0", StartPos: 4000},
			startOK:  true,
			wantWarn: false,
		},
		{
			name:     "postgres unprovable warns hedged",
			flavor:   "postgres",
			bmeta:    baseline.DumpMetadata{LSN: 5000},
			first:    query.ResultRow{BinlogFile: "0/1A2B", StartPos: 9000, EventID: 3},
			start:    query.IndexStart{BinlogFile: "0/1B00", StartPos: 6912},
			startOK:  true,
			wantWarn: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer slog.SetDefault(prev)

			WarnBaselineFirstEventGap(tc.flavor, tc.bmeta, tc.first, tc.start, tc.startOK, "s", "t")

			got := strings.Contains(buf.String(), msgUnproven)
			if got != tc.wantWarn {
				t.Errorf("gap warning emitted = %v, want %v; log output:\n%s", got, tc.wantWarn, buf.String())
			}
		})
	}
}

// What the index records about the table itself decides whether the
// coverage start's proof is taken for it (#2862). Only a PostgreSQL table
// with a record of entering capture past the snapshot's floor, or one whose
// record could not be read, loses the proof; everything else is
// DecideBaselineGap unchanged.
func TestDecideBaselineGapForTable_2862(t *testing.T) {
	pgMeta := baseline.DumpMetadata{LSN: 5000}
	pgFirst := query.ResultRow{BinlogFile: "0/2328", StartPos: 9000}
	pgBefore := query.IndexStart{BinlogFile: "0/FA0", StartPos: 4000}
	pgAfter := query.IndexStart{BinlogFile: "0/1B00", StartPos: 6912}
	myMeta := baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000}
	myFirst := query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000}
	myBefore := query.IndexStart{BinlogFile: "binlog.000041", StartPos: 4}
	late := query.CaptureEntry{Late: true, LSN: 7000, Kind: "ALTER PUBLICATION"}
	unread := query.CaptureEntry{Err: errors.New("connection refused")}

	for _, tc := range []struct {
		name    string
		flavor  string
		bmeta   baseline.DumpMetadata
		first   query.ResultRow
		start   query.IndexStart
		startOK bool
		entry   query.CaptureEntry
		want    GapVerdict
	}{
		{"a table with no record keeps the proof", "postgres", pgMeta, pgFirst, pgBefore, true, query.CaptureEntry{}, GapVerdictNone},
		{"a table recorded as entering late loses it", "postgres", pgMeta, pgFirst, pgBefore, true, late, GapVerdictLateEntry},
		{"a record that could not be read loses it", "postgres", pgMeta, pgFirst, pgBefore, true, unread, GapVerdictLateEntry},
		{"no record and no proof is unproven, as before", "postgres", pgMeta, pgFirst, pgAfter, true, query.CaptureEntry{}, GapVerdictUnproven},
		{"a late entry with no proof either names the table", "postgres", pgMeta, pgFirst, pgAfter, true, late, GapVerdictLateEntry},
		{"a late entry with a first event at the floor still warns", "postgres", pgMeta, query.ResultRow{BinlogFile: "0/1388", StartPos: 5000}, pgBefore, true, late, GapVerdictLateEntry},
		{"MySQL never reads the entry", "mysql", myMeta, myFirst, myBefore, true, late, GapVerdictNone},
		{"MariaDB never reads the entry", "mariadb", myMeta, myFirst, myBefore, true, unread, GapVerdictNone},
		{"an unknown flavor never reads the entry", "", myMeta, myFirst, myBefore, true, late, GapVerdictNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecideBaselineGapForTable(tc.flavor, tc.bmeta, tc.first, tc.start, tc.startOK, tc.entry); got != tc.want {
				t.Fatalf("verdict = %v, want %v", got, tc.want)
			}
			// Without an entry it is DecideBaselineGap.
			if got, want := DecideBaselineGapForTable(tc.flavor, tc.bmeta, tc.first, tc.start, tc.startOK, query.CaptureEntry{}),
				DecideBaselineGap(tc.flavor, tc.bmeta, tc.first, tc.start, tc.startOK); got != want {
				t.Fatalf("with no entry the verdict is %v, DecideBaselineGap says %v", got, want)
			}
		})
	}
}

// The index is asked about a table entering capture for a PostgreSQL
// snapshot only. A MySQL or MariaDB fold sends no statement at all: the mock
// holds no expectation, so any read comes back as an error in the entry.
func TestTableCaptureEntry_2862(t *testing.T) {
	pgMeta := baseline.DumpMetadata{LSN: 0x5000}
	myMeta := baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000}
	for _, tc := range []struct {
		name   string
		flavor string
		bmeta  baseline.DumpMetadata
	}{
		{"mysql", "mysql", myMeta},
		{"mariadb", "mariadb", myMeta},
		{"mysql, even with an LSN in the footer", "mysql", pgMeta},
		{"mariadb, even with an LSN in the footer", "mariadb", pgMeta},
		{"an unknown flavor and a binlog anchor", "", myMeta},
		{"postgres with a snapshot that has no floor", "postgres", baseline.DumpMetadata{}},
	} {
		t.Run("nothing is read: "+tc.name, func(t *testing.T) {
			db, _, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if got := TableCaptureEntry(db, tc.flavor, tc.bmeta, "shop", "orders"); got != (query.CaptureEntry{}) {
				t.Fatalf("entry = %+v, want none and no read", got)
			}
		})
	}
	if got := TableCaptureEntry(nil, "postgres", pgMeta, "public", "orders"); got != (query.CaptureEntry{}) {
		t.Fatalf("with no index: %+v", got)
	}

	for _, flavor := range []string{"postgres", ""} {
		t.Run("read past the floor: flavor "+flavor, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("FROM schema_changes").
				WithArgs("public", "orders", "CREATE TABLE", "ALTER PUBLICATION", sqlmock.AnyArg(), uint64(0x5000)).
				WillReturnRows(sqlmock.NewRows([]string{"binlog_pos", "ddl_type"}).AddRow(int64(0x7000), "ALTER PUBLICATION"))
			got := TableCaptureEntry(db, flavor, pgMeta, "public", "orders")
			if !got.Late || got.LSN != 0x7000 || got.Kind != "ALTER PUBLICATION" || got.Err != nil {
				t.Fatalf("entry = %+v", got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The warning names what is known: the table entered capture late, or that
// could not be read. A table with no record is WarnBaselineFirstEventGap.
func TestWarnBaselineFirstEventGapForTable_2862(t *testing.T) {
	pgMeta := baseline.DumpMetadata{LSN: 5000}
	first := query.ResultRow{BinlogFile: "0/2328", StartPos: 9000}
	proven := query.IndexStart{BinlogFile: "0/FA0", StartPos: 4000}
	const gap = "possible gap between baseline and this table's first indexed event"
	for _, tc := range []struct {
		name   string
		flavor string
		bmeta  baseline.DumpMetadata
		entry  query.CaptureEntry
		want   []string // all in the log; none = no gap warning at all
	}{
		{"no record: quiet", "postgres", pgMeta, query.CaptureEntry{}, nil},
		{"entered late", "postgres", pgMeta, query.CaptureEntry{Late: true, LSN: 1<<32 + 0x10, Kind: "ALTER PUBLICATION"},
			[]string{gap, "entered capture after the snapshot", "entered_capture_lsn=1/10", `recorded_as="ALTER PUBLICATION"`, "table=audit_log"}},
		{"unreadable", "postgres", pgMeta, query.CaptureEntry{Err: errors.New("boom")},
			[]string{gap, "could not be read from the index", "error=boom"}},
		{"a flavor that could not be read, an LSN in the footer", "", pgMeta, query.CaptureEntry{Late: true, LSN: 7000, Kind: "CREATE TABLE"},
			[]string{gap, "entered capture after the snapshot"}},
		{"mysql ignores an entry", "mysql", baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000}, query.CaptureEntry{Late: true, LSN: 7000}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			defer slog.SetDefault(prev)
			f, s := first, proven
			if tc.flavor == "mysql" {
				f = query.ResultRow{BinlogFile: "binlog.000042", StartPos: 5000}
				s = query.IndexStart{BinlogFile: "binlog.000041", StartPos: 4}
			}
			WarnBaselineFirstEventGapForTable(tc.flavor, tc.bmeta, f, s, true, tc.entry, "public", "audit_log")
			out := buf.String()
			if len(tc.want) == 0 && strings.Contains(out, "possible gap") {
				t.Fatalf("warned:\n%s", out)
			}
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Fatalf("the log lacks %q:\n%s", w, out)
				}
			}
		})
	}
}
