//go:build integration

package reconstruct_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// inferredEntry2862 records, the way capture does, that schema.table was
// first seen in the stream (CREATE TABLE) or added to the publication at lsn.
func (f *pgFixture) inferredEntry2862(kind event.DDLKind, schema, table string, lsn uint64, d time.Duration) {
	f.t.Helper()
	pos := lsnString(lsn)
	ev := event.Event{
		BinlogFile: pos, StartPos: lsn, EndPos: lsn, GTID: pos, Timestamp: f.at(d),
		Schema: schema, Table: table, EventType: event.EventDDL, DDLType: kind,
		DDLQuery: fmt.Sprintf("%s oid=%d; instant=at-or-before; low=none; high=%s@%s */ %s %s.%s (first seen in the stream, or added to the publication)",
			event.InferredDDLPrefix, pgOID, pos, f.at(d).Format(time.RFC3339Nano), kind, schema, table),
	}
	if err := indexer.InsertSchemaChange(f.db, ev, nil); err != nil {
		f.t.Fatalf("record %s: %v", kind, err)
	}
}

// A fold of a PostgreSQL snapshot takes the capture's recorded start as proof
// that nothing is missing between the snapshot and a table's first change
// (#2843). That proof is about the index. For a table the index records as
// entering capture after the snapshot's floor it is not taken, and the fold
// warns, naming that (#2862); a table with no such record stays quiet.
func TestReconstructTables_PGTableEnteredCaptureLate_2862(t *testing.T) {
	const gap = "possible gap between baseline"
	const late = "entered capture after the snapshot"
	for _, tc := range []struct {
		name      string
		from      any           // stream_state.pg_capture_from_lsn
		kind      event.DDLKind // of the recorded entry; "" = none recorded
		schema    string
		table     string
		lsn       uint64
		wantLate  bool
		wantOther bool // the index-wide warning
	}{
		{name: "a table captured all along: quiet", from: 0x400},
		{name: "added to the publication after the snapshot: warns", from: 0x400, wantLate: true, kind: event.DDLAlterPublication, schema: "public", table: "orders", lsn: 0x700},
		{name: "first seen in the stream after the snapshot: warns", from: 0x400, wantLate: true, kind: event.DDLCreateTable, schema: "public", table: "orders", lsn: 0x700},
		{name: "added before the snapshot's floor: quiet", from: 0x400, kind: event.DDLAlterPublication, schema: "public", table: "orders", lsn: 0x480},
		{name: "added at the snapshot's floor: quiet", from: 0x400, kind: event.DDLAlterPublication, schema: "public", table: "orders", lsn: 0x500},
		{name: "the same name in another schema entered late: quiet", from: 0x400, kind: event.DDLAlterPublication, schema: "sales", table: "orders", lsn: 0x700},
		{name: "another table entered late: quiet", from: 0x400, kind: event.DDLAlterPublication, schema: "public", table: "items", lsn: 0x700},
		{name: "no recorded start and no entry: the index-wide warning, as before", from: nil, wantOther: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPGFixture(t)
			f.event(0x700, 15*time.Minute, 2, "1", s("changed"))
			f.checkpoint(0x700)
			if _, err := f.db.Exec("UPDATE stream_state SET pg_capture_from_lsn = ? WHERE id = 1", tc.from); err != nil {
				t.Fatal(err)
			}
			if tc.kind != "" {
				// Dated inside the fold's window, whatever its position.
				f.inferredEntry2862(tc.kind, tc.schema, tc.table, tc.lsn, 15*time.Minute)
			}
			root := t.TempDir()
			f.baseline(root, f.at(10*time.Minute), 0x500, "", [2]string{"1", "paid"})

			var logs bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			got, _, _, err := f.run(root, pgRun{at: f.at(30 * time.Minute)})
			slog.SetDefault(prev)
			if err != nil {
				t.Fatalf("the fold: %v", err)
			}
			if got["1"] != "changed" {
				t.Fatalf("rows = %v: the warning must not change what is folded", got)
			}
			out := logs.String()
			if has := strings.Contains(out, late); has != tc.wantLate {
				t.Fatalf("warned of a late entry = %v, want %v:\n%s", has, tc.wantLate, out)
			}
			if tc.wantLate && !(strings.Contains(out, gap) && strings.Contains(out, "table=orders") && strings.Contains(out, "entered_capture_lsn=0/700")) {
				t.Fatalf("the warning does not name the table and where it entered:\n%s", out)
			}
			if has := strings.Contains(out, "cannot be proven"); has != tc.wantOther {
				t.Fatalf("the index-wide warning = %v, want %v:\n%s", has, tc.wantOther, out)
			}
			if !tc.wantLate && !tc.wantOther && strings.Contains(out, gap) {
				t.Fatalf("warned of a gap for a covered table:\n%s", out)
			}
		})
	}
}

// A MySQL or MariaDB index is not asked: it records nothing about a table
// entering capture, so even rows that look like the PostgreSQL ones (which no
// MySQL capture writes) change nothing for it.
func TestTableCaptureEntry_MySQLIndexIsNotAsked_2862(t *testing.T) {
	f := newPGFixture(t)
	f.inferredEntry2862(event.DDLAlterPublication, "public", "orders", 0x700, 15*time.Minute)
	f.inferredEntry2862(event.DDLCreateTable, "public", "orders", 0x700, 15*time.Minute)
	binlog := baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000}
	withLSN := baseline.DumpMetadata{BinlogFile: "binlog.000042", BinlogPos: 1000, LSN: 0x500}
	for _, flavor := range []string{"mysql", "mariadb"} {
		for _, md := range []baseline.DumpMetadata{binlog, withLSN} {
			if got := reconstruct.TableCaptureEntry(f.db, flavor, md, "public", "orders"); got != (query.CaptureEntry{}) {
				t.Fatalf("%s: entry = %+v, want none", flavor, got)
			}
		}
	}
	// The same index, asked as PostgreSQL: the rows are there.
	if got := reconstruct.TableCaptureEntry(f.db, "postgres", withLSN, "public", "orders"); !got.Late || got.LSN != 0x700 {
		t.Fatalf("postgres: entry = %+v, want late at 0x700", got)
	}
}
