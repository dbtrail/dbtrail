//go:build integration

package query

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// What an index records about a PostgreSQL table entering capture after a
// position (#2862), read from rows written the way the capture writes them
// (indexer.InsertSchemaChange over an inferred event, #2441). Only a table
// first seen in the stream or added to the publication past the position is
// "late"; a table with no such row is not, whatever else the index holds.
func TestPGTableEnteredCaptureAfter_2862(t *testing.T) {
	const floor = uint64(0x5000)
	at := time.Now().UTC().Truncate(time.Second)
	// inferred is one inferred row as pgcapture builds it: the marker, then
	// the prose; placed at the commit that first showed the change.
	inferred := func(kind event.DDLKind, schema, table string, lsn uint64, text string, tables ...event.DDLTable) event.Event {
		pos := fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn))
		return event.Event{
			BinlogFile: pos, StartPos: lsn, EndPos: lsn, GTID: pos, Timestamp: at,
			Schema: schema, Table: table, EventType: event.EventDDL, DDLType: kind, DDLTables: tables,
			DDLQuery: fmt.Sprintf("%s oid=16500; instant=at-or-before; low=none; high=%s@%s */ %s",
				event.InferredDDLPrefix, pos, at.Format(time.RFC3339Nano), text),
		}
	}
	added := func(schema, table string, lsn uint64) event.Event {
		return inferred(event.DDLAlterPublication, schema, table, lsn, "ALTER PUBLICATION ... ADD TABLE "+schema+"."+table+": added to the publication; the table existed before")
	}
	addedBack := func(schema, table string, lsn uint64) event.Event {
		e := added(schema, table, lsn)
		head := event.InferredDDLPrefix + " oid=16500; "
		e.DDLQuery = head + "gap=" + event.InferredGapUncaptured + "; " + e.DDLQuery[len(head):]
		return e
	}
	firstSeen := func(schema, table string, lsn uint64) event.Event {
		return inferred(event.DDLCreateTable, schema, table, lsn, "CREATE TABLE "+schema+"."+table+" (first seen in the stream: created, or added to the publication)")
	}
	renamed := func(schema, from, to string, lsn uint64) event.Event {
		return inferred(event.DDLRenameTable, schema, to, lsn, "RENAME TABLE "+schema+"."+from+" TO "+schema+"."+to,
			event.DDLTable{Schema: schema, Table: from}, event.DDLTable{Schema: schema, Table: to})
	}
	plain := func(kind event.DDLKind, schema, table string, lsn uint64, text string) event.Event {
		e := inferred(kind, schema, table, lsn, "")
		e.DDLQuery = text
		return e
	}

	type ask struct {
		schema, table string
		after         uint64
		late          bool
		lsn           uint64
		kind          string
	}
	for _, tc := range []struct {
		name string
		rows []event.Event
		asks []ask
	}{
		{name: "a table the publication held all along has no row",
			asks: []ask{{schema: "public", table: "orders", after: floor}}},
		{name: "added to the publication after the position",
			rows: []event.Event{added("public", "audit_log", 0x7000)},
			asks: []ask{{schema: "public", table: "audit_log", after: floor, late: true, lsn: 0x7000, kind: "ALTER PUBLICATION"},
				{schema: "public", table: "orders", after: floor}}},
		{name: "first seen in the stream after the position (a server without commit times)",
			rows: []event.Event{firstSeen("public", "audit_log", 0x7000)},
			asks: []ask{{schema: "public", table: "audit_log", after: floor, late: true, lsn: 0x7000, kind: "CREATE TABLE"}}},
		{name: "added before the position, and at it",
			rows: []event.Event{added("public", "audit_log", 0x4000), firstSeen("public", "items", floor)},
			asks: []ask{{schema: "public", table: "audit_log", after: floor}, {schema: "public", table: "items", after: floor},
				{schema: "public", table: "items", after: floor - 1, late: true, lsn: floor, kind: "CREATE TABLE"}}},
		{name: "added, taken out and added back: the earliest row past the position",
			rows: []event.Event{added("public", "audit_log", 0x4000), addedBack("public", "audit_log", 0x9000), addedBack("public", "audit_log", 0x7000)},
			asks: []ask{{schema: "public", table: "audit_log", after: floor, late: true, lsn: 0x7000, kind: "ALTER PUBLICATION"},
				{schema: "public", table: "audit_log", after: 0x9000}}},
		{name: "a rename is not an entry, under either name",
			rows: []event.Event{renamed("public", "audit_log", "audit", 0x7000)},
			asks: []ask{{schema: "public", table: "audit_log", after: floor}, {schema: "public", table: "audit", after: floor}}},
		{name: "entered late, then renamed: the row stays under the name it entered with",
			rows: []event.Event{added("public", "audit_log", 0x7000), renamed("public", "audit_log", "audit", 0x8000)},
			asks: []ask{{schema: "public", table: "audit_log", after: floor, late: true, lsn: 0x7000, kind: "ALTER PUBLICATION"},
				{schema: "public", table: "audit", after: floor}}},
		{name: "the same table name in two schemas",
			rows: []event.Event{added("sales", "orders", 0x7000)},
			asks: []ask{{schema: "public", table: "orders", after: floor},
				{schema: "sales", table: "orders", after: floor, late: true, lsn: 0x7000, kind: "ALTER PUBLICATION"}}},
		{name: "names are read with the index's collation, as the fold reads events",
			rows: []event.Event{added("public", "Orders", 0x7000)},
			asks: []ask{{schema: "public", table: "orders", after: floor, late: true, lsn: 0x7000, kind: "ALTER PUBLICATION"}}},
		{name: "rows that are not an inferred entry",
			rows: []event.Event{
				plain(event.DDLCreateTable, "public", "orders", 0x7000, "CREATE TABLE orders (id int)"),
				plain(event.DDLTruncateTable, "public", "orders", 0x7100, "TRUNCATE public.orders"),
				inferred(event.DDLAlterTable, "public", "orders", 0x7200, "ALTER TABLE public.orders: added a (text)"),
				inferred(event.DDLDropTable, "public", "orders", 0x7300, "DROP TABLE public.orders"),
			},
			asks: []ask{{schema: "public", table: "orders", after: floor}}},
		{name: "past the 32-bit boundary the position is a number",
			rows: []event.Event{added("public", "audit_log", 1<<32+0x10)},
			asks: []ask{{schema: "public", table: "audit_log", after: 0xFFFFFFF0, late: true, lsn: 1<<32 + 0x10, kind: "ALTER PUBLICATION"},
				{schema: "public", table: "audit_log", after: 1<<32 + 0x10}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := headsRig(t, 4)
			for _, ev := range tc.rows {
				if err := indexer.InsertSchemaChange(db, ev, nil); err != nil {
					t.Fatalf("record %s: %v", ev.DDLType, err)
				}
			}
			for _, a := range tc.asks {
				got := PGTableEnteredCaptureAfter(db, a.schema, a.table, a.after)
				if got.Err != nil || got.Late != a.late || got.LSN != a.lsn || got.Kind != a.kind {
					t.Errorf("%s.%s after %#x = %+v; want late=%v at %#x as %q", a.schema, a.table, a.after, got, a.late, a.lsn, a.kind)
				}
			}
		})
	}

	t.Run("an index with no schema_changes table holds no row", func(t *testing.T) {
		db, _ := headsRig(t, 4)
		testutil.MustExec(t, db, "DROP TABLE schema_changes")
		if got := PGTableEnteredCaptureAfter(db, "public", "orders", floor); got != (CaptureEntry{}) {
			t.Fatalf("entry = %+v", got)
		}
	})
	t.Run("a read that fails says so, and is not an answer", func(t *testing.T) {
		db, _ := headsRig(t, 4)
		testutil.MustExec(t, db, "ALTER TABLE schema_changes DROP COLUMN ddl_query")
		got := PGTableEnteredCaptureAfter(db, "public", "orders", floor)
		if got.Err == nil || got.Late {
			t.Fatalf("entry = %+v, want the read's error", got)
		}
	})
	if got := PGTableEnteredCaptureAfter((*sql.DB)(nil), "public", "orders", floor); got != (CaptureEntry{}) {
		t.Fatalf("with no index: %+v", got)
	}
}
