package console

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/views"
)

const cutCreateOrders = "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"

var cutStamp = time.Date(2026, 4, 30, 3, 20, 0, 0, time.UTC)

// cutFooter is a table file's footer that vouches for everything copyCutOf
// asks: a locked dump with its position, stamp and definition.
func cutFooter() baseline.DumpMetadata {
	return baseline.DumpMetadata{
		BinlogFile: "binlog.000007", BinlogPos: 4200, SnapshotTimestamp: cutStamp,
		CreateTableSQL: cutCreateOrders, LockMode: string(baseline.LockModeFTWRL),
		Producer: baseline.ProducerDump, FoldGeneration: -1,
	}
}

// #2085: where a table's view stops is read from the footer of the file the
// view reads, by the refresh's own rule, and every way of not knowing it is
// a refusal: a wrong cut calls a changed table unchanged.
func TestCopyCutOf(t *testing.T) {
	table := views.BaselineTable{Schema: "shop", Table: "orders", Path: "/snap/shop/orders.parquet"}
	withChain := table
	withChain.Delta = true
	withChain.DeltaFiles = []baseline.TableDeltaFile{
		{Seq: 0, Upserts: "/snap/shop/orders.000000.upserts"},
		{Seq: 1, SeqLo: 1, Upserts: "/snap/shop/orders.000001.upserts"},
	}
	pairStamp := cutStamp.Add(26 * time.Hour)
	pair := func() baseline.DumpMetadata {
		md := cutFooter()
		md.BinlogFile, md.BinlogPos, md.SnapshotTimestamp, md.LastEventID = "binlog.000009", 900, pairStamp, 5150
		md.Producer, md.LastDumpAt, md.FoldGeneration = baseline.ProducerReconstruct, cutStamp, 1
		return md
	}
	cases := []struct {
		name    string
		table   views.BaselineTable
		base    func(*baseline.DumpMetadata)
		last    func(*baseline.DumpMetadata)
		baseErr error
		lastErr error
		refusal string // substring; "" = a cut
		want    copyCut
	}{
		{name: "a locked dump", table: table,
			want: copyCut{anchor: query.BinlogPos{File: "binlog.000007", Pos: 4200}, since: cutStamp, sourceRead: cutStamp}},
		{name: "a chain: the LAST pair's position, stamp and event id; the base's read of the source", table: withChain,
			want: copyCut{anchor: query.BinlogPos{File: "binlog.000009", Pos: 900}, since: pairStamp, lastEventID: 5150, sourceRead: cutStamp}},
		{name: "a carried-forward file answers with its own stamp, not the directory's", table: table,
			base: func(md *baseline.DumpMetadata) { md.SnapshotTimestamp = cutStamp.Add(-72 * time.Hour) },
			want: copyCut{anchor: query.BinlogPos{File: "binlog.000007", Pos: 4200}, since: cutStamp.Add(-72 * time.Hour), sourceRead: cutStamp.Add(-72 * time.Hour)}},

		{name: "the file cannot be read", table: table, baseErr: errors.New("permission denied"), refusal: "could not be read"},
		{name: "the chain's last pair cannot be read", table: withChain, lastErr: errors.New("gone"), refusal: "could not be read"},
		{name: "no binlog position", table: table, base: func(md *baseline.DumpMetadata) { md.BinlogFile = "" }, refusal: "does not record its binlog position"},
		{name: "a zero binlog position", table: table, base: func(md *baseline.DumpMetadata) { md.BinlogPos = 0 }, refusal: "does not record its binlog position"},
		{name: "the pair has no position, the base does", table: withChain, last: func(md *baseline.DumpMetadata) { md.BinlogPos = 0 }, refusal: "does not record its binlog position"},
		{name: "no stamp", table: table, base: func(md *baseline.DumpMetadata) { md.SnapshotTimestamp = time.Time{} }, refusal: "does not record when it was written"},
		{name: "built across a capture gap", table: table, base: func(md *baseline.DumpMetadata) { md.CaptureGap = "2026-04-29: 3 files lost" }, refusal: "gap in capture"},
		{name: "the pair was built across a capture gap", table: withChain, last: func(md *baseline.DumpMetadata) { md.CaptureGap = "lost" }, refusal: "gap in capture"},
		{name: "a dump taken with no locks", table: table, base: func(md *baseline.DumpMetadata) { md.LockMode = string(baseline.LockModeNoLock) }, refusal: "its read is torn"},
		{name: "no lock mode on record", table: table, base: func(md *baseline.DumpMetadata) { md.LockMode = "" }, refusal: "its read is unknown"},
		{name: "no table definition", table: table, base: func(md *baseline.DumpMetadata) { md.CreateTableSQL = "" }, refusal: "does not record the table's definition"},
		{name: "a foreign key that cascades deletes into the table", table: table,
			base: func(md *baseline.DumpMetadata) {
				md.CreateTableSQL = strings.Replace(cutCreateOrders, "PRIMARY KEY (`id`)", "PRIMARY KEY (`id`),\n  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `customers` (`id`) ON DELETE CASCADE", 1)
			},
			refusal: "foreign key"},
		{name: "a foreign key that nulls the column on update", table: table,
			base: func(md *baseline.DumpMetadata) {
				md.CreateTableSQL = strings.Replace(cutCreateOrders, "PRIMARY KEY (`id`)", "PRIMARY KEY (`id`),\n  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `customers` (`id`) on update  set\tnull", 1)
			},
			refusal: "foreign key"},
		{name: "a fold with no record of the last read of the source", table: table,
			base: func(md *baseline.DumpMetadata) {
				md.Producer, md.LastDumpAt = baseline.ProducerReconstruct, time.Time{}
			},
			refusal: "last read from the source"},
		{name: "the v0.83.0 pair says nothing about where it stops", table: views.BaselineTable{Schema: "shop", Table: "orders", Path: table.Path, Delta: true, DeltaLegacy: true},
			refusal: "does not record where they stop"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			footer := func(path string) (baseline.DumpMetadata, error) {
				switch path {
				case table.Path:
					md := cutFooter()
					if c.base != nil {
						c.base(&md)
					}
					return md, c.baseErr
				case "/snap/shop/orders.000001.upserts":
					md := pair()
					if c.last != nil {
						c.last(&md)
					}
					return md, c.lastErr
				}
				t.Fatalf("read the footer of %s: neither the table file nor the chain's last pair", path)
				return baseline.DumpMetadata{}, nil
			}
			got := copyCutOf(c.table, footer)
			if c.refusal != "" {
				if !strings.Contains(got.refusal, c.refusal) || !strings.Contains(got.refusal, "shop.orders") {
					t.Fatalf("refusal = %q, want one naming shop.orders and %q", got.refusal, c.refusal)
				}
				return
			}
			if got != c.want {
				t.Fatalf("cut = %+v\nwant  %+v", got, c.want)
			}
		})
	}
}

// A foreign key that cannot change the table's rows is not a reason to refuse.
func TestCopyCutOf_restrictingForeignKeyIsFine(t *testing.T) {
	for _, action := range []string{"", " ON DELETE RESTRICT", " ON DELETE NO ACTION ON UPDATE RESTRICT"} {
		md := cutFooter()
		md.CreateTableSQL = strings.Replace(cutCreateOrders, "PRIMARY KEY (`id`)", "PRIMARY KEY (`id`),\n  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `customers` (`id`)"+action, 1)
		got := copyCutOf(views.BaselineTable{Schema: "shop", Table: "orders", Path: "p"}, func(string) (baseline.DumpMetadata, error) { return md, nil })
		if got.refusal != "" {
			t.Errorf("foreign key with %q refused: %s", action, got.refusal)
		}
	}
}

// The lookup's time floor is the refresh's: the stamp's hour, minus one more.
func TestCopyTimeFloor(t *testing.T) {
	if got, want := copyTimeFloor(cutStamp), time.Date(2026, 4, 30, 2, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("floor of %v = %v, want %v", cutStamp, got, want)
	}
	onTheHour := time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC)
	if got, want := copyTimeFloor(onTheHour), time.Date(2026, 4, 30, 2, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("floor of %v = %v, want %v", onTheHour, got, want)
	}
}

// What the capture's own record rules out: a loss or a dropped event at or
// after the table's rows were last read from the source, and any record that
// cannot be read. A loss older than that read was read again by it.
func TestCaptureLossSince(t *testing.T) {
	read := cutStamp
	healthy := func() *status.StreamStateInfo {
		return &status.StreamStateInfo{Mode: "gtid", GapColumnsPresent: true,
			CaptureSkips: sql.NullString{String: "{}", Valid: true}}
	}
	skips := func(json string) *status.StreamStateInfo {
		st := healthy()
		st.CaptureSkips = sql.NullString{String: json, Valid: true}
		return st
	}
	cases := []struct {
		name string
		st   *status.StreamStateInfo
		want string // substring; "" = nothing rules it out
	}{
		{"healthy", healthy(), ""},
		{"no capture on record", nil, "no live capture"},
		{"an index older than the loss record", func() *status.StreamStateInfo { st := healthy(); st.GapColumnsPresent = false; return st }(), "predates"},
		{"a gap before the read of the source", func() *status.StreamStateInfo {
			st := healthy()
			st.GapLostAt = sql.NullTime{Time: read.Add(-time.Second), Valid: true}
			return st
		}(), ""},
		{"a gap at the read of the source", func() *status.StreamStateInfo {
			st := healthy()
			st.GapLostAt = sql.NullTime{Time: read, Valid: true}
			return st
		}(), "binlog gap"},
		{"a gap after it", func() *status.StreamStateInfo {
			st := healthy()
			st.GapLostAt = sql.NullTime{Time: read.Add(time.Hour), Valid: true}
			return st
		}(), "binlog gap"},
		{"a skip ledger that does not parse", skips("{not json"), "not readable"},
		{"no skip ledger written yet", func() *status.StreamStateInfo { st := healthy(); st.CaptureSkips = sql.NullString{}; return st }(), "not readable"},
		{"a dropped event before the read of the source", skips(`{"statement_format_dml":{"count":3,"last_at":"2026-04-30T03:19:59Z"}}`), ""},
		{"a dropped event after it", skips(`{"statement_format_dml":{"count":3,"last_at":"2026-04-30T09:00:00Z"}}`), "dropped events"},
		{"a dropped event with no date", skips(`{"column_count_mismatch":{"count":1}}`), "dropped events"},
		{"a reason with a zero count", skips(`{"column_count_mismatch":{"count":0}}`), ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := captureLossSince(c.st, read)
			if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
				t.Errorf("captureLossSince = %q, want %q", got, c.want)
			}
		})
	}
}

// A schema change is placed by position, and matched the way schema_changes
// stores names: as the statement typed them.
func TestDDLTouches(t *testing.T) {
	cut := query.BinlogPos{File: "binlog.000007", Pos: 4200}
	after := query.BinlogPos{File: "binlog.000007", Pos: 4201}
	cases := []struct {
		name string
		row  ddlRow
		want bool
	}{
		{"the table, after the cut", ddlRow{"shop", "orders", "ALTER TABLE orders ADD c int", after}, true},
		{"the table, ending exactly at the cut", ddlRow{"shop", "orders", "ALTER TABLE orders ADD c int", cut}, false},
		{"the table, before the cut", ddlRow{"shop", "orders", "TRUNCATE TABLE orders", query.BinlogPos{File: "binlog.000007", Pos: 100}}, false},
		{"the table, in an earlier file with a larger offset", ddlRow{"shop", "orders", "TRUNCATE TABLE orders", query.BinlogPos{File: "binlog.000006", Pos: 999999}}, false},
		{"the table, in a later file with a smaller offset", ddlRow{"shop", "orders", "TRUNCATE TABLE orders", query.BinlogPos{File: "binlog.000008", Pos: 4}}, true},
		{"past the .999999 rollover", ddlRow{"shop", "orders", "TRUNCATE TABLE orders", query.BinlogPos{File: "binlog.1000000", Pos: 4}}, true},
		{"typed in another case", ddlRow{"SHOP", "Orders", "alter table Orders add c int", after}, true},
		{"recorded with no schema", ddlRow{"", "orders", "ALTER TABLE orders ADD c int", after}, true},
		{"the same table name in another schema", ddlRow{"crm", "orders", "ALTER TABLE crm.t ADD c int", after}, false},
		{"another table", ddlRow{"shop", "lines", "ALTER TABLE `lines` ADD c int", after}, false},
		{"another table whose name contains this one", ddlRow{"shop", "orders_archive", "DROP TABLE orders_archive", after}, false},
		{"a rename recorded under the old name only", ddlRow{"shop", "staging", "ALTER TABLE staging RENAME TO `orders`", after}, true},
		{"another table's statement naming this one (a foreign key)", ddlRow{"shop", "lines", "ALTER TABLE `lines` ADD FOREIGN KEY (o) REFERENCES shop.orders (id)", after}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ddlTouches(c.row, "shop", "orders", cut); got != c.want {
				t.Errorf("ddlTouches = %v, want %v", got, c.want)
			}
		})
	}
}

func TestContainsIdentifier(t *testing.T) {
	for _, c := range []struct {
		text, name string
		want       bool
	}{
		{"DROP TABLE orders", "orders", true},
		{"DROP TABLE `Orders`", "orders", true},
		{"DROP TABLE shop.orders, shop.lines", "orders", true},
		{"orders", "orders", true},
		{"DROP TABLE orders2", "orders", false},
		{"DROP TABLE my_orders", "orders", false},
		{"DROP TABLE reorders", "orders", false},
		{"DROP TABLE órders", "rders", false},
		{"DROP TABLE xorders, orders", "orders", true},
		{"DROP TABLE t", "", false},
	} {
		if got := containsIdentifier(c.text, c.name); got != c.want {
			t.Errorf("containsIdentifier(%q, %q) = %v, want %v", c.text, c.name, got, c.want)
		}
	}
}

// Capture must be known complete as of an instant within the limit.
func TestCaptureBehind(t *testing.T) {
	now := cutStamp
	for _, c := range []struct {
		name string
		wm   CaptureWatermark
		want string
	}{
		{"complete a moment ago", CaptureWatermark{Through: now.Add(-20 * time.Second)}, ""},
		{"complete exactly the limit ago", CaptureWatermark{Through: now.Add(-time.Minute)}, ""},
		{"complete longer ago than the limit", CaptureWatermark{Through: now.Add(-61 * time.Second)}, "more than the limit of 1m0s"},
		{"never confirmed, with why", CaptureWatermark{Detail: "the source did not answer"}, "the source did not answer"},
		{"never confirmed, no reason given", CaptureWatermark{}, "not known to be up to date"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := captureBehind(c.wm, now, time.Minute)
			if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
				t.Errorf("captureBehind = %q, want %q", got, c.want)
			}
		})
	}
}

// fakeWatermark is a capture status reporter that also answers the
// watermark, with a canned one.
type fakeWatermark struct {
	wm    CaptureWatermark
	asked []string
	// ago, when set, makes every answer complete as of that long ago: a
	// capture that keeps up, however long the test runs.
	ago time.Duration
}

func (f *fakeWatermark) CaptureStatus(context.Context, ServerEntry) CaptureStatus {
	return CaptureStatus{State: CaptureStateUnknown}
}

func (f *fakeWatermark) CaptureWatermark(_ context.Context, e ServerEntry) CaptureWatermark {
	f.asked = append(f.asked, e.ID)
	wm := f.wm
	if f.ago > 0 {
		wm.Through = time.Now().Add(-f.ago)
	}
	return wm
}

// Through runSQLVouched and the real worker: the question is asked about
// the tables the statement's views read and no others, a statement whose
// tables are not certain is refused without asking, and a session that does
// not ask pays for nothing.
func TestSQL_realWorkerVouchesOnlyForNamedTables_2085(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "lines", "CREATE TABLE `lines` (\n  `id` int NOT NULL,\n  `qty` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "26"})
	ctx := context.Background()
	asking := sqlsandbox.Session{UnchangedWithin: time.Minute}

	var asked [][]string
	answer := ""
	unchanged := func(_ context.Context, tables []views.BaselineTable) string {
		names := make([]string, len(tables))
		for i, tb := range tables {
			names[i] = tb.Schema + "." + tb.Table
		}
		asked = append(asked, names)
		return answer
	}
	run := func(stmt string, sess sqlsandbox.Session, u sqlUnchanged) error {
		t.Helper()
		_, err := f.s.runSQLVouched(ctx, f.s.cm.boot, "u", stmt, "", 0, sess, u)
		return err
	}
	refusedFor := func(err error, want string) {
		t.Helper()
		var refusal *sqlChangedRefusal
		if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, want) {
			t.Errorf("err = %v, want a refusal saying %q", err, want)
		}
	}

	// One table named: one table asked about, and the copy answers.
	if err := run("SELECT count(*) FROM shop.orders", asking, unchanged); err != nil {
		t.Fatalf("unchanged table: %v", err)
	}
	if len(asked) != 1 || strings.Join(asked[0], ",") != "shop.orders" {
		t.Fatalf("asked about %v, want exactly [shop.orders]", asked)
	}
	// A join: both, and only those.
	asked = nil
	if err := run("SELECT count(*) FROM shop.orders o JOIN shop.lines l ON l.id = o.id", asking, unchanged); err != nil {
		t.Fatalf("join: %v", err)
	}
	if len(asked) != 1 || len(asked[0]) != 2 || !strings.Contains(strings.Join(asked[0], ","), "shop.orders") || !strings.Contains(strings.Join(asked[0], ","), "shop.lines") {
		t.Fatalf("asked about %v, want shop.orders and shop.lines", asked)
	}
	// A table behind a WITH and a subquery is still a table the statement reads.
	asked = nil
	if err := run("WITH q AS (SELECT id FROM shop.lines) SELECT (SELECT count(*) FROM shop.orders), count(*) FROM q", asking, unchanged); err != nil {
		t.Fatalf("with + subquery: %v", err)
	}
	if len(asked) != 1 || len(asked[0]) != 2 {
		t.Fatalf("asked about %v, want both tables", asked)
	}
	// What the question answers is the refusal, word for word.
	answer = "shop.orders changed since its snapshot"
	refusedFor(run("SELECT count(*) FROM shop.orders", asking, unchanged), "shop.orders changed since its snapshot")
	answer = ""

	// Not certain which tables: refused without asking.
	asked = nil
	refusedFor(run("SHOW ALL TABLES", asking, unchanged), "may read tables it does not name")
	refusedFor(run("SELECT range FROM range(3)", asking, unchanged), "may read tables it does not name")
	refusedFor(run("SELECT count(*) FROM shop.nope", asking, unchanged), "may read tables it does not name")
	refusedFor(run("SELECT count(*) FROM events", asking, unchanged), "")
	if len(asked) != 0 {
		t.Errorf("asked about %v for statements whose tables are not certain", asked)
	}
	// Nobody to ask: refused, never answered unvouched. runSQL is that caller.
	refusedFor(run("SELECT count(*) FROM shop.orders", asking, nil), "nothing here can tell")
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT count(*) FROM shop.orders", "", 0, asking); err == nil {
		t.Error("runSQL answered a session that asked for unchanged tables with nobody to vouch for them")
	}
	// A session that does not ask: no question, and an answer.
	asked = nil
	if err := run("SELECT count(*) FROM shop.orders", sqlsandbox.Session{}, unchanged); err != nil || len(asked) != 0 {
		t.Errorf("a session that does not ask: err %v, asked %v; want an answer and no question", err, asked)
	}
}

// The port's entry point: a capture that is not known to be up to date
// settles it before a slot is taken, and the statement's own tables are
// asked about after. The fixture's files carry no binlog position, so with a
// fresh watermark the refusal is the table's.
func TestSQLOnCopy_unchangedWithin_2085(t *testing.T) {
	runner := &fakeSQLRunner{res: sqlsandbox.Result{}, refs: &sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{{Schema: "shop", Name: "orders"}}}}
	f := newSQLFixture(t, runner, false)
	q := &SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:default", id: bootServerID}
	ctx := context.Background()
	asking := sqlsandbox.Session{UnchangedWithin: time.Minute}
	changedErr := func(err error, want string) {
		t.Helper()
		var changed *sqlsandbox.MayHaveChangedError
		if !errors.As(err, &changed) || !strings.Contains(changed.Reason, want) {
			t.Errorf("err = %v, want a MayHaveChangedError saying %q", err, want)
		}
	}

	// No reporter at all (the read-only web interface).
	_, err := q.Run(ctx, "SELECT count(*) FROM shop.orders", "", asking)
	changedErr(err, "not connected to the source")
	reserved := func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		return runner.reserveCtx != nil
	}
	if reserved() {
		t.Error("a slot was reserved with capture not known to be up to date")
	}
	// A reporter with no watermark, and one that is too old.
	wm := &fakeWatermark{wm: CaptureWatermark{Detail: "the source is ahead of capture's saved position"}}
	f.s.captureStatus = wm
	_, err = q.Run(ctx, "SELECT count(*) FROM shop.orders", "", asking)
	changedErr(err, "the source is ahead")
	wm.wm = CaptureWatermark{Through: time.Now().Add(-time.Hour)}
	_, err = q.Run(ctx, "SELECT count(*) FROM shop.orders", "", asking)
	changedErr(err, "more than the limit")
	if reserved() || len(wm.asked) != 2 || wm.asked[0] != bootServerID {
		t.Errorf("slot reserved: %v, watermark asked for %v; want no slot and the boot server asked twice", reserved(), wm.asked)
	}
	// Capture is current: the slot is taken, the tables are asked about, and
	// this fixture's table cannot be vouched for.
	wm.wm = CaptureWatermark{Through: time.Now().Add(-time.Second)}
	_, err = q.Run(ctx, "SELECT count(*) FROM shop.orders", "", asking)
	changedErr(err, "shop.orders does not record its binlog position")
	if !reserved() || runner.calls() != 0 {
		t.Errorf("slot reserved: %v, jobs run: %d; want the slot taken and no statement run", reserved(), runner.calls())
	}
	// A table outside what capture records is refused by name.
	wm.wm.Captures = func(schema, table string) bool { return schema != "shop" }
	_, err = q.Run(ctx, "SELECT count(*) FROM shop.orders", "", asking)
	changedErr(err, "shop.orders is outside what capture records")
	// A session that does not ask reaches nobody.
	before := len(wm.asked)
	if _, err := q.Run(ctx, "SELECT count(*) FROM shop.orders", "", sqlsandbox.Session{}); err != nil {
		t.Errorf("a session that does not ask: %v", err)
	}
	if len(wm.asked) != before {
		t.Error("the watermark was asked for by a session that did not ask for unchanged tables")
	}
}
