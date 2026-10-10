package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// #1938, the check itself. Three things it got wrong or could not see:
// it refused on a size that counts secondary indexes, which a dump does not
// hold; it called its estimate an upper bound for tables whose reported size
// is their COMPRESSED size; and its query could go on working on the source
// after the read had stopped waiting for it.

func nstr(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
func nint(n int64) sql.NullInt64   { return sql.NullInt64{Int64: n, Valid: true} }

func row(schema, table, engine, format string, data, index int64) dumpTableRow {
	return dumpTableRow{schema: schema, table: table, engine: nstr(engine), rowFormat: nstr(format), data: nint(data), index: nint(index)}
}

func TestSummarizeDumpTables_1938(t *testing.T) {
	t.Run("no tables", func(t *testing.T) {
		if est := summarizeDumpTables(nil); est.Tables != 0 || est.Bytes != 0 || est.DataBytes != 0 || est.Compressed != 0 || len(est.CompressedTop) != 0 {
			t.Fatalf("est = %+v", est)
		}
	})
	t.Run("data and indexes are summed apart", func(t *testing.T) {
		est := summarizeDumpTables([]dumpTableRow{
			row("shop", "orders", "InnoDB", "Dynamic", 10, 30),
			row("shop", "items", "InnoDB", "Dynamic", 5, 0),
		})
		if est.Tables != 2 || est.DataBytes != 15 || est.Bytes != 45 || est.Unsized != 0 || est.Compressed != 0 {
			t.Fatalf("est = %+v", est)
		}
	})
	t.Run("a table with no reported size adds nothing and is counted", func(t *testing.T) {
		est := summarizeDumpTables([]dumpTableRow{
			{schema: "shop", table: "a", engine: nstr("InnoDB"), rowFormat: nstr("Dynamic")},
			{schema: "shop", table: "b", engine: nstr("InnoDB"), rowFormat: nstr("Dynamic"), data: nint(7)},
			row("shop", "c", "InnoDB", "Dynamic", 1, 2),
		})
		if est.Tables != 3 || est.Unsized != 2 || est.DataBytes != 8 || est.Bytes != 10 {
			t.Fatalf("est = %+v", est)
		}
	})
	t.Run("compressed storage is the row format or the engine, in any case", func(t *testing.T) {
		cases := []struct {
			engine, format string
			want           bool
		}{
			{"InnoDB", "Compressed", true}, {"InnoDB", "COMPRESSED", true}, {"innodb", "compressed", true},
			{"ROCKSDB", "Fixed", true}, {"RocksDB", "Dynamic", true},
			{"InnoDB", "Dynamic", false}, {"InnoDB", "Compact", false}, {"InnoDB", "Redundant", false},
			{"MyISAM", "Fixed", false}, {"", "", false},
			// Close to the word is not the word.
			{"InnoDB", "Compressed2", false}, {"ROCKSDB2", "Dynamic", false},
		}
		for _, c := range cases {
			est := summarizeDumpTables([]dumpTableRow{row("s", "t", c.engine, c.format, 1, 1)})
			if got := est.Compressed == 1; got != c.want {
				t.Errorf("engine %q, row format %q: compressed = %v, want %v", c.engine, c.format, got, c.want)
			}
		}
		// The server reports neither for a table it cannot open.
		if est := summarizeDumpTables([]dumpTableRow{{schema: "s", table: "t", data: nint(1)}}); est.Compressed != 0 {
			t.Errorf("a table with no engine and no row format was called compressed: %+v", est)
		}
	})
	t.Run("the two largest compressed tables are named, largest first", func(t *testing.T) {
		est := summarizeDumpTables([]dumpTableRow{
			row("shop", "small", "InnoDB", "Compressed", 1, 1),
			row("shop", "plain", "InnoDB", "Dynamic", 900, 900),
			row("shop", "big", "InnoDB", "Compressed", 50, 50),
			row("logs", "events", "ROCKSDB", "Fixed", 80, 0),
			row("shop", "tie_b", "InnoDB", "Compressed", 40, 40),
			row("shop", "tie_a", "InnoDB", "Compressed", 40, 40),
		})
		if est.Compressed != 5 || len(est.CompressedTop) != 2 || est.CompressedTop[0] != "shop.big" || est.CompressedTop[1] != "logs.events" {
			t.Fatalf("compressed = %d, named = %q", est.Compressed, est.CompressedTop)
		}
		// Same size: by name, so the sentence does not change between runs.
		est = summarizeDumpTables([]dumpTableRow{
			row("shop", "tie_b", "InnoDB", "Compressed", 40, 40), row("shop", "tie_a", "InnoDB", "Compressed", 40, 40), row("shop", "tie_c", "InnoDB", "Compressed", 40, 40),
		})
		if strings.Join(est.CompressedTop, ",") != "shop.tie_a,shop.tie_b" {
			t.Fatalf("named = %q", est.CompressedTop)
		}
	})
	t.Run("only the largest are kept, however many there are and in whatever order they come", func(t *testing.T) {
		var rows []dumpTableRow
		for i := range 500 {
			rows = append(rows, row("s", fmt.Sprintf("t%03d", i), "InnoDB", "Compressed", int64((i*37)%500), 0))
		}
		var sum dumpTableSum
		for _, r := range rows {
			sum.add(r)
			if len(sum.top) > dumpCompressedNamed {
				t.Fatalf("holding %d names", len(sum.top))
			}
		}
		// (i*37)%500 is 499 at i=27 and 498 at i=54.
		if est := sum.estimate(); est.Compressed != 500 || strings.Join(est.CompressedTop, ",") != "s.t027,s.t054" {
			t.Fatalf("compressed = %d, named = %q", est.Compressed, est.CompressedTop)
		}
	})
	t.Run("a name cannot break the line it is printed on", func(t *testing.T) {
		est := summarizeDumpTables([]dumpTableRow{row("sh\nop", "or\tders\x00", "InnoDB", "Compressed", 1, 1), row("año", "a b.c", "InnoDB", "Compressed", 2, 2)})
		if got := strings.Join(est.CompressedTop, "|"); got != "año.a b.c|sh?op.or?ders?" {
			t.Fatalf("named = %q", got)
		}
	})
}

func TestCompressedTablesSentence_1938(t *testing.T) {
	for _, c := range []struct {
		n    int
		top  []string
		want string
	}{
		{0, nil, ""},
		{1, []string{"shop.orders"}, " 1 table uses compressed storage (shop.orders): the server reports its compressed size, and a full read writes it uncompressed, so a full read can need more than the sizes say."},
		{2, []string{"shop.orders", "shop.events"}, " 2 tables use compressed storage (shop.orders, shop.events): the server reports their compressed size, and a full read writes them uncompressed, so a full read can need more than the sizes say."},
		{3, []string{"shop.orders", "shop.events"}, " 3 tables use compressed storage (shop.orders, shop.events and 1 more): the server reports their compressed size, and a full read writes them uncompressed, so a full read can need more than the sizes say."},
		{120, []string{"shop.orders", "shop.events"}, " 120 tables use compressed storage (shop.orders, shop.events and 118 more): the server reports their compressed size, and a full read writes them uncompressed, so a full read can need more than the sizes say."},
	} {
		if got := compressedTablesSentence(dumpEstimate{Compressed: c.n, CompressedTop: c.top}); got != c.want {
			t.Errorf("%d compressed:\n got %q\nwant %q", c.n, got, c.want)
		}
	}
}

// A dump holds no secondary indexes, so a table that is mostly indexes is no
// reason to refuse. The refusal reads the data alone; the warnings still read
// data plus indexes.
func TestDumpDiskVerdict_refusesOnDataAlone_1938(t *testing.T) {
	stage := t.TempDir()
	// 10 GiB of rows under 30 GiB of secondary indexes.
	est := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(40 * gib), Tables: 1}

	t.Run("15 GiB free: runs with a warning (it was refused: 15 < half of 40)", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 15 * gib})
		check, note, err := dumpDiskVerdict(stage, "", est, nil)
		if err != nil || check != dumpDiskLow {
			t.Fatalf("check = %q, err = %v, want a warning and no refusal", check, err)
		}
		t.Logf("note: %s", note)
	})
	t.Run("under half the data: refused, with the two numbers a reader needs", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 4 * gib})
		_, _, err := dumpDiskVerdict(stage, "", est, nil)
		if !errors.Is(err, errFoldDiskFull) {
			t.Fatalf("err = %v, want a disk refusal", err)
		}
		t.Logf("refusal: %v", err)
		msg := err.Error()
		for _, want := range []string{
			"the full read was not started",
			"take up about 10.0 GiB on the database server, not counting indexes",
			"the working folder " + stage + " has 4.0 GiB free",
			"Nothing was read from your database, and earlier snapshots are unchanged",
			`"Working folder" setting`, "BINTRAIL_CONSOLE_BASELINE_STAGING",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("refusal lacks %q:\n%s", want, msg)
			}
		}
		// Not the size with indexes, not the line it was measured against,
		// and none of the check's own reasoning.
		for _, not := range []string{"40.0 GiB", "5.0 GiB", "20.0 GiB", "upper bound", "half", "measured", "dump"} {
			if strings.Contains(msg, not) {
				t.Errorf("refusal says %q:\n%s", not, msg)
			}
		}
	})
	t.Run("exactly half the data is not refused", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 5 * gib})
		if _, _, err := dumpDiskVerdict(stage, "", est, nil); err != nil {
			t.Fatalf("refused at the line: %v", err)
		}
		diskByPath(t, map[string]uint64{stage: 5*gib - 1})
		if _, _, err := dumpDiskVerdict(stage, "", est, nil); !errors.Is(err, errFoldDiskFull) {
			t.Fatalf("one byte under the line was let through: %v", err)
		}
	})
	t.Run("a server that reports no data size never refuses", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 0})
		check, _, err := dumpDiskVerdict(stage, "", dumpEstimate{DataBytes: 0, Bytes: int64(40 * gib), Tables: 1, Unsized: 1}, nil)
		if err != nil || check != dumpDiskLow {
			t.Fatalf("check = %q, err = %v", check, err)
		}
	})
}

// With compressed tables the sizes are not a bound, so no verdict may say
// they are, and "there is room" becomes "the check cannot vouch for it".
func TestDumpDiskVerdict_compressedTables_1938(t *testing.T) {
	stage := t.TempDir()
	est := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4, Compressed: 3, CompressedTop: []string{"shop.orders", "shop.events"}}
	const sentence = "3 tables use compressed storage (shop.orders, shop.events and 1 more)"

	t.Run("room by the reported sizes: not vouched for", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: 100 * gib})
		check, note, err := dumpDiskVerdict(stage, "", est, nil)
		t.Logf("note: %s", note)
		if err != nil || check != dumpDiskUnchecked {
			t.Fatalf("check = %q, err = %v, want unchecked", check, err)
		}
		// Its first words say what it could not do: after "Snapshot complete"
		// a note that opened "Disk check: a full read needs..." read as a pass.
		if !strings.HasPrefix(note, "Disk check cannot tell whether this read fits. By the sizes the server reports: a full read needs about 10.0 GiB free at ") ||
			!strings.Contains(note, sentence) || !strings.Contains(note, "100.0 GiB free") || strings.Contains(note, "upper bound") {
			t.Fatalf("note = %q", note)
		}
	})
	// The snapshot folder on another disk, the usual local setup: the same.
	t.Run("room on both disks: not vouched for either", func(t *testing.T) {
		local := t.TempDir()
		stubSameFS(t, false, nil)
		diskByPath(t, map[string]uint64{stage: 100 * gib, local: 100 * gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		if err != nil || check != dumpDiskUnchecked || !strings.HasPrefix(note, "Disk check cannot tell whether this read fits. By the sizes the server reports: the dump needs about 10.0 GiB free at ") ||
			!strings.Contains(note, sentence) {
			t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
		}
		plain := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4}
		if check, note, _ := dumpDiskVerdict(stage, local, plain, nil); check != dumpDiskOK || !strings.HasPrefix(note, "Disk check: the dump needs about ") {
			t.Fatalf("without compressed tables: check = %q, note = %q", check, note)
		}
	})
	// The snapshot folder on another disk and short of room: the warning made
	// two claims about a bound ("can take up to", "the dump itself fits").
	t.Run("the other disk is short: a warning with no claim the sizes cannot back", func(t *testing.T) {
		local := t.TempDir()
		stubSameFS(t, false, nil)
		diskByPath(t, map[string]uint64{stage: 100 * gib, local: gib})
		check, note, err := dumpDiskVerdict(stage, local, est, nil)
		t.Logf("note: %s", note)
		if err != nil || check != dumpDiskLow || !strings.Contains(note, sentence) {
			t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
		}
		for _, not := range []string{"up to", "The dump itself fits", "upper bound"} {
			if strings.Contains(note, not) {
				t.Errorf("the note says %q over compressed tables: %s", not, note)
			}
		}
		if !strings.Contains(note, "By those sizes the dump fits at "+stage) {
			t.Errorf("the note no longer says where the dump goes: %s", note)
		}
		// Without compressed tables the sentence is what it was.
		plain := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4}
		if _, note, _ := dumpDiskVerdict(stage, local, plain, nil); !strings.Contains(note, "the copy can take up to about 10.0 GiB") || !strings.Contains(note, "The dump itself fits at "+stage) {
			t.Errorf("the plain note changed: %s", note)
		}
	})
	for name, free := range map[string]uint64{"below the sizes": 9 * gib, "below the peak": 15 * gib} {
		t.Run(name+": a warning that does not call the sizes a bound", func(t *testing.T) {
			diskByPath(t, map[string]uint64{stage: free})
			check, note, err := dumpDiskVerdict(stage, "", est, nil)
			if err != nil || check != dumpDiskLow || !strings.Contains(note, sentence) || strings.Contains(note, "upper bound") {
				t.Fatalf("check = %q, err = %v, note = %q", check, err, note)
			}
		})
	}
	t.Run("a refusal names them too", func(t *testing.T) {
		diskByPath(t, map[string]uint64{stage: gib})
		_, _, err := dumpDiskVerdict(stage, "", est, nil)
		if !errors.Is(err, errFoldDiskFull) || !strings.Contains(err.Error(), sentence) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("without compressed tables the verdicts are what they were", func(t *testing.T) {
		plain := dumpEstimate{DataBytes: int64(10 * gib), Bytes: int64(10 * gib), Tables: 4}
		diskByPath(t, map[string]uint64{stage: 100 * gib})
		if check, note, _ := dumpDiskVerdict(stage, "", plain, nil); check != dumpDiskOK || strings.Contains(note, "compressed storage") {
			t.Fatalf("check = %q, note = %q", check, note)
		}
		diskByPath(t, map[string]uint64{stage: 9 * gib})
		if check, note, _ := dumpDiskVerdict(stage, "", plain, nil); check != dumpDiskLow || !strings.Contains(note, "upper bound") {
			t.Fatalf("check = %q, note = %q", check, note)
		}
	})
}

type fakeExec struct {
	stmts []string
	fail  map[string]error
}

func (f *fakeExec) ExecContext(_ context.Context, q string, _ ...any) (sql.Result, error) {
	f.stmts = append(f.stmts, q)
	return nil, f.fail[q]
}

// The session is told to stop its own query: the read's timeout only stops
// the waiting, and the driver closes the connection without a KILL.
func TestPrepareEstimateSession_1938(t *testing.T) {
	const (
		fresh   = "SET SESSION information_schema_stats_expiry = 0"
		mysqlTL = "SET SESSION max_execution_time = 12000"
		mariaTL = "SET SESSION max_statement_time = 12"
	)
	unknown := &mysqldriver.MySQLError{Number: 1193, Message: "Unknown system variable"}
	denied := &mysqldriver.MySQLError{Number: 1227, Message: "Access denied"}

	t.Run("MySQL: fresh sizes and its own limit, and MariaDB's is never sent", func(t *testing.T) {
		f := &fakeExec{}
		if stale := prepareEstimateSession(context.Background(), f); stale {
			t.Fatal("stale with nothing refused")
		}
		if strings.Join(f.stmts, "|") != fresh+"|"+mysqlTL {
			t.Fatalf("stmts = %q", f.stmts)
		}
	})
	t.Run("MariaDB: no size cache, no MySQL limit, so its own limit", func(t *testing.T) {
		f := &fakeExec{fail: map[string]error{fresh: unknown, mysqlTL: unknown}}
		if stale := prepareEstimateSession(context.Background(), f); stale {
			t.Fatal("MariaDB's sizes are not cached")
		}
		if strings.Join(f.stmts, "|") != fresh+"|"+mysqlTL+"|"+mariaTL {
			t.Fatalf("stmts = %q", f.stmts)
		}
	})
	t.Run("a refused cache setting still marks the sizes stale", func(t *testing.T) {
		f := &fakeExec{fail: map[string]error{fresh: denied}}
		if stale := prepareEstimateSession(context.Background(), f); !stale || len(f.stmts) != 2 {
			t.Fatalf("stale = %v, stmts = %q", stale, f.stmts)
		}
	})
	// The same name means milliseconds on some Percona builds: a limit that
	// was refused for another reason must not fall through to it.
	t.Run("a limit refused for another reason does not try the other one, and is not a reason to skip the check", func(t *testing.T) {
		f := &fakeExec{fail: map[string]error{mysqlTL: denied}}
		if stale := prepareEstimateSession(context.Background(), f); stale || strings.Join(f.stmts, "|") != fresh+"|"+mysqlTL {
			t.Fatalf("stale = %v, stmts = %q", stale, f.stmts)
		}
		f = &fakeExec{fail: map[string]error{mysqlTL: unknown, mariaTL: errors.New("proxy says no")}}
		if stale := prepareEstimateSession(context.Background(), f); stale || len(f.stmts) != 3 {
			t.Fatalf("stale = %v, stmts = %q", stale, f.stmts)
		}
	})
	// The limit ends before the read stops waiting, so the source's own
	// error is what comes back, not a connection cut under it.
	if dumpEstimateServerLimit >= dumpEstimateTimeout {
		t.Fatalf("the server-side limit %v is not under the wait %v", dumpEstimateServerLimit, dumpEstimateTimeout)
	}
}
