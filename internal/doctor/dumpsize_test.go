package doctor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// The estimate of what a full read needs (#1938): what it counts, what it
// says about tables stored compressed, and the session its query runs on.
// Moved here with the code from consoleapp (#2259).

func nstr(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
func nint(n int64) sql.NullInt64   { return sql.NullInt64{Int64: n, Valid: true} }

func dumpRow(schema, table, engine, format string, data, index int64) dumpTableRow {
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
			dumpRow("shop", "orders", "InnoDB", "Dynamic", 10, 30),
			dumpRow("shop", "items", "InnoDB", "Dynamic", 5, 0),
		})
		if est.Tables != 2 || est.DataBytes != 15 || est.Bytes != 45 || est.Unsized != 0 || est.Compressed != 0 {
			t.Fatalf("est = %+v", est)
		}
	})
	t.Run("a table with no reported size adds nothing and is counted", func(t *testing.T) {
		est := summarizeDumpTables([]dumpTableRow{
			{schema: "shop", table: "a", engine: nstr("InnoDB"), rowFormat: nstr("Dynamic")},
			{schema: "shop", table: "b", engine: nstr("InnoDB"), rowFormat: nstr("Dynamic"), data: nint(7)},
			dumpRow("shop", "c", "InnoDB", "Dynamic", 1, 2),
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
			est := summarizeDumpTables([]dumpTableRow{dumpRow("s", "t", c.engine, c.format, 1, 1)})
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
			dumpRow("shop", "small", "InnoDB", "Compressed", 1, 1),
			dumpRow("shop", "plain", "InnoDB", "Dynamic", 900, 900),
			dumpRow("shop", "big", "InnoDB", "Compressed", 50, 50),
			dumpRow("logs", "events", "ROCKSDB", "Fixed", 80, 0),
			dumpRow("shop", "tie_b", "InnoDB", "Compressed", 40, 40),
			dumpRow("shop", "tie_a", "InnoDB", "Compressed", 40, 40),
		})
		if est.Compressed != 5 || len(est.CompressedTop) != 2 || est.CompressedTop[0] != "shop.big" || est.CompressedTop[1] != "logs.events" {
			t.Fatalf("compressed = %d, named = %q", est.Compressed, est.CompressedTop)
		}
		// Same size: by name, so the sentence does not change between runs.
		est = summarizeDumpTables([]dumpTableRow{
			dumpRow("shop", "tie_b", "InnoDB", "Compressed", 40, 40), dumpRow("shop", "tie_a", "InnoDB", "Compressed", 40, 40), dumpRow("shop", "tie_c", "InnoDB", "Compressed", 40, 40),
		})
		if strings.Join(est.CompressedTop, ",") != "shop.tie_a,shop.tie_b" {
			t.Fatalf("named = %q", est.CompressedTop)
		}
	})
	t.Run("only the largest are kept, however many there are and in whatever order they come", func(t *testing.T) {
		var rows []dumpTableRow
		for i := range 500 {
			rows = append(rows, dumpRow("s", fmt.Sprintf("t%03d", i), "InnoDB", "Compressed", int64((i*37)%500), 0))
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
		est := summarizeDumpTables([]dumpTableRow{dumpRow("sh\nop", "or\tders\x00", "InnoDB", "Compressed", 1, 1), dumpRow("año", "a b.c", "InnoDB", "Compressed", 2, 2)})
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
		if got := DumpCompressedSentence(DumpEstimate{Compressed: c.n, CompressedTop: c.top}); got != c.want {
			t.Errorf("%d compressed:\n got %q\nwant %q", c.n, got, c.want)
		}
	}
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
		if stale := PrepareDumpEstimateSession(context.Background(), f); stale {
			t.Fatal("stale with nothing refused")
		}
		if strings.Join(f.stmts, "|") != fresh+"|"+mysqlTL {
			t.Fatalf("stmts = %q", f.stmts)
		}
	})
	t.Run("MariaDB: no size cache, no MySQL limit, so its own limit", func(t *testing.T) {
		f := &fakeExec{fail: map[string]error{fresh: unknown, mysqlTL: unknown}}
		if stale := PrepareDumpEstimateSession(context.Background(), f); stale {
			t.Fatal("MariaDB's sizes are not cached")
		}
		if strings.Join(f.stmts, "|") != fresh+"|"+mysqlTL+"|"+mariaTL {
			t.Fatalf("stmts = %q", f.stmts)
		}
	})
	t.Run("a refused cache setting still marks the sizes stale", func(t *testing.T) {
		f := &fakeExec{fail: map[string]error{fresh: denied}}
		if stale := PrepareDumpEstimateSession(context.Background(), f); !stale || len(f.stmts) != 2 {
			t.Fatalf("stale = %v, stmts = %q", stale, f.stmts)
		}
	})
	// The same name means milliseconds on some Percona builds: a limit that
	// was refused for another reason must not fall through to it.
	t.Run("a limit refused for another reason does not try the other one, and is not a reason to skip the check", func(t *testing.T) {
		f := &fakeExec{fail: map[string]error{mysqlTL: denied}}
		if stale := PrepareDumpEstimateSession(context.Background(), f); stale || strings.Join(f.stmts, "|") != fresh+"|"+mysqlTL {
			t.Fatalf("stale = %v, stmts = %q", stale, f.stmts)
		}
		f = &fakeExec{fail: map[string]error{mysqlTL: unknown, mariaTL: errors.New("proxy says no")}}
		if stale := PrepareDumpEstimateSession(context.Background(), f); stale || len(f.stmts) != 3 {
			t.Fatalf("stale = %v, stmts = %q", stale, f.stmts)
		}
	})
	// The limit ends before the read stops waiting, so the source's own
	// error is what comes back, not a connection cut under it.
	if DumpEstimateServerLimit >= DumpEstimateTimeout {
		t.Fatalf("the server-side limit %v is not under the wait %v", DumpEstimateServerLimit, DumpEstimateTimeout)
	}
}

// The estimate selects exactly the tables the dump selects: every
// non-system schema for an empty list, the names verbatim otherwise.
func TestDumpSizeQuery(t *testing.T) {
	q, args := dumpSizeQuery(nil)
	if !strings.Contains(q, "TABLE_SCHEMA NOT IN ('mysql','sys','performance_schema','information_schema')") || len(args) != 0 {
		t.Fatalf("empty list: %s %v", q, args)
	}
	// One row per table, with what tells a compressed one apart (#1938).
	if !strings.HasPrefix(q, "SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, ROW_FORMAT, DATA_LENGTH, INDEX_LENGTH FROM information_schema.TABLES WHERE ") ||
		strings.Contains(q, "SUM(") || strings.Contains(q, "GROUP_CONCAT") || !strings.Contains(q, "'BASE TABLE', 'SYSTEM VERSIONED'") {
		t.Fatalf("query = %s", q)
	}
	_, args = dumpSizeQuery([]string{"Shop", " b "})
	if len(args) != 2 || args[0] != "Shop" || args[1] != " b " {
		t.Fatalf("names were changed on the way: %v", args)
	}
	where, _ := DumpableTablesWhere([]string{"a"})
	if where != "TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED') AND TABLE_SCHEMA IN (?)" {
		t.Fatalf("filter = %s", where)
	}
}
