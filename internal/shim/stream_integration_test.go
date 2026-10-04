//go:build integration

package shim

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql" // database/sql driver registration

	"github.com/go-mysql-org/go-mysql/server"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// startShimServer runs a shim listener that binds each accepted connection's
// Handler (so the streaming full-table path is live) and loops HandleCommand
// until the client disconnects or the listener closes. Returns the address;
// the listener is closed on test cleanup. Read deadlines guarantee the server
// goroutines can never wedge a test.
func startShimServer(t *testing.T, db *sql.DB, cfg Config, defaultSchema string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	srv := server.NewDefaultServer()
	auth, err := NewTenantAuth(map[string]string{"u": "p"})
	if err != nil {
		t.Fatalf("NewTenantAuth: %v", err)
	}

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go func(c net.Conn) {
				defer c.Close()
				c.SetReadDeadline(time.Now().Add(15 * time.Second))
				h := NewHandlerWithConfig(db, cfg, slog.Default())
				if defaultSchema != "" {
					_ = h.UseDB(defaultSchema)
				}
				mc, err := server.NewCustomizedConn(c, srv, auth, h)
				if err != nil {
					return
				}
				h.BindConn(mc) // enable the streaming full-table _snapshot path (#998)
				session := NewSession(mc, h)
				for {
					c.SetReadDeadline(time.Now().Add(15 * time.Second))
					if err := session.HandleCommand(); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

// queryShim connects a real go-sql-driver client and runs one SELECT, returning
// the column names and every row as string cells (NULL → "NULL"). This drives
// the full MySQL wire protocol end-to-end, so a streaming-framing or
// packet-sequence bug surfaces here as a driver error rather than passing a
// server-side-only assertion.
func queryShim(t *testing.T, addr, schema, query string, args ...any) ([]string, [][]string, error) {
	t.Helper()
	db, err := sql.Open("mysql", "u:p@tcp("+addr+")/"+schema)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()
	// With args the driver sends COM_STMT_PREPARE + COM_STMT_EXECUTE (the
	// binary protocol); without, a text COM_QUERY.
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	var out [][]string
	for rows.Next() {
		cells := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make([]string, len(cols))
		for i, c := range cells {
			if c.Valid {
				row[i] = c.String
			} else {
				row[i] = "NULL"
			}
		}
		out = append(out, row)
	}
	return cols, out, rows.Err()
}

func byID(rows [][]string) map[string]string {
	m := make(map[string]string, len(rows))
	for _, r := range rows {
		m[r[0]] = r[1]
	}
	return m
}

// TestStreamSnapshotFullTable_OverWire is the end-to-end proof of #998: a
// full-table _snapshot AS OF query streams its whole resultset over a real
// MySQL connection even with FullTableRowCap set to 1 — the streaming path
// lifts the cap that would abort the buffered path — while a LIMIT keeps the
// bounded buffered path (which still honours the cap). Driving a real
// go-sql-driver client validates the wire framing and packet sequencing the
// streamWriter emits.
func TestStreamSnapshotFullTable_OverWire(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	asOfStr := asOf.Format("2006-01-02 15:04:05")

	seedUsersSnapshot(t, db, snapTime)
	// Baseline: id=1 alice (never touched), id=2 bob (updated), id=3 carol
	// (deleted). id=4 is inserted only in the binlog.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "users", usersBaselineCols(), [][]string{
		{"1", "alice"},
		{"2", "bob"},
		{"3", "carol"},
	})
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 2 /*update*/, "2", nil,
		[]byte(`{"id":2,"name":"bob"}`), []byte(`{"id":2,"name":"bob2"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, eventTS, nil,
		"myapp", "users", 3 /*delete*/, "3", nil,
		[]byte(`{"id":3,"name":"carol"}`), nil)
	testutil.InsertEvent(t, db, "mysql-bin.000001", 300, 400, eventTS, nil,
		"myapp", "users", 1 /*insert*/, "4", nil, nil,
		[]byte(`{"id":4,"name":"dave"}`))

	// FullTableRowCap=1 would abort the buffered path at the 2nd row; the
	// streaming path must ignore it and return the whole table.
	cfg := Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: baselineDir, FullTableRowCap: 1}
	addr := startShimServer(t, db, cfg, "myapp")

	t.Run("streaming_returns_full_table_ignoring_cap", func(t *testing.T) {
		cols, data, err := queryShim(t, addr, "myapp", "SELECT * FROM _snapshot.users AS OF '"+asOfStr+"'")
		if err != nil {
			t.Fatalf("streamed full-table _snapshot failed over the wire: %v", err)
		}
		if !slices.Equal(cols, []string{"id", "name"}) {
			t.Errorf("columns = %v, want [id name]", cols)
		}
		got := byID(data)
		want := map[string]string{"1": "alice", "2": "bob2", "4": "dave"}
		if len(got) != len(want) {
			t.Fatalf("streamed %d rows %v, want %d %v", len(got), got, len(want), want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("streamed row id=%s = %q, want %q (full: %v)", k, got[k], v, got)
			}
		}
		if _, resurrected := got["3"]; resurrected {
			t.Errorf("streamed row set resurrected deleted id=3: %v", got)
		}
	})

	// A prepared statement (#2036) cannot be answered with streamed text
	// rows: its client reads binary ones. The same whole-table read must
	// come back buffered and binary, so it returns the table on a server
	// with the default cap and is refused by the 1-row cap the streamed
	// read ignores.
	t.Run("prepared_statement_is_buffered_not_streamed", func(t *testing.T) {
		roomy := cfg
		roomy.FullTableRowCap = 0
		addr2 := startShimServer(t, db, roomy, "myapp")
		cols, data, err := queryShim(t, addr2, "myapp", "SELECT * FROM _snapshot.users AS OF ?", asOfStr)
		if err != nil {
			t.Fatalf("prepared full-table _snapshot failed over the wire: %v", err)
		}
		if !slices.Equal(cols, []string{"id", "name"}) {
			t.Errorf("columns = %v, want [id name]", cols)
		}
		got := byID(data)
		want := map[string]string{"1": "alice", "2": "bob2", "4": "dave"}
		if len(got) != len(want) {
			t.Fatalf("prepared read returned %d rows %v, want %d %v", len(got), got, len(want), want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("prepared row id=%s = %q, want %q (full: %v)", k, got[k], v, got)
			}
		}
		if _, _, err := queryShim(t, addr, "myapp", "SELECT * FROM _snapshot.users AS OF ?", asOfStr); err == nil {
			t.Error("the prepared read ignored the 1-row cap: it was streamed, not buffered")
		} else if !strings.Contains(err.Error(), "1104") {
			t.Errorf("the prepared read under a 1-row cap failed for another reason than the cap: %v", err)
		}
		// A point read with two arguments, one of them the PK.
		_, one, err := queryShim(t, addr, "myapp", "SELECT * FROM _flashback.users AS OF ? WHERE id = ?", asOfStr, 2)
		if err != nil || len(one) != 1 || one[0][1] != "bob2" {
			t.Errorf("prepared _flashback point read = %v, %v; want bob2", one, err)
		}
	})

	t.Run("limit_under_cap_browses_via_buffered_path", func(t *testing.T) {
		// LIMIT keeps the buffered path (streaming only fires for a LIMIT-less
		// query). LIMIT 1 <= cap 1, so it must SUCCEED with exactly one row —
		// the "add a LIMIT to browse" remedy under the cap.
		_, data, err := queryShim(t, addr, "myapp", "SELECT * FROM _snapshot.users AS OF '"+asOfStr+"' LIMIT 1")
		if err != nil {
			t.Fatalf("LIMIT 1 under cap should browse, not error: %v", err)
		}
		if len(data) != 1 {
			t.Errorf("LIMIT 1 returned %d rows, want 1: %v", len(data), data)
		}
	})

	t.Run("limit_above_cap_still_trips_cap", func(t *testing.T) {
		// A LIMIT never RAISES the cap: LIMIT 2 > cap 1 must still error rather
		// than buffer past the cap.
		_, _, err := queryShim(t, addr, "myapp", "SELECT * FROM _snapshot.users AS OF '"+asOfStr+"' LIMIT 2")
		if err == nil {
			t.Fatal("LIMIT 2 above cap 1 should trip ER_TOO_BIG_SELECT, got no error")
		}
	})

	t.Run("column_list_streams_verbatim_projection", func(t *testing.T) {
		// A column list on the LIMIT-less full-table shape must STREAM the
		// requested columns only — not the full ddlOrder. Under cap=1 this also
		// proves the projected query still streams (uncapped): a regression that
		// fell back to the buffered path would trip ER_TOO_BIG_SELECT.
		cols, data, err := queryShim(t, addr, "myapp", "SELECT id FROM _snapshot.users AS OF '"+asOfStr+"'")
		if err != nil {
			t.Fatalf("projected full-table _snapshot failed over the wire: %v", err)
		}
		if !slices.Equal(cols, []string{"id"}) {
			t.Errorf("columns = %v, want [id] (projection must not stream all of ddlOrder)", cols)
		}
		ids := make(map[string]bool, len(data))
		for _, r := range data {
			ids[r[0]] = true
		}
		if len(data) != 3 || !ids["1"] || !ids["2"] || !ids["4"] {
			t.Errorf("projected ids = %v, want the 3 live rows {1,2,4}", data)
		}
	})
}

// TestStreamSnapshotFullTable_EmptyOverWire pins the empty-resultset streaming
// path over a real client: a full-table _snapshot whose rows are all deleted by
// the AS OF instant must send a well-formed zero-row resultset (column
// definitions + terminating EOF, no rows), not a malformed or hung response.
func TestStreamSnapshotFullTable_EmptyOverWire(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	asOfStr := asOf.Format("2006-01-02 15:04:05")

	seedUsersSnapshot(t, db, snapTime)
	// Baseline has one row; it is deleted before AS OF → zero live rows.
	baselineDir := writeBaselineSnapshot(t, snapTime, "myapp", "users", usersBaselineCols(), [][]string{
		{"1", "alice"},
	})
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 3 /*delete*/, "1", nil,
		[]byte(`{"id":1,"name":"alice"}`), nil)

	cfg := Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: baselineDir}
	addr := startShimServer(t, db, cfg, "myapp")

	cols, data, err := queryShim(t, addr, "myapp", "SELECT * FROM _snapshot.users AS OF '"+asOfStr+"'")
	if err != nil {
		t.Fatalf("empty streamed full-table _snapshot failed over the wire: %v", err)
	}
	if !slices.Equal(cols, []string{"id", "name"}) {
		t.Errorf("columns = %v, want [id name] even for an empty result", cols)
	}
	if len(data) != 0 {
		t.Errorf("expected zero rows (all deleted by AS OF), got %v", data)
	}
}

// TestRunFullTable_LimitBoundsFetchOverWire is the FAITHFUL binlog full-table
// LIMIT test: unlike the sqlmock unit test (sqlmock ignores the SQL LIMIT), a
// real MySQL honours it, so a table with MORE than the cap of live rows proves
// LIMIT actually bounds the fetch. `_flashback` full-table (no baseline) is the
// buffered binlog path (runFullTable), NOT the streaming _snapshot path.
func TestRunFullTable_LimitBoundsFetchOverWire(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)
	eventTS := hourTop.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	asOfStr := asOf.Format("2006-01-02 15:04:05")

	seedUsersSnapshot(t, db, snapTime)
	// Five live rows (distinct PKs), cap 3 → the table exceeds the cap.
	for i := 1; i <= 5; i++ {
		testutil.InsertEvent(t, db, "mysql-bin.000001", uint64(i*100), uint64(i*100+100), eventTS, nil,
			"myapp", "users", 1 /*insert*/, fmt.Sprintf("%d", i), nil, nil,
			[]byte(fmt.Sprintf(`{"id":%d,"name":"u%d"}`, i, i)))
	}

	// No baseline → _flashback full-table = the binlog buffered path.
	cfg := Config{AllowGaps: true, NoArchive: true, IndexDBName: dbName, FullTableRowCap: 3}
	addr := startShimServer(t, db, cfg, "myapp")

	t.Run("limit_under_cap_bounds_fetch", func(t *testing.T) {
		// LIMIT 2 <= cap 3: real MySQL returns exactly 2, no cap error. Without
		// #997, the fetch would probe cap+1=4 rows (>cap) and trip the cap.
		_, data, err := queryShim(t, addr, "myapp", "SELECT * FROM _flashback.users AS OF '"+asOfStr+"' LIMIT 2")
		if err != nil {
			t.Fatalf("_flashback full-table LIMIT 2 under cap should browse, got %v", err)
		}
		if len(data) != 2 {
			t.Errorf("LIMIT 2 returned %d rows, want 2", len(data))
		}
	})
	t.Run("no_limit_trips_cap", func(t *testing.T) {
		// 5 live rows > cap 3, no LIMIT → ER_TOO_BIG_SELECT.
		_, _, err := queryShim(t, addr, "myapp", "SELECT * FROM _flashback.users AS OF '"+asOfStr+"'")
		if err == nil {
			t.Fatal("5 rows > cap 3 with no LIMIT should trip ER_TOO_BIG_SELECT")
		}
	})
	t.Run("limit_above_cap_trips_cap", func(t *testing.T) {
		// LIMIT 5 > cap 3: a LIMIT never raises the cap → still errors.
		_, _, err := queryShim(t, addr, "myapp", "SELECT * FROM _flashback.users AS OF '"+asOfStr+"' LIMIT 5")
		if err == nil {
			t.Fatal("LIMIT 5 above cap 3 should trip ER_TOO_BIG_SELECT")
		}
	})
}
