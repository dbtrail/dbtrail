//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// mariaDBVersion returns the (major, minor) of the connected MariaDB server,
// parsed from VERSION() ("11.4.12-MariaDB-ubu2404-log").
func mariaDBVersion(t *testing.T, db *sql.DB) (int, int) {
	t.Helper()
	var v string
	if err := db.QueryRow("SELECT VERSION()").Scan(&v); err != nil {
		t.Fatalf("SELECT VERSION(): %v", err)
	}
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		t.Fatalf("unparseable MariaDB version %q", v)
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		t.Fatalf("unparseable MariaDB version %q", v)
	}
	return major, minor
}

func mariaDBAtLeast(major, minor, wantMajor, wantMinor int) bool {
	return major > wantMajor || (major == wantMajor && minor >= wantMinor)
}

// mariaDBRoundTrip is the MariaDB sibling of the #942 apply-and-assert harness
// (TestRecoverRoundTrip_ApplyAndAssert). It runs setup on the MariaDB source,
// captures `capture` (the ground truth), streams `mutations` through the REAL
// StreamParser -> indexer -> streamLoop pipeline into a MySQL index, generates
// the reversal script with recover, APPLIES it to the live MariaDB source, and
// returns the before and after captures plus the script.
type mariaDBRoundTrip struct {
	setup      []string // DDL + pre-window rows; never streamed
	mutations  []string // the window recover reverses
	table      string
	wantEvents int    // events indexed for the whole schema
	reversed   int    // events recover reverses for table; 0 means wantEvents
	capture    string // SELECT returning the canonical form of every row
}

type roundTripResult struct {
	cols     []string
	before   [][]*string
	restored [][]*string
	script   string
}

func (rt mariaDBRoundTrip) run(t *testing.T, indexDB, sourceDB *sql.DB, sourceName string) roundTripResult {
	t.Helper()
	for _, stmt := range rt.setup {
		testutil.MustExec(t, sourceDB, stmt)
	}

	capture := func() ([]string, [][]*string) {
		t.Helper()
		rows, err := sourceDB.Query(rt.capture)
		if err != nil {
			t.Fatalf("capture query: %v", err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("capture columns: %v", err)
		}
		var out [][]*string
		for rows.Next() {
			cells := make([]sql.RawBytes, len(cols))
			ptrs := make([]any, len(cols))
			for i := range cells {
				ptrs[i] = &cells[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatalf("capture scan: %v", err)
			}
			row := make([]*string, len(cols))
			for i, c := range cells {
				if c != nil {
					s := string(c) // copy: RawBytes is reused
					row[i] = &s
				}
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("capture rows: %v", err)
		}
		return cols, out
	}
	cols, before := capture()

	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition against MariaDB: %v", err)
	}
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	mc, err := drivermysql.ParseDSN(testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, err := net.SplitHostPort(mc.Addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	portN, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("ParseUint(port): %v", err)
	}

	// Same syncer shape as production for a MariaDB source (streamrun.One).
	const serverID = 99961
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:                serverID,
		Flavor:                  gomysql.MariaDBFlavor,
		Host:                    hostStr,
		Port:                    uint16(portN),
		User:                    mc.User,
		Password:                mc.Passwd,
		TimestampStringLocation: time.UTC,
		DumpCommandFlag:         replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT,
		FillZeroLogPos:          true,
	})
	defer syncer.Close()
	streamer, err := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if err != nil {
		testutil.SkipOrFailMariaDB(t, "StartSync against MariaDB failed: %v", err)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	sp := parser.NewStreamParser(resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	sp.SetFlavor(gomysql.MariaDBFlavor)
	idx := indexer.New(indexDB, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()
	state := &streamState{mode: "position", flavor: gomysql.MariaDBFlavor, serverID: serverID}
	loopErrCh := make(chan error, 1)
	go func() {
		loopErrCh <- streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test-mariadb-types"), nil)
	}()

	for _, stmt := range rt.mutations {
		testutil.MustExec(t, sourceDB, stmt)
	}
	waitIndexedCount(t, indexDB, sourceName, rt.wantEvents, 20*time.Second)
	cancel()
	if err := <-loopErrCh; err != nil {
		t.Fatalf("streamLoop: %v", err)
	}
	if perr := <-parseErrCh; perr != nil &&
		!errors.Is(perr, context.Canceled) && !errors.Is(perr, context.DeadlineExceeded) {
		t.Fatalf("StreamParser error: %v", perr)
	}

	var buf strings.Builder
	n, err := recovery.New(indexDB, resolver).
		GenerateSQL(context.Background(), query.Options{Schema: sourceName, Table: rt.table, Limit: 1000}, &buf)
	if err != nil {
		t.Fatalf("GenerateSQL: %v", err)
	}
	wantReversed := rt.reversed
	if wantReversed == 0 {
		wantReversed = rt.wantEvents
	}
	if n != wantReversed {
		t.Fatalf("recover reversed %d events, want %d\nSQL:\n%s", n, wantReversed, buf.String())
	}

	applyDB, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/"+sourceName+"?parseTime=true&multiStatements=true")
	if err != nil {
		t.Fatalf("open apply conn: %v", err)
	}
	defer applyDB.Close()
	if _, err := applyDB.Exec(buf.String()); err != nil {
		t.Fatalf("reversal script failed to apply against MariaDB: %v\nSQL:\n%s", err, buf.String())
	}

	_, restored := capture()
	return roundTripResult{cols: cols, before: before, restored: restored, script: buf.String()}
}

func cellString(p *string) string {
	if p == nil {
		return "<NULL>"
	}
	return *p
}

// assertExact fails, per row and per column, wherever the restored capture is
// not byte-identical to the pre-window capture.
func (r roundTripResult) assertExact(t *testing.T) {
	t.Helper()
	if len(r.restored) != len(r.before) {
		t.Fatalf("row count after restore: got %d, want %d\nSQL:\n%s", len(r.restored), len(r.before), r.script)
	}
	failed := false
	for i := range r.before {
		for j, col := range r.cols {
			want, got := cellString(r.before[i][j]), cellString(r.restored[i][j])
			if want != got {
				failed = true
				t.Errorf("row %d column %s not restored byte-exact:\n  want: %s\n  got:  %s", i+1, col, want, got)
			}
		}
	}
	if failed {
		t.Logf("reversal SQL:\n%s", r.script)
	}
}

// TestRecoverRoundTrip_MariaDBTypes closes the capture -> index -> recover ->
// APPLY loop for the column types that exist only in MariaDB: UUID (10.7+),
// INET4 (10.10+), INET6 (10.5+) and VECTOR (11.7+). Each type is gated on the
// server version so the test runs on every MariaDB CI leg.
//
// The values are chosen for the two ways these types can be corrupted:
//
//   - Byte order. MariaDB may keep some UUIDs byte-swapped internally, but the
//     binlog row image carries the value in TEXT order (checked with
//     mariadb-binlog -vv on 11.4), and an X'..' literal assigned to a UUID column
//     is read in text order too, so no swap is needed. Version 1, 4 and 7 UUIDs
//     are covered so a server that did write swapped bytes for some versions
//     would fail here.
//   - Trimmed zeros. The row image trims trailing zero bytes from these
//     fixed-width types, like BINARY(n): 10.0.0.0 arrives as one byte, the nil
//     UUID as an empty string. Unlike BINARY(n), a UUID/INET column refuses a
//     short binary value, so the capture must pad them back.
//
// Rows 1-2 are reverse-UPDATEd, rows 3-4 reverse-DELETEd (re-INSERTed), row 5
// stays untouched and row 6 is reverse-INSERTed (deleted). The comparison is the
// text form AND HEX() of every column, so a value that reads the same but is
// stored differently still fails.
func TestRecoverRoundTrip_MariaDBTypes(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	sourceDB.SetMaxOpenConns(1)

	major, minor := mariaDBVersion(t, sourceDB)
	type typedCol struct {
		name, ddl, since string
		ok               bool
		vals             [6]string // SQL expressions for rows 1..6
		mutated          string    // value the UPDATE writes on rows 1-2
		canon            []string  // capture expressions
	}
	cols := []typedCol{
		{
			name: "c_uuid", ddl: "UUID", since: "10.7", ok: mariaDBAtLeast(major, minor, 10, 7),
			vals: [6]string{
				"'12345678-9abc-1def-8012-3456789abcde'", // v1, variant 10xx
				"'01890a5d-ac96-774b-bcce-b302099a8057'", // v7
				"'e0b5a0f4-3c9d-4b8e-9f1a-000000000000'", // v4, trailing zero bytes
				"'00000000-0000-0000-0000-000000000000'", // nil UUID
				"'ffffffff-ffff-ffff-ffff-ffffffffffff'", // all bits set
				"'6ba7b810-9dad-11d1-80b4-00c04fd430c8'", // v1 (RFC 4122 namespace)
			},
			mutated: "'11111111-2222-4333-8444-555555555555'",
			canon:   []string{"c_uuid", "HEX(c_uuid)"},
		},
		{
			name: "c_inet4", ddl: "INET4", since: "10.10", ok: mariaDBAtLeast(major, minor, 10, 10),
			vals:    [6]string{"'10.0.0.0'", "'192.168.1.1'", "'0.0.0.0'", "'255.255.255.255'", "'1.0.0.0'", "'8.8.4.4'"},
			mutated: "'127.0.0.1'",
			canon:   []string{"c_inet4", "HEX(c_inet4)"},
		},
		{
			name: "c_inet6", ddl: "INET6", since: "10.5", ok: mariaDBAtLeast(major, minor, 10, 5),
			vals: [6]string{
				"'2001:db8::'", "'::ffff:10.0.0.0'", "'::'", "'fe80::1'",
				"'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'", "'2001:db8:0:0:1::'",
			},
			mutated: "'::1'",
			canon:   []string{"c_inet6", "HEX(c_inet6)"},
		},
		{
			name: "c_vec", ddl: "VECTOR(3)", since: "11.7", ok: mariaDBAtLeast(major, minor, 11, 7),
			vals: [6]string{
				"VEC_FromText('[1.5,-2,0]')", "VEC_FromText('[0,0,0]')", "VEC_FromText('[123456.78,-0.000001,0.1]')",
				"VEC_FromText('[1,0,0]')", "VEC_FromText('[-0,1,2]')", "VEC_FromText('[9,9,9]')",
			},
			mutated: "VEC_FromText('[7,7,7]')",
			canon:   []string{"HEX(c_vec)"},
		},
	}

	var ddl, canon, insertCols []string
	var active []typedCol
	for _, c := range cols {
		if !c.ok {
			t.Logf("MariaDB %d.%d: %s (%s) needs %s+, not covered on this server", major, minor, c.name, c.ddl, c.since)
			continue
		}
		active = append(active, c)
		ddl = append(ddl, c.name+" "+c.ddl+" NULL")
		canon = append(canon, c.canon...)
		insertCols = append(insertCols, c.name)
	}
	if len(active) == 0 {
		t.Skipf("MariaDB %d.%d has none of UUID/INET4/INET6/VECTOR", major, minor)
	}

	insert := func(id int) string {
		vals := []string{strconv.Itoa(id)}
		for _, c := range active {
			vals = append(vals, c.vals[id-1])
		}
		return fmt.Sprintf("INSERT INTO mtypes (id, %s) VALUES (%s)", strings.Join(insertCols, ", "), strings.Join(vals, ", "))
	}
	var sets []string
	for _, c := range active {
		sets = append(sets, c.name+" = "+c.mutated)
	}

	rt := mariaDBRoundTrip{
		table: "mtypes",
		setup: []string{
			"CREATE TABLE mtypes (id INT PRIMARY KEY, " + strings.Join(ddl, ", ") + ") ENGINE=InnoDB",
			insert(1), insert(2), insert(3), insert(4), insert(5),
		},
		mutations: []string{
			"UPDATE mtypes SET " + strings.Join(sets, ", ") + " WHERE id = 1",
			"UPDATE mtypes SET " + strings.Join(sets, ", ") + " WHERE id = 2",
			"DELETE FROM mtypes WHERE id = 3",
			"DELETE FROM mtypes WHERE id = 4",
			insert(6),
		},
		wantEvents: 5,
		capture:    "SELECT id, " + strings.Join(canon, ", ") + " FROM mtypes ORDER BY id",
	}
	res := rt.run(t, indexDB, sourceDB, sourceName)
	res.assertExact(t)
}

// TestRecoverRoundTrip_MariaDBUUIDKey covers UUID and INET6 as the PRIMARY KEY,
// the common MariaDB shape. The key travels through pk_values and the reversal's
// WHERE clause, so a trimmed or re-encoded key would make the reverse-UPDATE and
// reverse-INSERT miss their row (zero rows affected, no error).
func TestRecoverRoundTrip_MariaDBUUIDKey(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	sourceDB.SetMaxOpenConns(1)
	major, minor := mariaDBVersion(t, sourceDB)
	if !mariaDBAtLeast(major, minor, 10, 7) {
		t.Skipf("MariaDB %d.%d has no UUID type (10.7+)", major, minor)
	}

	rt := mariaDBRoundTrip{
		table: "ukeys",
		setup: []string{
			"CREATE TABLE ukeys (id UUID PRIMARY KEY, ip INET6 NOT NULL, n INT NOT NULL) ENGINE=InnoDB",
			"INSERT INTO ukeys VALUES ('e0b5a0f4-3c9d-4b8e-9f1a-000000000000', '2001:db8::', 1)",
			"INSERT INTO ukeys VALUES ('00000000-0000-0000-0000-000000000000', '::', 2)",
			"INSERT INTO ukeys VALUES ('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'fe80::1', 3)",
			"INSERT INTO ukeys VALUES ('01890a5d-ac96-774b-bcce-b302099a8057', '::ffff:10.0.0.0', 4)",
		},
		mutations: []string{
			"UPDATE ukeys SET ip = '::1', n = 10 WHERE id = 'e0b5a0f4-3c9d-4b8e-9f1a-000000000000'",
			"UPDATE ukeys SET n = 20 WHERE id = '00000000-0000-0000-0000-000000000000'",
			"DELETE FROM ukeys WHERE id = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'",
			"INSERT INTO ukeys VALUES ('12345678-9abc-1def-8012-000000000000', '10::', 5)",
		},
		wantEvents: 4,
		capture:    "SELECT id, HEX(id), ip, HEX(ip), n FROM ukeys ORDER BY HEX(id)",
	}
	res := rt.run(t, indexDB, sourceDB, sourceName)
	res.assertExact(t)
}

// TestRecoverRoundTrip_MariaDBJSONText measures what recover restores into a
// MariaDB JSON column. MariaDB's JSON is LONGTEXT with a json_valid() CHECK, so
// the server keeps the exact text the application wrote: key order, whitespace,
// \u escapes, duplicate keys and number spelling are all part of the value. The
// assertion is byte equality of that text.
//
// KNOWN FAILURE, pinned: the documents in knownDiff do NOT come back byte-exact
// today. The indexer stores any text that parses as a JSON object or array in
// the index's MySQL JSON column, which reorders keys, drops whitespace, keeps
// only the last of duplicate keys, turns 1e2 into 100.0 and -0 into 0, and
// stores an integer too large for 64 bits as a double (so its digits are LOST,
// not just respelled). recover then re-serializes the object with Go, which
// sorts keys and escapes <, > and & as \u003c, \u003e and \u0026. The same
// happens to a MySQL TEXT column that holds JSON. The byte assertion is kept for
// those rows too: a mismatch is logged, and a MATCH fails the test so whoever
// fixes the storage moves the row out of knownDiff.
func TestRecoverRoundTrip_MariaDBJSONText(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	sourceDB.SetMaxOpenConns(1)

	const bs = `\` // spelled out so the escapes below stay literal JSON escapes
	docs := []string{
		`{"z":1,"a":{"y":[1,2,{"k":"v"}],"b":null}}`,                                  // 1 key order + nesting
		`{ "a" : 1,  "b" : [ 1 , 2 ] }`,                                               // 2 whitespace
		"{\n  \"multi\": \"line\",\n  \"tab\":\t\"x\"\n}",                             // 3 newlines and a tab
		`{"e":"caf` + bs + `u00e9","s":"` + bs + `ud83d` + bs + `ude00","h":"<a&b>"}`, // 4 \u escapes + HTML chars
		`{"big":12345678901234567890123,"f":1.0,"e":1e2,"n":-0}`,                      // 5 large and odd numbers
		`[3,1,2]`,                    // 6 top-level array, already compact
		`{"dup":1,"dup":2}`,          // 7 duplicate keys (MariaDB accepts them)
		`"just a string"`,            // 8 top-level scalar: never promoted
		`{"a":"quote\"back\\slash"}`, // 9 escaped quote and backslash, sorted and compact
	}
	knownDiff := map[int]string{
		1: "keys reordered",
		2: "whitespace removed",
		3: "newlines and tab removed",
		4: "unicode escapes decoded, keys reordered, <>& re-escaped",
		5: "big integer rounded to a double (digits lost), 1e2 -> 100.0, -0 -> 0",
		7: "first duplicate key dropped",
	}
	quote := func(s string) string { return "'" + recovery.EscapeString(s) + "'" }

	setup := []string{"CREATE TABLE mjson (id INT PRIMARY KEY, doc JSON NULL) ENGINE=InnoDB"}
	var mutations []string
	for i, d := range docs {
		setup = append(setup, fmt.Sprintf("INSERT INTO mjson VALUES (%d, %s)", i+1, quote(d)))
		// Odd rows are reverse-UPDATEd, even rows reverse-DELETEd, so both the
		// SET path and the INSERT path of recover are measured.
		if i%2 == 0 {
			mutations = append(mutations, fmt.Sprintf(`UPDATE mjson SET doc = '{"changed":true}' WHERE id = %d`, i+1))
		} else {
			mutations = append(mutations, fmt.Sprintf("DELETE FROM mjson WHERE id = %d", i+1))
		}
	}
	rt := mariaDBRoundTrip{
		table:      "mjson",
		setup:      setup,
		mutations:  mutations,
		wantEvents: len(docs),
		capture:    "SELECT id, HEX(doc) FROM mjson ORDER BY id",
	}
	res := rt.run(t, indexDB, sourceDB, sourceName)
	if len(res.restored) != len(res.before) {
		t.Fatalf("row count after restore: got %d, want %d\nSQL:\n%s", len(res.restored), len(res.before), res.script)
	}
	for i := range res.before {
		id := i + 1
		want, got := cellString(res.before[i][1]), cellString(res.restored[i][1])
		reason, known := knownDiff[id]
		switch {
		case want == got && known:
			t.Errorf("doc %d now restores byte-exact; the known difference (%s) is gone, move it out of knownDiff", id, reason)
		case want != got && known:
			t.Logf("doc %d KNOWN DIFFERENCE (%s):\n  wrote:    %s\n  restored: %s", id, reason, hexText(want), hexText(got))
		case want != got:
			t.Errorf("doc %d not restored byte-exact:\n  wrote:    %s\n  restored: %s", id, hexText(want), hexText(got))
		}
	}
}

// hexText decodes a HEX() capture back to text for a readable failure message.
func hexText(h string) string {
	b, err := hex.DecodeString(h)
	if err != nil {
		return h
	}
	return strconv.Quote(string(b))
}

// TestRecoverSequence_MariaDB pins what happens to a MariaDB SEQUENCE. MariaDB
// stores a sequence as a one-row table and binlogs every state change (each
// time NEXTVAL runs past the cache, and every SETVAL) as an INSERT on it. The
// capture indexes those as ordinary INSERT events of a table with no primary
// key. recover's reverse of an INSERT is a DELETE, and a sequence refuses DELETE
// and UPDATE (ER 1031), so a reversal that covers the sequence fails loud at
// apply time and changes nothing. A reversal scoped to the real table applies
// cleanly and does not touch the sequence: the counter is NOT rewound, so ids
// handed out during the window are never handed out again.
func TestRecoverSequence_MariaDB(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	sourceDB.SetMaxOpenConns(1)

	rt := mariaDBRoundTrip{
		table: "orders",
		setup: []string{
			"CREATE SEQUENCE seq START WITH 1 INCREMENT BY 1 CACHE 2",
			"CREATE TABLE orders (id BIGINT PRIMARY KEY, n INT NOT NULL) ENGINE=InnoDB",
			"INSERT INTO orders VALUES (NEXTVAL(seq), 1)",
		},
		mutations: []string{
			"SELECT NEXTVAL(seq)",                         // cache refill -> INSERT on seq
			"SELECT NEXTVAL(seq)",                         // served from cache, nothing logged
			"SELECT SETVAL(seq, 100)",                     // -> INSERT on seq
			"INSERT INTO orders VALUES (NEXTVAL(seq), 2)", // -> INSERT on seq + INSERT on orders
			"UPDATE orders SET n = 10 WHERE id = 1",
		},
		wantEvents: 5,
		reversed:   2,
		capture:    "SELECT id, n FROM orders ORDER BY id",
	}
	res := rt.run(t, indexDB, sourceDB, sourceName)
	res.assertExact(t)

	var seqInserts, seqOther int
	if err := indexDB.QueryRow(`SELECT COALESCE(SUM(event_type = 1), 0), COALESCE(SUM(event_type <> 1), 0)
		FROM binlog_events WHERE schema_name = ? AND table_name = 'seq'`, sourceName).Scan(&seqInserts, &seqOther); err != nil {
		t.Fatalf("count seq events: %v", err)
	}
	if seqInserts != 3 || seqOther != 0 {
		t.Errorf("sequence state changes: got %d INSERT and %d other events, want 3 INSERT and 0 other", seqInserts, seqOther)
	}

	// The counter was not rewound by the orders-only reversal.
	var next int64
	if err := sourceDB.QueryRow("SELECT NEXTVAL(seq)").Scan(&next); err != nil {
		t.Fatalf("NEXTVAL after restore: %v", err)
	}
	if next <= 101 {
		t.Errorf("NEXTVAL after restore = %d, want > 101 (the sequence must not be rewound)", next)
	}

	// A reversal that covers the sequence itself is refused by the server.
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	var buf strings.Builder
	if _, err := recovery.New(indexDB, resolver).GenerateSQL(context.Background(),
		query.Options{Schema: sourceName, Table: "seq", Limit: 100}, &buf); err != nil {
		t.Fatalf("GenerateSQL(seq): %v", err)
	}
	applyDB, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/"+sourceName+"?parseTime=true&multiStatements=true")
	if err != nil {
		t.Fatalf("open apply conn: %v", err)
	}
	defer applyDB.Close()
	_, err = applyDB.Exec(buf.String())
	var me *drivermysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1031 {
		t.Errorf("applying the sequence reversal: want ER 1031 (a sequence refuses DELETE), got %v\nSQL:\n%s", err, buf.String())
	}
}
