//go:build integration

package indexer

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestInsertBatch_binaryPrimaryKey pins #1132: a table whose PRIMARY KEY is
// BINARY/VARBINARY used to stop capture for the WHOLE source. Its raw key bytes
// were written verbatim into binlog_events.pk_values (VARCHAR(512), utf8mb4 by
// server default — the DDL declares no charset), so MySQL rejected the
// multi-row INSERT with error 1366 ("Incorrect string value"), and because a
// batch flush failure is fail-loud by contract (internal/streamrun, #652) that
// took down every table in the source, not just the offending one —
// `bintrail stream` exits the process, `bintrail-console watch` crash-loops the
// source on backoff and then marks it permanently failed.
//
// The assertion has to run against a REAL utf8mb4 index: a unit test on
// event.BuildPKValues can prove the hex spelling but never touches a charset,
// so it cannot prove error 1366 is gone. This drives the production chain end
// to end — go-mysql's raw bytes → metadata.MapRow (which reinterprets a
// BINARY/VARBINARY column as []byte, #756) → event.BuildPKValues →
// Indexer.InsertBatch → MySQL — and then reads the row back.
func TestInsertBatch_binaryPrimaryKey(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// The bytes from the issue's own error message. 0xB2 is a UTF-8
	// continuation byte and can never lead a sequence, so this is exactly the
	// unstorable shape a BINARY(16) MD5/UUID key produces in practice.
	keyBytes := []byte{
		0xB2, 0x81, 0x5C, 0xC3, 0xC2, 0x00, 0xFF, 0x7C,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x80,
	}
	const wantPK = "0xB2815CC3C200FF7C0102030405060780"

	// A resolver over the repro schema: BINARY(16) PK + a VARCHAR payload.
	// Building the event through MapRow rather than hand-writing PKValues is
	// the point — it is MapRow's #756 string→[]byte reinterpretation that
	// feeds formatPKValue, and hand-writing the encoded string would test the
	// literal instead of the code path that produces it.
	tm := &metadata.TableMeta{
		Schema: "mydb", Table: "t_pk_binary",
		Columns: []metadata.ColumnMeta{
			{Name: "k", DataType: "binary", ColumnType: "binary(16)", IsPK: true},
			{Name: "val", DataType: "varchar", ColumnType: "varchar(255)", CharacterSet: "utf8mb4"},
		},
		PKColumns: []string{"k"},
	}
	res := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"mydb.t_pk_binary": tm})

	// go-mysql delivers a BINARY column as a Go string of raw source bytes.
	row, err := res.MapRow("mydb", "t_pk_binary", []any{string(keyBytes), "bin pk 1"})
	if err != nil {
		t.Fatalf("MapRow: %v", err)
	}
	if _, ok := row["k"].([]byte); !ok {
		t.Fatalf("MapRow returned %T for the BINARY PK column, want []byte "+
			"(the #756 coercion is what routes this value into formatPKValue)", row["k"])
	}

	pkValues := event.BuildPKValues(tm.PKColumnMetas(), row)
	if pkValues != wantPK {
		t.Fatalf("BuildPKValues = %q, want %q", pkValues, wantPK)
	}

	idx := New(db, 1000)
	ts := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)

	// ── Premise check + negative control ─────────────────────────────────────
	// Everything below is only meaningful if pk_values actually rejects these
	// bytes. That is NOT a declared schema fact: schema.go:119 declares
	// `pk_values VARCHAR(512) NOT NULL` with no CHARACTER SET and no
	// table-level DEFAULT CHARSET, and testutil.CreateTestDB issues a bare
	// CREATE DATABASE — so the charset is inherited from the server default all
	// the way down. On a latin1-default server this whole test would pass green
	// having exercised no charset rejection at all, and the "hex-encoding only
	// ever touches values that could not be stored" claim would be untested
	// prose. Assert the premise, then assert the rejection.
	var charset string
	if err := db.QueryRow(`SELECT CHARACTER_SET_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events' AND COLUMN_NAME = 'pk_values'`).
		Scan(&charset); err != nil {
		t.Fatalf("reading pk_values charset: %v", err)
	}
	if charset != "utf8mb4" {
		t.Fatalf("this test's premise is a utf8mb4 pk_values column, got %q — the column declares no "+
			"CHARACTER SET, so it inherits the server default; on this server the charset rejection "+
			"under test does not happen and nothing below is proving anything", charset)
	}
	// The raw form is what #1132 reported. Driving it through the SAME
	// production insert path pins the pre-fix failure permanently instead of
	// leaving it as a manual verification someone has to redo.
	if _, err := idx.InsertBatch([]event.Event{{
		BinlogFile: "binlog.000001", StartPos: 1, EndPos: 2,
		Timestamp: ts, Schema: "mydb", Table: "t_pk_binary",
		EventType: event.EventDelete, PKValues: string(keyBytes),
		RowBefore: row,
	}}); err == nil {
		t.Fatal("InsertBatch accepted RAW (non-UTF-8) PK bytes — this is the #1132 failure the hex " +
			"encoding exists to avoid; if the column stopped rejecting them, the encoding is now " +
			"changing spellings for no reason")
	}

	n, err := idx.InsertBatch([]event.Event{{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: ts, Schema: "mydb", Table: "t_pk_binary",
		EventType: event.EventDelete, PKValues: pkValues,
		RowBefore: row,
	}})
	// Before the fix this is where the daemon died: Error 1366 on pk_values.
	if err != nil {
		t.Fatalf("InsertBatch with a BINARY(16) PK failed (#1132 regression): %v", err)
	}
	if n != 1 {
		t.Fatalf("InsertBatch inserted %d rows, want 1", n)
	}

	// Read back through the predicate the single-PK live-MySQL lookup builds
	// (internal/query/query.go:420): pk_hash for the index scan AND pk_values
	// for the collision guard (CLAUDE.md). If the stored spelling and the
	// rebuilt one ever diverged, this returns zero rows — the silent-miss
	// failure mode, not a loud one.
	//
	// The multi-PK --pks filter uses the same pair as IN-lists since #2007.
	// NOT every read path does: the Parquet archive path matches on
	// pk_values alone (internal/parquetquery/parquetquery.go), which is
	// exactly where the case-collation reasoning below would bite, since
	// nothing disambiguates two case-variant spellings there.
	var got string
	err = db.QueryRow(
		`SELECT pk_values FROM binlog_events WHERE pk_hash = SHA2(?, 256) AND pk_values = ?`,
		pkValues, pkValues).Scan(&got)
	if err != nil {
		t.Fatalf("indexed row is not findable by its own pk_values: %v", err)
	}
	if got != wantPK {
		t.Errorf("stored pk_values = %q, want %q", got, wantPK)
	}

	// The stored form must be reproducible FROM THE SOURCE TABLE, which is the
	// whole reason for the "0x" + uppercase-HEX spelling: an operator can run
	// SELECT CONCAT('0x', HEX(k)) on the source and paste the result into --pk.
	//
	// HEX() must be applied to a real BINARY(16) COLUMN, not to a placeholder
	// parameter: `SELECT CONCAT('0x', HEX(?))` hexes whatever the driver
	// transmits under whatever charset the server types the expression as,
	// which is a different code path from the operator affordance being
	// claimed here — it could pass for a reason unrelated to the claim.
	testutil.MustExec(t, db, `CREATE TABLE t_pk_binary_src (k BINARY(16) NOT NULL PRIMARY KEY)`)
	testutil.MustExec(t, db, `INSERT INTO t_pk_binary_src (k) VALUES (?)`, keyBytes)
	var sourceSpelling string
	if err := db.QueryRow(`SELECT CONCAT('0x', HEX(k)) FROM t_pk_binary_src`).Scan(&sourceSpelling); err != nil {
		t.Fatalf("HEX() probe on a real BINARY(16) column: %v", err)
	}
	if sourceSpelling != wantPK {
		t.Errorf("stored pk_values %q does not match the source table's CONCAT('0x', HEX(k)) = %q "+
			"— an operator could not reproduce the stored key to feed it back to --pk", wantPK, sourceSpelling)
	}

	// pk_values is utf8mb4 with a case-INSENSITIVE collation, so "0xAB…" and
	// "0xab…" compare EQUAL on that column alone. The uppercase spelling is
	// therefore load-bearing, and the pk_hash half of the predicate is what
	// actually disambiguates (SHA2 is byte-exact). Insert the lowercase
	// spelling as a second row and pin that the AND-predicate still resolves
	// to exactly the uppercase one — if a future change ever let a second
	// producer emit lowercase hex, this is the silent wrong-row/zero-row
	// failure it would cause.
	lower := "0x" + strings.ToLower(wantPK[2:])
	if _, err := idx.InsertBatch([]event.Event{{
		BinlogFile: "binlog.000001", StartPos: 300, EndPos: 400,
		Timestamp: ts, Schema: "mydb", Table: "t_pk_binary",
		EventType: event.EventDelete, PKValues: lower,
		RowBefore: map[string]any{"k": lower},
	}}); err != nil {
		t.Fatalf("InsertBatch of the lowercase spelling: %v", err)
	}
	var matched int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM binlog_events WHERE pk_values = ?`, pkValues).Scan(&matched); err != nil {
		t.Fatalf("collation probe: %v", err)
	}
	if matched != 2 {
		t.Fatalf("pk_values = ? matched %d rows, want 2 — the premise of this check is that the "+
			"column's collation is case-insensitive; if it is not, the pk_hash guard below is untested", matched)
	}
	if err := db.QueryRow(
		`SELECT pk_values FROM binlog_events WHERE pk_hash = SHA2(?, 256) AND pk_values = ?`,
		pkValues, pkValues).Scan(&got); err != nil {
		t.Fatalf("pk_hash failed to disambiguate two case-variant pk_values: %v", err)
	}
	if got != wantPK {
		t.Errorf("AND-predicate resolved to %q, want the uppercase %q", got, wantPK)
	}
}

// TestInsertBatch_binaryPrimaryKeyValidUTF8Unchanged is the other half of
// #1132's contract: hex-encoding is content-gated, so a BINARY/VARBINARY PK
// whose bytes ARE valid UTF-8 keeps the byte-identical spelling it had before
// the fix. Those rows store and query correctly today; changing their
// pk_values would change their generated pk_hash and orphan every row already
// indexed under the old spelling.
func TestInsertBatch_binaryPrimaryKeyValidUTF8Unchanged(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Valid UTF-8: 7-bit ASCII, plus U+07AD written as the 2-byte sequence
	// {0xDE,0xAD} — the literal case the #756 unit test pinned.
	keyBytes := append([]byte("id-"), 0xDE, 0xAD)
	wantPK := string(keyBytes)

	tm := &metadata.TableMeta{
		Schema: "mydb", Table: "t_pk_varbinary",
		Columns: []metadata.ColumnMeta{
			{Name: "k", DataType: "varbinary", ColumnType: "varbinary(32)", IsPK: true},
		},
		PKColumns: []string{"k"},
	}
	res := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"mydb.t_pk_varbinary": tm})

	row, err := res.MapRow("mydb", "t_pk_varbinary", []any{string(keyBytes)})
	if err != nil {
		t.Fatalf("MapRow: %v", err)
	}
	// Without this the guard can go VACUOUS without failing: if MapRow's #756
	// coercion ever stopped returning []byte, the value would take
	// formatPKValue's fmt.Sprintf("%v", …) path, print identically, and this
	// test would keep passing while no longer exercising the branch it guards.
	// This test IS the regression guard for the PR's central safety claim, so
	// it must not be able to quietly stop guarding.
	if _, ok := row["k"].([]byte); !ok {
		t.Fatalf("MapRow returned %T for the VARBINARY PK column, want []byte — this test no longer "+
			"exercises formatPKValue's []byte branch and is not guarding anything", row["k"])
	}
	pkValues := event.BuildPKValues(tm.PKColumnMetas(), row)
	if pkValues != wantPK {
		t.Fatalf("BuildPKValues = %q, want the unchanged raw spelling %q "+
			"(hex-encoding must only touch bytes utf8mb4 cannot store)", pkValues, wantPK)
	}

	idx := New(db, 1000)
	n, err := idx.InsertBatch([]event.Event{{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
		Schema:    "mydb", Table: "t_pk_varbinary",
		EventType: event.EventInsert, PKValues: pkValues,
		RowAfter:  row,
	}})
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if n != 1 {
		t.Fatalf("InsertBatch inserted %d rows, want 1", n)
	}

	var got string
	if err := db.QueryRow(
		`SELECT pk_values FROM binlog_events WHERE pk_hash = SHA2(?, 256) AND pk_values = ?`,
		pkValues, pkValues).Scan(&got); err != nil {
		t.Fatalf("indexed row is not findable by its own pk_values: %v", err)
	}
	if got != wantPK {
		t.Errorf("stored pk_values = %q, want %q", got, wantPK)
	}
}

// TestInsertBatch_binaryPrimaryKeyOverLengthLimit pins the reachability change
// #1132 introduces, which is otherwise only asserted in prose on hexPKPrefix:
// hex DOUBLES a component's length, so a wide binary PK that used to die on
// error 1366 now trips indexer.checkPKValuesLength instead. That is the
// pre-existing wide-PK limit (#944) becoming reachable by a narrower set of
// PKs — not a new failure mode — but "which guard fires" is exactly the kind
// of claim that rots silently.
//
// TestInsertBatch_oversizedPKValues already covers the guard itself, with a
// hand-built strings.Repeat("a", 513). What is unproven without this test is
// that a HEX-EXPANDED binary PK reaches that guard and yields its actionable
// message rather than some other failure.
func TestInsertBatch_binaryPrimaryKeyOverLengthLimit(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// 300 invalid-UTF-8 bytes: "0x" + 600 hex characters = 602 > 512.
	keyBytes := make([]byte, 300)
	for i := range keyBytes {
		keyBytes[i] = 0xB2 // a UTF-8 continuation byte can never lead a sequence
	}

	tm := &metadata.TableMeta{
		Schema: "mydb", Table: "t_pk_wide_binary",
		Columns: []metadata.ColumnMeta{
			{Name: "k", DataType: "varbinary", ColumnType: "varbinary(300)", IsPK: true},
		},
		PKColumns: []string{"k"},
	}
	res := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"mydb.t_pk_wide_binary": tm})
	row, err := res.MapRow("mydb", "t_pk_wide_binary", []any{string(keyBytes)})
	if err != nil {
		t.Fatalf("MapRow: %v", err)
	}
	pkValues := event.BuildPKValues(tm.PKColumnMetas(), row)
	if len(pkValues) != 602 {
		t.Fatalf("hex-encoded PK is %d characters, want 602 — the doubling this test exists to "+
			"pin did not happen", len(pkValues))
	}

	idx := New(db, 1000)
	_, err = idx.InsertBatch([]event.Event{{
		BinlogFile: "binlog.000001", StartPos: 100, EndPos: 200,
		Timestamp: time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
		Schema:    "mydb", Table: "t_pk_wide_binary",
		EventType: event.EventInsert, PKValues: pkValues,
		RowAfter:  row,
	}})
	if err == nil {
		t.Fatal("InsertBatch accepted a 602-character pk_values, want the checkPKValuesLength refusal")
	}
	// The actionable #944 message, not a raw MySQL error: the operator needs to
	// be told the PK is too wide, not handed error 1406.
	if !strings.Contains(err.Error(), "exceeding the 512-character limit") {
		t.Errorf("error did not come from checkPKValuesLength: %v", err)
	}
}
