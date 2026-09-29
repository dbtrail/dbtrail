//go:build integration

package consistency

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

func TestConsistentTableChecksum_TemporalDigestParseTimeIndependent(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, ts DATETIME(6), d DATE, tstamp TIMESTAMP NULL)")
	testutil.MustExec(t, db, "INSERT INTO t VALUES (1,'2021-01-01 00:00:00.123456','2021-03-04','2021-01-01 00:00:00')")

	ctx := context.Background()
	// Default test DSN sets parseTime=true (driver decodes DATE/DATETIME/
	// TIMESTAMP into time.Time and re-renders RFC3339 on RawBytes scan).
	withParse, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum (parseTime=true): %v", err)
	}

	// Same data over a connection WITHOUT parseTime — the CAST(... AS CHAR) on
	// temporal columns must make the digest identical, proving the canonical
	// form is MySQL's native text, not a driver artifact.
	rawDB, err := sql.Open("mysql", testutil.BaseDSN()+"/"+schema)
	if err != nil {
		t.Fatalf("open no-parseTime db: %v", err)
	}
	defer rawDB.Close()
	noParse, err := ConsistentTableChecksum(ctx, rawDB, schema, "t")
	if err != nil {
		t.Fatalf("checksum (parseTime=false): %v", err)
	}

	if withParse.Digest != noParse.Digest {
		t.Errorf("temporal digest depends on parseTime: %s != %s", withParse.Digest, noParse.Digest)
	}
}

func TestConsistentTableChecksum_BasicAndDeterministic(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64), amount DECIMAL(10,2))")
	testutil.MustExec(t, db, "INSERT INTO t VALUES (1,'alice',1.50),(2,'bob',2.00),(3,'carol',3.25)")

	ctx := context.Background()
	c1, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if c1.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3", c1.RowCount)
	}
	if c1.Digest == "" || c1.Digest == "0000000000000000" {
		t.Errorf("Digest = %q, want a non-empty non-zero hash", c1.Digest)
	}

	// Recomputing over identical data yields an identical digest.
	c2, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum (2nd): %v", err)
	}
	if c1.Digest != c2.Digest {
		t.Errorf("digest not deterministic: %s != %s", c1.Digest, c2.Digest)
	}
}

func TestConsistentTableChecksum_OrderIndependent(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, name VARCHAR(64))")
	// Same rows, inserted in different physical order.
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1,'alice'),(2,'bob'),(3,'carol')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (3,'carol'),(1,'alice'),(2,'bob')")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest != cb.Digest {
		t.Errorf("digest depends on insertion order: %s != %s", ca.Digest, cb.Digest)
	}
}

func TestConsistentTableChecksum_SingleByteChangeDiffers(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1,'alice')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1,'alicf')") // one byte different

	ctx := context.Background()
	ca, _ := ConsistentTableChecksum(ctx, db, schema, "a")
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("single-byte change did not change digest: both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_RepresentationOnlyDiffMatches(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, doc JSON)")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, doc JSON)")
	// Same logical JSON, different source whitespace/key order; the server
	// normalizes both to the same canonical text form.
	testutil.MustExec(t, db, `INSERT INTO a VALUES (1, '{"a": 1, "b": 2}')`)
	testutil.MustExec(t, db, `INSERT INTO b VALUES (1, '{"b":2,"a":1}')`)

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest != cb.Digest {
		t.Errorf("representation-only JSON difference changed digest: %s != %s", ca.Digest, cb.Digest)
	}
}

func TestConsistentTableChecksum_GeneratedColumnsExcluded(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	// Table b has an extra STORED generated column derived from base columns.
	// Since mydumper omits generated columns, the checksum must ignore it, so
	// a and b must match.
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, price INT, qty INT)")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, price INT, qty INT, total INT AS (price*qty) STORED)")
	testutil.MustExec(t, db, "INSERT INTO a (id,price,qty) VALUES (1,10,2),(2,5,4)")
	testutil.MustExec(t, db, "INSERT INTO b (id,price,qty) VALUES (1,10,2),(2,5,4)")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest != cb.Digest {
		t.Errorf("generated column affected digest: %s != %s", ca.Digest, cb.Digest)
	}
}

func TestConsistentTableChecksum_DefaultTimestampColumnIncluded(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	// A `DEFAULT CURRENT_TIMESTAMP` column reports EXTRA="DEFAULT_GENERATED" but
	// is an ordinary, mydumper-dumped data column — it MUST be in the digest.
	// Tables a and b carry the same id but different timestamp values, so if the
	// column is included (correct) the digests differ; if it were wrongly
	// excluded as "generated" the digests would falsely match.
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	testutil.MustExec(t, db, "INSERT INTO a (id, created_at) VALUES (1, '2021-01-01 00:00:00')")
	testutil.MustExec(t, db, "INSERT INTO b (id, created_at) VALUES (1, '2022-06-15 12:30:00')")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest == cb.Digest {
		t.Errorf("DEFAULT CURRENT_TIMESTAMP column excluded from digest (false match): both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_UnsignedHighValuesDiffer(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	// Two BIGINT UNSIGNED values above 2^63 that differ by 1 must produce
	// different digests. This guards the classic UNSIGNED⇒negative corruption
	// class (#490): a regression to typed/int64 scanning would collapse both to
	// -1 / a wrapped value and the digests would falsely match. Asserting the
	// digests DIFFER proves the high value is actually captured, not maxed.
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, big BIGINT UNSIGNED)")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, big BIGINT UNSIGNED)")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, 18446744073709551615)")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, 18446744073709551614)")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest == cb.Digest {
		t.Errorf("distinct high-unsigned values produced the same digest (UNSIGNED corruption escape): both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_BinaryBytesByteExact(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	// VARBINARY values that differ only in an embedded NUL, a trailing NUL, or a
	// high byte must produce different digests. sql.RawBytes is byte-exact; a
	// regression to typed string scanning would truncate at the first 0x00 and
	// silently collapse these — the project's _binary/\0 data-loss history.
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, v VARBINARY(16))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, v VARBINARY(16))")
	// 0x61 0x00 0x62  vs  0x61 0x00 0x63 — identical up to and past an embedded NUL.
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, X'610062')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, X'610063')")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest == cb.Digest {
		t.Errorf("binary values differing only past an embedded NUL produced the same digest (truncation escape): both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_DatetimeMicrosecondsDiffer(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	// DATETIME(6) values differing only in microseconds must differ — guards
	// against fractional-precision collapse.
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, ts DATETIME(6))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, ts DATETIME(6))")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, '2021-01-01 00:00:00.000001')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, '2021-01-01 00:00:00.000002')")

	ctx := context.Background()
	ca, _ := ConsistentTableChecksum(ctx, db, schema, "a")
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("DATETIME(6) microsecond difference collapsed: both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_DoubleLastDigitDiffersAndStable(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, d DOUBLE)")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, d DOUBLE)")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, 1.0000000000001)")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, 1.0000000000002)")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("DOUBLE last-digit difference collapsed: both %s", ca.Digest)
	}
	// Same-server re-read is deterministic despite float text-rendering caveat.
	ca2, _ := ConsistentTableChecksum(ctx, db, schema, "a")
	if ca.Digest != ca2.Digest {
		t.Errorf("DOUBLE digest not stable across reads: %s != %s", ca.Digest, ca2.Digest)
	}
}

func TestConsistentTableChecksum_MultibyteStringsDiffer(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	// Documents the charset contract: utf8mb4 multibyte content is captured
	// byte-exact, so distinct multibyte strings differ and a re-read is stable.
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, s VARCHAR(32)) CHARACTER SET utf8mb4")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, s VARCHAR(32)) CHARACTER SET utf8mb4")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, '日本語😀')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, '日本語😁')")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("distinct multibyte strings produced the same digest: both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_Latin1RawBytesNotTranscoded(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	// #792: the scan pins character_set_results = binary, so a latin1 column's
	// stored bytes are hashed RAW (matching mydumper's SET NAMES binary dump
	// contract the baseline Parquet is written under), NOT transcoded to the
	// connection's utf8mb4. 'é' is one byte 0xE9 in latin1; under a transcode it
	// would become two bytes 0xC3 0xA9. Proven by hashing a VARBINARY column that
	// stores the raw byte X'E9' and asserting the two digests MATCH — they only
	// match if no transcoding happened. Without the pin this is the permanent,
	// conclusive false-MISMATCH class #792 describes.
	testutil.MustExec(t, db, "CREATE TABLE lat (id INT PRIMARY KEY, s VARCHAR(8) CHARACTER SET latin1)")
	testutil.MustExec(t, db, "CREATE TABLE raw (id INT PRIMARY KEY, s VARBINARY(8))")
	testutil.MustExec(t, db, "INSERT INTO lat VALUES (1, _latin1 X'E9')") // 'é' → stored byte 0xE9
	testutil.MustExec(t, db, "INSERT INTO raw VALUES (1, X'E9')")         // raw byte 0xE9

	ctx := context.Background()
	cl, err := ConsistentTableChecksum(ctx, db, schema, "lat")
	if err != nil {
		t.Fatalf("checksum lat: %v", err)
	}
	cr, err := ConsistentTableChecksum(ctx, db, schema, "raw")
	if err != nil {
		t.Fatalf("checksum raw: %v", err)
	}
	if cl.Digest != cr.Digest {
		t.Errorf("latin1 byte was transcoded (charset pin not applied): latin1=%s raw=%s", cl.Digest, cr.Digest)
	}
}

func TestConsistentTableChecksum_NullDistinctFromEmpty(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, NULL)")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, '')")

	ctx := context.Background()
	ca, _ := ConsistentTableChecksum(ctx, db, schema, "a")
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("NULL and empty string hashed the same: both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_EmptyTable(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64))")

	ctx := context.Background()
	c, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if c.RowCount != 0 {
		t.Errorf("RowCount = %d, want 0", c.RowCount)
	}
	if c.Digest != digestVersion+"0000000000000000" {
		t.Errorf("Digest = %q, want version-tagged all-zero for empty table", c.Digest)
	}
}

func TestConsistentTableChecksum_MissingTableErrors(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	ctx := context.Background()
	if _, err := ConsistentTableChecksum(ctx, db, schema, "does_not_exist"); err == nil {
		t.Error("expected error for missing table, got nil")
	}
}

func TestConsistentTableChecksum_MariaDBGTIDBinlogPos(t *testing.T) {
	testutil.SkipIfNoMariaDB(t)
	db, schema := testutil.CreateTestMariaDB(t)
	testutil.MustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64))")
	testutil.MustExec(t, db, "INSERT INTO t VALUES (1,'alice'),(2,'bob')")

	ctx := context.Background()
	// MariaDB has no @@global.gtid_executed (error 1193). The checksum must
	// still succeed, not fail (a wrong error constant or non-matching
	// errors.As would break every MariaDB-source checksum), and it must carry
	// the MariaDB executed position instead of an empty anchor: an empty one
	// made verify assume the index was current.
	c, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum on MariaDB: %v", err)
	}
	// Exact equality holds because nothing writes to this MariaDB between the
	// two reads (the CI job runs one test at a time, -p 1). A concurrent
	// writer would make this fail, never pass wrongly.
	var pos string
	if err := db.QueryRowContext(ctx, "SELECT @@global.gtid_binlog_pos").Scan(&pos); err != nil {
		t.Fatalf("read @@gtid_binlog_pos: %v", err)
	}
	if pos == "" {
		t.Fatal("@@gtid_binlog_pos is empty after an INSERT; is the binlog on in this MariaDB?")
	}
	if c.GTIDFlavor != GTIDFlavorMariaDB || c.GTIDSet != pos {
		t.Errorf("anchor = (%q, %q), want (%q, %q)", c.GTIDSet, c.GTIDFlavor, pos, GTIDFlavorMariaDB)
	}
	if c.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", c.RowCount)
	}
}

func TestConsistentTableChecksum_CapturesGTIDWhenEnabled(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	testutil.MustExec(t, db, "INSERT INTO t VALUES (1)")

	var gtidMode string
	if err := db.QueryRow("SELECT @@gtid_mode").Scan(&gtidMode); err != nil {
		t.Skipf("cannot read @@gtid_mode: %v", err)
	}

	ctx := context.Background()
	c, err := ConsistentTableChecksum(ctx, db, schema, "t")
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if gtidMode == "ON" && c.GTIDSet == "" {
		t.Error("gtid_mode=ON but GTIDSet is empty")
	}
}

// ─── MariaDB mirrors of the byte-exactness proofs (#620 sweep) ────────────────
//
// ConsistentTableChecksum's canonical form is MySQL-text-protocol rendering
// (see the ConsistentTableChecksum doc comment) — a SQL-level concern, not a
// binlog-decode one, so these mirror the MySQL byte-exactness assertions
// against a live MariaDB source to confirm the same SELECT+CAST/binary-pin
// contract holds there too. TestConsistentTableChecksum_RepresentationOnlyDiffMatches
// is deliberately NOT mirrored: MariaDB stores JSON as LONGTEXT and renders it
// verbatim (no server-side normalization, unlike MySQL's native JSON type) —
// documented at ConsistentTableChecksum's doc comment ("JSON is normalized
// (MySQL; MariaDB stores JSON as LONGTEXT and renders it verbatim)") and by
// design mitigated one layer up, in internal/verify's
// ConsistentTableChecksumNormalized + canonicalizeJSONContainer hook, not in
// this bare primitive — so asserting digest equality here would fail by
// design, not by bug.

func TestConsistentTableChecksum_UnsignedHighValuesDiffer_MariaDB(t *testing.T) {
	db, schema := testutil.CreateTestMariaDB(t)
	// Same repro as the MySQL sibling: two BIGINT UNSIGNED values above 2^63
	// differing by 1 must produce different digests (#490 class).
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, big BIGINT UNSIGNED)")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, big BIGINT UNSIGNED)")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, 18446744073709551615)")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, 18446744073709551614)")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest == cb.Digest {
		t.Errorf("distinct high-unsigned values produced the same digest on MariaDB (UNSIGNED corruption escape): both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_BinaryBytesByteExact_MariaDB(t *testing.T) {
	db, schema := testutil.CreateTestMariaDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, v VARBINARY(16))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, v VARBINARY(16))")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, X'610062')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, X'610063')")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, err := ConsistentTableChecksum(ctx, db, schema, "b")
	if err != nil {
		t.Fatalf("checksum b: %v", err)
	}
	if ca.Digest == cb.Digest {
		t.Errorf("binary values differing only past an embedded NUL produced the same digest on MariaDB (truncation escape): both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_DatetimeMicrosecondsDiffer_MariaDB(t *testing.T) {
	db, schema := testutil.CreateTestMariaDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, ts DATETIME(6))")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, ts DATETIME(6))")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, '2021-01-01 00:00:00.000001')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, '2021-01-01 00:00:00.000002')")

	ctx := context.Background()
	ca, _ := ConsistentTableChecksum(ctx, db, schema, "a")
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("DATETIME(6) microsecond difference collapsed on MariaDB: both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_DoubleLastDigitDiffersAndStable_MariaDB(t *testing.T) {
	db, schema := testutil.CreateTestMariaDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, d DOUBLE)")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, d DOUBLE)")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, 1.0000000000001)")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, 1.0000000000002)")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("DOUBLE last-digit difference collapsed on MariaDB: both %s", ca.Digest)
	}
	ca2, _ := ConsistentTableChecksum(ctx, db, schema, "a")
	if ca.Digest != ca2.Digest {
		t.Errorf("DOUBLE digest not stable across reads on MariaDB: %s != %s", ca.Digest, ca2.Digest)
	}
}

func TestConsistentTableChecksum_MultibyteStringsDiffer_MariaDB(t *testing.T) {
	db, schema := testutil.CreateTestMariaDB(t)
	testutil.MustExec(t, db, "CREATE TABLE a (id INT PRIMARY KEY, s VARCHAR(32)) CHARACTER SET utf8mb4")
	testutil.MustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, s VARCHAR(32)) CHARACTER SET utf8mb4")
	testutil.MustExec(t, db, "INSERT INTO a VALUES (1, '日本語😀')")
	testutil.MustExec(t, db, "INSERT INTO b VALUES (1, '日本語😁')")

	ctx := context.Background()
	ca, err := ConsistentTableChecksum(ctx, db, schema, "a")
	if err != nil {
		t.Fatalf("checksum a: %v", err)
	}
	cb, _ := ConsistentTableChecksum(ctx, db, schema, "b")
	if ca.Digest == cb.Digest {
		t.Errorf("distinct multibyte strings produced the same digest on MariaDB: both %s", ca.Digest)
	}
}

func TestConsistentTableChecksum_Latin1RawBytesNotTranscoded_MariaDB(t *testing.T) {
	db, schema := testutil.CreateTestMariaDB(t)
	// #792 class: the scan pins character_set_results = binary, so a latin1
	// column's stored bytes are hashed RAW, not transcoded to utf8mb4.
	testutil.MustExec(t, db, "CREATE TABLE lat (id INT PRIMARY KEY, s VARCHAR(8) CHARACTER SET latin1)")
	testutil.MustExec(t, db, "CREATE TABLE raw (id INT PRIMARY KEY, s VARBINARY(8))")
	testutil.MustExec(t, db, "INSERT INTO lat VALUES (1, _latin1 X'E9')")
	testutil.MustExec(t, db, "INSERT INTO raw VALUES (1, X'E9')")

	ctx := context.Background()
	cl, err := ConsistentTableChecksum(ctx, db, schema, "lat")
	if err != nil {
		t.Fatalf("checksum lat: %v", err)
	}
	cr, err := ConsistentTableChecksum(ctx, db, schema, "raw")
	if err != nil {
		t.Fatalf("checksum raw: %v", err)
	}
	if cl.Digest != cr.Digest {
		t.Errorf("latin1 byte was transcoded on MariaDB (charset pin not applied): latin1=%s raw=%s", cl.Digest, cr.Digest)
	}
}
