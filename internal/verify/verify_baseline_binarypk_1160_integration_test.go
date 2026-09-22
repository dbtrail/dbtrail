//go:build integration

package verify

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The three PK shapes shared by the tests below, all 16 bytes at rest and all
// carrying trailing 0x00 bytes — the only inputs that can distinguish a
// correct canonicalization from an inverted one (#1155):
//
//	bpPKTouched   — one trailing 0x00, updated inside the (prev, new] window
//	bpPKUntouched — four trailing 0x00, no events at all (pure pass-through)
//	bpPKASCII     — payload is printable ASCII, so pk_values stores it VERBATIM
//	                with no 0x prefix (formatPKValue is content-gated)
//
// binlog_events.pk_values holds the ROW image's spelling: trailing padding
// stripped, hex uppercased. That premise is asserted against a live server by
// reconstruct.TestBinaryPKBaselineJoin_endToEnd (assertPaddingStripped); here
// the events are hand-inserted in that pinned spelling, following this
// package's fixture convention.
const (
	bpPKTouched   = "0B2815CC3C200FF7C010203040506000"
	bpPKUntouched = "11223344556677889900AABB00000000"
	bpPKASCII     = "41420000000000000000000000000000"
)

// bpStrippedB64 renders the base64 the indexer stores for the touched key's
// ROW image: marshalRow base64-encodes the []byte go-mysql delivers, which
// carries the value with its (single) trailing 0x00 already stripped.
func bpStrippedB64(t *testing.T) string {
	t.Helper()
	raw, err := hex.DecodeString(strings.TrimSuffix(bpPKTouched, "00"))
	if err != nil {
		t.Fatalf("bad hex %q: %v", bpPKTouched, err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// seedBinaryPKPair builds the shared fixture for the baseline-anchored (#642
// DEFAULT mode) binary-PK tests: a schema snapshot for table bp whose BINARY
// PK column carries kColumnType, a prev baseline (padded 16-byte keys, initial
// values), a new baseline (same keys, the touched row updated), and ONE UPDATE
// event inside the (prev anchor, new anchor] window keyed by the STRIPPED
// pk_values spelling with stripped base64 row images.
func seedBinaryPKPair(t *testing.T, kColumnType string) (BaselineConfig, BaselinePair, func(untouchedVal string)) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'bp', 'k', 1, 'PRI', 'binary', ?, 'NO', 0)`, dbName, kColumnType)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name,
		 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'bp', 'val', 2, '', 'varchar', 'varchar(32)', 'NO', 0)`, dbName)

	baseDir := t.TempDir()
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `bp` (\n  `k` BINARY(16) NOT NULL,\n  `val` VARCHAR(32) NOT NULL,\n  PRIMARY KEY (`k`)\n);\n"
	cols := []baseline.Column{
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}

	// Baselines carry the FULL padded 16 bytes as mydumper's --hex-blob 0x…
	// literal; the writer decodes it by column type — the production reader of
	// exactly this dump shape.
	writeNew := func(untouchedVal string) {
		writeTestBaseline(t, baseDir, newTS, dbName, "bp", createSQL, cols, [][]string{
			{"0x" + bpPKTouched, "after"},
			{"0x" + bpPKUntouched, untouchedVal},
			{"0x" + bpPKASCII, "static"},
		}, "binlog.000001", 500)
	}
	writeTestBaseline(t, baseDir, prevTS, dbName, "bp", createSQL, cols, [][]string{
		{"0x" + bpPKTouched, "before"},
		{"0x" + bpPKUntouched, "static"},
		{"0x" + bpPKASCII, "static"},
	}, "binlog.000001", 200)
	writeNew("static")

	// The one in-window event: pk_values in the stripped ROW-image spelling,
	// row images with the stripped bytes base64-encoded (as marshalRow stores
	// them). Position 300 lands inside the (200, 500] anchor window.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	b64 := bpStrippedB64(t)
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ets, nil, dbName, "bp", 2 /*UPDATE*/,
		"0x"+strings.TrimSuffix(bpPKTouched, "00"), []byte(`["val"]`),
		[]byte(fmt.Sprintf(`{"k":"%s","val":"before"}`, b64)),
		[]byte(fmt.Sprintf(`{"k":"%s","val":"after"}`, b64)))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}

	pairs, prevOnly, err := FindBaselinePair(context.Background(), baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	if len(pairs) != 1 || pairs[0].Settled != nil || len(prevOnly) != 0 {
		t.Fatalf("expected exactly one compared pair for bp, got pairs=%+v prevOnly=%v", pairs, prevOnly)
	}
	return cfg, pairs[0], writeNew
}

// TestVerifyBaselinePair_BinaryPrimaryKey_Match closes the first gap of #1160:
// #1155's verify coverage only drove the LIVE-SOURCE mode (VerifyTable with
// --source-dsn), while the DEFAULT mode — baseline-anchored VerifyBaselinePair,
// the one an unattended cron runs — flipped from inconclusive to checked with
// its own SupportedPKType gate and nothing pinning the outcome. The fixture is
// the padded-vs-stripped shape that produced the #1135 false-mismatch class: a
// BINARY(16) PK whose trailing 0x00 bytes make the baseline spelling (padded)
// and the ROW-image spelling (stripped) disagree on BOTH sides of the
// comparison. A false MISMATCH here exits non-zero and breaks the cron, so
// asserting merely "not inconclusive" would be worthless — the assertion is
// MATCH, at equal row count.
func TestVerifyBaselinePair_BinaryPrimaryKey_Match(t *testing.T) {
	cfg, pair, writeNew := seedBinaryPKPair(t, "binary(16)")
	ctx := context.Background()

	got, err := VerifyBaselinePair(ctx, cfg, pair)
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if strings.Contains(got.Detail, "unsupported by the baseline canonicalizer") {
		t.Fatalf("default-mode verify still refuses the binary PK type (#1155 gate not applied): %s", got.Detail)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — a BINARY(16)-keyed table must be CHECKED in the default "+
			"(baseline-anchored) mode, and a false mismatch breaks the cron that runs it",
			got.Status, got.Detail)
	}
	if got.SourceRows != 3 || got.ReconstructRows != 3 {
		t.Fatalf("rows new/recon = %d/%d, want 3/3 — a PK that fails to join duplicates the changed row "+
			"(stale prev-baseline row + the event appended as a new PK)", got.SourceRows, got.ReconstructRows)
	}

	// Positive anchor: the comparison must be able to FAIL, and conclusively.
	// Tamper the never-touched row in the new baseline — a real at-rest
	// divergence on a table whose PK is a deferred (binary) type must stay a
	// conclusive MISMATCH, not get downgraded to inconclusive by the mere
	// presence of the binary column (its event-carried value resolved: typed
	// decode + known width).
	writeNew("tampered")
	got2, err := VerifyBaselinePair(ctx, cfg, pair)
	if err != nil {
		t.Fatalf("VerifyBaselinePair (tampered): %v", err)
	}
	if got2.Status != StatusMismatch {
		t.Errorf("after tampering the new baseline: status = %q (%s); want a conclusive mismatch", got2.Status, got2.Detail)
	}
}

// TestVerifyBaselinePair_BinaryPKNoWidth_Inconclusive pins the cheap adjacent
// case #1160 names: a BINARY PK whose schema snapshot has NO ColumnType width
// (pre-#212 snapshot) now clears SupportedPKType — but renderCell cannot
// re-pad the event side without the declared width, so the touched row renders
// stripped against the new baseline's padded bytes. That content difference
// must land on an honest INCONCLUSIVE (deferredValueUnresolved's width gate),
// never on a false MISMATCH: the join itself still works (canonicalizePKValue
// trims unconditionally and never reads the width), so the row counts agree
// and only the rendering is unresolvable.
//
// If a future change makes the no-width rendering genuinely byte-faithful,
// this would legitimately become a MATCH — update the assertion then; what it
// must never become is a MISMATCH.
func TestVerifyBaselinePair_BinaryPKNoWidth_Inconclusive(t *testing.T) {
	cfg, pair, _ := seedBinaryPKPair(t, "" /* pre-#212 snapshot: no COLUMN_TYPE */)

	got, err := VerifyBaselinePair(context.Background(), cfg, pair)
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status == StatusMismatch {
		t.Fatalf("status = mismatch (%s) — a width-less BINARY PK cannot be re-padded for rendering, so "+
			"this must degrade to inconclusive, not fail the cron with a false mismatch", got.Detail)
	}
	if got.Status != StatusInconclusive {
		t.Fatalf("status = %q (%s); want inconclusive — the current no-width behavior", got.Status, got.Detail)
	}
	// The reason must be the deferred-representation gate naming the actual
	// column, not some other inconclusive path (missing anchor, coverage gap).
	if !strings.Contains(got.Detail, `"k"`) || !strings.Contains(got.Detail, "could not be normalized") {
		t.Errorf("inconclusive for the wrong reason: %q — want the deferred-representation detail naming column k", got.Detail)
	}
	// The JOIN still worked: equal row counts on both sides. A row-count
	// divergence would mean the canonicalizer started depending on the width,
	// which classify would report as a conclusive mismatch.
	if got.SourceRows != 3 || got.ReconstructRows != 3 {
		t.Errorf("rows new/recon = %d/%d, want 3/3 — the PK join must not depend on the declared width",
			got.SourceRows, got.ReconstructRows)
	}
}
