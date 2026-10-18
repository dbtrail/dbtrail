package verify

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pgResolver stubs a one-table schema whose PK column has an EMPTY DataType —
// the PostgreSQL shape (no MySQL DATA_TYPE token). SupportedPKType("") is false,
// so on the MySQL path this table is inconclusive; on the PG path the type gate
// is bypassed entirely.
func pgResolver() *metadata.Resolver {
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.orders": {Schema: "app", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: ""},
		}},
	})
}

// TestVerifyBaselinePair_pgBypassesPKTypeGate proves B1: with SourceFlavor
// "postgres" the SupportedPKType gate is skipped, so an empty-DATA_TYPE PK does
// NOT go inconclusive as "unsupported by the canonicalizer" — it proceeds to the
// LSN-anchor precondition (B3) and reports the LSN reason instead. No index DB is
// touched (the anchor check returns before any fetch), so IndexDB stays nil.
func TestVerifyBaselinePair_pgBypassesPKTypeGate(t *testing.T) {
	cfg := BaselineConfig{Resolver: pgResolver(), SourceFlavor: flavorPostgres}
	// NewLSN == 0 → the PG anchor precondition fires (B3), which is only reachable
	// if the PK-type gate (B1) was skipped first.
	res, err := VerifyBaselinePair(context.Background(), cfg, BaselinePair{Schema: "app", Table: "orders", NewLSN: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if strings.Contains(res.Detail, "unsupported by the baseline canonicalizer") {
		t.Errorf("PG must bypass the PK-type gate, but got the MySQL type message: %q", res.Detail)
	}
	if !strings.Contains(res.Detail, "LSN anchor") {
		t.Errorf("want the PG LSN-anchor reason, got: %q", res.Detail)
	}
	if !strings.HasPrefix(res.Anchor, "LSN:") {
		t.Errorf("PG anchor label = %q, want an LSN: form", res.Anchor)
	}
}

// TestVerifyBaselinePair_mysqlKeepsPKTypeGate is the contrast: the SAME
// empty-DATA_TYPE PK on the MySQL path still trips the SupportedPKType gate, so
// the flag — not some unrelated change — is what gates B1. Since #1009 the
// gate's MESSAGE discriminates the empty-DATA_TYPE (PostgreSQL snapshot shape)
// case: the verdict names the wrong-path cause plainly instead of blaming the
// PK type — the misleading `has type "" unsupported by the baseline
// canonicalizer` the issue reproduced.
func TestVerifyBaselinePair_mysqlKeepsPKTypeGate(t *testing.T) {
	cfg := BaselineConfig{Resolver: pgResolver(), SourceFlavor: ""} // "" == mysql
	res, err := VerifyBaselinePair(context.Background(), cfg, BaselinePair{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if strings.Contains(res.Detail, "unsupported by the baseline canonicalizer") {
		t.Errorf("empty DATA_TYPE must get the PostgreSQL-shape verdict, not the misleading PK-type one: %q", res.Detail)
	}
	if !strings.Contains(res.Detail, "PostgreSQL snapshot shape") {
		t.Errorf("want the PostgreSQL-shape wrong-path reason, got: %q", res.Detail)
	}
}

// TestVerifyBaselinePair_mysqlUnsupportedTypeKeepsHonestTypeMessage pins the
// other half of the discrimination: a REAL MySQL type the canonicalizer does
// not handle keeps the per-table PK-type message — that one is accurate, and
// the #1009 fix must not blur it into the PostgreSQL verdict.
func TestVerifyBaselinePair_mysqlUnsupportedTypeKeepsHonestTypeMessage(t *testing.T) {
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.orders": {Schema: "app", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "float"},
		}},
	})
	cfg := BaselineConfig{Resolver: resolver, SourceFlavor: ""}
	res, err := VerifyBaselinePair(context.Background(), cfg, BaselinePair{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if !strings.Contains(res.Detail, `primary-key column "id" has type "float" unsupported by the baseline canonicalizer`) {
		t.Errorf("a real unsupported MySQL type must keep the PK-type message, got: %q", res.Detail)
	}
	if strings.Contains(res.Detail, "PostgreSQL") {
		t.Errorf("a real MySQL type must not get the PostgreSQL verdict: %q", res.Detail)
	}
}

// TestVerifyTable_pgShapedSnapshotGetsWrongPathVerdict drives the SAME
// discrimination through the live-source mode's gate (VerifyTable shares
// pkTypeGateReason with the baseline path). The gate fires before any DB is
// touched, so SourceDB/IndexDB stay nil.
func TestVerifyTable_pgShapedSnapshotGetsWrongPathVerdict(t *testing.T) {
	res, err := VerifyTable(context.Background(), Config{Resolver: pgResolver()}, "app", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive {
		t.Fatalf("status = %q, want inconclusive", res.Status)
	}
	if strings.Contains(res.Detail, "unsupported by the baseline canonicalizer") {
		t.Errorf("live mode must also drop the misleading PK-type message: %q", res.Detail)
	}
	if !strings.Contains(res.Detail, "PostgreSQL snapshot shape") {
		t.Errorf("want the PostgreSQL-shape wrong-path reason, got: %q", res.Detail)
	}
}

// TestReport_pgShapedVerdictFailsRunInBothFormats runs the real verify path for
// the PG-shaped/wrong-path case end to end into the report: the honest reason
// survives into the JSON shape (TableReport.Reason via NewReport), and the run
// FAILS via Report.ExitError — the single exit decision both the text and JSON
// renderings share — so a cron/CI gate cannot read "nothing proven" as success
// in either format.
func TestReport_pgShapedVerdictFailsRunInBothFormats(t *testing.T) {
	cfg := BaselineConfig{Resolver: pgResolver(), SourceFlavor: ""}
	res, err := VerifyBaselinePair(context.Background(), cfg, BaselinePair{Schema: "app", Table: "orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rep := NewReport(ModeBaselinePair, []TableResult{res})
	if rep.Verdict != VerdictUnproven {
		t.Fatalf("verdict = %q, want %q", rep.Verdict, VerdictUnproven)
	}
	if got := rep.Tables[0].Reason; !strings.Contains(got, "PostgreSQL snapshot shape") {
		t.Errorf("report reason lost the honest verdict: %q", got)
	}
	exitErr := rep.ExitError()
	if exitErr == nil {
		t.Fatal("ExitError = nil; an all-inconclusive PG-shaped run must fail the run in both formats")
	}
	if !strings.Contains(exitErr.Error(), "nothing proven") {
		t.Errorf("exit error = %q, want the nothing-proven contract", exitErr)
	}
	// The JSON document a --format json consumer reads carries the same reason.
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	if !strings.Contains(string(b), "PostgreSQL snapshot shape") {
		t.Errorf("JSON report lost the honest verdict: %s", b)
	}
}

// TestBaselineFetchOptions_pgBoundsByTheFloor guards the window VerifyBaselinePair
// and ExplainBaselinePairMismatch share. PostgreSQL (#1596): from the previous
// snapshot's LSN floor and recorded time bound when it has a floor, by number,
// and never an upper position bound; time only without a floor. MySQL pins the
// exact binlog cut.
func TestBaselineFetchOptions_pgBoundsByTheFloor(t *testing.T) {
	p := BaselinePair{
		Schema: "app", Table: "orders",
		PrevSnapshot: time.Unix(100, 0).UTC(), NewSnapshot: time.Unix(200, 0).UTC(),
		NewAnchor:  query.BinlogPos{File: "mysql-bin.000007", Pos: 4242},
		PrevAnchor: query.BinlogPos{File: "mysql-bin.000006", Pos: 100},
	}

	pgOpts := baselineFetchOptions(p, true)
	if pgOpts.UntilPos != nil || pgOpts.SincePos != nil {
		t.Errorf("PG window with no floor must set NO position bound: UntilPos=%v SincePos=%v", pgOpts.UntilPos, pgOpts.SincePos)
	}
	if pgOpts.Since == nil || !pgOpts.Since.Equal(p.PrevSnapshot) || pgOpts.Until == nil || pgOpts.LimitPerPK != 1 {
		t.Errorf("PG window must keep the time bounds + LimitPerPK: %+v", pgOpts)
	}
	p.PrevPGFrom = &query.BinlogPos{File: "1/0", Pos: 1 << 32}
	p.PrevPGSince = time.Unix(40, 0).UTC()
	pgOpts = baselineFetchOptions(p, true)
	// Settled: the read stays at the recorded bound (no widening into older
	// partitions and archives).
	want := query.Options{Schema: "app", Table: "orders", Since: &p.PrevPGSince, Until: &p.NewSnapshot, LimitPerPK: 1, SincePos: p.PrevPGFrom}.SinceProven()
	if !reflect.DeepEqual(pgOpts, want) {
		t.Errorf("PG window from the floor: %+v, want %+v", pgOpts, want)
	}

	myOpts := baselineFetchOptions(p, false)
	if myOpts.UntilPos == nil || *myOpts.UntilPos != p.NewAnchor {
		t.Errorf("MySQL window must cut at the new anchor, got %v", myOpts.UntilPos)
	}
	if myOpts.SincePos == nil || *myOpts.SincePos != p.PrevAnchor {
		t.Errorf("MySQL window must lower-bound at the prev anchor, got %v", myOpts.SincePos)
	}

	// A MySQL baseline with no recorded prev anchor (#797) falls back to the time
	// lower bound — SincePos stays nil.
	p.PrevAnchor = query.BinlogPos{}
	if got := baselineFetchOptions(p, false); got.SincePos != nil {
		t.Errorf("a zero prev anchor must not set SincePos, got %v", got.SincePos)
	}
}
