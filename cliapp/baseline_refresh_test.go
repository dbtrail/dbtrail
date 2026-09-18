package cliapp

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestBuildRefreshOutcomes pins the classification.
//
// It reads reconstruct's exported sentinels rather than message text, and this
// test is what keeps that true: "the events are permanently gone" and "the table
// changed shape" have opposite remedies (accept the loss with --allow-gaps vs.
// take a real dump), so a summary that blurs them sends the operator down the
// wrong one.
func TestBuildRefreshOutcomes(t *testing.T) {
	tables := []string{"shop.orders", "shop.items", "shop.users", "shop.audit", "shop.late"}
	reports := []*reconstruct.TableReport{{Schema: "shop", Table: "orders"}}
	failures := []reconstruct.TableFailure{
		{Schema: "shop", Table: "items", Err: fmt.Errorf("window: %w", reconstruct.ErrCaptureGap)},
		{Schema: "shop", Table: "users", Err: fmt.Errorf("shape: %w", reconstruct.ErrSchemaChanged)},
		{Schema: "shop", Table: "audit", Err: fmt.Errorf("boom: %w", errors.New("disk full"))},
	}

	got := buildRefreshOutcomes(tables, reports, failures)
	want := map[string]string{
		"shop.orders": "refreshed",
		"shop.items":  "refused-gap",
		"shop.users":  "refused-ddl",
		"shop.audit":  "refused",
		// Requested but neither reported nor failed: the run ended first. This
		// must not read as success.
		"shop.late": "skipped",
	}
	if len(got) != len(tables) {
		t.Fatalf("got %d outcomes for %d tables", len(got), len(tables))
	}
	for _, o := range got {
		if want[o.Table] != o.Verdict {
			t.Errorf("%s = %q, want %q", o.Table, o.Verdict, want[o.Table])
		}
	}
}

// TestBuildRefreshOutcomes_destructiveDDLIsDDL: a TRUNCATE/DROP/RENAME in the
// window is the same remedy as a schema change (re-dump), so it must not fall
// into the generic bucket.
func TestBuildRefreshOutcomes_destructiveDDLIsDDL(t *testing.T) {
	got := buildRefreshOutcomes(
		[]string{"shop.orders"}, nil,
		[]reconstruct.TableFailure{{Schema: "shop", Table: "orders",
			Err: fmt.Errorf("truncate: %w", reconstruct.ErrDestructiveDDL)}},
	)
	if got[0].Verdict != "refused-ddl" {
		t.Fatalf("verdict = %q, want refused-ddl", got[0].Verdict)
	}
}

// TestWriteRefreshSummary_refusalSaysNothingWasPublished: the all-or-nothing
// rule is only useful if the operator is told it applied. A summary listing one
// refusal among four successes reads as "three tables were refreshed" unless it
// says otherwise.
func TestWriteRefreshSummary_refusalSaysNothingWasPublished(t *testing.T) {
	var buf bytes.Buffer
	writeRefreshSummary(&buf, []refreshOutcome{
		{"shop.orders", "refreshed", ""},
		{"shop.items", "refused-gap", "events permanently lost"},
	}, "/data/baselines", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), false)

	out := buf.String()
	if !strings.Contains(out, "NOTHING was published") {
		t.Errorf("summary does not say the run published nothing:\n%s", out)
	}
	if !strings.Contains(out, "events permanently lost") {
		t.Errorf("summary drops the refusal detail:\n%s", out)
	}
	if strings.Contains(out, "published /data/baselines") {
		t.Errorf("summary claims a publication that did not happen:\n%s", out)
	}
}

// TestWriteRefreshSummary_successNamesTheSnapshot: on success the operator needs
// the path the snapshot actually landed at, in the directory form discovery uses.
func TestWriteRefreshSummary_successNamesTheSnapshot(t *testing.T) {
	var buf bytes.Buffer
	writeRefreshSummary(&buf, []refreshOutcome{{"shop.orders", "refreshed", ""}},
		"/data/baselines", time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), true)
	if !strings.Contains(buf.String(), "/data/baselines/2026-05-01T12-00-00Z") {
		t.Errorf("summary does not name the published snapshot:\n%s", buf.String())
	}
}

// TestRunBaselineRefresh_flagValidation covers the refusals that happen before
// any connection, including the one that would otherwise write a snapshot the
// operator can never find.
func TestRunBaselineRefresh_flagValidation(t *testing.T) {
	reset := func() {
		brIndexDSN, brBaselineDir, brBaselineS3, brOutput, brTables, brAt = "", "", "", "", "", ""
		brAllowGaps = false
		brWarnEvents = 5_000_000
	}
	for _, tc := range []struct {
		name    string
		set     func()
		wantErr string
	}{
		{"no index dsn", func() {}, "--index-dsn is required"},
		{"no baseline source", func() { brIndexDSN = "d" }, "one of --baseline-dir or --baseline-s3"},
		{"both sources", func() { brIndexDSN = "d"; brBaselineDir = "/b"; brBaselineS3 = "s3://b/" }, "mutually exclusive"},
		{
			// Reading from S3 with no local destination: writing somewhere we
			// picked would produce a snapshot nobody finds.
			name:    "s3 source without output",
			set:     func() { brIndexDSN = "d"; brBaselineS3 = "s3://b/" },
			wantErr: "--output is required with --baseline-s3",
		},
		{"negative warn threshold", func() { brIndexDSN = "d"; brBaselineDir = "/b"; brWarnEvents = -1 }, "must be >= 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset()
			tc.set()
			err := runBaselineRefresh(baselineRefreshCmd, nil)
			if err == nil {
				t.Fatalf("expected an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
	reset()
}

// TestResolveRefreshTables_explicitList covers --tables parsing; the discovery
// path needs a snapshot on disk and is covered by the integration test.
func TestResolveRefreshTables_explicitList(t *testing.T) {
	defer func() { brTables = "" }()

	brTables = " shop.orders , shop.items ,, "
	got, err := resolveRefreshTables(t.Context(), "/unused")
	if err != nil {
		t.Fatalf("resolveRefreshTables: %v", err)
	}
	if len(got) != 2 || got[0] != "shop.orders" || got[1] != "shop.items" {
		t.Fatalf("got %v, want [shop.orders shop.items]", got)
	}

	brTables = "orders"
	if _, err := resolveRefreshTables(t.Context(), "/unused"); err == nil ||
		!strings.Contains(err.Error(), "must be schema.table") {
		t.Fatalf("error = %v, want a schema.table refusal", err)
	}
}

// TestBaselineRefreshCmd_registered: a subcommand defined but never attached is
// invisible, and nothing else in the build notices.
func TestBaselineRefreshCmd_registered(t *testing.T) {
	for _, c := range baselineCmd.Commands() {
		if c.Name() == "refresh" {
			return
		}
	}
	t.Fatal("`baseline refresh` is not registered under `baseline`")
}

// TestResolveRefreshTables_discoversNewestSnapshot: with no --tables, the run
// refreshes exactly the tables the source snapshot has.
//
// Defaulting to the newest snapshot (rather than every table the index has ever
// seen) keeps the result a strict successor of what it was folded from: a table
// absent from the source has nothing to fold onto, and inventing an entry would
// publish a snapshot claiming coverage it does not have.
func TestResolveRefreshTables_discoversNewestSnapshot(t *testing.T) {
	defer func() { brTables = "" }()
	brTables = ""

	root := t.TempDir()
	// An older snapshot with a table the newest one no longer has.
	writeSnapshotFixture(t, root, "2026-04-01T00-00-00Z", map[string][]string{"shop": {"orders", "retired"}})
	writeSnapshotFixture(t, root, "2026-05-01T00-00-00Z", map[string][]string{"shop": {"orders", "items"}})

	got, err := resolveRefreshTables(t.Context(), root)
	if err != nil {
		t.Fatalf("resolveRefreshTables: %v", err)
	}
	want := []string{"shop.items", "shop.orders"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// writeSnapshotFixture lays out a complete snapshot directory. The Parquet files
// are placeholders: ListBaselines discovers by layout and marker, and this test
// is about which tables get selected, not about their contents.
func writeSnapshotFixture(t *testing.T, root, ts string, tables map[string][]string) {
	t.Helper()
	dir := filepath.Join(root, ts)
	for schema, names := range tables {
		if err := os.MkdirAll(filepath.Join(dir, schema), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		for _, name := range names {
			if err := os.WriteFile(filepath.Join(dir, schema, name+".parquet"), []byte("placeholder"), 0o644); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
		}
	}
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
}

// A carried-forward table WAS published, so it must not read as skipped, and it
// was NOT rewritten, so calling it "refreshed" would hide the one thing an
// operator reads this summary for: which tables are costing a full rewrite
// every cycle. Those are the two wrong answers this pins against.
func TestBuildRefreshOutcomes_carriedForwardIsItsOwnVerdict(t *testing.T) {
	got := buildRefreshOutcomes(
		[]string{"shop.orders", "shop.cold"},
		[]*reconstruct.TableReport{
			{Schema: "shop", Table: "orders"},
			{Schema: "shop", Table: "cold", CarriedForward: true},
		},
		nil,
	)
	want := map[string]string{"shop.orders": "refreshed", "shop.cold": "unchanged"}
	if len(got) != len(want) {
		t.Fatalf("got %d outcomes, want %d: the loops below assert nothing on an empty result", len(got), len(want))
	}
	for _, o := range got {
		if w := want[o.Table]; o.Verdict != w {
			t.Errorf("%s: verdict = %q, want %q", o.Table, o.Verdict, w)
		}
	}
	for _, o := range got {
		if o.Table == "shop.cold" && o.Detail == "" {
			t.Error("the unchanged verdict carries no detail, so it does not say WHY the file was not rewritten")
		}
	}
}

// TestBaselineRefreshCarryForwardIsOffByDefault: see the twin in internal/cli.
// A one-character regression here turns the opt-in into opt-out for every user
// of `bintrail baseline refresh`, and nothing else in the suite would notice.
func TestBaselineRefreshCarryForwardIsOffByDefault(t *testing.T) {
	f := baselineRefreshCmd.Flags().Lookup("carry-forward-unchanged")
	if f == nil {
		t.Fatal("--carry-forward-unchanged is gone from baseline refresh; this guard covers nothing")
	}
	if f.DefValue != "false" {
		t.Fatalf("default = %q, want \"false\"", f.DefValue)
	}
}

// TestBaselineRefreshTableDeltasIsOffByDefault: table deltas (#1638) change how
// a snapshot is stored and who can read it, so they are asked for, never
// landed on. Against the REAL command, for the reason the twin above gives.
func TestBaselineRefreshTableDeltasIsOffByDefault(t *testing.T) {
	f := baselineRefreshCmd.Flags().Lookup("table-deltas")
	if f == nil {
		t.Fatal("--table-deltas is gone from baseline refresh; this guard covers nothing")
	}
	if f.DefValue != "false" {
		t.Fatalf("default = %q, want \"false\"", f.DefValue)
	}
}

// TestBuildRefreshOutcomes_tableDeltaDetail (#1718): the detail names this
// window's pair and rows, an empty window says no pair was written (the case
// order is load-bearing: TableDelta alone would claim "pair N of M (0 rows)"),
// and a compaction says why.
func TestBuildRefreshOutcomes_tableDeltaDetail(t *testing.T) {
	got := buildRefreshOutcomes(
		[]string{"shop.a", "shop.b", "shop.c"},
		[]*reconstruct.TableReport{
			{Schema: "shop", Table: "a", TableDelta: true, DeltaPairWritten: true, DeltaSeq: 3, DeltaChainFiles: 4, DeltaDeadRows: 7, DeltaUpsertRows: 9},
			{Schema: "shop", Table: "b", TableDelta: true, DeltaPairWritten: false, DeltaSeq: 2, DeltaChainFiles: 3},
			{Schema: "shop", Table: "c", DeltaCompacted: "the chain is 25h0m0s old"},
		}, nil)
	byTable := map[string]refreshOutcome{}
	for _, o := range got {
		byTable[o.Table] = o
	}
	if d := byTable["shop.a"].Detail; !strings.Contains(d, "pair 3 of 4") || !strings.Contains(d, "7 rows replaced or removed, 9 changed or new rows") {
		t.Errorf("shop.a detail = %q", d)
	}
	if d := byTable["shop.b"].Detail; !strings.Contains(d, "no events in the window") || !strings.Contains(d, "3 delta pairs") ||
		!strings.Contains(d, "last pair 2") || strings.Contains(d, "pair 2 of") {
		t.Errorf("shop.b detail = %q", d)
	}
	if d := byTable["shop.c"].Detail; d != "written again in full: the chain is 25h0m0s old" {
		t.Errorf("shop.c detail = %q", d)
	}
}
