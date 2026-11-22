package verify

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestPGCoverageVerdict pins the pure coverage grid for a PostgreSQL source —
// especially its two load-bearing asymmetries: checkpoint >= anchor PROVES
// coverage (no note), while checkpoint < anchor proves nothing and must
// degrade to a covered-with-note (never a permanent inconclusive: WAL from
// other databases advances the anchor without producing indexable events), and
// a stamped permanent loss is a hard stop (inconclusive beats a possible false
// mismatch).
func TestPGCoverageVerdict(t *testing.T) {
	cases := []struct {
		name          string
		flavor        string
		checkpoint    uint64
		anchor        uint64
		gapLost       bool
		gapDetail     string
		wantCovered   bool
		wantInNote    string
		wantEmptyNote bool
	}{
		{name: "wrong flavor", flavor: "mysql", checkpoint: 100, anchor: 50,
			wantCovered: false, wantInNote: "not a PostgreSQL capture"},
		{name: "empty flavor", flavor: "", checkpoint: 100, anchor: 50,
			wantCovered: false, wantInNote: "not a PostgreSQL capture"},
		{name: "gap lost", flavor: "postgres", checkpoint: 100, anchor: 50, gapLost: true, gapDetail: "slot invalidated",
			wantCovered: false, wantInNote: "slot invalidated"},
		{name: "gap lost without detail", flavor: "postgres", checkpoint: 100, anchor: 50, gapLost: true,
			wantCovered: false, wantInNote: "no detail recorded"},
		{name: "no checkpoint yet", flavor: "postgres", checkpoint: 0, anchor: 50,
			wantCovered: false, wantInNote: "no LSN checkpoint"},
		{name: "checkpoint proves coverage", flavor: "postgres", checkpoint: 100, anchor: 100,
			wantCovered: true, wantEmptyNote: true},
		{name: "checkpoint past anchor", flavor: "postgres", checkpoint: 200, anchor: 100,
			wantCovered: true, wantEmptyNote: true},
		{name: "checkpoint behind anchor is unprovable, not blocking", flavor: "postgres", checkpoint: 50, anchor: 100,
			wantCovered: true, wantInNote: "coverage unverified"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			covered, note := pgCoverageVerdict(tc.flavor, tc.checkpoint, tc.anchor, tc.gapLost, tc.gapDetail)
			if covered != tc.wantCovered {
				t.Fatalf("covered = %v, want %v (note=%q)", covered, tc.wantCovered, note)
			}
			if tc.wantEmptyNote && note != "" {
				t.Errorf("want no note on proven coverage, got %q", note)
			}
			if tc.wantInNote != "" && !strings.Contains(note, tc.wantInNote) {
				t.Errorf("note = %q, want it to contain %q", note, tc.wantInNote)
			}
		})
	}
}

// TestGapLostInWindow pins the window scoping of the gap_lost stamp: a loss
// stamped BEFORE the baseline's snapshot time is outside the comparison
// window (the baseline is a fresh dump — it re-covers whatever the gap lost)
// and must NOT degrade the verdict, or one historical gap would make the
// index permanently unverifiable no matter how many clean baselines follow.
func TestGapLostInWindow(t *testing.T) {
	windowStart := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		gap  sql.NullTime
		want bool
	}{
		{"no stamp", sql.NullTime{}, false},
		{"loss before the window (pre-baseline)", sql.NullTime{Valid: true, Time: windowStart.Add(-time.Hour)}, false},
		{"loss exactly at window start", sql.NullTime{Valid: true, Time: windowStart}, true},
		{"loss inside the window", sql.NullTime{Valid: true, Time: windowStart.Add(time.Hour)}, true},
		// The loss is stamped by the daemon's clock and the window starts by
		// the PostgreSQL server's: a daemon 2 minutes behind stamps a loss
		// made after the snapshot as 2 minutes before it.
		{"loss stamped before the start by a daemon clock behind", sql.NullTime{Valid: true, Time: windowStart.Add(-2 * time.Minute)}, true},
		{"loss well before the margin", sql.NullTime{Valid: true, Time: windowStart.Add(-reconstruct.PGSkipClockMargin - time.Second)}, false},
	}
	for _, tc := range cases {
		if got := gapLostInWindow(tc.gap, windowStart); got != tc.want {
			t.Errorf("%s: gapLostInWindow = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestVerifyTablePG_noPK: same PK-required gate as every other verify mode.
func TestVerifyTablePG_noPK(t *testing.T) {
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.orders": {Schema: "app", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, DataType: ""},
		}},
	})
	res, err := VerifyTablePG(context.Background(), PGLiveConfig{Resolver: resolver}, "app", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != StatusInconclusive || !strings.Contains(res.Detail, "no primary key") {
		t.Fatalf("status=%q detail=%q, want inconclusive/no primary key", res.Status, res.Detail)
	}
}

// TestVerifyTablePG_bypassesPKTypeGate: a PG snapshot's empty DATA_TYPE must
// NOT trip the MySQL PK-type gate (PG PKs are text-identity on both sides —
// the same bypass VerifyBaselinePair's pg branch has). With the gate bypassed
// and no source checksum wired, the next step (the live fingerprint) is what
// fails — proving the call got PAST the gate.
func TestVerifyTablePG_bypassesPKTypeGate(t *testing.T) {
	res, err := VerifyTablePG(context.Background(), PGLiveConfig{Resolver: pgResolver()}, "app", "orders")
	if err == nil {
		t.Fatalf("want the source-checksum error (no source checksum wired), got status %q", res.Status)
	}
	if !strings.Contains(err.Error(), "no PostgreSQL source checksum wired") {
		t.Fatalf("error = %v, want the no-source-checksum error", err)
	}
	if strings.Contains(err.Error(), "unsupported by the baseline canonicalizer") {
		t.Fatalf("the MySQL PK-type gate must not run on the PG path: %v", err)
	}
}

// TestPGTargetTables covers the explicit filter (parse + sort), the invalid
// entry error, and the resolver enumeration that replaces the MySQL path's
// MAX(snapshot_id) query (which on a PG index — one relation per snapshot_id —
// silently names a single table).
func TestPGTargetTables(t *testing.T) {
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"app.orders": {Schema: "app", Table: "orders"},
		"app.items":  {Schema: "app", Table: "items"},
		"crm.leads":  {Schema: "crm", Table: "leads"},
	})

	t.Run("resolver enumeration, sorted", func(t *testing.T) {
		got, err := PGTargetTables(resolver, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"app.items", "app.orders", "crm.leads"}
		if len(got) != len(want) {
			t.Fatalf("got %d tables, want %d", len(got), len(want))
		}
		for i, st := range got {
			if st.Schema+"."+st.Table != want[i] {
				t.Errorf("tables[%d] = %s.%s, want %s", i, st.Schema, st.Table, want[i])
			}
		}
	})

	t.Run("explicit filter wins, sorted", func(t *testing.T) {
		got, err := PGTargetTables(resolver, []string{"crm.leads", "app.orders"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Schema != "app" || got[1].Schema != "crm" {
			t.Fatalf("got %v, want app.orders then crm.leads", got)
		}
	})

	t.Run("invalid entry", func(t *testing.T) {
		if _, err := PGTargetTables(resolver, []string{"nodot"}); err == nil {
			t.Fatal("want an error for a filter entry without schema.table form")
		}
	})
}

// TestPGNormalizeSymmetry is the wiring guarantee behind the whole PG
// live-source comparison: for any text value, the live-scan hook
// (pgNormalizeRenderedBytes, applied to the source's raw text) must produce
// exactly what the reconstruct side produces for the same text
// (renderCellNormalized over a string value under a PG snapshot's EMPTY
// DataType — metadata.WritePGSnapshot stores "" for every PG column). If
// these two ever diverge, identical data digests differently and every row
// becomes a false MISMATCH.
func TestPGNormalizeSymmetry(t *testing.T) {
	emptyTypeCol := metadata.ColumnMeta{DataType: ""}
	values := []string{
		"plain text",
		"42",
		"3.14",
		"t",                             // bool output-function form
		"\\x6465616462656566",           // bytea under bytea_output=hex
		"2024-01-02 03:04:05.123456+00", // timestamptz under TimeZone=UTC, DateStyle=ISO
		`{"b":2,"a":1}`,                 // JSON container: canonicalized on BOTH sides
		`{"a": 1, "b": 2}`,              // jsonb output spacing: canonicalized the same way
		"0000-00-00 00:00:00",           // MySQL zero-date sentinel text: must NOT be nulled under an empty type
		"",                              // empty string stays an empty (non-NULL) value
		"weird\tescapes\nand café 日本語",  // multibyte + control chars pass through
	}
	for _, v := range values {
		live := pgNormalizeRenderedBytes([]byte(v))
		recon := renderCellNormalized(v, emptyTypeCol)
		if !bytes.Equal(live, recon) {
			t.Errorf("normalization diverged for %q: live=%q recon=%q", v, live, recon)
		}
		if live == nil {
			t.Errorf("normalization must never turn a non-NULL value into NULL (value %q)", v)
		}
	}
	// The two JSON spellings of the same document must canonicalize to the
	// same bytes — that IS the representation gap the hook exists to close.
	if !bytes.Equal(pgNormalizeRenderedBytes([]byte(`{"b":2,"a":1}`)), pgNormalizeRenderedBytes([]byte(`{"a": 1, "b": 2}`))) {
		t.Error("equivalent JSON containers must canonicalize identically")
	}
}
