//go:build integration

package pgstreamrun_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// pgTypeCase is one row of the shared PostgreSQL type-fidelity matrix, used by
// BOTH TestOne_PGTypeRoundTripMatrix (recover round-trip) and
// TestOne_PGTypeMatrixThroughReconstructFold (baseline+delta fold). valSQL is
// the initial literal; updSQL is a SECOND literal used by the fold test for the
// post-baseline UPDATE/INSERT deltas (empty ⇒ reuse valSQL). For the
// GUC-sensitive types both literals are chosen SKEW-DIVERGENT: their text
// rendering differs between the pinned rendering GUCs (#593 slice D) and the
// skewed session the fold test sets up — so an unpinned reader fails the fold
// assertions rather than passing vacuously.
type pgTypeCase struct{ name, typeDDL, valSQL, updSQL string }

// pgTypeMatrixCases: name → (column type DDL, value SQL as it appears in
// VALUES). Values are chosen scary-first: precision (>2^53), scale (trailing
// zero), escaping (quote+backslash), and the format-bearing types (bytea \x,
// arrays, ranges, bit, inet, json). The float literals are extra_float_digits-
// sensitive on purpose (a value like 3.5 renders identically under efd=0 and
// shortest-precise, which would make the fold's fail-before leg vacuous).
var pgTypeMatrixCases = []pgTypeCase{
	{"smallint", "smallint", "32767", ""},
	{"integer", "integer", "-2147483648", ""},
	{"bigint", "bigint", "9223372036854775807", ""},
	{"numeric_big", "numeric", "18446744073709551615", ""},                      // > 2^53, precision
	{"numeric_scale", "numeric(12,2)", "1.50", ""},                              // trailing-zero scale
	{"real", "real", "3.14159274", "2.7182817"},                                 // efd-sensitive (efd=0 → 6 sig digits)
	{"double", "double precision", "0.30000000000000004", "0.7000000000000001"}, // efd-sensitive (efd=0 → "0.3"/"0.7")
	{"text_tricky", "text", `'O''Brien \ C:\back'`, ""},                         // quote + backslash escaping
	{"varchar", "varchar(32)", "'hello world'", ""},
	{"char", "char(5)", "'ab'", ""}, // trailing blanks are insignificant in bpchar (trimmed on ::text); proves coercion, not padding-through-capture
	{"boolean", "boolean", "true", ""},
	{"uuid", "uuid", "'11111111-2222-3333-4444-555555555555'", ""},
	{"bytea", "bytea", `'\xdeadbeef00'`, `'\xcafebabe'`}, // bytea_output-sensitive (escape vs hex)
	{"json", "json", `'{"k": "v"}'`, ""},
	{"jsonb", "jsonb", `'{"k": "v''s"}'`, ""},                                              // embedded quote
	{"date", "date", "'2026-06-22'", "'2027-01-15'"},                                       // DateStyle-sensitive (German → 22.06.2026)
	{"time", "time", "'12:34:56'", "'23:45:01'"},                                           // control: unaffected by the five pinned GUCs
	{"timestamp", "timestamp", "'2026-06-22 12:34:56'", "'2027-01-15 23:45:01'"},           // DateStyle-sensitive
	{"timestamptz", "timestamptz", "'2026-06-22 12:34:56+00'", "'2027-01-15 23:45:01+00'"}, // TimeZone-sensitive (highest severity)
	{"interval", "interval", "'1 day 02:03:04'", "'2 days 03:04:05'"},                      // IntervalStyle-sensitive (sql_standard → "1 2:03:04")
	{"inet", "inet", "'192.168.1.10'", ""},
	{"cidr", "cidr", "'10.0.0.0/8'", ""},
	{"macaddr", "macaddr", "'08:00:2b:01:02:03'", ""},
	{"bit", "bit(4)", "'1010'", ""},
	{"varbit", "varbit", "'101'", ""},
	{"int4range", "int4range", "'[1,10)'", ""},
	{"int_array", "integer[]", "'{1,2,3}'", ""},
	{"text_array", "text[]", `'{"a","b,c"}'`, ""}, // element with a comma
	{"point", "point", "'(1,2)'", ""},
	{"money", "money", "'1.50'", ""}, // locale-dependent output ('$1.50'); lc_monetary deliberately NOT pinned — same instance, so stable
	{"enum", "mood", "'happy'", "'sad'"},
	{"composite", "pgt_pair", `'(7,"a,b ""c""")'`, `'(8,"x y")'`}, // user-defined composite (#1210): embedded comma + doubled quote exercise record field quoting
}

// updOf returns the fold test's second literal for a case (falls back to valSQL).
func updOf(c pgTypeCase) string {
	if c.updSQL != "" {
		return c.updSQL
	}
	return c.valSQL
}

// TestOne_PGTypeMatrixThroughReconstructFold is the #593 slice-D proof: the
// baseline+delta identity — baseline COPY text ≡ pgoutput delta text — holds
// for the full type matrix THROUGH the single-row reconstruct fold
// (FindBaseline → ReadBaselineRow → query.Fetch → ApplyAt), because the
// rendering GUCs are pinned identically on the baseline COPY connections and
// the logical-decoding (walsender) session.
//
// The fail-before/pass-after oracle: the test deliberately SKEWS the rendering
// GUCs two ways before anything connects —
//   - database-level defaults (ALTER DATABASE ... SET), which every NEW
//     backend inherits, INCLUDING the walsender and the baseline COPY conns;
//   - an operator options=-c ... string on the baseline QueryDSN, which must
//     LOSE to the pin (startup-packet GUCs are applied after the options
//     string).
//
// The comparison oracle runs on a dedicated connection that explicitly SETs
// the five pinned canonical values (TimeZone=UTC, DateStyle=ISO,
// extra_float_digits=3, bytea_output=hex, IntervalStyle=postgres) — it must
// NOT rely on session defaults, which the skew poisons. Without the pin
// (#593 slice D), the GUC-sensitive subtests fail: the baseline leg renders
// under the skew (id=1, id=2's baseline text) and the delta leg renders under
// the skew (id=3, id=2's overlay). With the pin, every leg matches the
// canonical oracle byte-for-byte.
//
// Three rows per type:
//   - id=1: baseline pass-through (no post-baseline delta touches it);
//   - id=2: baseline + UPDATE overlay (delta row_after must win, and its text
//     must be pin-rendered);
//   - id=3: delta-only (INSERTed after the baseline; no baseline row).
//
// CI-only: needs a live PostgreSQL source (BINTRAIL_TEST_PG_DSN) + MySQL index.
func TestOne_PGTypeMatrixThroughReconstructFold(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgtypesfold"
	const pub = "bintrail_pgtypesfold_pub"
	tblOf := func(name string) string { return "pgtypefold_" + name }

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	// ── GUC skew (the fail-before oracle) ───────────────────────────────────
	// Database-level defaults: every backend opened AFTER this — the baseline
	// COPY conns and the walsender — inherits them. The test's own `pg` conn
	// (already open) and the pinned oracle conn (explicit SETs) are unaffected.
	var curDB string
	if err := pg.QueryRow(ctx, "SELECT current_database()").Scan(&curDB); err != nil {
		t.Fatalf("current_database: %v", err)
	}
	dbIdent := pgx.Identifier{curDB}.Sanitize()
	skews := map[string]string{
		"timezone":           "'America/New_York'",
		"datestyle":          "'German'",
		"intervalstyle":      "'sql_standard'",
		"extra_float_digits": "0",
		"bytea_output":       "'escape'",
	}
	// The RESET cleanup is registered BEFORE the skew loop: if an ALTER fails
	// mid-loop (t.Fatalf), the already-applied skews still get reset rather
	// than leaking database-level defaults into later tests of this `go test`
	// invocation (-p 1 runs pgbaseline's package after this one).
	t.Cleanup(func() {
		bg := context.Background()
		for k := range skews {
			if _, err := pg.Exec(bg, fmt.Sprintf("ALTER DATABASE %s RESET %s", dbIdent, k)); err != nil {
				t.Logf("skew cleanup: RESET %s failed: %v", k, err)
			}
		}
	})
	for k, v := range skews {
		if _, err := pg.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s SET %s TO %s", dbIdent, k, v)); err != nil {
			t.Fatalf("skew %s: %v", k, err)
		}
	}

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		for _, c := range pgTypeMatrixCases {
			_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tblOf(c.name))
		}
		_, _ = pg.Exec(bg, "DROP TYPE IF EXISTS mood")
		_, _ = pg.Exec(bg, "DROP TYPE IF EXISTS pgt_pair")
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sqlStr string) {
		t.Helper()
		if _, err := pg.Exec(ctx, sqlStr); err != nil {
			t.Fatalf("exec %q: %v", sqlStr, err)
		}
	}
	mustExec("CREATE TYPE mood AS ENUM ('happy','sad')")
	mustExec("CREATE TYPE pgt_pair AS (a int, b text)")
	tbls := make([]string, len(pgTypeMatrixCases))
	for i, c := range pgTypeMatrixCases {
		tbl := tblOf(c.name)
		tbls[i] = tbl
		mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, val %s)", tbl, c.typeDDL))
		mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	}
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + strings.Join(tbls, ", "))

	// Pre-baseline rows: id=1 (never touched again) and id=2 (updated later).
	for _, c := range pgTypeMatrixCases {
		tbl := tblOf(c.name)
		mustExec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, %s), (2, %s)", tbl, c.valSQL, c.valSQL))
	}

	// ── The pinned canonical oracle connection ──────────────────────────────
	oracleConn, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect oracle: %v", err)
	}
	t.Cleanup(func() { oracleConn.Close(context.Background()) })
	for _, set := range []string{
		"SET TimeZone TO 'UTC'", "SET DateStyle TO 'ISO'", "SET extra_float_digits TO 3",
		"SET bytea_output TO 'hex'", "SET IntervalStyle TO 'postgres'",
	} {
		if _, err := oracleConn.Exec(ctx, set); err != nil {
			t.Fatalf("oracle %s: %v", set, err)
		}
	}
	// The oracle reads the OUTPUT-FUNCTION rendering, not a ::text cast: the
	// simple protocol returns text-format results, whose wire bytes ARE the
	// type's output function under this session's (pinned) GUCs — the exact
	// text a correct fold must produce. A ::text CAST diverges for some types
	// (bool::text = "true"/"false" vs boolout's "t"/"f"; inet::text appends
	// /32 to a host address; bpchar::text trims the padding), which would
	// flag internally-consistent fold output as a mismatch.
	oracle := func(tbl string, id int) string {
		t.Helper()
		rows, err := oracleConn.Query(ctx, fmt.Sprintf("SELECT val FROM %s WHERE id=%d", tbl, id), pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatalf("oracle %s id=%d: %v", tbl, id, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("oracle %s id=%d: no row (err=%v)", tbl, id, rows.Err())
		}
		got := string(rows.RawValues()[0]) // copy before Close invalidates it
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("oracle %s id=%d: %v", tbl, id, err)
		}
		return got
	}
	orig1 := make(map[string]string, len(pgTypeMatrixCases))
	for _, c := range pgTypeMatrixCases {
		orig1[c.name] = oracle(tblOf(c.name), 1)
	}

	// ── Baseline (creates the slot; COPY conns run under the skewed DSN) ────
	// The operator-DSN skew: an options string carrying the same five GUCs —
	// the pin's startup-packet RuntimeParams must beat it (guc_options are
	// applied after the options string).
	skewOpts := url.QueryEscape("-c TimeZone=America/New_York -c datestyle=German -c extra_float_digits=0 -c bytea_output=escape -c intervalstyle=sql_standard")
	sep := "?"
	if strings.Contains(pgDSN, "?") {
		sep = "&"
	}
	outDir := t.TempDir()
	stats, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN:    pgDSN + sep + "options=" + skewOpts,
		ReplDSN:     replDSN(pgDSN),
		SlotName:    slot,
		Publication: pub,
		OutputDir:   outDir,
		Parallelism: 2, // exercises openWorkerConn's pinned path, not just the anchor conn
	})
	if err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}
	if stats.TablesProcessed != len(pgTypeMatrixCases) {
		t.Fatalf("baseline processed %d tables, want %d", stats.TablesProcessed, len(pgTypeMatrixCases))
	}
	if stats.DeltaStartLSN == 0 {
		t.Fatal("baseline DeltaStartLSN is 0 — no LSN anchor")
	}

	// ── Stream deltas on the SAME slot the baseline created ────────────────
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 46,
		BatchSize: 200, Checkpoint: 200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active AND confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	// Post-baseline deltas: UPDATE id=2 to the second literal, INSERT id=3.
	for _, c := range pgTypeMatrixCases {
		tbl := tblOf(c.name)
		mustExec(fmt.Sprintf("UPDATE %s SET val = %s WHERE id = 2", tbl, updOf(c)))
		mustExec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (3, %s)", tbl, updOf(c)))
	}
	orig2 := make(map[string]string, len(pgTypeMatrixCases))
	orig3 := make(map[string]string, len(pgTypeMatrixCases))
	for _, c := range pgTypeMatrixCases {
		orig2[c.name] = oracle(tblOf(c.name), 2)
		orig3[c.name] = oracle(tblOf(c.name), 3)
	}

	wantEvents := 2 * len(pgTypeMatrixCases) // UPDATE + INSERT per table
	waitFor(t, 30*time.Second, func() bool {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name='public'").Scan(&n); err != nil {
			return false
		}
		return n >= wantEvents
	}, "all delta events indexed")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	// ── The fold: FindBaseline → ReadBaselineRow → Fetch → ApplyAt ─────────
	at := time.Now().Add(time.Hour)
	engine := query.New(indexDB)
	// valText normalizes a fold output value for comparison (baseline values
	// scan as string or []byte depending on the DuckDB driver path).
	valText := func(v any) string {
		switch x := v.(type) {
		case nil:
			return "<nil>"
		case string:
			return x
		case []byte:
			return string(x)
		default:
			return fmt.Sprint(x)
		}
	}
	for _, c := range pgTypeMatrixCases {
		t.Run(c.name, func(t *testing.T) {
			tbl := tblOf(c.name)
			path, snapTime, _, err := reconstruct.FindBaseline(ctx, outDir, "public", tbl, at)
			if err != nil {
				t.Fatalf("FindBaseline: %v", err)
			}

			fold := func(id string, wantEvents int) string {
				t.Helper()
				row, err := reconstruct.ReadBaselineRow(ctx, path, map[string]string{"id": id}, nil)
				if err != nil {
					t.Fatalf("ReadBaselineRow id=%s: %v", id, err)
				}
				if id == "3" && row != nil {
					t.Fatalf("id=3 must have NO baseline row (inserted after the baseline), got %v", row)
				}
				events, err := engine.Fetch(ctx, query.Options{
					Schema: "public", Table: tbl, PKValues: id,
					Since: &snapTime, Until: &at, Order: "ASC",
				})
				if err != nil {
					t.Fatalf("Fetch id=%s: %v", id, err)
				}
				if len(events) != wantEvents {
					t.Fatalf("id=%s: %d indexed events in the window, want %d", id, len(events), wantEvents)
				}
				state, err := reconstruct.ApplyAt(row, events, at)
				if err != nil {
					t.Fatalf("ApplyAt id=%s: %v", id, err)
				}
				if state == nil {
					t.Fatalf("id=%s: ApplyAt returned nil state", id)
				}
				return valText(state["val"])
			}

			// Byte-equality against the output-function oracle — no per-type
			// normalization needed (the simple-protocol oracle renders exactly
			// what a correct fold produces, bpchar padding included).
			//
			// id=1 — baseline pass-through: the sharpest pre-pin failure (the
			// baseline COPY rendered under the skewed session).
			if got := fold("1", 0); got != orig1[c.name] {
				t.Errorf("id=1 baseline pass-through (%s):\n got  %q\n want %q", c.typeDDL, got, orig1[c.name])
			}
			// id=2 — baseline + UPDATE overlay: the delta row_after must win
			// and be pin-rendered.
			if got := fold("2", 1); got != orig2[c.name] {
				t.Errorf("id=2 baseline+delta overlay (%s):\n got  %q\n want %q", c.typeDDL, got, orig2[c.name])
			}
			// id=3 — delta-only: pre-pin failure under the DB-level skew (the
			// walsender rendered under the skewed defaults).
			if got := fold("3", 1); got != orig3[c.name] {
				t.Errorf("id=3 delta-only (%s):\n got  %q\n want %q", c.typeDDL, got, orig3[c.name])
			}

			// bytea must surface as PostgreSQL hex text (\x…) through the fold
			// on every leg — never base64, never octal-escape form.
			if c.name == "bytea" {
				for _, id := range []string{"1", "2", "3"} {
					want := 0
					if id != "1" {
						want = 1
					}
					if got := fold(id, want); !strings.HasPrefix(got, `\x`) {
						t.Errorf("bytea id=%s not in \\x hex form through the fold: %q", id, got)
					}
				}
			}
		})
	}
}
