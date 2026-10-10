package status

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// ─── Scoping the capture-health table names (#1452) ──────────────────────────
//
// The console renders /api/status for sessions whose data access is
// restricted, and the ledger names the tables whose capture stopped. These pin
// the one rule: a name the reader may not see is COUNTED, never shown, in the
// per-reason list AND in the explanation prose built from it; the counts stay
// whole; a nil predicate renders the ledger verbatim.

// onlyApp admits every table of the "app" schema and nothing else.
func onlyApp(schema, _ string) bool { return schema == "app" }

func TestScopeCaptureSkips_withholdsNamesAndKeepsCounts(t *testing.T) {
	in := map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {
			Count: 5, LastAt: time.Date(2026, 8, 4, 19, 49, 33, 0, time.UTC),
			Tables: []string{"app.users", "hr.payroll", "app.orders"}, TablesTruncated: true,
		},
		CaptureSkipReasonTableExcludedFromSnapshot: {
			Count: 2, Tables: []string{"hr.people"}, LastDetail: "no primary key",
		},
		CaptureSkipReasonColumnCountMismatch: {Count: 1}, // legacy: no names at all
	}
	out := ScopeCaptureSkips(in, onlyApp)

	st := out[CaptureSkipReasonTableNotInSnapshot]
	if got, want := strings.Join(st.Tables, ","), "app.users,app.orders"; got != want {
		t.Errorf("visible names = %q, want %q (order preserved, hr.payroll dropped)", got, want)
	}
	if st.TablesWithheld != 1 || st.Count != 5 || !st.TablesTruncated {
		t.Errorf("withheld=%d count=%d truncated=%v; want 1, 5 (untouched), true (untouched)", st.TablesWithheld, st.Count, st.TablesTruncated)
	}
	st = out[CaptureSkipReasonTableExcludedFromSnapshot]
	if len(st.Tables) != 0 || st.TablesWithheld != 1 || st.Count != 2 || st.LastDetail != "no primary key" {
		t.Errorf("a reason whose every table is withheld keeps its count and detail: %+v", st)
	}
	st = out[CaptureSkipReasonColumnCountMismatch]
	if len(st.Tables) != 0 || st.TablesWithheld != 0 || st.Count != 1 {
		t.Errorf("a legacy reason with no names has nothing to withhold: %+v", st)
	}
	// The caller's ledger is not mutated: the unscoped rendering (the text
	// report, an unrestricted session) must still see every name.
	if len(in[CaptureSkipReasonTableNotInSnapshot].Tables) != 3 {
		t.Errorf("ScopeCaptureSkips mutated its input: %v", in[CaptureSkipReasonTableNotInSnapshot].Tables)
	}
}

// A name that does not split into schema.table is withheld, not shown: this
// filter decides what a restricted reader learns, so the unknown case fails
// in the withholding direction.
func TestScopeCaptureSkips_unsplittableNameIsWithheld(t *testing.T) {
	out := ScopeCaptureSkips(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {Count: 1, Tables: []string{"nodot"}},
	}, func(string, string) bool { return true })
	st := out[CaptureSkipReasonTableNotInSnapshot]
	if len(st.Tables) != 0 || st.TablesWithheld != 1 {
		t.Errorf("a name with no schema part must be withheld, got %+v", st)
	}
}

func TestScopeCaptureSkips_nilPredicateIsVerbatim(t *testing.T) {
	in := map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {Count: 1, Tables: []string{"hr.payroll"}},
	}
	out := ScopeCaptureSkips(in, nil)
	st := out[CaptureSkipReasonTableNotInSnapshot]
	if len(st.Tables) != 1 || st.TablesWithheld != 0 {
		t.Errorf("nil predicate must not touch the ledger: %+v", st)
	}
}

// The explanation is rebuilt from the scoped ledger: the withheld names are
// absent from every line, and the sentence says how many tables it is not
// naming instead of pretending the list is complete. The ledger goes through
// ScopeCaptureSkips here, so the withheld names exist and the absence
// assertion has something to catch.
func TestExplainCaptureSkips_withheldTablesAreCountedNotNamed(t *testing.T) {
	out := explainJoined(ScopeCaptureSkips(map[string]CaptureSkipStat{
		CaptureSkipReasonTableNotInSnapshot: {
			Count: 5, Tables: []string{"app.users", "hr.payroll", "hr.people"},
		},
	}, onlyApp))
	for _, withheld := range []string{"hr.payroll", "hr.people", "hr."} {
		if strings.Contains(out, withheld) {
			t.Errorf("withheld name %q reached the prose:\n%s", withheld, out)
		}
	}
	if !strings.Contains(out, "app.users and 2 tables outside your access changed on the source but are missing") {
		t.Errorf("the subject must name the visible table and count the withheld ones:\n%s", out)
	}
	if strings.Contains(out, "cannot name them") {
		t.Errorf("a scoped ledger is not a legacy ledger; the index CAN name the tables:\n%s", out)
	}
}

// Every table withheld: still a sentence about that reason, in the singular
// when it is one table, and never the legacy-ledger wording.
func TestExplainCaptureSkips_allTablesWithheldStillExplains(t *testing.T) {
	out := explainJoined(map[string]CaptureSkipStat{
		CaptureSkipReasonTableExcludedFromSnapshot: {
			Count: 2, TablesWithheld: 1, LastDetail: "no primary key",
		},
	})
	if !strings.Contains(out, "1 table outside your access is left out of the schema snapshot") {
		t.Errorf("a fully withheld reason must still be explained, in the singular:\n%s", out)
	}
	if strings.Contains(out, "cannot name them") || strings.Contains(out, "no tables") {
		t.Errorf("fully withheld must not read as a legacy ledger or as 'no tables':\n%s", out)
	}
	// The visible copy this change adds follows the copy rules (no em dash);
	// the pre-existing lines are not this test's concern, so only the new
	// subject is checked.
	if strings.Contains(namedTables(CaptureSkipStat{Tables: []string{"a.b"}, TablesWithheld: 3, TablesTruncated: true}), "—") {
		t.Error("the withheld subject must not carry an em dash")
	}
}

func TestNamedTables_withheldCombinations(t *testing.T) {
	cases := []struct {
		name string
		st   CaptureSkipStat
		want string
	}{
		{"visible only", CaptureSkipStat{Tables: []string{"a.b", "a.c"}}, "a.b, a.c"},
		{"visible and withheld", CaptureSkipStat{Tables: []string{"a.b"}, TablesWithheld: 1}, "a.b and 1 table outside your access"},
		{"withheld only, plural", CaptureSkipStat{TablesWithheld: 3}, "3 tables outside your access"},
		{"withheld and truncated", CaptureSkipStat{TablesWithheld: 2, TablesTruncated: true}, "2 tables outside your access and others"},
		{"all three", CaptureSkipStat{Tables: []string{"a.b"}, TablesWithheld: 1, TablesTruncated: true}, "a.b and 1 table outside your access and others"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := namedTables(tc.st); got != tc.want {
				t.Errorf("namedTables = %q, want %q", got, tc.want)
			}
		})
	}
	if isAre(CaptureSkipStat{TablesWithheld: 1}) != "is" || hasHave(CaptureSkipStat{Tables: []string{"a.b"}, TablesWithheld: 1}) != "have" {
		t.Error("number agreement must count withheld tables as tables")
	}
}

// The wire rendering: the JSON built by WriteJSON through StatusData.TableVisible
// carries only the visible names in skipped[*].tables AND in explanation,
// counts the rest in tables_withheld, and leaves the tallies whole. Asserted by
// searching the serialized body for the withheld name, which catches the prose
// as well as the list.
func TestWriteStatusJSON_tableVisibleScopesNamesEverywhere(t *testing.T) {
	ledger := `{"table_not_in_snapshot":{"count":5,"last_at":"2026-08-04T19:49:33Z","tables":["app.users","hr.payroll"]},` +
		`"table_excluded_from_snapshot":{"count":2,"last_at":"2026-08-04T19:50:00Z","tables":["hr.people"],"last_detail":"no primary key"}}`
	data := &StatusData{Stream: captureStream(ledger), TableVisible: onlyApp}
	var buf bytes.Buffer
	if err := data.WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	body := buf.String()
	for _, withheld := range []string{"hr.payroll", "hr.people", "payroll", "people"} {
		if strings.Contains(body, withheld) {
			t.Errorf("withheld name %q reached the wire:\n%s", withheld, body)
		}
	}
	if !strings.Contains(body, "app.users") {
		t.Errorf("the visible name must still be carried:\n%s", body)
	}
	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	ch := out["stream"].(map[string]any)["capture_health"].(map[string]any)
	if ch["total_skipped"] != float64(7) {
		t.Errorf("total_skipped = %v, want 7 (counts are not names)", ch["total_skipped"])
	}
	skipped := ch["skipped"].(map[string]any)
	nis := skipped["table_not_in_snapshot"].(map[string]any)
	if nis["count"] != float64(5) || nis["tables_withheld"] != float64(1) {
		t.Errorf("table_not_in_snapshot: count=%v withheld=%v, want 5 and 1", nis["count"], nis["tables_withheld"])
	}
	if tables, _ := nis["tables"].([]any); len(tables) != 1 || tables[0] != "app.users" {
		t.Errorf("tables = %v, want [app.users]", nis["tables"])
	}
	exc := skipped["table_excluded_from_snapshot"].(map[string]any)
	if _, present := exc["tables"]; present {
		t.Errorf("a fully withheld reason must carry no tables key: %v", exc)
	}
	if exc["count"] != float64(2) || exc["tables_withheld"] != float64(1) {
		t.Errorf("table_excluded_from_snapshot: count=%v withheld=%v, want 2 and 1", exc["count"], exc["tables_withheld"])
	}
	expl, _ := json.Marshal(ch["explanation"])
	if !strings.Contains(string(expl), "outside your access") {
		t.Errorf("the explanation must say names were withheld:\n%s", expl)
	}
}

// Without a predicate the wire is byte-for-byte what WriteStatusJSON (the
// CLI's renderer, which has no predicate) emits for the same stream: no
// tables_withheld key, every name present.
func TestWriteStatusJSON_noPredicateRendersVerbatim(t *testing.T) {
	ledger := `{"table_not_in_snapshot":{"count":5,"last_at":"2026-08-04T19:49:33Z","tables":["app.users","hr.payroll"]}}`
	stream := captureStream(ledger)
	// Each render reads the clock for checkpoint_age_seconds, so two renders
	// on either side of a second boundary differ by one. No checkpoint, no age.
	stream.LastCheckpoint = time.Time{}
	var scoped, cli bytes.Buffer
	if err := (&StatusData{Stream: stream}).WriteJSON(&scoped); err != nil {
		t.Fatal(err)
	}
	if err := WriteStatusJSON(&cli, nil, nil, nil, nil, nil, stream); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(scoped.Bytes(), cli.Bytes()) {
		t.Errorf("a nil predicate must render exactly what the CLI renders:\nStatusData: %s\nCLI:        %s", scoped.String(), cli.String())
	}
	if strings.Contains(scoped.String(), "tables_withheld") {
		t.Errorf("an unscoped rendering must not carry tables_withheld:\n%s", scoped.String())
	}
	if !strings.Contains(scoped.String(), "hr.payroll") {
		t.Errorf("an unscoped rendering must name every table:\n%s", scoped.String())
	}
}

// tables_withheld is a fact about THIS rendering, never about the ledger: a
// persisted document that carries the key (a hostile or future writer) must
// not be able to set it. Under a nil predicate the key is absent; under a
// predicate the count is what the predicate withheld. This is the pin on
// CaptureSkipStat.TablesWithheld's json:"-" tag, and the ledger carries the
// key under BOTH spellings a weakened tag would decode: the wire name
// (`tables_withheld`, what a `json:"tables_withheld"` tag reads) and the Go
// field name (`TablesWithheld`, what NO tag reads, case-insensitively).
// Seeding only the wire name lets the tag-dropped mutation survive, because
// encoding/json's case-insensitive match does not bridge the underscore.
func TestWriteStatusJSON_persistedTablesWithheldIsIgnored(t *testing.T) {
	ledger := `{"table_not_in_snapshot":{"count":5,"last_at":"2026-08-04T19:49:33Z","tables":["app.users","hr.payroll"],"tables_withheld":9,"TablesWithheld":9}}`
	render := func(visible func(schema, table string) bool) (string, map[string]any) {
		var buf bytes.Buffer
		if err := (&StatusData{Stream: captureStream(ledger), TableVisible: visible}).WriteJSON(&buf); err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		stat := out["stream"].(map[string]any)["capture_health"].(map[string]any)["skipped"].(map[string]any)["table_not_in_snapshot"].(map[string]any)
		return buf.String(), stat
	}
	body, stat := render(nil)
	if _, present := stat["tables_withheld"]; present || strings.Contains(body, "tables_withheld") {
		t.Errorf("a persisted tables_withheld reached the wire under a nil predicate: %v", stat)
	}
	if stat["count"] != float64(5) {
		t.Errorf("count = %v, want 5", stat["count"])
	}
	_, stat = render(onlyApp)
	if stat["tables_withheld"] != float64(1) {
		t.Errorf("tables_withheld = %v, want 1 (what THIS rendering withheld, not the persisted 9)", stat["tables_withheld"])
	}
}

// A legacy ledger (no table names at all) under a predicate: nothing to
// withhold, so no tables_withheld key, and the legacy wording stays, since
// the index really cannot name the tables.
func TestWriteStatusJSON_legacyLedgerUnderPredicate(t *testing.T) {
	ledger := `{"table_not_in_snapshot":{"count":3,"last_at":"2026-08-04T19:49:33Z"}}`
	var buf bytes.Buffer
	if err := (&StatusData{Stream: captureStream(ledger), TableVisible: onlyApp}).WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	body := buf.String()
	if strings.Contains(body, "tables_withheld") || strings.Contains(body, "outside your access") {
		t.Errorf("a legacy ledger has no names to withhold, so nothing may say it did:\n%s", body)
	}
	if !strings.Contains(body, "cannot name them") {
		t.Errorf("the legacy wording must survive a predicate:\n%s", body)
	}
}
