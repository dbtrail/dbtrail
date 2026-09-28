package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func outcome(table, verdict, detail string) reconstruct.RefreshOutcome {
	return reconstruct.RefreshOutcome{Table: table, Verdict: verdict, Detail: detail}
}

// Only the tables that stopped the run are kept. A run with none keeps
// nothing, and nil is what "no list" is everywhere else.
func TestRefusedTablesOf_keepsOnlyWhatStoppedTheRun(t *testing.T) {
	kept, omitted := RefusedTablesOf([]reconstruct.RefreshOutcome{
		outcome("shop.a", reconstruct.RefreshVerdictRefreshed, "pair 3"),
		outcome("shop.b", reconstruct.RefreshVerdictUnchanged, "kept"),
		outcome("shop.c", reconstruct.RefreshVerdictSkipped, "the run ended before this table was reached"),
	})
	if kept != nil || omitted != 0 {
		t.Fatalf("kept %+v, omitted %d; want nothing: no table refused", kept, omitted)
	}
	if kept, omitted := RefusedTablesOf(nil); kept != nil || omitted != 0 {
		t.Fatalf("no outcomes: kept %+v, omitted %d", kept, omitted)
	}

	kept, omitted = RefusedTablesOf([]reconstruct.RefreshOutcome{
		outcome("shop.a", reconstruct.RefreshVerdictRefusedDDL, "schema changed"),
		outcome("shop.b", reconstruct.RefreshVerdictRefusedGap, "capture gap"),
		outcome("shop.c", reconstruct.RefreshVerdictRefused, "disk"),
	})
	if len(kept) != 3 || omitted != 0 {
		t.Fatalf("every table refused: kept %+v, omitted %d", kept, omitted)
	}
	for i, want := range []RefusedTable{
		{"shop.a", "refused-ddl", "schema changed"},
		{"shop.b", "refused-gap", "capture gap"},
		{"shop.c", "refused", "disk"},
	} {
		if kept[i] != want {
			t.Errorf("entry %d = %+v, want %+v", i, kept[i], want)
		}
	}
}

// The names a server can produce, and the ones only a damaged listing can.
// Each is kept as its own entry, in the order of the run.
func TestRefusedTablesOf_names(t *testing.T) {
	names := []string{
		"shop.orders",
		"crm.orders", // the same table name in another schema
		"shop.Orders",
		"shop.with.dot",
		"shop.it's \"quoted\" `too`",
		"shop.<img src=x onerror=alert(1)>",
		"",
		"shop.two\nlines",
	}
	var in []reconstruct.RefreshOutcome
	for _, n := range names {
		in = append(in, outcome(n, reconstruct.RefreshVerdictRefusedDDL, "r"))
	}
	kept, _ := RefusedTablesOf(in)
	if len(kept) != len(names) {
		t.Fatalf("kept %d of %d names: %+v", len(kept), len(names), kept)
	}
	for i, n := range names {
		want := n
		if n == "shop.two\nlines" {
			want = "shop.two lines"
		}
		if kept[i].Name != want {
			t.Errorf("name %d = %q, want %q", i, kept[i].Name, want)
		}
	}
	long, _ := RefusedTablesOf([]reconstruct.RefreshOutcome{
		outcome(strings.Repeat("ñ", 5000), reconstruct.RefreshVerdictRefused, "")})
	if n := utf8.RuneCountInString(long[0].Name); n != refusedNameCap || !strings.HasSuffix(long[0].Name, "...") {
		t.Errorf("a 5000 rune name was kept at %d runes: %q", n, long[0].Name)
	}
	if long[0].Reason != "" {
		t.Errorf("an empty reason became %q", long[0].Reason)
	}
}

// A reason is an error text written far from here. It is folded to one line,
// cut, and never carries a connection string.
func TestRefusedTablesOf_reasons(t *testing.T) {
	const indexDSN = "idx:s3cr3t-pw@tcp(db.internal:3306)/bintrail_index"
	for _, tc := range []struct {
		name, in string
		want     string
		never    []string
	}{
		{"several lines", "shop.orders: first\n\tsecond\r\n\n third  ", "shop.orders: first second third", nil},
		{"the run's own dsn", "open " + indexDSN + ": refused", "open <dsn>: refused", []string{"s3cr3t-pw", "idx:"}},
		{"its password alone", "access denied using password s3cr3t-pw", "access denied using password ***", []string{"s3cr3t-pw"}},
		{"another dsn", "dial src:0ther@tcp(10.0.0.5:3306)/shop failed", "dial ***@tcp(10.0.0.5:3306)/shop failed", []string{"0ther", "src:"}},
		{"a url with credentials", "GET https://key:hunter2@minio.local/b/k 403", "GET https://***@minio.local/b/k 403", []string{"hunter2", "key:"}},
		{"a password with an at sign", "dial root:p@ss@tcp(10.0.0.1:3306)/idx", "dial ***@tcp(10.0.0.1:3306)/idx", []string{"p@ss", "root"}},
		{"a dsn with no address", "open root:secret@/idx", "open ***@/idx", []string{"secret"}},
		{"a socket", "open root:secret@unix(/tmp/my.sock)/idx", "open ***@unix(/tmp/my.sock)/idx", []string{"secret"}},
		{"a url password with an at sign", "GET https://user:p@ss@host/x", "GET https://***@host/x", []string{"p@ss", "ss@"}},
		{"key and value", "connect host=db password=hunter2 user=x", "connect host=db password=*** user=x", []string{"hunter2"}},
		{"a time is not a credential", "lost between 10:00 and 10:05 on shop.orders", "lost between 10:00 and 10:05 on shop.orders", nil},
		{"a url without", "read s3://bucket/prefix/2026-01-01T00:00:00Z", "read s3://bucket/prefix/2026-01-01T00:00:00Z", nil},
		{"bytes that are not text", "bad \xff\xfe name", "bad ? name", nil},
		{"empty", "", "", nil},
		{"only spaces", " \n\t ", "", nil},
	} {
		kept, _ := RefusedTablesOf([]reconstruct.RefreshOutcome{outcome("shop.orders", reconstruct.RefreshVerdictRefused, tc.in)}, indexDSN, "")
		got := kept[0].Reason
		if got != tc.want {
			t.Errorf("%s: reason = %q, want %q", tc.name, got, tc.want)
		}
		for _, n := range tc.never {
			if strings.Contains(got, n) {
				t.Errorf("%s: reason %q still carries %q", tc.name, got, n)
			}
		}
	}
	// A short password that is also a word: the reason keeps naming its table.
	word, _ := RefusedTablesOf([]reconstruct.RefreshOutcome{
		outcome("shop.orders", reconstruct.RefreshVerdictRefusedDDL, "shop.orders: schema changed, reading idx:orders@tcp(db:3306)/i")},
		"idx:orders@tcp(db:3306)/i")
	if got := word[0].Reason; got != "shop.orders: schema changed, reading <dsn>" {
		t.Errorf("reason = %q", got)
	}
	long, _ := RefusedTablesOf([]reconstruct.RefreshOutcome{
		outcome("shop.orders", reconstruct.RefreshVerdictRefused, strings.Repeat("é ", 4000))})
	if n := utf8.RuneCountInString(long[0].Reason); n != refusedReasonCap || !strings.HasSuffix(long[0].Reason, "...") || !utf8.ValidString(long[0].Reason) {
		t.Errorf("a long reason was kept at %d runes: %q", n, long[0].Reason)
	}
}

// 5,000 refused tables: the first RefusedTablesCap by name, the rest counted,
// and the list's bytes bounded whatever the run refused.
func TestRefusedTablesOf_isCapped(t *testing.T) {
	var in []reconstruct.RefreshOutcome
	for i := range 5000 {
		in = append(in, outcome(fmt.Sprintf("shop.t%04d", i), reconstruct.RefreshVerdictRefusedGap, strings.Repeat("x", 2000)))
	}
	kept, omitted := RefusedTablesOf(in)
	if len(kept) != RefusedTablesCap || omitted != 5000-RefusedTablesCap {
		t.Fatalf("kept %d, omitted %d; want %d and %d", len(kept), omitted, RefusedTablesCap, 5000-RefusedTablesCap)
	}
	if kept[0].Name != "shop.t0000" || kept[RefusedTablesCap-1].Name != fmt.Sprintf("shop.t%04d", RefusedTablesCap-1) {
		t.Errorf("the kept tables are not the first of the run: %q .. %q", kept[0].Name, kept[RefusedTablesCap-1].Name)
	}
	raw, err := json.Marshal(kept)
	if err != nil {
		t.Fatal(err)
	}
	if limit := RefusedTablesCap * 4 * (refusedNameCap + refusedReasonCap + 64); len(raw) > limit {
		t.Errorf("the list takes %d bytes, over the %d its caps allow", len(raw), limit)
	}
}

func writeHistoryFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A run recorded before the list existed has a count and no list. It loads,
// keeps its count, and reads as "not recorded": the page shows the number.
func TestRunHistory_aRunFromBeforeTheListKeepsItsCount(t *testing.T) {
	path := writeHistoryFile(t, `{"version":1,"servers":{"a":[
		{"server_id":"a","kind":"refresh","trigger":"scheduled","started_at":"2026-09-01T10:00:00Z","finished_at":"2026-09-01T10:00:05Z",
		 "tables":12,"refused":3,"error":"shop.orders: schema changed since the baseline"}]}}`)
	h, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	run, _ := h.LastScheduled("a")
	if run == nil || run.Refused != 3 || run.RefusedTables != nil || run.RefusedTablesOmitted != 0 {
		t.Fatalf("old record = %+v, want refused 3 and no list", run)
	}
	dto := scheduleRunFromRecord(run)
	raw, _ := json.Marshal(dto)
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if wire["refused"] != float64(3) {
		t.Errorf("the count did not reach the wire: %s", raw)
	}
	if _, ok := wire["refused_tables"]; ok {
		t.Errorf("an old run serialises a list it never had: %s", raw)
	}
}

// What a newer version may write: a verdict this one does not know, and keys
// it does not know. Both load; the verdict is kept as written. The second half
// is also why a version from before the list reads a file written by this one:
// the loader has never refused an unknown key, and the file version did not
// move (a higher one IS refused).
func TestRunHistory_readsWhatANewerVersionWrote(t *testing.T) {
	path := writeHistoryFile(t, `{"version":1,"later_key":true,"servers":{"a":[
		{"server_id":"a","kind":"refresh","trigger":"scheduled","started_at":"2026-09-01T10:00:00Z","finished_at":"2026-09-01T10:00:05Z",
		 "tables":2,"refused":1,"error":"x","later_field":{"k":1},
		 "refused_tables":[{"name":"shop.orders","verdict":"refused-quota","reason":"over quota","later":"x"}],
		 "refused_tables_omitted":4}]}}`)
	h, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatalf("a file with keys this version does not know did not load: %v", err)
	}
	run, _ := h.LastScheduled("a")
	if run == nil || len(run.RefusedTables) != 1 || run.RefusedTablesOmitted != 4 {
		t.Fatalf("record = %+v", run)
	}
	if got := run.RefusedTables[0]; got != (RefusedTable{"shop.orders", "refused-quota", "over quota"}) {
		t.Errorf("entry = %+v, want the verdict as written", got)
	}
	if baselineHistoryVersion != 1 {
		t.Errorf("the history file version moved to %d: a version from before the list refuses any file above 1, "+
			"and the list only adds keys", baselineHistoryVersion)
	}
}

// The list survives the file, per server, newest run last.
func TestRunHistory_keepsTheListPerServerAndPerRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.json")
	h, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	add := func(server, started, errText string, refused []RefusedTable) {
		t.Helper()
		if err := h.Append(BaselineRunRecord{ServerID: server, Kind: BaselineRunRefresh, Trigger: BaselineRunTriggerScheduled,
			StartedAt: started, FinishedAt: started, Tables: 3, Refused: len(refused), RefusedTables: refused, Error: errText}); err != nil {
			t.Fatal(err)
		}
	}
	add("a", "2026-09-01T10:00:00Z", "refused", []RefusedTable{{"shop.orders", "refused-ddl", "changed"}})
	add("b", "2026-09-01T10:00:01Z", "refused", []RefusedTable{{"crm.leads", "refused-gap", "gap"}})
	add("a", "2026-09-01T11:00:00Z", "refused", []RefusedTable{{"shop.users", "refused-gap", "gap"}})

	again, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	runA, _ := again.LastScheduled("a")
	if runA == nil || len(runA.RefusedTables) != 1 || runA.RefusedTables[0] != (RefusedTable{"shop.users", "refused-gap", "gap"}) {
		t.Errorf("server a's last run = %+v, want its NEWEST list (shop.users)", runA)
	}
	runB, _ := again.LastScheduled("b")
	if runB == nil || len(runB.RefusedTables) != 1 || runB.RefusedTables[0].Name != "crm.leads" {
		t.Errorf("server b's last run = %+v, want only its own table", runB)
	}
	if dto := scheduleRunFromRecord(runA); len(dto.RefusedTables) != 1 || dto.RefusedTables[0].Reason != "gap" {
		t.Errorf("the schedule's last run lost the list or the reason: %+v", dto)
	}
}

// The loop's live view of a run carries the same list as its record.
func TestScheduleRunFromStatus_carriesTheList(t *testing.T) {
	st := BackupScheduleState{LastMethod: BackupMethodRefresh, LastStartedAt: "2026-09-01T10:00:00Z",
		Last: &BaselineStatus{State: "failed", Tables: 4, Refused: 25, LastError: "refused",
			RefusedTables: []RefusedTable{{"shop.orders", "refused-ddl", "changed"}}, RefusedTablesOmitted: 5}}
	dto := scheduleRunFromStatus(st)
	if len(dto.RefusedTables) != 1 || dto.RefusedTables[0] != (RefusedTable{"shop.orders", "refused-ddl", "changed"}) || dto.RefusedTablesOmitted != 5 {
		t.Errorf("dto = %+v", dto)
	}
}

// The wire names the page reads, taken from the bytes and not from the tags.
func TestRefusedTablesWireNamesMatchTheFrontend(t *testing.T) {
	raw, err := json.Marshal(BaselineStatus{State: "failed", Refused: 2, RefusedTablesOmitted: 1,
		RefusedTables: []RefusedTable{{"shop.orders", "refused-ddl", "changed"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"refused_tables":[{"name":"shop.orders","verdict":"refused-ddl","reason":"changed"}]`, `"refused_tables_omitted":1`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("the status does not serialise %s: %s", key, raw)
		}
	}
	body := jsFunctionBody(t, readAsset(t, "app.js"), "refusedTableRows")
	for _, ref := range []string{"refused_tables", "refused_tables_omitted", ".name", ".verdict", ".reason", ".refused"} {
		if !strings.Contains(body, ref) {
			t.Errorf("refusedTableRows does not read %s", ref)
		}
	}
}

// A session with a data profile is refused the snapshot listing outright. The
// statuses served outside it follow the same rule for the names: the count
// stays, the names and reasons go.
func TestRefusedTables_withheldFromASessionWithADataProfile(t *testing.T) {
	st := BaselineStatus{State: "failed", Refused: 2, LastError: "refused",
		RefusedTables: []RefusedTable{{"hr.salaries", "refused-ddl", "changed"}}, RefusedTablesOmitted: 1}
	open := httptest.NewRequest("GET", "/api/servers/a/baseline/restore", nil)
	if got := withholdRefusedTables(open, st); len(got.RefusedTables) != 1 || got.RefusedTablesOmitted != 1 {
		t.Fatalf("a session with no profile lost the list: %+v", got)
	}
	profiled := open.WithContext(context.WithValue(open.Context(), policyCtxKey{},
		&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
	if !sessionRestricted(profiled) {
		t.Fatal("the fixture session is not restricted; this test covers nothing")
	}
	got := withholdRefusedTables(profiled, st)
	if got.RefusedTables != nil || got.RefusedTablesOmitted != 0 {
		t.Errorf("a profiled session was handed the names: %+v", got)
	}
	if got.Refused != 2 {
		t.Errorf("the count went with the names: %+v", got)
	}
	if len(st.RefusedTables) != 1 {
		t.Error("withholding changed the supervisor's own status")
	}

	dto := &backupScheduleDTO{
		LastRun:      &backupScheduleRunDTO{Refused: 1, RefusedTables: []RefusedTable{{"hr.salaries", "refused-ddl", "x"}}},
		LastFallback: &backupScheduleSkipDTO{Refused: 1, RefusedTables: []RefusedTable{{"hr.salaries", "refused-ddl", "x"}}, RefusedTablesOmitted: 2},
	}
	if kept := withholdScheduleTables(open, dto); len(kept.LastRun.RefusedTables) != 1 || len(kept.LastFallback.RefusedTables) != 1 {
		t.Fatalf("a session with no profile lost the schedule's list: %+v", kept)
	}
	cut := withholdScheduleTables(profiled, dto)
	if cut.LastRun.RefusedTables != nil || cut.LastFallback.RefusedTables != nil || cut.LastFallback.RefusedTablesOmitted != 0 {
		t.Errorf("a profiled session was handed the schedule's names: run %+v fallback %+v", cut.LastRun, cut.LastFallback)
	}
	if cut.LastRun.Refused != 1 || cut.LastFallback.Refused != 1 {
		t.Error("the counts went with the names")
	}
	if withholdScheduleTables(profiled, nil) != nil {
		t.Error("no schedule became one")
	}
}
