package status

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// ─── Tables left out of capture (#1802) ──────────────────────────────────────
//
// A degraded schema snapshot leaves a table out (no primary key, not InnoDB)
// and records it in snapshot_exclusions. Before #1802 only the capture log
// said so. These pin how each recorded reason is named and fixed, how the
// list is scoped, and that no state that could not be read is ever rendered
// as "every table is captured".

func TestUncapturedTableKindSummaryAndFix(t *testing.T) {
	const addPK = "ADD COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;"
	cases := []struct {
		name, reason, kind, summary, fix string
	}{
		{"no primary key, as the writer records it", metadata.ExclusionReasonNoPrimaryKey, UncapturedNoPrimaryKey,
			"shop.audit_log is not captured: no primary key.",
			"ALTER TABLE `shop`.`audit_log` " + addPK},
		{"not InnoDB", metadata.ExclusionReasonNotInnoDB, UncapturedNotInnoDB,
			"shop.audit_log is not captured: not an InnoDB table.",
			"ALTER TABLE `shop`.`audit_log` ENGINE=InnoDB;"},
		{"both, as the writer joins them", metadata.ExclusionReasonNotInnoDB + metadata.ExclusionReasonSeparator + metadata.ExclusionReasonNoPrimaryKey,
			UncapturedNotInnoDBNoPrimaryKey,
			"shop.audit_log is not captured: not an InnoDB table, and no primary key.",
			"ALTER TABLE `shop`.`audit_log` ENGINE=InnoDB, " + addPK},
		{"both, in the other order", "no primary key; not InnoDB", UncapturedNotInnoDBNoPrimaryKey,
			"shop.audit_log is not captured: not an InnoDB table, and no primary key.",
			"ALTER TABLE `shop`.`audit_log` ENGINE=InnoDB, " + addPK},
		{"case and spaces around a known reason", "  No Primary KEY ", UncapturedNoPrimaryKey,
			"shop.audit_log is not captured: no primary key.",
			"ALTER TABLE `shop`.`audit_log` " + addPK},
		{"a reason this build does not know", "partitioned table", UncapturedOther,
			`shop.audit_log is not captured: "partitioned table".`, ""},
		{"a known reason joined to an unknown one gets no partial fix", "no primary key; partitioned table", UncapturedOther,
			`shop.audit_log is not captured: "no primary key; partitioned table".`, ""},
		{"no reason recorded at all", "", UncapturedOther,
			"shop.audit_log is not captured, and no reason was recorded.", ""},
		{"a reason made only of spaces", "   ", UncapturedOther,
			"shop.audit_log is not captured, and no reason was recorded.", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// PKColumn is what a snapshot records after seeing the table's
			// columns; the no-column case is its own test below.
			u := UncapturedTable{Schema: "shop", Table: "audit_log", Reason: c.reason, PKColumn: "id"}
			if got := u.Kind(); got != c.kind {
				t.Errorf("Kind() = %q, want %q", got, c.kind)
			}
			if got := u.Summary(); got != c.summary {
				t.Errorf("Summary() = %q, want %q", got, c.summary)
			}
			if got := u.FixSQL(); got != c.fix {
				t.Errorf("FixSQL() = %q, want %q", got, c.fix)
			}
			// A statement never travels without the words that say what it
			// costs. The reverse is allowed and tested separately: with no
			// column name recorded there is no statement, and the words then
			// carry the step that produces one.
			if u.FixSQL() != "" && u.FixIntro() == "" {
				t.Errorf("FixSQL %q was printed with no explanation", u.FixSQL())
			}
		})
	}
}

// TestUncapturedFixIntroNamesTheCost: the intro says what happens to the data
// until the fix runs and what running it costs, BEFORE the statement.
func TestUncapturedFixIntroNamesTheCost(t *testing.T) {
	for _, reason := range []string{"no primary key", "not InnoDB", "not InnoDB; no primary key"} {
		intro := UncapturedTable{Schema: "shop", Table: "t", Reason: reason, PKColumn: "id"}.FixIntro()
		for _, want := range []string{"not kept until", "on your MySQL", "writes to the table wait"} {
			if !strings.Contains(intro, want) {
				t.Errorf("%q: intro %q lacks %q", reason, intro, want)
			}
		}
		if strings.Contains(intro, "—") {
			t.Errorf("%q: intro holds an em dash: %q", reason, intro)
		}
	}
}

func TestQuoteIdentifier(t *testing.T) {
	for in, want := range map[string]string{
		"audit_log":    "`audit_log`",
		"AuditLog":     "`AuditLog`",     // case is kept: on Linux it names a different table
		"audit log":    "`audit log`",    // spaces
		"odd`name":     "`odd``name`",    // an embedded backtick is doubled
		"``":           "``````",         // two backticks, both doubled
		"order":        "`order`",        // a reserved word is only safe quoted
		"shop.archive": "`shop.archive`", // a dot inside one name stays inside it
		"años":         "`años`",         // non-ASCII passes through
		"line\nbreak":  "`line\nbreak`",  // quoted identifiers may hold a newline
		"":             "``",
	} {
		if got := QuoteIdentifier(in); got != want {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestUncapturedFixSQLQuotesEveryName: the ALTER names the real table, quoted,
// whatever its schema and table look like.
func TestUncapturedFixSQLQuotesEveryName(t *testing.T) {
	u := UncapturedTable{Schema: "Shop Data", Table: "odd`Name", Reason: "no primary key", PKColumn: "id"}
	want := "ALTER TABLE `Shop Data`.`odd``Name` ADD COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;"
	if got := u.FixSQL(); got != want {
		t.Errorf("FixSQL() = %q, want %q", got, want)
	}
	// The display name is verbatim, never quoted or case-folded.
	if got := u.Summary(); got != "Shop Data.odd`Name is not captured: no primary key." {
		t.Errorf("Summary() = %q", got)
	}
}

func sampleCapture() *TableCapture {
	return &TableCapture{
		State:         TableCaptureChecked,
		SnapshotID:    12,
		CoverageKnown: true,
		Captured: []TableRef{
			{"crm", "leads"}, {"shop", "customers"}, {"shop", "orders"},
		},
		Uncaptured: []UncapturedTable{
			{Schema: "crm", Table: "log", Reason: "no primary key", PKColumn: "id"},
			{Schema: "shop", Table: "audit_log", Reason: "no primary key", PKColumn: "id"},
			{Schema: "shop", Table: "secret_log", Reason: "not InnoDB"},
		},
	}
}

func TestTableCaptureCounts(t *testing.T) {
	c := sampleCapture()
	if c.TablesCaptured() != 3 || c.TablesTotal() != 6 {
		t.Errorf("captured %d of %d, want 3 of 6", c.TablesCaptured(), c.TablesTotal())
	}
}

// TestTableCaptureInSchemas: tables outside the server's capture filter are
// neither "captured" nor "uncaptured" (#1802). The filter compares the way a
// case-insensitive MySQL stores names, so a filter typed "SHOP" still finds
// the lowercase schema rather than hiding its tables.
func TestTableCaptureInSchemas(t *testing.T) {
	c := sampleCapture()

	if got := c.WithFilter(CaptureFilter{Known: true}); got.TablesTotal() != c.TablesTotal() {
		t.Error("an empty filter must keep every table")
	}
	if got := c.WithFilter(CaptureFilter{Known: true, Schemas: []string{"", "  "}}); got.TablesTotal() != c.TablesTotal() {
		t.Error("a filter of blanks is no filter")
	}

	got := c.WithFilter(CaptureFilter{Known: true, Schemas: []string{" SHOP "}})
	if got.TablesCaptured() != 2 || got.TablesTotal() != 4 {
		t.Errorf("filtered to shop: captured %d of %d, want 2 of 4", got.TablesCaptured(), got.TablesTotal())
	}
	for _, u := range got.Uncaptured {
		if u.Schema != "shop" {
			t.Errorf("a table outside the filter is still listed: %+v", u)
		}
	}
	if got.State != TableCaptureChecked || got.SnapshotID != 12 {
		t.Errorf("filtering must keep state and snapshot: %+v", got)
	}
	// The receiver is not mutated: the caller may hold it for another server.
	if c.TablesTotal() != 6 || len(c.Uncaptured) != 3 {
		t.Errorf("InSchemas mutated its receiver: %+v", c)
	}

	// States with nothing to count pass through untouched.
	for _, st := range []string{TableCaptureNoSnapshot, TableCaptureNotApplicable, TableCaptureUnavailable} {
		in := &TableCapture{State: st}
		if out := in.WithFilter(CaptureFilter{Known: true, Schemas: []string{"shop"}}); out.State != st {
			t.Errorf("%s: filtering changed the state to %s", st, out.State)
		}
	}
}

// onlyNotSecret admits every table except the ones named *secret*.
func onlyNotSecret(_, table string) bool { return !strings.Contains(table, "secret") }

// TestTableCaptureViewScopesNames: a table the reader may not see is COUNTED,
// never named, and its fix goes with it (the fix names the table). Counts stay
// whole, as they do for the capture-skip ledger (#1452): a count is not a name.
func TestTableCaptureViewScopesNames(t *testing.T) {
	v := sampleCapture().View(onlyNotSecret)
	if v.UncapturedWithheld != 1 {
		t.Errorf("uncaptured_withheld = %d, want 1", v.UncapturedWithheld)
	}
	if len(v.Uncaptured) != 2 {
		t.Fatalf("visible uncaptured = %+v, want crm.log and shop.audit_log", v.Uncaptured)
	}
	if v.TablesCaptured == nil || *v.TablesCaptured != 3 || v.TablesTotal == nil || *v.TablesTotal != 6 {
		t.Errorf("counts = %v of %v, want 3 of 6 (scoping never changes a count)", v.TablesCaptured, v.TablesTotal)
	}
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "secret") {
		t.Errorf("a withheld name reached the serialized view: %s", body)
	}
	if !strings.Contains(string(body), "ALTER TABLE `shop`.`audit_log`") {
		t.Errorf("a visible table lost its fix: %s", body)
	}

	// Unrestricted: every name, and no withheld key at all.
	body, _ = json.Marshal(sampleCapture().View(nil))
	if !strings.Contains(string(body), "secret_log") || strings.Contains(string(body), "uncaptured_withheld") {
		t.Errorf("an unrestricted view must name everything and withhold nothing: %s", body)
	}
}

// TestTableCaptureViewStates: only a snapshot that was read AND checked
// carries a total; "not checked" knows the captured count and nothing else;
// the states that read nothing carry no numbers at all. The list is never
// null on the wire.
func TestTableCaptureViewStates(t *testing.T) {
	notChecked := (&TableCapture{State: TableCaptureNotChecked, SnapshotID: 3, Captured: []TableRef{{"shop", "orders"}}}).
		WithFilter(CaptureFilter{Known: true}).View(nil)
	if notChecked.TablesCaptured == nil || *notChecked.TablesCaptured != 1 || notChecked.TablesTotal != nil {
		t.Errorf("not_checked view = %+v, want tables_captured 1 and no total", notChecked)
	}
	unavailable := (&TableCapture{State: TableCaptureUnavailable, Err: sql.ErrConnDone}).View(nil)
	if unavailable.Error == "" || unavailable.TablesCaptured != nil || unavailable.TablesTotal != nil {
		t.Errorf("unavailable view = %+v, want the error and no counts", unavailable)
	}
	for _, st := range []string{TableCaptureNoSnapshot, TableCaptureNotApplicable} {
		v := (&TableCapture{State: st}).View(nil)
		if v.TablesCaptured != nil || v.TablesTotal != nil {
			t.Errorf("%s view carries counts: %+v", st, v)
		}
	}
	for _, v := range []TableCaptureView{notChecked, unavailable, (&TableCapture{State: TableCaptureChecked}).View(nil)} {
		body, _ := json.Marshal(v)
		if !strings.Contains(string(body), `"uncaptured":[]`) {
			t.Errorf("uncaptured must be [] on the wire, got %s", body)
		}
	}
	var nilCapture *TableCapture
	if v := nilCapture.View(nil); v.State != TableCaptureUnavailable {
		t.Errorf("a nil capture must read as unavailable, never checked: %+v", v)
	}
}

// TestLoadTableCaptureUnreadableIsUnavailable: an index that cannot be read
// yields "unavailable" with the error, never a checked empty list.
func TestLoadTableCaptureUnreadableIsUnavailable(t *testing.T) {
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/x?timeout=200ms")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	c := LoadTableCapture(t.Context(), db)
	if c.State != TableCaptureUnavailable || c.Err == nil {
		t.Errorf("LoadTableCapture on a closed db = %+v, want unavailable with its error", c)
	}
}

// TestWriteTableCaptureText: the text report (bintrail status, and the MCP
// status tool, which prints it) names every uncaptured table with its fix,
// honors the reader's scope, and prints nothing when nothing was loaded.
func TestWriteTableCaptureText(t *testing.T) {
	var buf bytes.Buffer
	(&StatusData{TableCapture: sampleCapture()}).Write(&buf)
	out := buf.String()
	for _, want := range []string{
		"=== Tables captured ===",
		"Capturing 3 of 6 tables. Schema snapshot 12.",
		"shop.audit_log is not captured: no primary key.",
		"ALTER TABLE `shop`.`audit_log` ADD COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;",
		"shop.secret_log is not captured: not an InnoDB table.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("text report lacks %q:\n%s", want, out)
		}
	}

	buf.Reset()
	(&StatusData{TableCapture: sampleCapture(), TableVisible: onlyNotSecret}).Write(&buf)
	out = buf.String()
	if strings.Contains(out, "secret") {
		t.Errorf("the text report named a withheld table:\n%s", out)
	}
	if !strings.Contains(out, "1 table outside your access is not captured.") {
		t.Errorf("the text report must count the withheld table:\n%s", out)
	}

	buf.Reset()
	(&StatusData{}).Write(&buf)
	if strings.Contains(buf.String(), "Tables captured") {
		t.Errorf("no capture loaded must print no section:\n%s", buf.String())
	}

	// Only the states that have something to report print at all; the two
	// silent ones are pinned by TestSilentStatesAgreeAcrossSurfaces.
	for st, want := range map[string]string{
		TableCaptureNotChecked:  "is not known",
		TableCaptureUnavailable: "could not be read",
	} {
		buf.Reset()
		(&StatusData{TableCapture: &TableCapture{State: st, Err: sql.ErrConnDone}}).Write(&buf)
		if !strings.Contains(buf.String(), want) {
			t.Errorf("%s: text lacks %q:\n%s", st, want, buf.String())
		}
		if strings.Contains(buf.String(), "Capturing") && st != TableCaptureNotChecked {
			t.Errorf("%s: text claims a capture count it never read:\n%s", st, buf.String())
		}
	}
}

// TestStatusJSONCarriesTableCapture: `bintrail status --format json` carries
// the same view, scoped the same way, under table_capture; absent when nothing
// was loaded (the console's /api/status does not load it).
func TestStatusJSONCarriesTableCapture(t *testing.T) {
	var buf bytes.Buffer
	if err := (&StatusData{TableCapture: sampleCapture(), TableVisible: onlyNotSecret}).WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	var got struct {
		TableCapture *TableCaptureView `json:"table_capture"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.TableCapture == nil || got.TableCapture.State != TableCaptureChecked || got.TableCapture.UncapturedWithheld != 1 {
		t.Fatalf("table_capture = %+v", got.TableCapture)
	}
	if strings.Contains(buf.String(), "secret") {
		t.Errorf("status JSON named a withheld table:\n%s", buf.String())
	}

	buf.Reset()
	if err := (&StatusData{}).WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "table_capture") {
		t.Errorf("nothing loaded must emit no table_capture key:\n%s", buf.String())
	}
}

// TestTableCaptureViewSentences: the words the page shows come from here, so
// the terminal and the page cannot disagree about a count.
func TestTableCaptureViewSentences(t *testing.T) {
	v := sampleCapture().View(onlyNotSecret)
	if v.Headline != "Capturing 3 of 6 tables." {
		t.Errorf("headline = %q", v.Headline)
	}
	if v.WithheldSummary != "1 table outside your access is not captured." {
		t.Errorf("withheld summary = %q", v.WithheldSummary)
	}
	two := sampleCapture()
	two.Uncaptured[0].Table = "secret_a"
	if got := two.View(onlyNotSecret).WithheldSummary; got != "2 tables outside your access are not captured." {
		t.Errorf("withheld summary for two = %q", got)
	}
	one := &TableCapture{State: TableCaptureChecked, CoverageKnown: true, Captured: []TableRef{{"shop", "orders"}}}
	if got := one.View(nil).Headline; got != "Capturing 1 of 1 table." {
		t.Errorf("single-table headline = %q", got)
	}
	none := (&TableCapture{State: TableCaptureNotChecked, Captured: []TableRef{{"a", "b"}, {"a", "c"}}}).
		WithFilter(CaptureFilter{Known: true})
	if got := none.View(nil).Headline; got != "Capturing 2 tables." {
		t.Errorf("not-checked headline = %q", got)
	}
	if got := sampleCapture().View(nil); got.WithheldSummary != "" {
		t.Errorf("nothing withheld must say nothing: %q", got.WithheldSummary)
	}
	for _, st := range []string{TableCaptureNoSnapshot, TableCaptureNotApplicable, TableCaptureUnavailable} {
		if h := (&TableCapture{State: st}).View(nil).Headline; h != "" {
			t.Errorf("%s: headline %q states a count nobody read", st, h)
		}
	}
}

// ─── Review round 2 (#1802) ──────────────────────────────────────────────────

// TestCaptureFilterNarrowsAndClaims: a table the operator's own filter leaves
// out is a DELIBERATE choice, so it is neither captured nor "uncaptured" — it
// leaves both lists, exactly as a schema outside the filter does. Only then
// may a count be claimed.
func TestCaptureFilterNarrowsAndClaims(t *testing.T) {
	c := sampleCapture()
	// The table entries are spelled as capture spells them: an entry that
	// does not match a snapshot table exactly withdraws the count, which
	// TestFilterThatDoesNotDescribeCaptureClaimsNoCount pins.
	// Spelled as capture spells them, padding included: both sides trim, so
	// spaces alone cannot make the two disagree. Case is another matter —
	// TestSchemaFilterIsCheckedLikeTheTableFilter pins that.
	got := c.WithFilter(CaptureFilter{Known: true, Schemas: []string{" shop "}, Tables: []string{"shop.orders", "shop.audit_log"}})
	if got.TablesCaptured() != 1 || got.TablesTotal() != 2 {
		t.Errorf("filtered: captured %d of %d, want 1 of 2 (shop.orders captured, shop.audit_log left out)", got.TablesCaptured(), got.TablesTotal())
	}
	if len(got.Uncaptured) != 1 || got.Uncaptured[0].Table != "audit_log" {
		t.Errorf("uncaptured = %+v, want shop.audit_log only", got.Uncaptured)
	}
	if !got.CoverageKnown {
		t.Error("a known filter must let the count be claimed")
	}
	if c.TablesTotal() != 6 {
		t.Errorf("WithFilter mutated its receiver: %+v", c)
	}
	// A table filter that names nothing means every table of the schemas.
	all := c.WithFilter(CaptureFilter{Known: true})
	if all.TablesCaptured() != 3 || all.TablesTotal() != 6 || !all.CoverageKnown {
		t.Errorf("no filter: captured %d of %d known=%v, want 3 of 6 known", all.TablesCaptured(), all.TablesTotal(), all.CoverageKnown)
	}
}

// TestCaptureFilterUnknownClaimsNothing: where the scope capture runs with
// cannot be known (a reader that did not start it), no count is claimed at
// all — not the total, and not the captured count either, since a filtered-out
// table sits in the snapshot looking captured. The tables left out are still
// named: that they are not captured is true under any filter.
func TestCaptureFilterUnknownClaimsNothing(t *testing.T) {
	v := sampleCapture().WithFilter(CaptureFilter{}).View(nil)
	if v.TablesCaptured != nil || v.TablesTotal != nil || v.Headline != "" {
		t.Errorf("unknown filter still counts: captured=%v total=%v headline=%q", v.TablesCaptured, v.TablesTotal, v.Headline)
	}
	if v.CoverageNote == "" || strings.Contains(v.CoverageNote, "—") {
		t.Errorf("coverage note = %q, want a plain sentence saying why nothing is counted", v.CoverageNote)
	}
	if len(v.Uncaptured) != 3 {
		t.Errorf("uncaptured = %+v, want all three still named", v.Uncaptured)
	}
	// And with a KNOWN filter the note is gone and the count is back.
	known := sampleCapture().WithFilter(CaptureFilter{Known: true}).View(nil)
	if known.CoverageNote != "" || known.Headline != "Capturing 3 of 6 tables." {
		t.Errorf("known filter: note %q headline %q", known.CoverageNote, known.Headline)
	}
}

// TestCaptureFilterKeepsNonCountingStates: a state with nothing to count is
// returned untouched whatever the filter says.
func TestCaptureFilterKeepsNonCountingStates(t *testing.T) {
	// A state with nothing to count keeps its state, and NONE of them counts
	// with the scope unknown — not_checked included: it used to print a raw
	// count there, which meant a legacy index claimed "Capturing 3 tables"
	// while capture watched one of them.
	for _, st := range []string{TableCaptureNoSnapshot, TableCaptureNotApplicable, TableCaptureUnavailable, TableCaptureNotChecked} {
		in := &TableCapture{State: st, Captured: []TableRef{{"shop", "orders"}}}
		out := in.WithFilter(CaptureFilter{})
		if out.State != st {
			t.Errorf("%s: state became %s", st, out.State)
		}
		if out.CoverageKnown || out.View(nil).TablesCaptured != nil {
			t.Errorf("%s: counted with the scope unknown", st)
		}
	}
	var nilCapture *TableCapture
	if nilCapture.WithFilter(CaptureFilter{Known: true}) != nil {
		t.Error("filtering a nil capture must stay nil")
	}
}

// TestUncapturedFixUsesTheRecordedColumn: the statement adds the column the
// snapshot chose when it saw the table's columns, so it cannot collide with a
// column the table already has (MySQL 8.4: ERROR 1060). With no recorded
// column (a row an older build wrote) there is NO statement to guess — the
// card says how to get one instead.
func TestUncapturedFixUsesTheRecordedColumn(t *testing.T) {
	withCol := UncapturedTable{Schema: "shop", Table: "audit_log", Reason: "no primary key", PKColumn: "dbtrail_id"}
	if got, want := withCol.FixSQL(), "ALTER TABLE `shop`.`audit_log` ADD COLUMN `dbtrail_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;"; got != want {
		t.Errorf("FixSQL() = %q, want %q", got, want)
	}
	odd := UncapturedTable{Schema: "shop", Table: "t", Reason: "not InnoDB; no primary key", PKColumn: "odd`name"}
	if got, want := odd.FixSQL(), "ALTER TABLE `shop`.`t` ENGINE=InnoDB, ADD COLUMN `odd``name` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;"; got != want {
		t.Errorf("a chosen column name is quoted too: %q", got)
	}
	// Non-InnoDB with a primary key needs no column, so it keeps its fix.
	engine := UncapturedTable{Schema: "shop", Table: "legacy", Reason: "not InnoDB"}
	if got, want := engine.FixSQL(), "ALTER TABLE `shop`.`legacy` ENGINE=InnoDB;"; got != want {
		t.Errorf("engine-only fix = %q, want %q", got, want)
	}
	// No column recorded, a key needed: no statement, and the intro says what
	// to do rather than leaving the table with no next step.
	for _, reason := range []string{"no primary key", "not InnoDB; no primary key"} {
		u := UncapturedTable{Schema: "shop", Table: "audit_log", Reason: reason}
		if sql := u.FixSQL(); sql != "" {
			t.Errorf("%q: a guessed statement was printed: %q", reason, sql)
		}
		intro := u.FixIntro()
		if !strings.Contains(intro, "read this server's tables again") {
			t.Errorf("%q: intro = %q, want the next step that produces the statement", reason, intro)
		}
		if !strings.Contains(intro, "not kept until") {
			t.Errorf("%q: intro = %q, want what happens to the data", reason, intro)
		}
	}
}

// TestUncapturedListIsCapped: a schema full of key-less tables must not ship
// every one with its own statement. The list is capped, the count of what was
// left out of the LIST is reported, and the totals still cover them all.
func TestUncapturedListIsCapped(t *testing.T) {
	c := uncapturedMany(MaxUncapturedListed + 7)
	v := c.View(nil)
	if len(v.Uncaptured) != MaxUncapturedListed {
		t.Errorf("listed %d, want the cap %d", len(v.Uncaptured), MaxUncapturedListed)
	}
	if v.UncapturedOmitted != 7 {
		t.Errorf("omitted = %d, want 7", v.UncapturedOmitted)
	}
	if v.OmittedSummary == "" || !strings.Contains(v.OmittedSummary, "7") {
		t.Errorf("omitted summary = %q, want it to count them", v.OmittedSummary)
	}
	if *v.TablesTotal != c.TablesTotal() {
		t.Errorf("the cap changed the total: %d", *v.TablesTotal)
	}
	// Exactly at the cap: nothing omitted, nothing said.
	at := uncapturedMany(MaxUncapturedListed).View(nil)
	if at.UncapturedOmitted != 0 || at.OmittedSummary != "" {
		t.Errorf("at the cap: omitted=%d summary=%q", at.UncapturedOmitted, at.OmittedSummary)
	}
	// A withheld table is not "omitted": the two counts mean different things.
	mixed := uncapturedMany(MaxUncapturedListed + 2)
	mixed.Uncaptured[0].Table = "secret_log"
	mv := mixed.View(onlyNotSecret)
	if mv.UncapturedWithheld != 1 || mv.UncapturedOmitted != 1 {
		t.Errorf("withheld=%d omitted=%d, want 1 and 1", mv.UncapturedWithheld, mv.UncapturedOmitted)
	}
}

func uncapturedMany(n int) *TableCapture {
	c := &TableCapture{State: TableCaptureChecked, SnapshotID: 3, CoverageKnown: true,
		Captured: []TableRef{{"shop", "orders"}}}
	for i := range n {
		c.Uncaptured = append(c.Uncaptured, UncapturedTable{
			Schema: "shop", Table: fmt.Sprintf("log_%03d", i), Reason: "no primary key", PKColumn: "id"})
	}
	return c
}

// TestNotCheckedSaysTheHonestCause: absence of the record is not proof of an
// old build — a current one that never migrated this index (the console never
// migrates a registry server's) leaves the same shape. Say what is known and
// what changes it, never a cause nobody checked.
func TestNotCheckedSaysTheHonestCause(t *testing.T) {
	var buf bytes.Buffer
	(&StatusData{TableCapture: (&TableCapture{State: TableCaptureNotChecked, SnapshotID: 2,
		Captured: []TableRef{{"shop", "orders"}}}).WithFilter(CaptureFilter{Known: true})}).Write(&buf)
	out := buf.String()
	if strings.Contains(strings.ToLower(out), "older version") || strings.Contains(strings.ToLower(out), "predates") {
		t.Errorf("the report asserts a cause it did not check:\n%s", out)
	}
	if !strings.Contains(out, "has not recorded") || !strings.Contains(out, "leaves one out") {
		t.Errorf("the report does not say what is known and what changes it:\n%s", out)
	}
}

// TestWriteTableCaptureTextWithoutACount: with the capture scope unknown the
// text report names what is left out and says why nothing is counted; it never
// prints a total, and never the word "Capturing".
func TestWriteTableCaptureTextWithoutACount(t *testing.T) {
	var buf bytes.Buffer
	(&StatusData{TableCapture: sampleCapture().WithFilter(CaptureFilter{})}).Write(&buf)
	out := buf.String()
	if strings.Contains(out, "Capturing") || strings.Contains(out, " of 6 ") {
		t.Errorf("a count was claimed with the scope unknown:\n%s", out)
	}
	for _, want := range []string{"shop.audit_log is not captured: no primary key.", "is set where capture runs"} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q:\n%s", want, out)
		}
	}

	// Everything captured, scope known: say so rather than print a bare count.
	buf.Reset()
	clean := &TableCapture{State: TableCaptureChecked, CoverageKnown: true, SnapshotID: 4, Captured: []TableRef{{"shop", "orders"}}}
	(&StatusData{TableCapture: clean}).Write(&buf)
	if !strings.Contains(buf.String(), "Every table in that scope is captured.") {
		t.Errorf("a clean report does not say so:\n%s", buf.String())
	}

	// The cap is reported in the text too.
	buf.Reset()
	(&StatusData{TableCapture: uncapturedMany(MaxUncapturedListed + 3)}).Write(&buf)
	if !strings.Contains(buf.String(), "3 more tables are not captured, not listed here.") {
		t.Errorf("the text report hides what the cap left out:\n%s", buf.String())
	}
}

// ─── Review round 3 (#1802): one rule, asked once ────────────────────────────

// TestUnknownScopeInventsNoFinding: with the scope unknown and NOTHING left
// out, the report must not say tables are left out. The two claims are
// separate — what the last schema read left out is knowable, how many tables
// capture watches is not — and welding them put a finding on every healthy
// index.
func TestUnknownScopeInventsNoFinding(t *testing.T) {
	clean := &TableCapture{State: TableCaptureChecked, SnapshotID: 7, Captured: []TableRef{{"shop", "orders"}}}
	v := clean.WithFilter(CaptureFilter{}).View(nil)
	if v.TablesCaptured != nil || v.TablesTotal != nil || v.Headline != "" {
		t.Errorf("a count was claimed with the scope unknown: %+v", v)
	}
	if strings.Contains(v.CoverageNote, "These tables are left out") {
		t.Errorf("coverage note invents a finding on a clean index: %q", v.CoverageNote)
	}
	if !strings.Contains(v.CoverageNote, "left no table out") {
		t.Errorf("coverage note = %q, want the knowable half stated plainly", v.CoverageNote)
	}

	var buf bytes.Buffer
	(&StatusData{TableCapture: clean.WithFilter(CaptureFilter{})}).Write(&buf)
	out := buf.String()
	if strings.Contains(out, "These tables are left out") || strings.Contains(out, "Capturing") {
		t.Errorf("the text report claims something it did not find:\n%s", out)
	}
	if !strings.Contains(out, "left no table out") {
		t.Errorf("the text report does not state what it did find:\n%s", out)
	}

	// With something actually left out, the list is introduced as a list.
	with := sampleCapture().WithFilter(CaptureFilter{}).View(nil)
	if !strings.Contains(with.CoverageNote, "These tables are left out") {
		t.Errorf("with exclusions the note must introduce them: %q", with.CoverageNote)
	}
	if !strings.Contains(with.CoverageNote, "is set where capture runs") {
		t.Errorf("the note must still say why nothing is counted: %q", with.CoverageNote)
	}
}

// TestNotCheckedAsksTheSameScopeRule: the not-checked state counts only what
// capture watches, like every other state. A legacy index read by a process
// that did not start its capture claims nothing; one whose capture this
// process runs with a table filter counts the filtered tables, not every
// table in the snapshot.
func TestNotCheckedAsksTheSameScopeRule(t *testing.T) {
	legacy := &TableCapture{State: TableCaptureNotChecked, SnapshotID: 4, Captured: []TableRef{
		{"shop", "orders"}, {"shop", "customers"}, {"crm", "leads"},
	}}

	unknown := legacy.WithFilter(CaptureFilter{}).View(nil)
	if unknown.TablesCaptured != nil || unknown.Headline != "" {
		t.Errorf("a legacy index counted with the scope unknown: %+v", unknown)
	}
	if unknown.CoverageNote == "" {
		t.Errorf("the missing count must say why: %+v", unknown)
	}

	narrow := legacy.WithFilter(CaptureFilter{Known: true, Schemas: []string{"shop"}, Tables: []string{"shop.orders"}}).View(nil)
	if narrow.TablesCaptured == nil || *narrow.TablesCaptured != 1 {
		t.Errorf("a legacy index counted tables nobody watches: %v", narrow.TablesCaptured)
	}
	if narrow.Headline != "Capturing 1 table." {
		t.Errorf("headline = %q", narrow.Headline)
	}

	whole := legacy.WithFilter(CaptureFilter{Known: true}).View(nil)
	if whole.TablesCaptured == nil || *whole.TablesCaptured != 3 {
		t.Errorf("an unfiltered scope must count every table it watches: %v", whole.TablesCaptured)
	}
	// And never a total: without the exclusions, "of" would read as "nothing
	// else was left out".
	if whole.TablesTotal != nil || narrow.TablesTotal != nil {
		t.Error("not_checked must never print a total")
	}
}

// TestFilterThatDoesNotDescribeCaptureClaimsNoCount: the list folds case
// because a case-insensitive server stores names lowercased, but capture
// matches its --tables entries BYTE FOR BYTE. A filter entry that matches no
// snapshot table exactly does not describe what capture watches — it is
// either mis-cased (capture then drops every event, with no counter and no
// log line) or names a table that is not there — so no count is claimed.
func TestFilterThatDoesNotDescribeCaptureClaimsNoCount(t *testing.T) {
	c := sampleCapture() // crm.leads, shop.customers, shop.orders captured
	cases := []struct {
		name    string
		tables  []string
		claimed bool
	}{
		{"exact", []string{"shop.orders"}, true},
		{"mis-cased table", []string{"Shop.Orders"}, false},
		{"a table that is not there", []string{"shop.orders", "shop.ghost"}, false},
		{"matching nothing at all", []string{"nope.nope"}, false},
		{"an excluded table counts as described", []string{"shop.audit_log"}, true},
		{"no table filter", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := c.WithFilter(CaptureFilter{Known: true, Tables: tc.tables}).View(nil)
			if claimed := v.Headline != ""; claimed != tc.claimed {
				t.Errorf("headline = %q, want a count: %v", v.Headline, tc.claimed)
			}
			if !tc.claimed && v.CoverageNote == "" {
				t.Error("a withdrawn count must say why")
			}
		})
	}
	// The LIST still folds case: a lowercase server plus an operator's
	// uppercase filter must not hide the table that is not captured.
	folded := c.WithFilter(CaptureFilter{Known: true, Tables: []string{"SHOP.AUDIT_LOG"}})
	if len(folded.Uncaptured) != 1 || folded.Uncaptured[0].Table != "audit_log" {
		t.Errorf("the list stopped folding case: %+v", folded.Uncaptured)
	}
	if folded.CoverageKnown {
		t.Error("a mis-cased filter entry must still withdraw the count")
	}
}

// TestSilentStatesAgreeAcrossSurfaces: the states with nothing to report
// print nothing at all, so the terminal and the card say the same thing —
// before, one printed a section and the other drew nothing.
func TestSilentStatesAgreeAcrossSurfaces(t *testing.T) {
	for _, st := range []string{TableCaptureNoSnapshot, TableCaptureNotApplicable, "something_new"} {
		var buf bytes.Buffer
		(&StatusData{TableCapture: &TableCapture{State: st}}).Write(&buf)
		if strings.Contains(buf.String(), "Tables captured") {
			t.Errorf("%s: the text report prints a section the card does not draw:\n%s", st, buf.String())
		}
	}
	// The three that DO report still do.
	for _, c := range []*TableCapture{
		sampleCapture().WithFilter(CaptureFilter{Known: true}),
		{State: TableCaptureNotChecked, Captured: []TableRef{{"a", "b"}}},
		{State: TableCaptureUnavailable, Err: sql.ErrConnDone},
	} {
		var buf bytes.Buffer
		(&StatusData{TableCapture: c}).Write(&buf)
		if !strings.Contains(buf.String(), "Tables captured") {
			t.Errorf("%s: nothing reported:\n%s", c.State, buf.String())
		}
	}
}

// ─── Review round 4 (#1802): the schema half of the same rule ────────────────

// TestSchemaFilterIsCheckedLikeTheTableFilter: capture matches BOTH halves of
// its filter byte for byte (event.Filters.Matches), so a mis-cased --schemas
// entry is the same hazard as a mis-cased --tables one. It is reachable on a
// server with lower_case_table_names=1, where MySQL stores `CREATE DATABASE
// bt_Mixed` as `bt_mixed` while information_schema still answers a query for
// 'BT_MIXED': the snapshot succeeds and records the lowercase name, the
// binlog carries the lowercase name, and the operator's filter matches
// neither — every event dropped, with nothing counting it.
func TestSchemaFilterIsCheckedLikeTheTableFilter(t *testing.T) {
	c := sampleCapture() // crm + shop
	cases := []struct {
		name    string
		schemas []string
		tables  []string
		claimed bool
	}{
		{"exact", []string{"shop"}, nil, true},
		{"both schemas, exact", []string{"shop", "crm"}, nil, true},
		{"mis-cased schema", []string{"SHOP"}, nil, false},
		{"one exact, one mis-cased", []string{"shop", "CRM"}, nil, false},
		{"a schema the snapshot does not have", []string{"shop", "ghost"}, nil, false},
		{"no schema filter", nil, nil, true},
		{"mis-cased schema with an exact table", []string{"SHOP"}, []string{"shop.orders"}, false},
		// Both sides trim their entries (cliutil.BuildIndexFilters on
		// capture's, nonBlank on this one), so padding cannot make them
		// disagree and must not withdraw the count.
		{"padding only", []string{"  shop  "}, []string{" shop.orders "}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := c.WithFilter(CaptureFilter{Known: true, Schemas: tc.schemas, Tables: tc.tables})
			if got.CoverageKnown != tc.claimed {
				t.Errorf("CoverageKnown = %v, want %v", got.CoverageKnown, tc.claimed)
			}
			if v := got.View(nil); (v.Headline != "") != tc.claimed {
				t.Errorf("headline = %q, want a count: %v", v.Headline, tc.claimed)
			}
		})
	}
	// The LIST keeps folding: a mis-cased filter must not hide a table that
	// is not captured, which is this whole card's failure mode.
	folded := c.WithFilter(CaptureFilter{Known: true, Schemas: []string{"SHOP"}})
	if len(folded.Uncaptured) != 2 {
		t.Errorf("the list stopped folding schema case: %+v", folded.Uncaptured)
	}
}

// TestIdentFoldMatchesMySQLIdentifiers: the list folds with the rule MySQL
// itself uses for identifiers, not Unicode's. Under plain EqualFold a filter
// typed `shop.INVOICES` against a real `shop.ınvoices` matched nothing, so a
// genuinely uncaptured table left the card altogether — this card's own
// failure mode, in miniature.
func TestIdentFoldMatchesMySQLIdentifiers(t *testing.T) {
	c := &TableCapture{State: TableCaptureChecked, Captured: []TableRef{{"shop", "orders"}},
		Uncaptured: []UncapturedTable{{Schema: "shop", Table: "ınvoices", Reason: "no primary key", PKColumn: "id"}}}
	got := c.WithFilter(CaptureFilter{Known: true, Tables: []string{"shop.INVOICES", "shop.orders"}})
	if len(got.Uncaptured) != 1 {
		t.Errorf("a table MySQL would call the same identifier left the list: %+v", got.Uncaptured)
	}
	// It is still not an EXACT match, so the count stays withdrawn.
	if got.CoverageKnown {
		t.Error("a case-style filter entry must not claim a count: capture matches byte for byte")
	}
}

// TestAllCapturedSaysSoOnEverySurface: the terminal and the card say the same
// thing when nothing is left out. The card used to stop at the count.
func TestAllCapturedSaysSoOnEverySurface(t *testing.T) {
	clean := (&TableCapture{State: TableCaptureChecked, SnapshotID: 3,
		Captured: []TableRef{{"shop", "orders"}}}).WithFilter(CaptureFilter{Known: true})
	v := clean.View(nil)
	if v.AllCapturedNote != "Every table in that scope is captured." {
		t.Errorf("all-captured note = %q", v.AllCapturedNote)
	}
	var buf bytes.Buffer
	(&StatusData{TableCapture: clean}).Write(&buf)
	if !strings.Contains(buf.String(), v.AllCapturedNote) {
		t.Errorf("the terminal and the view disagree:\n%s", buf.String())
	}
	// Not claimed where something IS left out, where nothing was counted, or
	// where the list was merely capped or scoped.
	for name, c := range map[string]*TableCapture{
		"with exclusions": sampleCapture().WithFilter(CaptureFilter{Known: true}),
		"uncounted":       clean.WithFilter(CaptureFilter{}),
		"capped":          uncapturedMany(MaxUncapturedListed + 1),
	} {
		if note := c.View(nil).AllCapturedNote; note != "" {
			t.Errorf("%s: claims everything is captured: %q", name, note)
		}
	}
	if note := sampleCapture().WithFilter(CaptureFilter{Known: true}).View(onlyNotSecret).AllCapturedNote; note != "" {
		t.Errorf("a withheld table must not read as all-captured: %q", note)
	}
}

// TestTextReportScopesTheCaptureSkipLedgerToo: the MCP status tool now scopes
// the uncaptured list to its surface's deny rules, and the capture-health
// prose in the SAME output must not name a table that list just withheld.
func TestTextReportScopesTheCaptureSkipLedgerToo(t *testing.T) {
	ledger := `{"table_excluded_from_snapshot":{"count":4,"last_at":"2026-08-04T19:50:00Z",` +
		`"tables":["shop.orders","shop.secret_log"],"last_detail":"no primary key"}}`
	stream := &StreamStateInfo{Mode: "gtid", CaptureSkips: sql.NullString{String: ledger, Valid: true}}

	var scoped bytes.Buffer
	(&StatusData{Stream: stream, TableVisible: onlyNotSecret}).Write(&scoped)
	if strings.Contains(scoped.String(), "secret_log") {
		t.Errorf("the capture-health prose names a table the same output withholds:\n%s", scoped.String())
	}
	for _, want := range []string{"shop.orders", "1 table outside your access"} {
		if !strings.Contains(scoped.String(), want) {
			t.Errorf("scoped report lacks %q:\n%s", want, scoped.String())
		}
	}
	// With no scope (the command line has none) the ledger renders verbatim.
	var whole bytes.Buffer
	(&StatusData{Stream: stream}).Write(&whole)
	if !strings.Contains(whole.String(), "secret_log") || strings.Contains(whole.String(), "outside your access") {
		t.Errorf("an unscoped report must render the ledger verbatim:\n%s", whole.String())
	}
	// Scoping the rendering must not mutate the caller's stream state.
	if !strings.Contains(stream.CaptureSkips.String, "secret_log") {
		t.Error("Write mutated the ledger it was given")
	}
}

// TestNotCheckedNeverSaysAllCaptured: an empty list is not an empty answer
// there — whether anything was left out is exactly what that state does not
// know, so the closing line the checked state earns must not appear.
func TestNotCheckedNeverSaysAllCaptured(t *testing.T) {
	c := (&TableCapture{State: TableCaptureNotChecked, SnapshotID: 8, Captured: []TableRef{{"shop", "orders"}}}).
		WithFilter(CaptureFilter{Known: true})
	if note := c.View(nil).AllCapturedNote; note != "" {
		t.Errorf("not_checked claims everything is captured: %q", note)
	}
	var buf bytes.Buffer
	(&StatusData{TableCapture: c}).Write(&buf)
	if strings.Contains(buf.String(), "Every table in that scope is captured.") {
		t.Errorf("the terminal claims it too:\n%s", buf.String())
	}
}
