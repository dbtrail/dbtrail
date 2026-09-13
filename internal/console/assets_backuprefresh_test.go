package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestBackupRefreshCard_neverClaimsLiveWhileDormant pins the one thing this
// card must not do: state two opposite things about the running system.
//
// The card says "Nothing uses this yet" whenever br.enabled is false. If the
// provenance line ALSO claims the setting is live purely because an override
// exists, the same card simultaneously tells the operator the setting is
// running and that nothing runs. That is not a wording preference: the setting
// changes the on-disk representation of their backups, and the panel is where
// consent for that is taken.
//
// The old shape put liveness in a kv row, as "this page (live)", and this
// guard pinned that ternary. The rewrite deleted the rows, so this guard now
// covers ONE half of the property: the provenance line answers who chose the
// value and says nothing about whether it runs.
//
// It is deliberately NOT the whole property. Everything the card renders
// before the provenance line is outside the window, so a state pill reading
// "On, live and running now" passes here. The whole-card version is the e2e
// scenario "backups: the disk-space card reports every state it can be in",
// which renders the real function across all 16 DTOs and matches a liveness
// WORD against the rendered text. The difference matters: the check below is
// the literal "(live", which the old kv row used, and a pill reading "On,
// running now" carries no parentheses and passes it. This guard is the cheap
// unit-level half; do not delete the e2e believing this one covers it.
func TestBackupRefreshCard_neverClaimsLiveWhileDormant(t *testing.T) {
	// jsFunctionBody, not functionBody: it strips comment lines before walking.
	// These guards search for the rendered LABELS, and this function documents
	// its own gate in a comment that quotes them, so an unstripped body matches
	// the prose instead of the code and reports a pass on a deleted gate.
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupRefreshCard")

	// The provenance line is the one that used to carry liveness. Anchor on the
	// say() call rather than on `br.source === "override"` alone, which also
	// gates the Use-the-default button and would let a liveness word sneak back
	// into the line this guard is about.
	const prov = `say(br.source === "override"`
	i := strings.Index(body, prov)
	if i < 0 {
		t.Fatal("backupRefreshCard renders no provenance line; this guard covers nothing")
	}
	// End at the statement, not at a raw character budget: 300 chars ran past
	// the say() call into the primary button's label and cut off mid-string, so
	// a future button reading "Live now" would fail a guard whose message
	// blames the provenance line. ("now" is a bare substring, so "know" trips
	// it too.)
	rest := body[i:]
	arm := rest
	if e := strings.Index(rest, ");"); e >= 0 {
		arm = rest[:e]
	}
	for _, w := range []string{"live", "running", "yet", "now"} {
		if strings.Contains(strings.ToLower(arm), w) {
			t.Errorf("the provenance line says %q. Whether the setting is running belongs to the "+
				"br.enabled / br.scheduled line alone; said in both places the card can call a dormant "+
				"setting live and tell the operator nothing runs, in the same card:\n%s", w, arm)
		}
	}
	// The old mechanism, pinned so it cannot come back through a row. Over the
	// SPAN, not the body: jsFunctionBody truncates each line at its first "//",
	// so a must-not-contain over it fails OPEN. A string carrying a URL removes
	// everything after the scheme's slashes from the body this guard would see
	// while the browser still renders it, which is how `say("… (live)")` passed
	// this check when it was written against the body.
	if strings.Contains(jsFunctionSpan(t, readAsset(t, "app.js"), "backupRefreshCard"), "(live") {
		t.Error("a `(live` label is back. Liveness in a label next to the value reintroduces the " +
			"contradiction this guard exists for; keep it in the sentence that owns it")
	}
	if !strings.Contains(body, "!br.enabled") {
		t.Fatal("the dormancy note is gone; a setting nothing consumes would look active")
	}
	// Three states, and the middle one is the whole point: --baseline-trigger
	// with no schedule runs no loop but DOES apply this to restores. Collapsing
	// it back into the dormant branch is the misreport this guard exists for.
	if !strings.Contains(body, "br.scheduled") {
		t.Fatal("the card no longer distinguishes 'no schedule' from 'nothing uses this', so a daemon whose " +
			"restores reuse files today would be told nothing runs yet")
	}
}

// TestSaveBackupRefresh_confirmsFromTheResponse: the toast must describe what
// the daemon reported back, not what was clicked.
//
// Two things make the clicked value wrong. "Use the daemon setting" does not
// know in advance what the daemon flag says, so it has no value to report. And
// a card rendered before a restart carries a stale schedule, so a confirmation
// built from it can claim a setting applies now when it does not. Reading the
// echoed DTO costs nothing and cannot be stale.
func TestSaveBackupRefresh_confirmsFromTheResponse(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "saveBackupRefresh")

	if !strings.Contains(body, "await api(") {
		t.Fatal("saveBackupRefresh no longer PUTs; this guard covers nothing")
	}
	// The response has to be captured, not discarded.
	if !strings.Contains(body, "= await api(") {
		t.Fatal("the PUT response is discarded, so the confirmation cannot describe what the daemon actually stored")
	}
	ti := strings.Index(body, "toast(")
	if ti < 0 {
		t.Fatal("saveBackupRefresh no longer confirms anything; this guard covers nothing " +
			"(and slicing on a missing needle would panic the whole package)")
	}
	_ = body[ti:] // the needle exists; the checks below read the whole body
	// Qualified by the RESPONSE variable, not the bare field name. A bare
	// "carry_forward_unchanged" also matches the request body that was just
	// sent, which is precisely the stale value this guard exists to reject, and
	// a mutation swapping now.* for body.* survived until this was tightened.
	for _, want := range []string{"now.enabled", "now.carry_forward_unchanged"} {
		if !strings.Contains(body, want) {
			t.Errorf("the confirmation does not read %s from the PUT response", want)
		}
	}
	// Ban the IDENTIFIERS, not the English word: "applies on the next start"
	// is correct prose and used to fail this.
	for _, banned := range []string{"next ?", "next)", "body.carry_forward_unchanged", "body.enabled"} {
		if strings.Contains(body, banned) {
			t.Errorf("the confirmation reads %q, the value that was SENT, rather than the daemon's answer", banned)
		}
	}
	if !strings.Contains(body, "renderRoute()") {
		t.Fatal("the card is not re-rendered after saving, so it would keep showing the previous state")
	}
}

// TestBaselineRefreshNote_partitionsTheTables: reused and refreshed must ADD UP
// to the run's table count, never overlap.
//
// The first version of this line printed the total beside the subset, so a run
// that reused everything rendered "5 table(s) refreshed, 5 of them reused
// unchanged" and asked the operator to do the subtraction. It also inverted the
// rule the CLI summary follows and the docs state: a reused table is not a
// refreshed one, and which tables actually cost a full rewrite is the number
// worth seeing.
func TestBaselineRefreshNote_partitionsTheTables(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "baselineRefreshNote")

	if !strings.Contains(body, "carried") {
		t.Fatal("the refresh note no longer reports reused tables; this guard covers nothing")
	}
	// The refreshed count has to be a DIFFERENCE. A bare rf.tables next to
	// rf.carried is the double count.
	if !strings.Contains(body, "- reused") && !strings.Contains(body, "-reused") {
		t.Error("the refreshed count is not the total minus the reused count, so the two overlap and " +
			"a fully reused run reads as fully refreshed")
	}
	if strings.Contains(body, `(rf.tables || 0) + " table(s) refreshed"`) {
		t.Error("the note prints the TOTAL table count as the refreshed count while also printing the " +
			"reused subset; those two numbers must partition the run")
	}
}

// TestBackupRefreshWireNamesMatchTheFrontend: the Go struct tag and the
// JavaScript that reads it are written independently and nothing else compares
// them.
//
// This is the `schema_name`/`table_name` class: renaming the tag leaves both
// sides internally consistent, compiles, and passes every test, while the
// number silently stops appearing. The tag is derived by MARSHALLING rather
// than by reading the source, so the assertion is about the bytes on the wire.
func TestBackupRefreshWireNamesMatchTheFrontend(t *testing.T) {
	js := readAsset(t, "app.js")

	raw, err := json.Marshal(BaselineStatus{State: "succeeded", Tables: 3, Carried: 2, CarriedCopied: 1})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if _, ok := wire["carried"]; !ok {
		t.Fatalf("BaselineStatus no longer serialises a \"carried\" key (got %s); the console reads rf.carried "+
			"and would silently stop showing reused tables", raw)
	}
	// carried_copied is the honesty half of the pair (#1578): without it every
	// reuse renders as a disk saving, including the copies that saved none.
	if _, ok := wire["carried_copied"]; !ok {
		t.Fatalf("BaselineStatus no longer serialises \"carried_copied\" (got %s); copied reuses would "+
			"silently render as disk savings again", raw)
	}
	for _, ref := range []string{"rf.carried", "rst.carried", "rf.carried_copied", "rst.carried_copied", "run.carried_copied"} {
		if !strings.Contains(js, ref) {
			t.Errorf("app.js does not read %s, so the reused count never reaches the page", ref)
		}
	}

	// Targets non-nil and SkippedS3Only non-zero, or their omitempty hides
	// the very keys this pin exists for.
	zero := 0
	dto, err := json.Marshal(baselineRefreshDTO{CarryForwardUnchanged: true, Source: "override",
		Enabled: true, Scheduled: true, Targets: &zero, SkippedS3Only: 1})
	if err != nil {
		t.Fatal(err)
	}
	var d map[string]any
	if err := json.Unmarshal(dto, &d); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"carry_forward_unchanged", "source", "enabled", "scheduled", "targets", "skipped_s3_only"} {
		if _, ok := d[key]; !ok {
			t.Errorf("baselineRefreshDTO does not serialise %q (got %s)", key, dto)
		}
		if !strings.Contains(js, "br."+key) && !strings.Contains(js, key) {
			t.Errorf("app.js never reads %q from the refresh DTO", key)
		}
	}
	// The zero-targets alarm keys on the exact shape #1579 names: scheduled
	// AND a REAL zero. `br.targets === 0` (strict) so an omitted field
	// (serve, no loop) can never fire it.
	if !strings.Contains(js, "br.targets === 0") {
		t.Error("the zero-targets alarm is gone or no longer strict; a running timer over zero " +
			"refreshable servers would fall back to silence (#1579)")
	}
}

// TestSettingsPageMountsTheRefreshCard: the card can be unmounted, or its
// fetch removed, with the whole suite green.
//
// The two guards above check what the card renders once it is called. Neither
// notices if nothing calls it: dropping the append makes the setting vanish,
// and dropping the fetch makes it render its error branch forever. A setting
// an operator cannot reach is the same as a setting that does not exist.
//
// The card moved from Storage to Backups (#1543) and from there to the
// Backup settings page (#1582), which owns settings the way the
// Backups page owns the work. The guard follows the card rather than the
// page, and it checks BOTH halves of a move so it cannot be half-done: the
// new page mounts and feeds it, and the old page no longer does — a control
// that renders twice saves to one store from two places, and one of them is
// always stale.
func TestSettingsPageMountsTheRefreshCard(t *testing.T) {
	js := readAsset(t, "app.js")
	body := jsFunctionBody(t, js, "renderBackupSettings") + jsFunctionBody(t, js, "buildBackupSettings")
	if !strings.Contains(body, "backupRefreshCard(") {
		t.Error("the Backup settings page no longer mounts backupRefreshCard, so the reuse setting has no UI at all")
	}
	if !strings.Contains(body, `api("/api/baseline-refresh")`) {
		t.Error("the settings page does not fetch /api/baseline-refresh, so the card can only ever render its error branch")
	}
	if strings.Contains(jsFunctionBody(t, js, "renderBaselines"), "backupRefreshCard(") {
		t.Error("the Backups page still mounts backupRefreshCard; the card moved to the settings page, one surface at a time")
	}
}

// The split itself (#1543): Storage held seven cards from five concerns, and
// the two halves answer different questions. A card that drifts back, or a
// half that quietly absorbs the other, is the failure this guards.
func TestStorageSplit_eachHalfHoldsOnlyItsOwnConcern(t *testing.T) {
	js := readAsset(t, "app.js")
	if strings.Contains(js, "function buildStorage(") {
		t.Fatal("buildStorage is back; Storage was split into Retention and This daemon (#1543)")
	}
	for _, tc := range []struct {
		fn    string
		want  []string
		never []string
	}{
		// What happens to your data over time. Nothing about this process.
		{"buildRetention", []string{"rotationCard(", "archivingPanel("},
			[]string{"credentialsCard(", "stagingCard(", "telemetryCard(", "backupRefreshCard(", "duckdbPanel("}},
		// What this process is reaching, holding and sending. Nothing about
		// the data lifecycle.
		{"buildDaemon", []string{"credentialsCard(", "stagingCard(", "telemetryCard("},
			[]string{"rotationCard(", "archivingPanel(", "backupRefreshCard(", "duckdbPanel("}},
	} {
		body := jsFunctionBody(t, js, tc.fn)
		for _, w := range tc.want {
			if !strings.Contains(body, w) {
				t.Errorf("%s does not mount %s, so that card is unreachable", tc.fn, w)
			}
		}
		for _, n := range tc.never {
			if strings.Contains(body, n) {
				t.Errorf("%s mounts %s, which belongs to the other half; the page is a drawer again", tc.fn, n)
			}
		}
	}
	// The old route must keep resolving, or every existing link and bookmark
	// lands on Overview with no explanation.
	if !strings.Contains(js, `route === "storage"`) {
		t.Error("nothing handles the old /storage route, so existing links break")
	}
	// And the DuckDB schema download has to be mounted SOMEWHERE, not merely
	// absent from the two halves above. Its home is Backups since #1581; this
	// only pins that buildConnect still CALLS duckdbPanel at all — the gate on
	// that call (`views && !monitor`, the serve-only fallback) is pinned
	// exact-string by TestDuckDBCardMountsOnBackupsWithConnectFallback, not
	// here. The #1549 lesson behind keeping any Connect mount: /sql gated the
	// card on a capability and a permission that are not the download's own,
	// so BINTRAIL_CONSOLE_SQL_PANEL=0 left `views` on with no route to it.
	//
	// Named explicitly rather than searched file-wide, because the failure
	// this guards is the card existing while nothing calls it.
	if !strings.Contains(jsFunctionBody(t, js, "buildConnect"), "duckdbPanel(") {
		t.Error("buildConnect no longer mounts duckdbPanel, so a serve-only console (views on, " +
			"monitor off — no /baselines page) has no route to the schema download")
	}
	// And it must not go back to the SQL page, which #1549 removed entirely.
	// Asserted as the page's ABSENCE rather than as "renderSQL does not mount
	// the card": jsFunctionBody fatals on a function it cannot find, so the
	// body form would fail for the wrong reason today and would only start
	// checking anything again if the page came back.
	if strings.Contains(js, "function renderSQL(") {
		t.Error("renderSQL is back in app.js; the SQL page was removed in #1549, and it is what gated this card behind the sql capability")
	}
}

// TestBackupRefreshCard_titleSaysWhatItDoes (#1528). This card controls one
// thing: whether an unchanged table reuses its previous file instead of being
// written again. It is a storage behaviour with no timetable in it, and it
// used to be titled "Automatic backup refresh" one route away from "Scheduled
// backups", which IS the timetable. Two names both promising "backups,
// automatically", for two unrelated settings.
//
// The third assertion is what makes this a guard rather than a spelling check:
// it fails if the collision is "fixed" by renaming the schedule instead.
func TestBackupRefreshCard_titleSaysWhatItDoes(t *testing.T) {
	js := readAsset(t, "app.js")
	body := jsFunctionBody(t, js, "backupRefreshCard")

	// Tolerates both shapes: the title as a `text:` on the card-title div, and
	// the title as a nested span once the div grew a second class to carry the
	// state pill beside it.
	m := regexp.MustCompile(`card-title[^"]*"[\s\S]{0,80}?text: "([^"]*)"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatal("backupRefreshCard renders no card title; this guard covers nothing")
	}
	title := m[1]
	for _, banned := range []string{"Automatic", "automatic", "Schedul", "schedul", "Refresh", "refresh"} {
		if strings.Contains(title, banned) {
			t.Errorf("the card title %q contains %q: this setting has no timetable in it, and the word "+
				"puts it back beside Scheduled backups, which is the timetable", title, banned)
		}
	}
	// It sits on the Backups page beside the schedule (#1543), so the title
	// says which of the two it is and what it costs or saves. Disk is the
	// whole trade: reusing a file means two snapshots share one, so a prune
	// reports space it will not reclaim while the newer one references it.
	low := strings.ToLower(title)
	if !strings.HasPrefix(low, "backups") {
		t.Errorf("the card title %q does not start with Backups, so on the Backups page it does not say "+
			"which control it is", title)
	}
	if !strings.Contains(low, "disk") {
		t.Errorf("the card title %q does not name what the control trades (disk space)", title)
	}
	if !strings.Contains(js, `"Scheduled backups: none"`) {
		t.Fatal("the schedule summary is no longer called Scheduled backups; the collision was resolved from " +
			"the wrong side, and this guard would have passed on a renamed timetable")
	}
}

// TestReusedCopiedNote_saysWhatACopyCost pins the only user-visible half of
// #1578: every layer under the render (carryForward's bool, the fold wiring,
// countReuse, applyFoldStatus, the wire names) is guarded, but the string the
// reporter actually reads was not — emptying reusedCopiedNote restored the
// exact bug (a copied reuse rendering as a disk saving) with the whole Go
// suite green, because the wire-name guard only checks the fields are READ.
func TestReusedCopiedNote_saysWhatACopyCost(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "reusedCopiedNote")
	// The zero arm: no copies, no qualifier — the unqualified "reused" note
	// is then the correct claim.
	if !strings.Contains(body, `if (!copied) return "";`) {
		t.Error("reusedCopiedNote no longer returns empty for zero copies; every reuse would carry a false qualifier")
	}
	// The non-empty arm carries the counter and the two load-bearing claims:
	// no disk was saved, and the cause is in the daemon log.
	for _, want := range []string{`+ copied +`, "which saved no disk", "the daemon log says why"} {
		if !strings.Contains(body, want) {
			t.Errorf("reusedCopiedNote lost %q; a copied reuse would render as a disk saving again (#1578)", want)
		}
	}
}

// TestBackupRefreshCard_prose (#1528): a line of help under a control is fine,
// a paragraph means the control explains itself instead of being clear. The
// on-state used to carry the shared-bytes consequence of a hard link in the
// card; that is a thing a reader wants while reading docs, not while flipping
// the switch, so it lives in docs/console.md now.
func TestBackupRefreshCard_prose(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupRefreshCard")
	if strings.Contains(body, "share the same bytes on disk") {
		t.Error("the hard-link consequence is back in the card; it belongs in docs/console.md")
	}
	if strings.Contains(body, "—") {
		t.Error("backupRefreshCard copy contains an em dash")
	}
	// docsNoWrap: the first version of this read the file raw and went red
	// because the sentence wrapped between two lines.
	if !strings.Contains(docsNoWrap(t), "share the same bytes on disk") {
		t.Error("docs/console.md does not carry the shared-bytes consequence, so removing it from the card lost it")
	}
}

// TestBackupScheduleCard_introIsNotAnEssay (#1528): the fold's opening
// paragraph explained the producer choice in general terms directly above the
// line that names the producer for the NEXT run specifically. The general half
// is docs material; the specific half is the state the operator acts on.
func TestBackupScheduleCard_introIsNotAnEssay(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupScheduleCard")
	if strings.Contains(body, "with no load on your database") {
		t.Error("the fold explains the producer choice in general above the line that names it for the next run")
	}
	// The specific half must survive the cut.
	if !strings.Contains(body, "will update the latest backup from the recorded changes") {
		t.Fatal("the per-run producer line is gone, so nothing says how the next run will be made")
	}
}

// docsNoWrap returns docs/console.md with every whitespace run collapsed to one
// space, so a needle cannot fail merely because the sentence wrapped between
// two lines. The first version of the guard below broke exactly that way.
func docsNoWrap(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../docs/console.md")
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(strings.Fields(string(b)), " ")
}

// TestBackupScheduleCard_saysWhatARunCosts (#1528, review pass 1). Cutting the
// intro paragraph went one clause too far.
//
// With NO schedule saved yet, every per-run line lives inside `if (sch)` and
// renders nothing, so the only thing describing a run was the rate line: "each
// a full copy of every table". True of the OUTPUT and misleading about the
// INPUT, because when a previous local backup exists the run reads the recorded
// changes and never touches the database. An operator typing 30m into an empty
// form was systematically overestimating what they were about to switch on.
//
// So the clause has to sit on the UNCONDITIONAL intro, above the canEdit
// branch, and the full producer rule has to be in the docs. Both halves are
// asserted: the reuse cut is pinned on the docs side too, and an unpinned half
// is what lets a later edit delete the explanation from both places.
func TestBackupScheduleCard_saysWhatARunCosts(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupScheduleCard")

	const clause = "When it can, a run is built from the recorded changes"
	i := strings.Index(body, clause)
	if i < 0 {
		t.Fatalf("the intro does not say a run is usually built from the recorded changes, so an empty "+
			"form describes every run as a full copy read from the database:\n%s", body)
	}
	// Unconditional: above the canEdit branch, and therefore above every
	// `if (sch)` line, so it renders on a page with no schedule saved.
	gate := strings.Index(body, "if (!canEdit)")
	if gate < 0 {
		t.Fatal("the canEdit branch is gone; this guard can no longer tell an unconditional line from a gated one")
	}
	if i > gate {
		t.Error("the clause is below the canEdit branch, so the state it exists for (no schedule yet) does not show it")
	}
	if !strings.Contains(body[i:min(len(body), i+220)], "your database") {
		t.Error("the clause does not say what is spared, which is the whole point of stating it")
	}
	// The full rule (which producer runs when, and why) is docs work, and it
	// has to actually be there.
	docs := docsNoWrap(t)
	for _, want := range []string{
		"otherwise the newest backup is **updated from the recorded changes**",
		// The condition that actually decides the producer since #1539. It
		// used to be the S3 destination, which is why this guard asked for
		// "only a full backup uploads" — a sentence the docs must NOT carry
		// any more, because an operator who reads it configures a nightly
		// full read of production to get an off-box copy.
		"a server with no local backup directory gets a **full backup**",
		// The whole of #1539 in the docs. Without this line the page still
		// reads as if an S3 destination meant a nightly full read.
		"reads its previous snapshot straight from the bucket and uploads its result back to the same place",
	} {
		if !strings.Contains(docs, want) {
			t.Errorf("docs/console.md does not carry the producer rule (missing %q), so the cut lost it from both places", want)
		}
	}
	// And the rule it REPLACED must be gone. Stating both would be worse than
	// stating the old one alone: an operator has no way to tell which of two
	// contradicting sentences describes the build they are running.
	// And the rule it REPLACED must be gone from every page that states the
	// producer rule, not only this one: the surviving copies were in
	// dump-and-baseline.md, which the loop above does not read. Paraphrases
	// count, because an operator acts on the meaning.
	//
	// Scoped to docs/ on purpose: CHANGELOG.md narrates the old rule as
	// history, correctly, and a repo-wide grep would ban that too.
	for _, page := range []string{"console.md", "dump-and-baseline.md"} {
		body, err := os.ReadFile(filepath.Join("..", "..", "docs", page))
		if err != nil {
			t.Fatalf("read docs/%s: %v", page, err)
		}
		flat := strings.Join(strings.Fields(string(body)), " ")
		for _, banned := range []string{
			"only a full backup uploads",
			"only a full backup can upload",
			"backups that go to S3 are always full backups",
			"backups that go to S3 are full backups",
		} {
			if strings.Contains(flat, banned) {
				t.Errorf("docs/%s still says %q, which #1539 made false: the scheduled update reads the "+
					"bucket and uploads its result there", page, banned)
			}
		}
	}
}

// jsLineContaining returns the ONE source line of body that holds anchor.
//
// This exists because a byte window around a needle is not a scope. The first
// version of the guard below read body[i-200:i+260] and asked whether "IAM
// role" appeared anywhere in it. It always did: jsFunctionBody BLANKS
// whole-line comments before the brace walk, so the five comment lines above
// the shared-config arm collapse to five empty lines and pull the PRECEDING
// arm ("Using an IAM role (found an EKS service-account role)") into the
// window. Deleting the hedge from the arm under test left the guard green, on
// the pristine tree as well as the mutated one. A guard satisfied by a
// different arm is not a guard.
//
// Even one line is too wide, though, and the second draft of this guard proved
// it: `strings.Contains(line, "profile")` was satisfied by `aws.profile` in the
// arm's own CONDITION, so the assertion that the copy names both probes passed
// while the copy named one. The claim under test is the STRING the arm assigns,
// so that is the scope: everything between `summary = "` and its closing quote.
func jsArmSummary(t *testing.T, body, anchor string) string {
	t.Helper()
	i := strings.Index(body, anchor)
	if i < 0 {
		t.Fatalf("no line contains %q; the arm was renamed or removed, so every assertion below checks nothing:\n%s", anchor, body)
	}
	end := strings.IndexByte(body[i:], '\n')
	if end < 0 {
		end = len(body) - i
	}
	line := body[i : i+end]
	const assign = `summary = "`
	j := strings.Index(line, assign)
	if j < 0 {
		t.Fatalf("the arm at %q assigns no string literal to summary, so there is no claim to check:\n%s", anchor, line)
	}
	rest := line[j+len(assign):]
	k := strings.IndexByte(rest, '"')
	if k < 0 {
		t.Fatalf("unterminated summary literal at %q:\n%s", anchor, line)
	}
	// The returned span is only the FIRST string literal on the line, so
	// anything after its closing quote is invisible to the negative assertions
	// below. Two shapes exploited that and stayed green:
	//
	//	summary = "...hedged..." + " Using credentials from that file.";
	//	summary = "...hedged... \"Using credentials from that file.\"";
	//
	// Neither is contrived: stagingCard, the next function in this file, builds
	// its hint as "lit" + expr + "lit". So the first literal must be the whole
	// right-hand side: the next non-space byte after the closing quote has to be
	// the `;`. An arm that legitimately needs concatenation gets told this guard
	// cannot read it, which is the safe direction to fail.
	//
	// What that does NOT buy, and pass 4 measured it: `;` is exactly what
	// separates two statements, so
	//
	//	summary = "...hedged..."; summary = "Using credentials from ...";
	//
	// satisfies the check, and so does a `summary += " ..."` on the NEXT line
	// (this helper never looks past the anchor's own line). app.js appends to a
	// half-built sentence in eleven places — 13 `+=` sites minus one numeric
	// counter and one URL-path append — including an else-if chain shaped
	// exactly like this one. So this scope proves the hedge is in the RIGHT ARM
	// and nothing more. Keeping the retired sentences OUT is a separate,
	// span-scoped ban in the caller; see the comment there for what that ban
	// does and does not reach.
	tail := strings.TrimLeft(rest[k+1:], " \t")
	if !strings.HasPrefix(tail, ";") {
		t.Fatalf("the arm at %q does not assign ONE plain string literal, so everything after the first "+
			"literal would escape every check below. Give the arm a single literal, or teach this helper "+
			"the new shape:\n%s", anchor, line)
	}
	return rest[:k]
}

// TestCredentialsCard_armsReportWhatWasProbed (#1528, review passes 1 and 2).
//
// The card answers one question: can this daemon reach S3 at all. Two of its
// arms used to answer it with a confident claim the daemon never observed.
//
//   - shared config: selected by `aws.shared_config || aws.profile`, which is
//     TWO independent probes (a file stat and an env var), and it sits below
//     the env-key, ECS and IRSA arms. An EC2 host with an instance role and a
//     region-only ~/.aws/config lands there with no credentials in that file;
//     so does a container with AWS_PROFILE exported and no ~/.aws mounted, and
//     for that one a sentence naming only the file contradicts the card's own
//     "~/.aws config: absent" row two lines below it.
//   - access keys: AccessKeyEnv is AWS_ACCESS_KEY_ID alone. The secret key is
//     never probed, so "Using access keys" is false whenever the ID is
//     exported without it.
//
// The role arms are covered here too since #1534 closed them, and each one
// says what its own evidence supports rather than one shared hedge: ECS is
// still env-var presence (probing the endpoint is a network call, a separate
// decision), so its sentence claims only that the variable is there, while the
// EKS arms report the token-file probe and AWS_ROLE_ARN separately — an
// unreadable token and a missing role ARN are different repairs.
//
// Each assertion is scoped to the STRING that arm assigns. See jsArmSummary
// for why neither a byte window nor the whole line is narrow enough.
func TestCredentialsCard_armsReportWhatWasProbed(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "credentialsCard")

	shared := jsArmSummary(t, body, "else if (aws.shared_config")
	if strings.Contains(shared, "Using credentials from") {
		t.Error("the shared-config arm still asserts the file is what signs the requests; the daemon only " +
			"observed that the file exists, and an instance role may be doing the work")
	}
	// Both probes named, or the sentence contradicts the Raw signals rows for
	// the shape it does not mention.
	if !strings.Contains(shared, "shared ~/.aws config file") {
		t.Error("the shared-config arm no longer names the file, which is also the string the Playwright " +
			"suite matches this arm by")
	}
	if !strings.Contains(shared, "profile") {
		t.Errorf("the arm is selected by aws.profile too, and names only the file, so with AWS_PROFILE set "+
			"and no ~/.aws present the card contradicts its own \"~/.aws config: absent\" row:\n%s", shared)
	}
	if !strings.Contains(shared, "IAM role") {
		t.Errorf("the shared-config arm does not say an IAM role can still be what signs the requests:\n%s", shared)
	}

	keys := jsArmSummary(t, body, "if (aws.access_key_env)")
	if strings.Contains(keys, "Using access keys") {
		t.Error("the access-key arm asserts the keys are in use; only AWS_ACCESS_KEY_ID was probed")
	}
	if !strings.Contains(keys, "access key ID") {
		t.Errorf("the access-key arm does not say WHICH signal was seen:\n%s", keys)
	}
	if !strings.Contains(keys, "secret") {
		t.Errorf("the access-key arm does not say the secret key was not checked, which is the whole "+
			"difference between what was observed and what it used to claim:\n%s", keys)
	}

	// The Raw signals row under the summary is part of the same claim. Saying
	// access keys PLURAL are "set", two lines below a sentence that says the
	// secret key was not checked, reproduces inside one card the contradiction
	// the shared-config arm was rewritten to remove.
	if strings.Contains(body, `"access keys (env)"`) {
		t.Error(`the Raw signals row is still labelled "access keys (env)" but only AWS_ACCESS_KEY_ID is ` +
			`probed, so it contradicts the summary two lines above it`)
	}
	if !strings.Contains(body, `"access key ID (env)"`) {
		t.Error(`the Raw signals row does not name the one variable that was read (AWS_ACCESS_KEY_ID)`)
	}

	// The ECS and EKS arms, hedged since #1534. The ECS one stays a
	// presence claim (probing the endpoint is a network call, a separate
	// decision); the EKS one is probed server-side, and each of its three
	// shapes names what was actually observed.
	ecs := jsArmSummary(t, body, "else if (aws.container_creds)")
	if !strings.Contains(ecs, "not checked here") {
		t.Errorf("the ECS arm no longer says the endpoint was not checked:\n%s", ecs)
	}
	broken := jsArmSummary(t, body, `summary = "An EKS service-account role is configured`)
	if !strings.Contains(broken, "cannot be read") || !strings.Contains(broken, "cannot sign") {
		t.Errorf("the unreadable-token arm does not say the token cannot be read or that nothing signs:\n%s", broken)
	}
	noArn := jsArmSummary(t, body, `summary = "An EKS service-account token is readable`)
	if !strings.Contains(noArn, "AWS_ROLE_ARN") || !strings.Contains(noArn, "cannot sign") {
		t.Errorf("the missing-role-arn arm does not name AWS_ROLE_ARN or say nothing signs:\n%s", noArn)
	}
	healthy := jsArmSummary(t, body, `summary = "Found an EKS service-account role`)
	if !strings.Contains(healthy, "not checked here") {
		t.Errorf("the healthy EKS arm claims more than the probes observed (readable token + named role):\n%s", healthy)
	}

	// And the retired sentences must not appear ANYWHERE in the function.
	//
	// The arm-scoped bans above are SUBSUMED by this one — jsArmSummary returns
	// a sub-slice of the same body, so anything they can see this sees too.
	// They survive only to name WHICH arm regressed; both are t.Error and both
	// fire together. Scope is what makes the POSITIVE assertions meaningful
	// (the hedge has to be in that arm, not merely present somewhere on the
	// card); for a negative, the smaller scope is the hole. Pass 4 built four
	// shapes — a second statement after the `;`, a `summary +=` on the next
	// line, a plain reassignment, a new `else if` below — and every one stayed
	// green under the arm scope alone.
	//
	// Read from jsFunctionSpan, NOT jsFunctionBody. jsFunctionBody cuts each
	// line at its first `//` without knowing whether it is inside a string, and
	// app.js has several code lines with a URL in a literal. Under that view a
	// line like `summary = "https://x — Using credentials from ...";` is
	// truncated before the phrase and this ban passes with the sentence on
	// screen (pass 5 built exactly that). A negative assertion fails OPEN under
	// truncation; a positive one fails closed, which is why the assertions above
	// were never exposed to it.
	//
	// The span still blanks WHOLE-LINE comments, which is what keeps the
	// pristine tree green: the one live occurrence of "Using access keys" is a
	// comment line inside credentialsCard explaining what the arm used to
	// render. A TRAILING comment quoting either phrase would now trip this —
	// noise, not a false pass, and the right direction for a ban.
	//
	// What it does not cover, said plainly so nobody reads it as more: a
	// reworded claim, one assembled from concatenated fragments, or one
	// returned by a helper defined outside this function. No string guard
	// closes those; what it closes is the phrase itself coming back verbatim.
	// "Using an IAM role" joined the retired list with #1534: both role arms
	// now report what was probed instead of asserting use.
	span := jsFunctionSpan(t, readAsset(t, "app.js"), "credentialsCard")
	for _, retired := range []string{"Using credentials from", "Using access keys", "Using an IAM role"} {
		if strings.Contains(span, retired) {
			t.Errorf("credentialsCard says %q somewhere in its body. That claim was retired in #1528: the "+
				"daemon probes presence, never use, and this is the card an operator opens precisely "+
				"because S3 is not working", retired)
		}
	}
}

// TestCredentialsCard_commentDoesNotOverclaim (#1528 pass 3, flipped by
// #1534). The comment above the if/else chain is what a later reader trusts
// instead of re-deriving, so it has to track the arms. It once had to say the
// role arms were NOT hedged; now that they are, the stale claim is the danger
// — it would send a reader to re-fix fixed code, and its own message named the
// worst way to quiet it (drop the hedging). This guard bans that text and
// requires the #1534 citation the arms' shapes need.
//
// Read from the RAW asset on purpose: jsFunctionBody blanks comment lines, so
// the body view cannot see a comment at all.
func TestCredentialsCard_commentDoesNotOverclaim(t *testing.T) {
	js := readAsset(t, "app.js")
	start := strings.Index(js, "function credentialsCard(")
	if start < 0 {
		t.Fatal("credentialsCard is gone; this guard covers nothing")
	}
	end := strings.Index(js[start:], "function stagingCard(")
	if end < 0 {
		t.Fatal("cannot bound credentialsCard's source region")
	}
	region := js[start : start+end]

	// The deficiency note flipped with #1534: every arm now reports what was
	// probed, so the STALE claim to ban is the old one — a comment saying the
	// role arms are still unhedged would send a reader to re-fix fixed code
	// (and its message named the worst way to quiet it).
	for _, stale := range []string{"NOT hedged", "Tracked in #1534; fixing", "still read \"Using an IAM role\""} {
		if strings.Contains(region, stale) {
			t.Errorf("the region still carries the pre-#1534 deficiency note (%q); the arms are probed now", stale)
		}
	}
	if !strings.Contains(region, "#1534") {
		t.Error("the region no longer cites #1534, so the next reader has to rediscover why the EKS arm " +
			"has three shapes and the ECS one stays a presence claim")
	}
}

// TestCredentialsCard_noEmDashPlaceholders: the console's copy rule is plain
// words and no em dashes. kvRow's own empty fallback is a shared helper, but
// these two call sites pass the character in themselves.
func TestCredentialsCard_noEmDashPlaceholders(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "credentialsCard")
	if strings.Contains(body, "—") {
		t.Error("credentialsCard passes an em dash as a placeholder; say \"not set\", like the row above it")
	}
}

// TestDuckDBCard_titleDoesNotPromiseAQuery (#1528, review pass 1). The card
// hands the operator a file that explicitly does NOT run here. A title naming a
// query, for a control that only downloads, is the same defect as the
// backup-refresh title.
//
// The cross-file pin this used to carry is GONE, and deliberately. It asserted
// that the generated views.sql names this card's title, because the file's
// remediation pointed at a checkbox on it. The card no longer offers the live
// leg, so the console stops overriding LiveLegHowTo and the file names the CLI
// flag instead — nothing in it refers to this card, and a pin against a
// reference that does not exist would only be a test asserting its own fixture.
func TestDuckDBCard_titleDoesNotPromiseAQuery(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "duckdbPanel")
	m := regexp.MustCompile(`ov-panel-title", text: "([^"]*)"`).FindStringSubmatch(body)
	if m == nil {
		t.Fatal("duckdbPanel renders no title; this guard covers nothing")
	}
	title := m[1]
	if strings.Contains(title, "Query in") {
		t.Errorf("the card title %q promises a query that happens somewhere else; it downloads a schema file", title)
	}
	if !strings.Contains(title, "Download") {
		t.Errorf("the card title %q does not name what the control does (download a file)", title)
	}
}

// TestBackupRefreshCard_saysReuseCannotHappenOnS3 ties the card's one factual
// claim about where the saving applies to the code that makes it true.
//
// carryForwardEligible refuses any s3:// previous snapshot, because carrying a
// file forward means hard-linking it and a link needs both ends on a
// filesystem. Before this sentence existed the card promised the saving
// unconditionally, so on every S3-backed server it advertised a disk saving
// that CANNOT happen: the refresh loop already logs "not applicable" on each
// published run, and the console was the surface contradicting it.
//
// The guard is two-sided on purpose. It fails if the card drops the sentence,
// and it fails if the exclusion is LIFTED in reconstruct while the card still
// carries it, because a card that understates what a setting does sends an
// operator looking for a saving they already have.
//
// What it does NOT cover, so nobody reads more into it than it does:
//
//   - the WORDING. This side only asserts that some visible string names both
//     S3 and the machine-local condition, so it passes on a sentence that says
//     the OPPOSITE about them. The e2e renders the real function across every
//     state and asserts the sentences; that is where the claim is pinned.
//   - the exclusion being defeated at the CALLER rather than in the rule, for
//     instance baselineFoldSource returning the local directory. Nothing here
//     sees that. internal/reconstruct's own TestCarryForwardEligible covers
//     the rule itself.
func TestBackupRefreshCard_saysReuseCannotHappenOnS3(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "backupRefreshCard")

	// Assert the CLAIM, not its spelling: any sentence naming S3 in the same
	// visible string as the machine-local condition satisfies this.
	var said string
	for _, s := range visibleStrings(body) {
		if strings.Contains(s, "S3") && strings.Contains(strings.ToLower(s), "this machine") {
			said = s
			break
		}
	}
	if said == "" {
		t.Error("no visible string on the card says the saving applies only to backups kept on this " +
			"machine and not to ones in S3. Without it the card promises a disk saving that an " +
			"S3-backed server can never get, which is what it did before this sentence existed")
	}

	// The other side: the rule the sentence describes.
	src, err := os.ReadFile(filepath.Join("..", "reconstruct", "carryforward.go"))
	if err != nil {
		t.Fatalf("read carryforward.go: %v", err)
	}
	// Comments stripped first. Reading the raw file let a comment SAYING the
	// rule was removed ("it used to read !strings.HasPrefix(srcPath, ...) right
	// here") satisfy the check that the rule is still there, which is the exact
	// inversion this side of the guard exists to catch.
	// Anchored BEFORE the literal's own slashes: "s3://" contains a "//", so
	// the comment strip truncates the real line of code at exactly this point.
	// That is what makes the anchor work on both sides. A comment quoting the
	// rule starts with "//" and is removed whole, so it cannot supply it, while
	// the code line always survives up to here.
	const rule = `!strings.HasPrefix(srcPath, "s3:`
	if !strings.Contains(stripGoLineComments(string(src)), rule) {
		t.Errorf("carryForwardEligible no longer excludes an s3:// previous snapshot (%s is gone), so the "+
			"card's sentence %q now understates what the setting does. Update the card in the same "+
			"change that lifts the exclusion", rule, said)
	}
}

// visibleStrings returns the user-facing strings a card function renders: the
// text: values, the arguments of its say() helper, and BOTH arms of a string
// ternary. The false arm needed its own pattern: a `: "Off" }))` and a
// `: "Turn on",` are on screen, and a rule anchored on a following paren
// dropped them, which would let a must-not-contain built on this helper pass
// on exactly the off and turn-on labels.
//
// It is still a best effort over source, not a render. A string assembled by
// concatenation or held in a variable is invisible to it, so use it for
// must-CONTAIN checks, which fail closed when it misses one. The e2e renders
// the real function when the assertion has to be must-NOT-contain.
func visibleStrings(body string) []string {
	var out []string
	for _, re := range []*regexp.Regexp{
		regexp.MustCompile(`text: "([^"]*)"`),
		regexp.MustCompile(`say\("([^"]*)"`),
		regexp.MustCompile(`\? "([^"]*)"`),
		regexp.MustCompile(`\? "[^"]*"\s*:\s*"([^"]*)"`),
	} {
		for _, m := range re.FindAllStringSubmatch(body, -1) {
			out = append(out, m[1])
		}
	}
	return out
}

// stripGoLineComments removes // comments so a must-contain over Go source
// cannot be satisfied by prose quoting the thing it looks for. Crude on
// purpose: a "//" inside a string literal truncates that line, which can only
// make a must-contain stricter, never looser.
func stripGoLineComments(src string) string {
	var out []string
	for _, ln := range strings.Split(src, "\n") {
		if c := strings.Index(ln, "//"); c >= 0 {
			ln = ln[:c]
		}
		out = append(out, ln)
	}
	return strings.Join(out, "\n")
}
