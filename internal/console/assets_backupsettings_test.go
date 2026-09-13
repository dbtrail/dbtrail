package console

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The Backup settings page (#1582): wire names, coverage of the
// daemon rows, the move of the per-server fields out of the server form, and
// the passthrough that makes the move safe.

// TestBackupSettingsWireNamesMatchTheFrontend pins the JSON keys the Go DTOs
// emit against what the page reads. A renamed tag on either side renders a
// page of blanks with the whole suite green otherwise.
func TestBackupSettingsWireNamesMatchTheFrontend(t *testing.T) {
	js := readAsset(t, "app.js")
	page := jsFunctionBody(t, js, "buildBackupSettings") +
		jsFunctionBody(t, js, "backupDaemonCard") +
		jsFunctionBody(t, js, "backupServersPanel") +
		jsFunctionBody(t, js, "backupServerRow")
	// Dotted READS, not bare tokens: "value" also matches dir.value.trim()
	// and "baseline_dir" matches the input's name: attribute, so a renamed
	// JSON tag stayed green while the page rendered blanks. The dotted form
	// is the actual dereference of the wire field.
	for _, read := range []string{
		"settings.daemon", "settings.servers", "settings.registry_read_only",
		"row.key", "row.value", "row.on", "row.cli", "row.needs_restart", "row.err",
		"srv.baseline_dir", "srv.baseline_s3", "srv.no_archive",
		"srv.resolved_dir", "srv.resolved_s3", "srv.source",
		"srv.schedule_every", "srv.schedule_at", "srv.schedule_refusal",
	} {
		if !strings.Contains(page, read) {
			t.Errorf("the page never reads %q; the server emits it and the page renders a blank instead", read)
		}
	}
}

// TestBackupSettingsDaemonRowsAreAllLabeled: every key the handler emits has
// a label in BACKUP_DAEMON_ROWS, or the page falls back to the raw key — a
// flag name where the label's whole job is saying what the flag means.
func TestBackupSettingsDaemonRowsAreAllLabeled(t *testing.T) {
	js := readAsset(t, "app.js")
	block := regexp.MustCompile(`const BACKUP_DAEMON_ROWS = \{[^}]+\}`).FindString(js)
	if block == "" {
		t.Fatal("BACKUP_DAEMON_ROWS is gone from app.js")
	}
	// The canonical key set, spelled here and in the handler; the API test
	// pins the handler side against the CLI names.
	for _, key := range []string{
		"baseline_dir", "baseline_s3", "baseline_retain", "refresh_every",
		"lock_mode", "trigger", "staging_dir", "verify_interval", "verify_tables",
	} {
		if !strings.Contains(block, key+":") {
			t.Errorf("BACKUP_DAEMON_ROWS has no label for %q; the row would render its raw key", key)
		}
	}
}

// TestServerFormCarriesTheBackupFieldsAsPassthrough is the wipe hazard the
// move created (#1582): PUT /api/servers/{id} REPLACES the entry, so a form
// that stopped sending baseline_dir/baseline_s3/no_archive would silently
// clear a server's backup configuration on every unrelated edit. The fields
// left the visible form for the settings page; they must survive in it as
// hidden passthroughs, prefilled and submitted like before.
func TestServerFormCarriesTheBackupFieldsAsPassthrough(t *testing.T) {
	js := readAsset(t, "app.js")
	form := jsFunctionBody(t, js, "buildServerForm")
	for _, field := range []string{`name: "baseline_dir"`, `name: "baseline_s3"`, `name: "no_archive"`} {
		if !strings.Contains(form, field) {
			t.Errorf("buildServerForm no longer carries %s; a plain edit now WIPES that field on the entry", field)
		}
	}
	// Hidden, not visible: the settings page is the one editor. A visible
	// duplicate saves to one store from two places, one of them stale.
	if strings.Contains(form, `srvField("Backup dir"`) || strings.Contains(form, `srvField("Backup S3"`) {
		t.Error("the server form still renders visible backup-location fields; they moved to the settings page")
	}
	body := jsFunctionBody(t, js, "serverFormBody")
	for _, read := range []string{"f.baseline_dir.value", "f.baseline_s3.value", "f.no_archive.checked"} {
		if !strings.Contains(body, read) {
			t.Errorf("serverFormBody no longer sends %s; the PUT will replace the entry without it", read)
		}
	}
	// And the prefill still fills the hidden halves, or the passthrough
	// passes empty strings through — the exact wipe it exists to prevent.
	show := jsFunctionBody(t, js, "showServerForm")
	if !strings.Contains(show, `"baseline_dir", "baseline_s3"`) {
		t.Error("showServerForm's prefill list no longer covers the hidden backup fields")
	}
	if !strings.Contains(show, "form.elements.no_archive.checked = !!prefill.no_archive") {
		t.Error("showServerForm no longer prefills no_archive; the hidden checkbox submits unchecked for every edit")
	}
}

// TestBackupSettingsPageIsWired: route in ROUTES, a renderRoute arm, the
// monitor gate, and a nav item — the four halves that make a page reachable.
func TestBackupSettingsPageIsWired(t *testing.T) {
	js := readAsset(t, "app.js")
	if !regexp.MustCompile(`"backup-settings"\]?`).MatchString(js) {
		t.Fatal("backup-settings is not in ROUTES")
	}
	if !strings.Contains(js, `case "backup-settings": return renderBackupSettings();`) {
		t.Error("renderRoute has no arm for backup-settings; the URL falls through to Overview")
	}
	// NOT monitor-gated, deliberately (the Access profiles precedent): the
	// server Edit form's backup fields became passthroughs, so this page is
	// the ONLY editor of the registry's backup location — and the registry
	// is state the standalone serve edits too. Gating the page left serve
	// with no UI path to a backup location at all. The daemon-side cards
	// inside the page carry the monitor gate instead.
	if strings.Contains(js, `route === "backup-settings") && !capsCache.monitor`) ||
		strings.Contains(js, `"backup-settings" || route`) {
		t.Error("backup-settings is behind the monitor gate again; on serve the per-server backup " +
			"location would have NO editor anywhere in the UI")
	}
	body := jsFunctionBody(t, js, "buildBackupSettings")
	if !strings.Contains(body, "capsCache.monitor") {
		t.Error("buildBackupSettings no longer gates the daemon-side cards on monitor; on serve the " +
			"daemon card renders empty rows and reads as an unconfigured install")
	}
	html := readAsset(t, "index.html")
	if !strings.Contains(html, `data-route="backup-settings"`) {
		t.Error("index.html has no nav item for backup-settings")
	}
	navRE := regexp.MustCompile(`(?s)data-route="backup-settings"[^>]*>`)
	if nav := navRE.FindString(html); strings.Contains(nav, `data-capability="monitor"`) {
		t.Error("the backup-settings nav item is capability-gated on monitor; serve users could not " +
			"reach the only editor of the per-server backup location")
	}
}

// ── #1603: the page shows the three kinds instead of describing them ──────

// jsObjectKeys reads the top-level keys of `const NAME = { ... };` in app.js.
// Keys are matched at line start, so a nested object's keys do not count.
func jsObjectKeys(t *testing.T, js, name string) []string {
	t.Helper()
	i := strings.Index(js, "const "+name+" = {")
	if i < 0 {
		t.Fatalf("%s is gone from app.js", name)
	}
	rest := js[i:]
	j := strings.Index(rest, "\n};")
	if j < 0 {
		t.Fatalf("%s is not terminated by a `};` line", name)
	}
	var keys []string
	// Digits allowed: baseline_s3 is a key, and a class that cannot read it
	// compared two tables that both silently lacked it.
	for _, m := range regexp.MustCompile(`(?m)^\s+"?([a-z0-9_]+)"?:`).FindAllStringSubmatch(rest[:j], -1) {
		keys = append(keys, m[1])
	}
	return keys
}

// TestBackupSettingsDrawingCannotLie pins the drawn cases to the verdicts
// the API can emit, in BOTH directions and by COUNT.
//
// A picture fails differently from prose: nobody reports a diagram that
// merely looks plausible. So the JS keys must equal the Go constants, every
// constant must be assigned somewhere, and the number of assignment sites
// must equal the number of constants. Driving the DTO over three fixtures
// would not see a fourth branch, because the fixture would not carry it.
func TestBackupSettingsDrawingCannotLie(t *testing.T) {
	js := readAsset(t, "app.js")
	goSrc, err := os.ReadFile("backup_settings_api.go")
	if err != nil {
		t.Fatal(err)
	}
	consts := map[string]string{} // name -> value
	for _, m := range regexp.MustCompile(`backupSource(\w+)\s*=\s*"([a-z]+)"`).FindAllStringSubmatch(string(goSrc), -1) {
		consts[m[1]] = m[2]
	}
	if len(consts) < 3 {
		t.Fatalf("expected the three backupSource* constants, found %d", len(consts))
	}
	assigned := map[string]int{}
	for _, m := range regexp.MustCompile(`dto\.Source = backupSource(\w+)`).FindAllStringSubmatch(string(goSrc), -1) {
		assigned[m[1]]++
	}
	if n := regexp.MustCompile(`dto\.Source = "`).FindAllString(string(goSrc), -1); len(n) > 0 {
		t.Errorf("dto.Source is assigned a bare string literal %d time(s); it must go through a backupSource* constant so the drawing guard can count it", len(n))
	}
	sites := 0
	for name, n := range assigned {
		if _, ok := consts[name]; !ok {
			t.Errorf("dto.Source is assigned backupSource%s, which is not declared", name)
		}
		sites += n
	}
	for name := range consts {
		if assigned[name] == 0 {
			t.Errorf("backupSource%s is declared but never assigned; a verdict the page draws and the API never emits", name)
		}
	}
	if sites != len(consts) {
		t.Errorf("%d assignment sites for %d constants; each verdict is assigned exactly once, so a new branch must come with a new constant (and a new drawn case)", sites, len(consts))
	}

	drawn := jsObjectKeys(t, js, "BACKUP_SOURCE_CASES")
	want := make([]string, 0, len(consts))
	for _, v := range consts {
		want = append(want, v)
	}
	slices.Sort(drawn)
	slices.Sort(want)
	if !slices.Equal(drawn, want) {
		t.Errorf("BACKUP_SOURCE_CASES draws %v, the API emits %v; the picture of the cases lies", drawn, want)
	}

	// The wiring: the legend iterates the table (so a new key is drawn), each
	// server row draws its own verdict as the current case, and the case
	// carries data-source so the e2e can hold it against the API.
	shape := functionBody(t, js, "function blCase(")
	if !strings.Contains(shape, `"data-source": source`) || !strings.Contains(shape, "is-current") {
		t.Error("blCase no longer stamps data-source / is-current; the e2e cannot compare the drawing to the API")
	}
	if strings.Contains(shape, "svgEl(") {
		t.Error("blCase builds through svgEl, which is for static icon constants; draw with el()")
	}
	if !strings.Contains(functionBody(t, js, "function backupServersPanel("), "Object.keys(BACKUP_SOURCE_CASES)") {
		t.Error("the legend does not iterate BACKUP_SOURCE_CASES, so a new case would be pinned by the guard yet never drawn")
	}
	if !strings.Contains(functionBody(t, js, "function backupServerRow("), "blCase(src, true)") {
		t.Error("backupServerRow does not draw the server's own verdict as the current case")
	}
}

// visibleChars sums the user-facing string literals (text: values, say()
// arguments, both arms of a string ternary) in the part of a function BEFORE
// its first cnFine( call: what a first-time reader meets. The compact block's
// contents are one click away and do not count. Over the comment-stripped
// body, so a comment quoting a sentence cannot count as rendering it. The Go
// half of the e2e's rendered-text budget: it cannot count rendered text, so
// it counts what the source can produce, and fails on the desk.
func visibleChars(body string) int {
	visible := body
	if i := strings.Index(body, "cnFine("); i >= 0 {
		visible = body[:i]
	}
	// Every literal inside a text: value or a say(...) call, so a sentence
	// split over `+` or built around a count still weighs what it renders.
	// Both arms of a ternary count: the source cannot tell which renders.
	lit := regexp.MustCompile(`"((?:[^"\\]|\\.)*)"`)
	total := 0
	for _, re := range []*regexp.Regexp{
		regexp.MustCompile(`text:\s*((?:"(?:[^"\\]|\\.)*"|[^,}\n])*)`),
		// Quoted strings first, so a ";" inside a sentence does not end the
		// call early and weigh it at zero; newlines allowed, so a ternary
		// split over lines weighs both arms.
		regexp.MustCompile(`say\(((?:"(?:[^"\\]|\\.)*"|[^;])*)\)`),
	} {
		for _, m := range re.FindAllStringSubmatch(visible, -1) {
			for _, l := range lit.FindAllStringSubmatch(m[1], -1) {
				total += len(l[1])
			}
		}
	}
	return total
}

// TestBackupSettingsStaysCompact: the two daemon-side cards carried ~247
// words of visible copy before the per-server list (#1603). They explain
// themselves by drawing now; what still needs saying is compact, not cut.
func TestBackupSettingsStaysCompact(t *testing.T) {
	js := readAsset(t, "app.js")
	refresh := jsFunctionBody(t, js, "backupRefreshCard")
	daemon := jsFunctionBody(t, js, "backupDaemonCard")
	row := jsFunctionBody(t, js, "backupServerRow")

	// Each surface keeps a compact block: folding is what makes the cut real.
	for name, body := range map[string]string{"backupRefreshCard": refresh, "backupDaemonCard": daemon, "backupServerRow": row} {
		if !strings.Contains(body, `cnFine("More about `) {
			t.Errorf("%s has no compact block; the prose was cut, not compacted", name)
		}
	}
	// The budget covers every arm of every conditional at once (the source
	// cannot tell which render), so it sits above any one rendered state. By
	// this exact count on both trees: the pre-#1603 refresh card 1236 and the
	// rewrite 497 (the drawing's one sentence, the alarm, the dormancy note
	// and the S3 skip note, all deliberately visible); the daemon card 222
	// (its hint paragraph) and 106 (title, chip, the refused-value line). The
	// caps sit ~25% and ~40% above the rewrite and well below the old cards,
	// so a copy edit breathes but one more paragraph rings here before the
	// e2e sees it.
	if n := visibleChars(refresh); n > 620 {
		t.Errorf("backupRefreshCard's visible text is %d characters; the drawing carries the rule, so put the rest behind cnFine", n)
	}
	if n := visibleChars(daemon); n > 150 {
		t.Errorf("backupDaemonCard's visible text is %d characters beyond its rows; explain in the compact block, not above the rows", n)
	}

	// Empty means what applies, never a fault. One word per key, because one
	// word for all nine would lie (an empty Backup dir is no shared location).
	if strings.Contains(daemon, `"not set"`) {
		t.Error(`backupDaemonCard renders "not set" again; on a healthy install that reads as nine faults`)
	}
	empty := jsObjectKeys(t, js, "BACKUP_DAEMON_EMPTY")
	labels := jsObjectKeys(t, js, "BACKUP_DAEMON_ROWS")
	slices.Sort(empty)
	slices.Sort(labels)
	if !slices.Equal(empty, labels) {
		t.Errorf("BACKUP_DAEMON_EMPTY keys %v differ from BACKUP_DAEMON_ROWS keys %v; an empty row would fall back to a word chosen for another key", empty, labels)
	}

	// One restart chip on the card, honest only while every row needs one.
	if !strings.Contains(daemon, `rows.every((r) => r.needs_restart)`) {
		t.Error("backupDaemonCard no longer derives the card-level chip from every row's needs_restart; the chip could claim more than the rows do")
	}
	if strings.Count(daemon, `class: "tag-pill bks-restart"`) != 2 {
		t.Error("backupDaemonCard should render the restart chip in exactly two places: once at card level, once per row as the fallback")
	}
	if !strings.Contains(daemon, "if (!allRestart && row.needs_restart)") {
		t.Error("the per-row chip is not the fallback for the card chip; the page would show both at once")
	}

	// The three kinds are told apart by layout: two section labels, and the
	// daemon card outside the tinted grid.
	build := functionBody(t, js, "function buildBackupSettings(")
	if strings.Count(build, `sect("`) != 2 {
		t.Error("buildBackupSettings does not open exactly two sections; the split between change-here and set-at-startup is not drawn")
	}
	if strings.Contains(build, "cards.append(backupDaemonCard") || !strings.Contains(daemon, `class: "card bks-boot"`) {
		t.Error("the daemon card is inside the tinted .cards grid again; tinted vs plain is the mark that tells the kinds apart")
	}

	// No em dash in any double-quoted literal these surfaces hold: text:
	// values, say() arguments, bare text children and aria labels alike.
	// Over the SPAN, not the comment-stripped body: a must-not-contain over
	// jsFunctionBody fails open, because that helper truncates each line at
	// its first "//" and a URL literal ("s3://...") hides everything after
	// it on the line. Comments carrying a dash ring here on purpose.
	for _, name := range []string{"backupRefreshCard", "backupDaemonCard", "backupServerRow", "buildBackupSettings", "cfShape", "blCase"} {
		body := jsFunctionSpan(t, js, name)
		for _, m := range regexp.MustCompile(`"([^"\n]*)"`).FindAllStringSubmatch(body, -1) {
			if strings.Contains(m[1], "—") {
				t.Errorf("%s holds an em dash in %q", name, m[1])
			}
		}
	}
}

// TestBackupSettingsIsNamedSettings: the page and its nav item say Settings,
// so the pair with the Backups page reads as work vs configuration. Not the
// bare word: the item lives inside the sidebar's Settings group already.
func TestBackupSettingsIsNamedSettings(t *testing.T) {
	js := readAsset(t, "app.js")
	if strings.Count(js, `pageHead("Backup settings"`) != 2 {
		t.Error("the page head (built and error arms) does not read Backup settings")
	}
	if !strings.Contains(js, `label: "Backup settings", run: () => navigate("backup-settings")`) {
		t.Error("the command palette entry does not read Backup settings")
	}
	html := readAsset(t, "index.html")
	nav := regexp.MustCompile(`(?s)data-route="backup-settings".*?</a>`).FindString(html)
	if !strings.Contains(nav, "<span>Backup settings</span>") {
		t.Error("the nav item does not read Backup settings")
	}
	if strings.Contains(js, "Backups & snapshots") || strings.Contains(html, "Backups &amp; snapshots") {
		t.Error("the old page name survives somewhere; a pointer now names a page that does not exist")
	}
}
