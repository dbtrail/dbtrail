package console

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The backup settings (#1582): wire names, coverage of the daemon rows, the
// move of the per-server fields out of the server form, and the passthrough
// that makes the move safe. They were a page of their own until #1573 made
// them the "Where and how often" half of Snapshots; the guards below follow
// the half, not the address it used to have.

// TestBackupSettingsWireNamesMatchTheFrontend pins the JSON keys the Go DTOs
// emit against what the page reads. A renamed tag on either side renders a
// page of blanks with the whole suite green otherwise.
func TestBackupSettingsWireNamesMatchTheFrontend(t *testing.T) {
	js := readAsset(t, "app.js")
	page := jsFunctionBody(t, js, "snapshotSetupSections") +
		jsFunctionBody(t, js, "backupDaemonEditCard") +
		jsFunctionBody(t, js, "backupDaemonEditRow") +
		jsFunctionBody(t, js, "backupServersPanel") +
		jsFunctionBody(t, js, "backupServerRow") +
		jsFunctionBody(t, js, "s3OnlyBackupWarning") +
		jsFunctionBody(t, js, "s3RetentionBox") +
		jsFunctionBody(t, js, "s3RetentionConflicts")
	// Dotted READS, not bare tokens: "value" also matches dir.value.trim()
	// and "baseline_dir" matches the input's name: attribute, so a renamed
	// JSON tag stayed green while the page rendered blanks. The dotted form
	// is the actual dereference of the wire field.
	for _, read := range []string{
		"settings.daemon", "settings.servers", "settings.registry_read_only",
		"row.key", "row.value", "row.cli", "row.needs_restart", "row.err",
		// The editable daemon rows (#1682). row.editable decides which card a
		// row lands in, row.source which sentence it gets, row.startup what
		// "use the startup value" would restore — a blank there would offer
		// the way back without saying where it goes.
		"row.editable", "row.source", "row.startup",
		"srv.baseline_dir", "srv.baseline_s3", "srv.no_archive",
		"srv.resolved_dir", "srv.resolved_s3", "srv.source",
		"srv.schedule_every", "srv.schedule_at", "srv.schedule_refusal", "srv.schedule_full_every",
		"srv.schedule_every_minutes", "srv.archive_s3", "srv.full_backup_possible",
		// How far back the kept count reaches, and a held folder (#1681).
		"srv.keep_in_force", "srv.snapshot_every_minutes", "srv.prune_retain_minutes", "srv.keep_held",
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

// TestServerFormLeavesTheSnapshotFieldsAlone (#1681): the connection form does
// not carry the snapshot folder, S3 destination or archive toggle at all. It
// used to post them back from hidden fields so a replacing PUT would not wipe
// them, which put back an OLD folder whenever the form had been opened before
// a change on the Snapshots page. The server now keeps what a request leaves
// out (TestServersUpdate_keepsSnapshotFieldsItWasNotSent).
func TestServerFormLeavesTheSnapshotFieldsAlone(t *testing.T) {
	js := readAsset(t, "app.js")
	form := jsFunctionBody(t, js, "buildServerForm")
	for _, field := range []string{`name: "baseline_dir"`, `name: "baseline_s3"`, `name: "no_archive"`} {
		if strings.Contains(form, field) {
			t.Errorf("buildServerForm carries %s again; a form opened before a Snapshots-page change would post the old value back", field)
		}
	}
	body := jsFunctionBody(t, js, "serverFormBody")
	for _, key := range []string{"baseline_dir", "baseline_s3", "no_archive"} {
		if strings.Contains(body, key) {
			t.Errorf("serverFormBody sends %s again", key)
		}
	}
}

// TestBackupSettingsSectionIsWired: route in ROUTES, a renderRoute arm, no
// monitor gate, a nav item, and the anchor the old address lands on — what
// makes these settings reachable now that they are a section (#1573) rather
// than a page.
func TestBackupSettingsSectionIsWired(t *testing.T) {
	js := readAsset(t, "app.js")
	if !regexp.MustCompile(`"snapshots"\]?`).MatchString(js) {
		t.Fatal("snapshots is not in ROUTES")
	}
	if !strings.Contains(js, `case "snapshots": return renderSnapshots();`) {
		t.Error("renderRoute has no arm for snapshots; the URL falls through to Overview")
	}
	// The old address has to keep landing on this half: a bookmark of the
	// settings page that arrives at the top of a page three times longer
	// looks like the settings were removed.
	if !strings.Contains(js, `["backup-settings", () => "snapshots#setup"]`) {
		t.Error("/backup-settings no longer lands on the setup section of Snapshots")
	}
	if !strings.Contains(js, `snapshotSection("Where and how often", "setup")`) {
		t.Error("the setup section heading is gone, so the anchor the old address carries answers nothing")
	}
	// NOT monitor-gated, deliberately (the Access profiles precedent): the
	// server Edit form's backup fields became passthroughs, so this is the
	// ONLY editor of the registry's backup location — and the registry is
	// state the standalone serve edits too. Gating the page left serve with
	// no UI path to a backup location at all. The daemon-side cards inside
	// carry the monitor gate instead.
	// Any shape of "this route needs the daemon", not one spelling of it: a
	// regression written without the closing paren read as fine to a
	// Contains() check. Behaviour is pinned by the serve rows of
	// TestOldAddressesLandOnTheirPage; this keeps the gate from creeping back
	// in on the navigate() side, where those rows do not look.
	gate := regexp.MustCompile(`route === "snapshots"[^;\n]{0,60}!capsCache\.monitor`)
	if gate.MatchString(js) {
		t.Error("snapshots is behind the monitor gate; on serve the per-server backup " +
			"location would have NO editor anywhere in the UI")
	}
	body := jsFunctionBody(t, js, "snapshotSetupSections")
	if !strings.Contains(body, "capsCache.monitor") {
		t.Error("snapshotSetupSections no longer gates the daemon-side cards on monitor; on serve the " +
			"daemon card renders empty rows and reads as an unconfigured install")
	}
	html := readAsset(t, "index.html")
	if !strings.Contains(html, `data-route="snapshots"`) {
		t.Error("index.html has no nav item for snapshots")
	}
	navRE := regexp.MustCompile(`(?s)data-route="snapshots"[^>]*>`)
	if nav := navRE.FindString(html); strings.Contains(nav, `data-capability="monitor"`) {
		t.Error("the snapshots nav item is capability-gated on monitor; serve users could not " +
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

	// The wiring: each server row draws its own verdict as the current case
	// (a key this build does not know draws as Unknown), and the case carries
	// data-source so the e2e can hold it against the API. The three-row legend
	// that drew every case once is gone (#1573 redesign).
	shape := functionBody(t, js, "function blCase(")
	if !strings.Contains(shape, `"data-source": source`) || !strings.Contains(shape, "is-current") {
		t.Error("blCase no longer stamps data-source / is-current; the e2e cannot compare the drawing to the API")
	}
	if strings.Contains(shape, "svgEl(") {
		t.Error("blCase builds through svgEl, which is for static icon constants; draw with el()")
	}
	if strings.Contains(stripJSLineComments(functionBody(t, js, "function backupServersPanel(")), "blCase(") {
		t.Error("backupServersPanel draws cases of its own again; the three-row legend was removed, each server row draws its own")
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

// TestBackupSettingsStaysCompact: the per-server row carried ~247 words of
// visible copy before #1603. It explains itself by drawing now; what still
// needs saying is compact, not cut. The daemon's startup card left the page
// with the Snapshots cut (D13: the interval, a manual read and retention are
// the settings the page offers; the rest lives in the launch command).
func TestBackupSettingsStaysCompact(t *testing.T) {
	js := readAsset(t, "app.js")
	words := jsFunctionBody(t, js, "localCopyWords")
	row := jsFunctionBody(t, js, "backupServerRow")

	// The row keeps a compact block: folding is what makes the cut real.
	if !strings.Contains(row, `cnFine("More about `) {
		t.Error("backupServerRow has no compact block; the prose was cut, not compacted")
	}
	// localCopyWords over every arm at once is 1107 characters today (ten
	// arms); a reader sees at most two of them (the reach line is
	// localReachWords', counted apart). The cap leaves room for a copy edit
	// and rings on one more paragraph.
	if n := visibleChars(words); n > 1200 {
		t.Errorf("localCopyWords' visible text is %d characters over all its arms; a reader sees two lines of it, keep them short", n)
	}

	empty := jsObjectKeys(t, js, "BACKUP_DAEMON_EMPTY")
	labels := jsObjectKeys(t, js, "BACKUP_DAEMON_ROWS")
	slices.Sort(empty)
	slices.Sort(labels)
	if !slices.Equal(empty, labels) {
		t.Errorf("BACKUP_DAEMON_EMPTY keys %v differ from BACKUP_DAEMON_ROWS keys %v; an empty row would fall back to a word chosen for another key", empty, labels)
	}

	// One section label (the editable daemon row), and only the retention
	// row of the daemon settings reaches the page.
	build := functionBody(t, js, "function snapshotSetupSections(")
	if strings.Count(build, `out.push(sect(`) != 1 {
		t.Error("snapshotSetupSections does not open exactly one section")
	}
	if strings.Contains(build, "backupDaemonCard(") || !strings.Contains(build, "SNAPSHOT_SETTING_KEYS.has(row.key)") {
		t.Error("the daemon rows are not filtered to the settings the page offers (D13)")
	}

	// No em dash in any double-quoted literal these surfaces hold: text:
	// values, say() arguments, bare text children and aria labels alike.
	// Over the SPAN, not the comment-stripped body: a must-not-contain over
	// jsFunctionBody fails open, because that helper truncates each line at
	// its first "//" and a URL literal ("s3://...") hides everything after
	// it on the line. Comments carrying a dash ring here on purpose.
	for _, name := range []string{"localCopyWords", "backupServerRow", "snapshotSetupSections", "blCase", "s3RetentionBox"} {
		body := jsFunctionSpan(t, js, name)
		for _, m := range regexp.MustCompile(`"([^"\n]*)"`).FindAllStringSubmatch(body, -1) {
			if strings.Contains(m[1], "—") {
				t.Errorf("%s holds an em dash in %q", name, m[1])
			}
		}
	}
}

// TestSnapshotsIsNamedOnce: one page, one name, in the three places a reader
// meets it — the heading, the sidebar and the command palette. Three pages
// merged into it (#1573), so the failure this catches is a half-rename: a
// sidebar that still says Backups over a page whose heading says Snapshots,
// or a palette entry for a page that no longer exists.
func TestSnapshotsIsNamedOnce(t *testing.T) {
	js := readAsset(t, "app.js")
	if strings.Count(js, `pageHead("Snapshots"`) != 2 {
		t.Error("the page head (built and error arms) does not read Snapshots")
	}
	if !strings.Contains(js, `label: "Snapshots",`) || !strings.Contains(js, `run: () => navigate("snapshots")`) {
		t.Error("the command palette entry does not read Snapshots")
	}
	// Typing what the pages used to be called has to find it: somebody who
	// has used this console looks for Backups, and an entry they cannot find
	// reads as a feature that was removed. Read inside the palette's own
	// function, and paired with the filter that consults the list — a list
	// nothing reads would pass a search over the whole file.
	palette := jsFunctionBody(t, js, "cmdkCommands")
	for _, old := range []string{"backups", "verification", "backup settings"} {
		if !strings.Contains(palette, `"`+old+`"`) {
			t.Errorf("the palette does not answer to %q, the name one of the merged pages had", old)
		}
	}
	if !strings.Contains(jsFunctionBody(t, js, "renderCmdk"), "c.alt") {
		t.Error("the palette filter no longer reads the alternate names, so the old page names find nothing")
	}
	html := readAsset(t, "index.html")
	nav := regexp.MustCompile(`(?s)data-route="snapshots".*?</a>`).FindString(html)
	if !strings.Contains(nav, "<span>Snapshots</span>") {
		t.Error("the nav item does not read Snapshots")
	}
	for _, gone := range []string{`pageHead("Backups"`, `pageHead("Verification"`, `pageHead("Backup settings"`} {
		if strings.Contains(js, gone) {
			t.Errorf("%s is back; the three pages are one page with one heading now", gone)
		}
	}
}
