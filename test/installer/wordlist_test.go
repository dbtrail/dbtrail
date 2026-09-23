package installer

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The promise the installer, the README and the site's start page all make
// where DBTrail does not know the server yet (#1799, #1807). One sentence,
// word for word the same on every surface.
const promise = "DBTrail keeps every change on your MySQL server, before and after, and writes the SQL that undoes the ones you didn't want."

// The start page every onboarding surface points at.
const startPage = "https://www.dbtrail.com/docs/quickstart/"

// The one sentence that tells a reader the command line and the web
// interface use two words for one thing. It lives in docs/quickstart.md and
// nowhere else, so the rest of the docs never teach the second word.
const bridge = "The command line calls a snapshot a baseline."

// ── the word list ─────────────────────────────────────────────────────────
//
// The closed word list for the first run lives ONCE, in
// test/console-e2e/first_run_scoreboard.mjs (BANNED_FORMS). That file is
// canonical: the first-run walk counts it on every screen and CI ratchets
// on the count. This test reads the same object out of that file instead of
// keeping a second copy that could drift from it, and bannedForms fails if
// the object stops being readable, so a reshaped list turns this red rather
// than turning the check off.

var (
	bannedFormsRE = regexp.MustCompile(`(?s)const BANNED_FORMS = \{\n(.*?)\n\};`)
	bannedLineRE  = regexp.MustCompile(`^\s*([a-z]+): \[((?:"[a-z]+"(?:, )?)+)\],\s*$`)
	quotedRE      = regexp.MustCompile(`"([a-z]+)"`)
)

// bannedForms returns every banned form, mapped to the word it belongs to.
func bannedForms(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "console-e2e", "first_run_scoreboard.mjs"))
	if err != nil {
		t.Fatal(err)
	}
	m := bannedFormsRE.FindSubmatch(b)
	if m == nil {
		t.Fatal("first_run_scoreboard.mjs has no `const BANNED_FORMS = {...};` block this test can read; " +
			"the word list moved or changed shape, so update bannedForms to read it again")
	}
	forms := map[string]string{}
	words := 0
	for _, line := range strings.Split(string(m[1]), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		lm := bannedLineRE.FindStringSubmatch(line)
		if lm == nil {
			t.Fatalf("BANNED_FORMS line %q is not `word: [\"form\", ...],`; every line must be read, "+
				"or a word would silently drop out of this check", line)
		}
		words++
		self := false
		for _, q := range quotedRE.FindAllStringSubmatch(lm[2], -1) {
			forms[q[1]] = lm[1]
			self = self || q[1] == lm[1]
		}
		if !self {
			t.Fatalf("BANNED_FORMS[%q] does not list the word itself among its forms", lm[1])
		}
	}
	if words == 0 {
		t.Fatal("BANNED_FORMS read as empty")
	}
	return forms
}

func TestWordList_isReadFromTheWalksScoreboard(t *testing.T) {
	forms := bannedForms(t)
	// A spot check that the read found the real list and not a fragment:
	// the first and last words, and an inflection.
	for form, word := range map[string]string{"index": "index", "stream": "stream", "backups": "backup"} {
		if forms[form] != word {
			t.Errorf("the list read from the scoreboard does not map %q to %q (got %q)", form, word, forms[form])
		}
	}
}

// "database" is not on the walk's list: the web interface still says it in
// fixed phrases the walk counts (the Snapshots redesign names them), and
// adding it there would move the walk's ratchet. The onboarding text this
// package owns uses "your MySQL" and "server" instead, and has no fixed
// phrase that needs the word, so the allowed list is empty here.
var databaseFixedPhrases = []string{}

var (
	urlRE      = regexp.MustCompile(`https?://\S+`)
	databaseRE = regexp.MustCompile(`(?i)\bdatabases?\b`)
	// A line that is something to run. The text after a # on it is a
	// comment a person reads, so it is still a sentence.
	commandRE = regexp.MustCompile(`^(docker compose |docker-compose |docker exec |curl -|cd )`)
)

// sentenceHits returns every banned word or em dash in the sentences of
// text, with the command part of command lines, URLs and the given paths
// left out: they are copied, not read, and a path under a temporary folder
// must not decide the result.
func sentenceHits(t *testing.T, text string, paths ...string) []string {
	t.Helper()
	forms := bannedForms(t)
	var hits []string
	for _, line := range strings.Split(text, "\n") {
		for _, p := range paths {
			line = strings.ReplaceAll(line, p, "")
		}
		line = strings.TrimSpace(line)
		if commandRE.MatchString(line) {
			c := strings.Index(line, "#")
			if c < 0 {
				continue
			}
			line = line[c+1:]
		}
		line = urlRE.ReplaceAllString(line, "")
		if strings.Contains(line, "—") {
			hits = append(hits, "em dash in: "+line)
		}
		for _, w := range regexp.MustCompile(`[A-Za-z]+`).FindAllString(line, -1) {
			if word, ok := forms[strings.ToLower(w)]; ok && isWholeWord(line, w) {
				hits = append(hits, word+" in: "+line)
			}
		}
		rest := line
		for _, p := range databaseFixedPhrases {
			rest = strings.ReplaceAll(rest, p, "")
		}
		if databaseRE.MatchString(rest) {
			hits = append(hits, "database in: "+line)
		}
	}
	return hits
}

// isWholeWord mirrors the scoreboard's \b boundaries: "index_dsn" and
// "INDEX_DSN" run on into an underscore and are identifiers, not words.
func isWholeWord(line, w string) bool {
	re := regexp.MustCompile(`(^|[^A-Za-z0-9_])` + regexp.QuoteMeta(w) + `($|[^A-Za-z0-9_])`)
	return re.MatchString(line)
}

func TestWordList_sentenceHitsSeesWhatItShould(t *testing.T) {
	for _, tc := range []struct {
		line string
		want int
	}{
		{"Open the console.", 1},
		{"Backups are kept.", 1},
		{"Nothing here — at all.", 1},
		{"docker compose logs -f bintrail   # see what it is doing", 0},
		{"docker compose logs -f bintrail   # watch the stream", 2},
		{"cd /tmp/x/index", 0},
		// A status line that starts with the word docker is not a command.
		{"docker ✓   docker compose ✓   daemon ✓", 1},
		{"Set INDEX_DSN in .env.", 0},
		{"See https://example.com/console/index for more.", 0},
		{"The trackpad works.", 0},
		{"your database", 1},
		{"Your MySQL server.", 0},
	} {
		if got := len(sentenceHits(t, tc.line)); got != tc.want {
			t.Errorf("%q: %d hits, want %d: %v", tc.line, got, tc.want, sentenceHits(t, tc.line))
		}
	}
}

// Everything a clean install prints is read by the person installing: the
// banner, the progress lines and the end text. None of it may use a word
// from the list, and the end text says what to have at hand, where the
// history lives, how to stop, and where the start page is.
func TestInstaller_aCleanRunSpeaksTheWordList(t *testing.T) {
	r := install(t)
	if r.failed || !strings.Contains(r.out, "DBTrail is up.") {
		t.Fatalf("the clean install did not finish:\n%s", r.out)
	}
	if n := strings.Count(r.out, "\n"); n < 20 {
		t.Fatalf("the clean install printed only %d lines; the check below would prove nothing:\n%s", n, r.out)
	}
	for _, h := range sentenceHits(t, r.out, r.dir) {
		t.Errorf("banned in a sentence the installer prints: %s", h)
	}
	for _, want := range []string{
		promise,
		startPage,
		"your MySQL",
		"host and port",
		"a MySQL login that can create users",
		"Docker volumes",
		"docker compose down",
	} {
		if !strings.Contains(r.out, want) {
			t.Errorf("the clean install does not say %q:\n%s", want, r.out)
		}
	}
	if strings.Contains(r.out, "github.com/dbtrail") {
		t.Errorf("the clean install points at the GitHub repository instead of the start page:\n%s", r.out)
	}
}

// The README makes the same promise, and not the older one.
func TestPromise_theREADMEMakesTheSamePromise(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	readme := string(b)
	if !strings.Contains(readme, promise) {
		t.Errorf("README.md does not make the promise word for word: %q", promise)
	}
	if !strings.Contains(readme, startPage) {
		t.Errorf("README.md does not link the start page %s", startPage)
	}
	for _, gone := range []string{"no locks", "no schema changes"} {
		if strings.Contains(strings.ToLower(readme), gone) {
			t.Errorf("README.md still says %q", gone)
		}
	}
}

// The bridge sentence is in the command-line quickstart and in no other
// page of this repository.
func TestBridge_onlyTheCommandLineQuickstartCarriesIt(t *testing.T) {
	root := filepath.Join("..", "..")
	var pages []string
	for _, glob := range []string{"*.md", "docs/*.md", "docs/*/*.md", "deploy/*.md", ".github/*.md"} {
		m, err := filepath.Glob(filepath.Join(root, glob))
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, m...)
	}
	pages = append(pages, filepath.Join(root, "install.sh"))
	found := 0
	for _, p := range pages {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		n := strings.Count(string(b), bridge)
		rel, _ := filepath.Rel(root, p)
		switch {
		case rel == filepath.Join("docs", "quickstart.md") && n != 1:
			t.Errorf("docs/quickstart.md carries the bridge sentence %d times, want once", n)
		case rel != filepath.Join("docs", "quickstart.md") && n > 0:
			t.Errorf("%s carries the bridge sentence; only docs/quickstart.md may", rel)
		}
		found += n
	}
	if len(pages) < 10 {
		t.Fatalf("found only %d pages to look in; the globs are wrong", len(pages))
	}
	if found == 0 {
		t.Error("no page carries the bridge sentence")
	}
}
