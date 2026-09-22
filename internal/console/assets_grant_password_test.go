package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The grant block on + Add server is copied and run as written: a walk of the
// first run pasted it verbatim, so a literal password in it became the real
// password of a replication user on a live MySQL. These tests pin that the
// block carries a password generated for this form, the same one the form
// saves, and never a literal anyone else can read in the source.

// runGrantJS evaluates the page's own sqlString, genSourcePassword and
// grantBlocks under node, followed by body, and returns what body printed.
func runGrantJS(t *testing.T, body string) string {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	js := readAsset(t, "app.js")
	script := functionBody(t, js, "function sqlString(") + "\n" +
		functionBody(t, js, "function genSourcePassword(") + "\n" +
		functionBody(t, js, "function grantBlocks(") + "\n" + body
	path := filepath.Join(t.TempDir(), "grant.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestGrantBlockCarriesNoLiteralPassword(t *testing.T) {
	js := readAsset(t, "app.js")
	if strings.Contains(js, "strong-password") {
		t.Error("app.js still carries the literal 'strong-password' a pasted block would create a user with")
	}
	// Any other quoted literal after IDENTIFIED BY would be the same defect.
	if m := regexp.MustCompile(`IDENTIFIED BY '[^']*'`).FindString(js); m != "" {
		t.Errorf("app.js carries a literal password in a grant: %s", m)
	}
}

func TestGrantBlockUsesTheFormsUserAndPassword(t *testing.T) {
	out := runGrantJS(t, `console.log(JSON.stringify(grantBlocks("dbtrail", "Ab3-xyzXYZ789_qq")));`)
	var b map[string]string
	if err := json.Unmarshal([]byte(out), &b); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	for _, flavor := range []string{"mysql", "mariadb"} {
		if !strings.Contains(b[flavor], "CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'Ab3-xyzXYZ789_qq';") {
			t.Errorf("%s: the block does not create the user with the form's password:\n%s", flavor, b[flavor])
		}
	}
}

// With no password in the form (editing a saved server, or no random source),
// the block must not be runnable as-is: a placeholder that MySQL rejects as a
// syntax error, never a quoted string it would accept.
func TestGrantBlockWithoutPasswordCannotRunAsIs(t *testing.T) {
	out := runGrantJS(t, `console.log(grantBlocks("dbtrail", "").mysql.split("\n")[0]);`)
	if !strings.HasPrefix(out, "CREATE USER 'dbtrail'@'%' IDENTIFIED BY <") {
		t.Errorf("a block without a password must hold an unquoted placeholder MySQL refuses, got %q", out)
	}
}

func TestGrantBlockQuotesUserAndPassword(t *testing.T) {
	out := runGrantJS(t, `const b = grantBlocks("o'brien", "a'b\\c");
console.log(JSON.stringify(b.mysql.split("\n").slice(0, 2)));`)
	var lines []string
	if err := json.Unmarshal([]byte(out), &lines); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if lines[0] != `CREATE USER 'o''brien'@'%' IDENTIFIED BY 'a''b\\c';` {
		t.Errorf("CREATE USER line not quoted for MySQL: %q", lines[0])
	}
	if !strings.Contains(lines[1], `TO 'o''brien'@'%';`) {
		t.Errorf("GRANT line does not name the same quoted account: %q", lines[1])
	}
}

// A blank user field falls back to the account the block has always named,
// so the block never reads ''@'%' (the anonymous user).
func TestGrantBlockNeverNamesTheAnonymousUser(t *testing.T) {
	out := runGrantJS(t, `console.log(grantBlocks("  ", "Ab3-xyzXYZ789_qq").mysql.split("\n")[0]);`)
	if !strings.HasPrefix(out, "CREATE USER 'dbtrail'@'%'") {
		t.Errorf("a blank user must fall back to 'dbtrail', got %q", out)
	}
}

// The generated password must pass MySQL's validate_password MEDIUM policy
// (length, both cases, a digit, a special character), carry nothing that
// needs quoting, and differ every time.
func TestGeneratedPasswordPassesMediumPolicyAndNeverRepeats(t *testing.T) {
	out := runGrantJS(t, `const a = [];
for (let i = 0; i < 200; i++) a.push(genSourcePassword());
console.log(JSON.stringify(a));`)
	var pws []string
	if err := json.Unmarshal([]byte(out), &pws); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	seen := map[string]bool{}
	for _, p := range pws {
		if len(p) < 20 {
			t.Errorf("password too short: %q", p)
		}
		if !regexp.MustCompile(`[a-z]`).MatchString(p) || !regexp.MustCompile(`[A-Z]`).MatchString(p) ||
			!regexp.MustCompile(`[0-9]`).MatchString(p) || !regexp.MustCompile(`[^A-Za-z0-9]`).MatchString(p) {
			t.Errorf("password misses a character class validate_password MEDIUM requires: %q", p)
		}
		if strings.ContainsAny(p, `'"\`+"`$ %") {
			t.Errorf("password carries a character that needs quoting in SQL or a shell: %q", p)
		}
		if seen[p] {
			t.Errorf("password repeated: %q", p)
		}
		seen[p] = true
	}
}

// With no cryptographic random source, the page must not invent a weak
// password: it leaves the field empty and the block shows the placeholder.
func TestGeneratedPasswordNeedsACryptoSource(t *testing.T) {
	// Node defines crypto as an accessor, so plain assignment is ignored.
	out := runGrantJS(t, `Object.defineProperty(globalThis, "crypto", { value: undefined, configurable: true });
console.log(JSON.stringify(genSourcePassword()));`)
	if out != `""` {
		t.Errorf("without crypto.getRandomValues the page must not generate a password, got %s", out)
	}
}

// The wiring: a NEW server gets a generated password and the user the block
// names; editing a saved one never does (its field stays blank = keep the
// stored password). Both fields redraw the block when edited.
func TestServerFormGeneratesOnlyForANewServer(t *testing.T) {
	show := functionBody(t, readAsset(t, "app.js"), "function showServerForm(")
	if !strings.Contains(show, "genSourcePassword()") {
		t.Fatal("showServerForm no longer fills a generated password")
	}
	gen := strings.Index(show, "genSourcePassword()")
	guard := strings.LastIndex(show[:gen], "if (!(prefill && prefill.id))")
	if guard < 0 {
		t.Error("the generated password is not guarded to a new server; editing a saved one would overwrite its stored password on Save")
	}
	for _, f := range []string{`"source_user"`, `"source_password"`} {
		if !strings.Contains(show, f) {
			t.Errorf("showServerForm does not redraw the block when %s changes", f)
		}
	}
}

func TestServerFormHintNamesTheRequiredFields(t *testing.T) {
	js := readAsset(t, "app.js")
	if strings.Contains(js, "Nothing else to fill in beyond a name") {
		t.Error("the add-server hint still claims a name is the only field, while host, user and password are required")
	}
}
