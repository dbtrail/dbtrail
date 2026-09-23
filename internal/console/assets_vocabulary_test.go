package console

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The old vocabulary — "backup(s)" and "baseline(s)" — is being replaced by
// "snapshot" across the console (#1573). The rename itself is a later step;
// what lands here is the ratchet that keeps it from going backwards, and
// keeps a new sentence from arriving in the old words in the meantime.
//
// WHAT IS COUNTED: every occurrence of those four words inside a STRING
// LITERAL — every one, including import paths and struct tags, which are
// plainly not text this program can emit. An identifier like
// backupScheduleCard never matches, because \b finds no boundary after
// "backup" there (and `_` is a word character, so baseline_dir does not
// match either). Comments are out for a mechanical reason rather than a
// principled one: go/parser is called without ParseComments, and the JS
// scanner steps over them. Do not restate that as a rule about what reaches
// a screen — the import paths would contradict it, and a rule nobody can
// apply consistently is how a guard starts getting argued with.
//
// WHY FROZEN NAMES ARE COUNTED TOO. Route paths, CLI flags, environment
// variables, JSON fields, CSS classes, the S3 rule name — the redesign
// freezes all of those, and it would read as obvious to exclude them. They
// are counted anyway, for two reasons. They are STABLE: a frozen name does
// not churn, so it costs the ratchet nothing to carry. And every rule that
// would exclude them needs a judgment this test cannot make reliably —
// "baseline-anchored" is an identifier while "Create-backup button" is
// English, and both are one lowercase word, one hyphen, one more word. A
// guard that guesses wrong in that direction goes quiet while new prose
// arrives, which is the failure it exists to prevent. Guessing wrong the
// other way just makes it complain, and a complaint is cheap to answer: if
// a change genuinely adds a frozen name, lower or raise the pin in the same
// commit and say why in the message.
//
// FOLD-BLIND, ON PURPOSE. This reads the source, so text that only appears
// behind a closed <details> counts exactly as much as text on the first
// screen — a reader who clicks still reads the word. The walk's own measure
// (test/console-e2e/first_run_measure.js, inClosedDetails) is deliberately
// the opposite: it answers what a first run meets WITHOUT clicking. The two
// can move in opposite directions on one commit, and that is them working.
// Do not narrow this one to match that one. They are not the same rule over
// the same words either: the walk bans twelve words plus the em dash, this
// one covers two of them.
//
// WHAT IT CANNOT CATCH, so nobody reads a green run as more than it is:
// it counts occurrences, not sentences. Remove one "backup" from an old
// literal, add a new sentence carrying one somewhere else, and the count is
// unchanged. The rename is what fixes that; this only keeps the total from
// climbing while the rename is in progress.
var oldVocabulary = regexp.MustCompile(`(?i)\b(backups?|baselines?)\b`)

// SCOPE. Two directories are read: this package, and consoleapp, whose
// strings reach the same screens — the daemon composes a build's staging
// error there and app.js renders it verbatim, so prose left unguarded in
// consoleapp is prose on the console. Neither glob recurses.
//
// What is still outside: ext, assets/index.html (its occurrences today are
// all inside HTML comments) and style.css. Text that MOVES to one of those
// during the rename leaves the count and so reads as progress. Relocation
// is not renaming; if a step moves copy out, check it there by hand.
//
// The pins were each measured three ways and agreed exactly: this scanner,
// a separate tokenizer written without sight of it, and a byte-for-byte
// comparison of this scanner's string ranges against acorn's over the real
// app.js (282 = 282, no desync anywhere in the file). Repeat it that way if
// a number ever looks wrong.
//
// The rule is NOT "must equal". Over the pin fails: that is the regression
// this exists to catch. Under it is allowed, up to vocabularySlack, and only
// a bigger drop asks for the pin to be lowered. The slack is there because
// several branches edit this package at once: two branches each removing one
// occurrence and each pinning one lower both merge, the count lands two
// under, and a must-equal rule would turn main red for a change no single
// pull request made.
//
// The slack is also exactly how much this guard can be beaten by: a drop
// that stays inside it never forces the pin down, so that many occurrences
// can come back later with the test still green. That is why it is three:
// enough for merges that land together, too small to hide a rename step. A
// run under a pin also logs the gap, but only `go test -v` prints a passing
// test's log, and CI does not run with -v — so the gap is visible locally on
// request, not announced.
const (
	assetVocabularyPin      = 282 // string literals in assets/app.js
	goVocabularyPin         = 225 // string literals in this package's non-test .go files
	consoleappVocabularyPin = 277 // string literals in consoleapp's non-test .go files
	vocabularySlack         = 3   // how far under a pin may drift before it must be lowered

	// Regular expressions in app.js that carry a quote character. Pinned
	// because a change here means one of two things, and both matter: a new
	// regex legitimately carries a quote (raise it), or a division was
	// misread as a regex, in which case the scan has swallowed whatever lay
	// between the two slashes and the counts above cannot be trusted. Left
	// unpinned, that second case is silent — and silence is worse now that
	// the counts tolerate drifting under their pins.
	quoteCarryingRegexes = 9
)

func TestOldVocabularyOnlyShrinks(t *testing.T) {
	assets, quoted := countInJSStrings(t, readAsset(t, "app.js"))
	if len(quoted) != quoteCarryingRegexes {
		t.Errorf("app.js: %d regular expressions carry a quote character, pinned at %d. Either a new "+
			"one does (raise quoteCarryingRegexes), or a division was read as a regular expression — "+
			"in which case the scan ran past it and every count below is wrong. What was read as one:\n\t%s",
			len(quoted), quoteCarryingRegexes, strings.Join(quoted, "\n\t"))
	}
	for _, c := range []struct {
		what string
		got  int
		pin  int
		how  string
	}{
		{"assets/app.js", assets, assetVocabularyPin, "assetVocabularyPin"},
		{"internal/console's Go files", countInGoStrings(t, "."), goVocabularyPin, "goVocabularyPin"},
		{"consoleapp's Go files", countInGoStrings(t, "../../consoleapp"), consoleappVocabularyPin, "consoleappVocabularyPin"},
	} {
		switch {
		case c.got > c.pin:
			t.Errorf("%s: %d occurrences of the old vocabulary (backup/backups/baseline/baselines) "+
				"in string literals, up from %d.\n"+
				"The console is being renamed to \"snapshot\" (#1573) and this count may only go down. "+
				"If the new text is unavoidable — a frozen route, flag, environment variable or file "+
				"name — raise %s in this commit and say why in the message.",
				c.what, c.got, c.pin, c.how)
		case c.got < c.pin-vocabularySlack:
			t.Errorf("%s: %d occurrences of the old vocabulary, %d under the pin of %d. Lower %s to "+
				"%d in this commit, so the next change cannot spend the ground already gained.\n"+
				"If you removed none of these words yourself, concurrent merges did: lowering the pin "+
				"is still the right answer.",
				c.what, c.got, c.pin-c.got, c.pin, c.how, c.got)
		case c.got < c.pin:
			t.Logf("%s: %d occurrences, %d under the pin of %d: room a later change could fill back "+
				"up without failing this test. Lower %s to %d.", c.what, c.got, c.pin-c.got, c.pin, c.how, c.got)
		}
	}
}

// countInGoStrings counts the vocabulary in the string literals of every
// non-test .go file directly in dir. go/parser does the work, so what counts
// as a string is the language's answer, not a guess.
func countInGoStrings(t *testing.T, dir string) int {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	// A directory that answers nothing is a moved or renamed package, not a
	// clean one: it would read as the count dropping to zero.
	if len(names) == 0 {
		t.Fatalf("no .go files under %s — did the package move? This guard counts its text.", dir)
	}
	fset := token.NewFileSet()
	n := 0
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(node ast.Node) bool {
			lit, isLit := node.(*ast.BasicLit)
			if !isLit || lit.Kind != token.STRING {
				return true
			}
			// Unquote so an escape sequence cannot hide or invent a word
			// boundary. A raw or malformed literal is counted as written.
			s := lit.Value
			if un, err := strconv.Unquote(s); err == nil {
				s = un
			}
			n += len(oldVocabulary.FindAllString(s, -1))
			return true
		})
	}
	return n
}

// countInJSStrings counts the vocabulary inside the string literals of a
// JavaScript source: double-quoted, single-quoted and template literals.
// Comments are skipped, and so are regular-expression literals — app.js has
// at least one carrying a quote character (/Unknown database '([^']+)'/),
// and a scanner that read that quote as the start of a string would swap
// code and text for the rest of the file.
//
// Telling a regex literal from a division is the one ambiguity in JS
// tokenizing, and it is resolved the usual way: a '/' opens a regex when the
// last thing that mattered cannot end an expression (an operator, a comma,
// an open bracket, the start of the file). After a name, a number or a
// closing bracket, '/' is division — EXCEPT after a keyword, which is where
// the character before the slash stops being enough. This file has
//
//	return /[",\r\n]/.test(s) ? ...
//
// whose last character is the "n" of return, and reading that as division
// swallows the quote inside the character class and swaps code for text from
// there on. So the previous WORD is tracked, not only the previous byte.
func countInJSStrings(t scanT, src string) (int, []string) {
	t.Helper()
	n, i := 0, 0
	var quoteRegexes []string
	prev := byte(0) // last significant character, for the regex/division call
	prevWord := ""  // and the identifier it belonged to, when it was one
	for i < len(src) {
		c := src[i]
		switch {
		case c == '/' && i+1 < len(src) && src[i+1] == '/':
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(src) && src[i+1] == '*':
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				t.Fatal("app.js: unterminated block comment")
			}
			i += 2 + end + 2
		case c == '/' && regexCanStart(prev, prevWord):
			from := i
			var quoted bool
			i, quoted = skipRegex(t, src, i)
			if quoted {
				quoteRegexes = append(quoteRegexes, src[from:i])
			}
			prev, prevWord = '/', ""
		case c == '"' || c == '\'' || c == '`':
			var lit string
			lit, i = scanString(t, src, i)
			n += len(oldVocabulary.FindAllString(lit, -1))
			prev, prevWord = c, ""
		case isWordByte(c):
			j := i
			for j < len(src) && isWordByte(src[j]) {
				j++
			}
			// A keyword only reads as a keyword when nothing dotted into it.
			// app.js has `t.delete`, `s.delete`, `vfyLive.delete` and more,
			// and `s.delete / s.total` is one edit away: read as a regex, the
			// scan runs to the NEXT slash and swallows whatever text is
			// between them.
			if prev == '.' {
				prevWord = ""
			} else {
				prevWord = src[i:j]
			}
			prev = src[j-1]
			i = j
		case (c == '+' || c == '-') && i+1 < len(src) && src[i+1] == c:
			// ++ and -- end an expression (i++ / 2 divides), but a lone + or -
			// is an operator after which a regex may start. Read as two
			// operators, `i++ / 2 + " backup " + b / 3` became one long regex
			// and the word inside it went uncounted without a sound.
			prev, prevWord = ')', ""
			i += 2
		default:
			if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
				prev, prevWord = c, ""
			}
			i++
		}
	}
	return n, quoteRegexes
}

// scanT is the slice of *testing.T the JS scanner uses, so the scanner's own
// test can check that a bad input fails loudly instead of miscounting.
type scanT interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

func isWordByte(c byte) bool {
	return c == '_' || c == '$' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// The keywords a regular expression may follow. After anything else that
// ends an expression — a name, a number, a closing bracket — a '/' divides.
var regexKeywords = map[string]bool{
	"return": true, "typeof": true, "instanceof": true, "in": true, "of": true,
	"new": true, "delete": true, "void": true, "throw": true, "case": true,
	"do": true, "else": true, "yield": true, "await": true,
}

// regexCanStart reports whether a '/' here opens a regular expression rather
// than dividing by something.
func regexCanStart(prev byte, prevWord string) bool {
	if prev == 0 || strings.IndexByte("=(,:[!&|?{};+-*%<>~^", prev) >= 0 {
		return true
	}
	return prevWord != "" && regexKeywords[prevWord]
}

// skipRegex steps over a regular-expression literal and reports whether it
// carried a quote character. That second answer is the scanner's own desync
// alarm: a real regex here may carry one (/Unknown database '([^']+)'/
// does), but so does a DIVISION misread as a regex — and that one silently
// swallows whatever strings lie between the two slashes, or silently counts
// a regex's own backticked words as prose. Counting them and pinning the
// count is what makes either event loud.
func skipRegex(t scanT, src string, i int) (int, bool) {
	t.Helper()
	start := i
	quoted := false
	i++ // the opening '/'
	class := false
	for i < len(src) {
		switch src[i] {
		case '\\':
			i++ // whatever follows is literal
		case '[':
			class = true
		case ']':
			class = false
		case '/':
			if !class {
				end := i + 1
				checkAfterRegex(t, src, end, start)
				return end, quoted
			}
		case '\n':
			t.Fatalf("app.js: newline inside what was read as a regular expression at offset %d — "+
				"a '/' was misread as opening one, and the scan ran past the end of its line", start)
		case '"', '\'', '`':
			quoted = true
		}
		i++
	}
	t.Fatalf("app.js: unterminated regular expression at offset %d", start)
	return i, quoted
}

// checkAfterRegex is the backstop for a DIVISION misread as a regex, in
// the cases the rules above do not foresee. A real regex literal is followed
// by its flags and then by something that can follow an expression; a
// division misread as a regex ends wherever the next slash happened to be,
// usually in the middle of an expression. Failing there turns a silent
// miscount into a loud one. The set it accepts is what this file writes
// after a regex, not everything JavaScript allows, so a new shape can fail
// here while being valid: extend the set when that happens.
//
// It cannot see the OPPOSITE mistake, a real regex read as a division
// (after a closing paren: `if (x) /re/`). That regex is then scanned as
// code, and a quote inside it opens a string that is not there. With an odd
// number of such quotes the scan fails loudly at the end of a line or the
// file; with an even number it can miscount silently. app.js has no regex
// after a closing paren today.
func checkAfterRegex(t scanT, src string, i, start int) {
	t.Helper()
	for i < len(src) && strings.IndexByte("dgimsuyv", src[i]) >= 0 {
		i++
	}
	for i < len(src) && (src[i] == ' ' || src[i] == '\t') {
		i++
	}
	if i >= len(src) || strings.IndexByte(".),;]}?:|&=!+\n\r", src[i]) >= 0 {
		return
	}
	// A comment after a regex: `re = /x/g // why`.
	if src[i] == '/' && i+1 < len(src) && (src[i+1] == '/' || src[i+1] == '*') {
		return
	}
	t.Fatalf("app.js: what was read as a regular expression at offset %d is followed by %q, which "+
		"this scanner does not expect after one. Either a division was read as a regex (and the "+
		"scan swallowed everything between two slashes), or a valid new shape needs adding to "+
		"checkAfterRegex's set.", start, src[i])
}

// scanString returns the contents of the string literal starting at i, with
// its escape sequences decoded, and the index just past its closing quote.
//
// Decoded, not kept: a kept escape puts its letter against the next word —
// "\nBackups" reads as "nBackups", \b finds no boundary there, and the
// occurrence goes uncounted. \n \r \t \b \f \v become a space (what they
// are, for word boundaries), \xHH \uXXXX \u{...} become their character
// ("\x62ackup" is "backup"), and any other escaped character stands for
// itself.
//
// Inside a template literal, ${...} holds code. Its braces are counted to
// find where it ends, and strings inside it are stepped over whole, so a
// "}" in one does not end the placeholder early. A backtick inside ${...}
// opens a NESTED template, which this scanner would take for the outer one
// closing: it fails instead. The code inside ${...} is counted as if it were
// text, which errs toward counting.
func scanString(t scanT, src string, i int) (string, int) {
	t.Helper()
	quote := src[i]
	start := i + 1
	var b strings.Builder
	depth := 0 // brace depth inside ${...}, template literals only
	for j := start; j < len(src); j++ {
		c := src[j]
		switch {
		case c == '\\':
			dec, n := decodeEscape(src, j)
			b.WriteString(dec)
			j += n - 1
			continue
		case depth > 0 && c == '`':
			t.Fatalf("app.js: a template literal nested inside ${...} at offset %d — this scanner "+
				"cannot follow it; teach scanString nesting before writing one", j)
		case depth > 0 && (c == '"' || c == '\''):
			lit, end := scanString(t, src, j)
			b.WriteString(lit)
			j = end - 1
			continue
		case quote == '`' && c == '$' && j+1 < len(src) && src[j+1] == '{':
			depth++
			b.WriteString("${")
			j++
			continue
		case depth > 0 && c == '{':
			depth++
		case depth > 0 && c == '}':
			depth--
		case depth == 0 && c == quote:
			return b.String(), j + 1
		case c == '\n' && quote != '`':
			t.Fatalf("app.js: newline inside a %c-quoted string at offset %d", quote, start)
		}
		b.WriteByte(c)
	}
	t.Fatalf("app.js: unterminated %c-quoted string at offset %d", quote, start)
	return "", len(src)
}

// decodeEscape decodes the escape sequence at the backslash at i, returning
// what it stands for and how many bytes it spans. Malformed input (only
// possible in JavaScript that would not parse) stands for itself.
func decodeEscape(src string, i int) (string, int) {
	if i+1 >= len(src) {
		return "\\", 1
	}
	hex := func(from, to int) (string, int, bool) {
		if to > len(src) {
			return "", 0, false
		}
		v, err := strconv.ParseUint(src[from:to], 16, 32)
		if err != nil {
			return "", 0, false
		}
		return string(rune(v)), to - i, true
	}
	switch c := src[i+1]; c {
	case 'n', 'r', 't', 'b', 'f', 'v':
		return " ", 2
	case 'x':
		if r, n, ok := hex(i+2, i+4); ok {
			return r, n
		}
	case 'u':
		if i+2 < len(src) && src[i+2] == '{' {
			if end := strings.IndexByte(src[i+3:], '}'); end >= 0 && end <= 6 {
				if r, n, ok := hex(i+3, i+3+end); ok {
					return r, n + 1
				}
			}
		} else if r, n, ok := hex(i+2, i+6); ok {
			return r, n
		}
	}
	return string(src[i+1]), 2
}

// The inputs that broke earlier versions of this scanner, each turning a
// word into an uncounted one without a sound. None occurs in app.js today;
// the point is that none can arrive silently. Each must either count the
// word or fail loudly — never neither.
func TestVocabularyScannerDoesNotGoQuiet(t *testing.T) {
	for _, c := range []struct {
		name, src string
		want      int    // -1: must fail loudly
		why       string // for -1: what the failure must say, so a case cannot pass by failing for another reason
	}{
		{"an escape against the word", `a = "First line.\nBackups are kept.";`, 1, ""},
		{"a tab escape against the word", `a = "x\tbaseline";`, 1, ""},
		{"a unicode escape against the word", `a = "\u2014backup";`, 1, ""},
		{"a unicode escape that spells the letter", `a = "\u0062ackup";`, 1, ""},
		{"a braced one", `a = "\u{62}ackup";`, 1, ""},
		{"a hex escape that spells the letter", `a = "\x62ackup";`, 1, ""},
		{"a keyword-named property before a division", `a = o.delete / 2 + " backup " + b / 3;`, 1, ""},
		{"another one", `a = o.in / 2 + " backup " + b / 3;`, 1, ""},
		{"a postfix increment before a division", `a = i++ / 2 + " backup " + b / 3;`, 1, ""},
		{"a postfix decrement before a division", `a = i-- / 2 + " backup " + b / 3;`, 1, ""},
		{"a keyword that really opens a regex", `function f(s) { return /[",]/.test(s) ? " backup " : ""; }`, 1, ""},
		// A real regex read as a division: loud only because its quote count
		// is odd (see checkAfterRegex for the even case it cannot see).
		{"a regex after a closing paren, odd quotes", `if (x) /"/.test(y); a = " backup ";`, -1, "unterminated"},
		{"a template nested in ${}", "a = `x ${ok ? `backup` : \"\"} y`;", -1, "nested inside"},
		{"a plain template", "a = `one backup, ${n} baselines`;", 2, ""},
		{"a closing brace in a string inside ${}", "a = `a ${ f(\"}\") } backup`;", 1, ""},
		{"then a nested template after it", "a = `a ${ \"}\" + `backup` } b`;", -1, "nested inside"},
		{"a regex followed by +", `a = /x/ + " backup ";`, 1, ""},
		{"a regex followed by a comment", "a = /x/g // why\nb = \" backup \";", 1, ""},
		// No rule above covers a division after a closing brace; only the
		// check on what follows a regex catches it.
		{"a division no rule foresees", `a = {} / 2 + " backup " + c / d * 3;`, -1, "does not expect after one"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ft := &fatalRecorder{}
			got := -1
			func() {
				defer func() {
					if r := recover(); r != nil && r != errScanFatal {
						panic(r)
					}
				}()
				got, _ = countInJSStrings(ft, c.src)
			}()
			switch {
			case c.want == -1 && !ft.failed:
				t.Errorf("counted %d and said nothing; this input must fail loudly", got)
			case c.want == -1 && !strings.Contains(ft.msg, c.why):
				t.Errorf("failed, but for another reason: %q (want it to say %q)", ft.msg, c.why)
			case c.want >= 0 && ft.failed:
				t.Errorf("failed (%s); want a count of %d", ft.msg, c.want)
			case c.want >= 0 && got != c.want:
				t.Errorf("counted %d, want %d: a word went uncounted without a sound", got, c.want)
			}
		})
	}
}

var errScanFatal = &struct{ string }{"scanner fatal"}

type fatalRecorder struct {
	failed bool
	msg    string
}

func (f *fatalRecorder) Helper() {}
func (f *fatalRecorder) Fatal(args ...any) {
	f.failed, f.msg = true, fmt.Sprint(args...)
	panic(errScanFatal)
}
func (f *fatalRecorder) Fatalf(format string, args ...any) {
	f.failed, f.msg = true, fmt.Sprintf(format, args...)
	panic(errScanFatal)
}
