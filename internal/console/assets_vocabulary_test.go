package console

import (
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
// LITERAL of app.js and of the non-test Go files of this package. Nothing
// else — an identifier like backupScheduleCard has no word boundary after
// "backup" and never matches, and comments are not counted because they are
// not text this program can emit.
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
// Do not narrow this one to match that one.
var oldVocabulary = regexp.MustCompile(`(?i)\b(backups?|baselines?)\b`)

// SCOPE, and the one way this can be satisfied without doing the work:
// filepath.Glob("*.go") reads THIS package only. Text that MOVES OUT of
// internal/console — into consoleapp/ or ext/ during the rename — leaves the
// count and so reads as progress. Relocation is not renaming; if a step
// moves copy out, check it there by hand.
//
// The pins. Both were measured on the commit that introduced this test, and
// were cross-checked against an independent tokenizer that agreed on both
// numbers.
//
// The rule is NOT "must equal". Over the pin fails: that is the regression
// this exists to catch. Under it is allowed, up to vocabularySlack, and only
// a bigger drop asks for the pin to be lowered. The slack is there because
// several branches edit this package at once: two branches each removing one
// occurrence and each pinning one lower both merge, the count lands two
// under, and a must-equal rule would turn main red for a change no single
// pull request made. A rename step moves these by hundreds, which is well
// past the slack, so the pin still gets lowered exactly when it should be.
const (
	assetVocabularyPin = 282 // string literals in assets/app.js
	goVocabularyPin    = 225 // string literals in this package's non-test .go files
	vocabularySlack    = 20  // how far under a pin may drift before it must be lowered
)

func TestOldVocabularyOnlyShrinks(t *testing.T) {
	assets := countInJSStrings(t, readAsset(t, "app.js"))
	gone := countInGoStrings(t)
	for _, c := range []struct {
		what string
		got  int
		pin  int
		how  string
	}{
		{"assets/app.js", assets, assetVocabularyPin, "assetVocabularyPin"},
		{"the package's Go files", gone, goVocabularyPin, "goVocabularyPin"},
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
		}
	}
}

// countInGoStrings counts the vocabulary in the string literals of every
// non-test .go file of this package. go/parser does the work, so what counts
// as a string is the language's answer, not a guess.
func countInGoStrings(t *testing.T) int {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
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
func countInJSStrings(t *testing.T, src string) int {
	t.Helper()
	n, i := 0, 0
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
			i = skipRegex(t, src, i)
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
			prev, prevWord = src[j-1], src[i:j]
			i = j
		default:
			if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
				prev, prevWord = c, ""
			}
			i++
		}
	}
	return n
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

func skipRegex(t *testing.T, src string, i int) int {
	t.Helper()
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
				return i + 1
			}
		case '\n':
			t.Fatal("app.js: newline inside what was read as a regular expression")
		}
		i++
	}
	t.Fatal("app.js: unterminated regular expression")
	return i
}

// scanString returns the contents of the string literal starting at i, and
// the index just past its closing quote. Escapes are stepped over rather
// than decoded: no escape sequence in this file spells a letter, so the word
// boundaries are the same either way.
func scanString(t *testing.T, src string, i int) (string, int) {
	t.Helper()
	quote := src[i]
	start := i + 1
	for j := start; j < len(src); j++ {
		switch src[j] {
		case '\\':
			j++
		case quote:
			return src[start:j], j + 1
		case '\n':
			if quote != '`' {
				t.Fatalf("app.js: newline inside a %c-quoted string at offset %d", quote, start)
			}
		}
	}
	t.Fatalf("app.js: unterminated %c-quoted string at offset %d", quote, start)
	return "", len(src)
}
