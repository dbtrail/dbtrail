package recovery

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── Apply-side codegen switches (#1003) ──────────────────────────────────────

// autoIncSection identifies the opt-in AUTO_INCREMENT block. It must be more
// specific than the bare word AUTO_INCREMENT: the always-on header note already
// says "AUTO_INCREMENT/serial counters are NOT restored by this script", so a
// substring check on the word alone would match every script.
const autoIncSection = "AUTO_INCREMENT restore (--restore-auto-increment)"

// applyCodegenRow is a minimal reversible event for the preamble/epilogue tests
// below (a DELETE reverses to an INSERT, which needs only row_before).
func applyCodegenRow(schema, table string) query.ResultRow {
	return query.ResultRow{
		EventID: 1, SchemaName: schema, TableName: table,
		EventType: parser.EventDelete, EventTimestamp: time.Unix(0, 0).UTC(),
		RowBefore: map[string]any{"id": "1"},
	}
}

func mustGenerate(t *testing.T, g *Generator, rows []query.ResultRow) string {
	t.Helper()
	var buf bytes.Buffer
	if _, err := g.GenerateSQLFromRows(rows, &buf); err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	return buf.String()
}

func newSuppressed(d Dialect) *Generator {
	g := NewForDialect(nil, nil, d)
	g.SetSuppressTriggers(true)
	return g
}

// TestSuppressTriggers_PGOnlyAndOptIn pins the #1003 PostgreSQL trigger
// suppression: the SET LOCAL appears only on the PG dialect AND only when the
// operator opted in (it requires superuser / GRANT SET ON PARAMETER and disables
// FK constraint triggers, so it must never be the silent default), and it lands
// INSIDE the transaction — a SET LOCAL outside one would warn and do nothing.
func TestSuppressTriggers_PGOnlyAndOptIn(t *testing.T) {
	const srr = "SET LOCAL session_replication_role = replica;"
	rows := []query.ResultRow{applyCodegenRow("public", "t")}

	pgOn := mustGenerate(t, newSuppressed(PostgresDialect), rows)
	if !strings.Contains(pgOn, srr) {
		t.Errorf("PG script with --suppress-triggers must contain %q, got:\n%s", srr, pgOn)
	}
	begin := strings.Index(pgOn, "\nBEGIN;")
	set := strings.Index(pgOn, srr)
	commit := strings.Index(pgOn, "\nCOMMIT;")
	if begin < 0 || set < begin || commit < set {
		t.Errorf("SET LOCAL session_replication_role must sit between BEGIN and COMMIT, got:\n%s", pgOn)
	}

	pgOff := mustGenerate(t, NewForDialect(nil, nil, PostgresDialect), rows)
	if strings.Contains(pgOff, "session_replication_role") {
		t.Errorf("PG script must NOT suppress triggers by default, got:\n%s", pgOff)
	}

	// MySQL never emits it — with or without the flag. MySQL has no session
	// toggle to suppress triggers; implying otherwise in the script would be a
	// false promise.
	for _, g := range []*Generator{New(nil, nil), newSuppressed(MySQLDialect)} {
		out := mustGenerate(t, g, []query.ResultRow{applyCodegenRow("db", "t")})
		if strings.Contains(out, "session_replication_role") {
			t.Errorf("MySQL script must NEVER emit session_replication_role, got:\n%s", out)
		}
	}
}

// TestRestoreAutoIncrement_MySQLOnlyAfterCommit pins the #1003 AUTO_INCREMENT
// checklist: absent by default, present (commented out, one entry per written
// table, positioned AFTER COMMIT because ALTER TABLE implicitly commits) when
// opted in, and never emitted on the PostgreSQL path.
func TestRestoreAutoIncrement_MySQLOnlyAfterCommit(t *testing.T) {
	rows := []query.ResultRow{applyCodegenRow("db", "orders"), applyCodegenRow("db", "items")}

	off := mustGenerate(t, New(nil, nil), rows)
	if strings.Contains(off, autoIncSection) || strings.Contains(off, "ALTER TABLE") {
		t.Errorf("MySQL script must NOT emit the AUTO_INCREMENT checklist by default, got:\n%s", off)
	}

	g := New(nil, nil)
	g.SetRestoreAutoIncrement(true)
	on := mustGenerate(t, g, rows)

	for _, want := range []string{
		"--   ALTER TABLE `db`.`items` AUTO_INCREMENT = <N>;",
		"--   ALTER TABLE `db`.`orders` AUTO_INCREMENT = <N>;",
		"--   SELECT IFNULL(MAX(`<auto_increment_column>`), 0) + 1 FROM `db`.`orders`;",
	} {
		if !strings.Contains(on, want) {
			t.Errorf("opted-in MySQL script must contain %q, got:\n%s", want, on)
		}
	}
	// Every emitted ALTER must be commented out: an uncommented DDL here would
	// run with an N nobody chose.
	for _, line := range strings.Split(on, "\n") {
		if strings.Contains(line, "ALTER TABLE") && !strings.HasPrefix(strings.TrimSpace(line), "--") {
			t.Errorf("AUTO_INCREMENT statement must be commented out, got line: %q", line)
		}
	}
	commit := strings.Index(on, "\nCOMMIT;")
	alter := strings.Index(on, "ALTER TABLE")
	if commit < 0 || alter < commit {
		t.Errorf("the AUTO_INCREMENT block must follow COMMIT (ALTER TABLE implicitly commits), got:\n%s", on)
	}

	pg := NewForDialect(nil, nil, PostgresDialect)
	pg.SetRestoreAutoIncrement(true)
	out := mustGenerate(t, pg, []query.ResultRow{applyCodegenRow("public", "t")})
	if strings.Contains(out, autoIncSection) {
		t.Errorf("PG script must NEVER emit the MySQL AUTO_INCREMENT checklist, got:\n%s", out)
	}
}

// ─── Comment-injection defense (#1120) ────────────────────────────────────────

// TestCommentInjection_PKNewlineCannotEscapeTheHeaderComment pins the vector
// that ships today: a newline inside a VARCHAR primary key is ordinary data, and
// the per-event header comment interpolated it raw. Because a "--" comment ends
// at the first newline, everything after it became executable SQL INSIDE the
// script's BEGIN/COMMIT.
//
// PKValues is the right probe for a whole-script assertion: it reaches the
// header comment ONLY — the reversal's WHERE/VALUES clauses are rebuilt from the
// row image — so the executable statements stay byte-identical to an ordinary
// script and every remaining line must be a comment or one of them. That is what
// makes this catch an unsanitized site anywhere in the body, not just the one
// line the fix touched.
func TestCommentInjection_PKNewlineCannotEscapeTheHeaderComment(t *testing.T) {
	row := applyCodegenRow("db", "orders")
	row.PKValues = "1\nDROP TABLE users;"

	out := mustGenerate(t, New(nil, nil), []query.ResultRow{row})

	// Positive anchor FIRST: without it this test passes vacuously if the header
	// ever stops rendering the PK — the payload would simply never appear, the
	// loop below would find nothing to reject, and a script that no longer
	// exercises the vector would report success. Asserting the SANITIZED form
	// also pins the other half of the contract: the value is still there, only
	// flattened.
	if !strings.Contains(out, `pk="1\nDROP TABLE users;" at `) {
		t.Fatalf("header must still carry the PK, losslessly escaped, got:\n%s", out)
	}

	// Every statement an ordinary MySQL reversal script is allowed to contain.
	// Exact-string equality, so it can never ADMIT injected SQL. It is coupled to
	// the MySQL preamble and to applyCodegenRow's single-column DELETE shape, so
	// a new preamble statement or a different row shape makes it fail loud —
	// brittle in the safe direction, but read a failure here as possible
	// formatting drift, not only as an injection regression.
	allowed := map[string]bool{
		"BEGIN;":                    true,
		"SET time_zone = '+00:00';": true,
		"SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';": true,
		"COMMIT;": true,
		"INSERT INTO `db`.`orders` (`id`) VALUES ('1');": true,
	}
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || allowed[trimmed] {
			continue
		}
		t.Errorf("line escaped its comment and would execute: %q\nfull script:\n%s", line, out)
	}
}

// TestCommentInjection_HeaderLineSurvivesLineBreaksInEveryField covers the
// per-event header field-by-field. The whole-script test above cannot: it needs
// a CLEAN table name, because a newline inside a backtick-quoted identifier is
// legal MySQL and makes the executable INSERT legitimately span two lines, which
// no "every line is a comment or a known statement" rule can accept.
//
// So this asserts the narrower, sharper invariant instead — the header renders
// as EXACTLY ONE line, carrying every field flattened. Schema, table, PK and
// GTID all reach this comment and nowhere else in the script, so reverting the
// sanitization on any one of them breaks it here.
//
// The schema uses \r rather than \n on purpose: both collapse to the same space,
// so the expectations are unchanged, and it buys carriage-return coverage
// through the real production path — load-bearing because PostgreSQL's lexer
// ends a "--" comment at a bare CR, where MySQL's does not.
func TestCommentInjection_HeaderLineSurvivesLineBreaksInEveryField(t *testing.T) {
	gtid := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1\nDROP TABLE audit;"
	row := applyCodegenRow("d\rb", "or\nders")
	row.PKValues = "1\nDROP TABLE users;"
	row.GTID = &gtid

	out := mustGenerate(t, New(nil, nil), []query.ResultRow{row})

	var headers []string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "-- [") {
			headers = append(headers, line)
		}
	}
	if len(headers) != 1 {
		t.Fatalf("the header must render as exactly 1 line, got %d:\n%s", len(headers), out)
	}
	for _, want := range []string{
		`on "d\rb"."or\nders" pk=`,
		`pk="1\nDROP TABLE users;" at `,
		`gtid="3e11fa47-71ca-11e1-9e33-c80aa9429562:1\nDROP TABLE audit;"`,
	} {
		if !strings.Contains(headers[0], want) {
			t.Errorf("header line must contain %q (losslessly escaped), got:\n%s", want, headers[0])
		}
	}
}

// TestSanitizeForComment_lineBreakForms pins the helper's own contract, which the
// production-path tests above cannot reach exhaustively: the \r-only and \r\n
// forms, and the identity case that keeps every ordinary name byte-identical
// (the property the existing golden-output tests rely on).
func TestSanitizeForComment_lineBreakForms(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"lf", "a\nb", `"a\nb"`},
		{"cr", "a\rb", `"a\rb"`},
		{"crlf", "a\r\nb", `"a\r\nb"`},
		// U+2028 ALONE is left untouched deliberately: it terminates a "--"
		// comment in neither lexer, so quoting it would churn legitimate data.
		// Once a real terminator makes the helper fire, strconv.Quote escapes it
		// too — the emitted line is then single-line under any definition.
		{"line separator alone", "a\u2028b", "a\u2028b"},
		{"line separator with lf", "a\u2028\nb", `"a\u2028\nb"`},
		// A NUL byte ends the statement text for the server's parser (MariaDB
		// answers ER 1064 "near ''"), so a binary PK such as the nil UUID,
		// whose pk_values is 16 NUL bytes (valid UTF-8, so not hex-rendered),
		// broke the whole reversal script from inside its "--" comment.
		{"nul", "a\x00b", `"a\x00b"`},
		{"nil uuid pk", string(make([]byte, 16)), `"` + strings.Repeat(`\x00`, 16) + `"`},
		{"none", "orders", "orders"}, // identity: golden output depends on this
		{"empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SanitizeForComment(tc.in); got != tc.want {
				t.Errorf("SanitizeForComment(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestCommentInjection_TableNewlineCannotEscapeTheAutoIncrementBlock covers the
// sharpest case (#1110's opt-in block): there the "--" prefix is the ENTIRE
// safety mechanism, and the block sits after COMMIT, so anything that breaks out
// runs standalone rather than inside the transaction. MySQL permits any BMP
// character except U+0000 in a backtick-quoted identifier, newline included.
//
// The assertion is over the whole post-COMMIT REGION — which is comment-only by
// design — so a site in that block left unsanitized fails here even though the
// fix never touched its Fprintf.
func TestCommentInjection_TableNewlineCannotEscapeTheAutoIncrementBlock(t *testing.T) {
	g := New(nil, nil)
	g.SetRestoreAutoIncrement(true)
	out := mustGenerate(t, g, []query.ResultRow{applyCodegenRow("db", "or\nders")})

	_, after, found := strings.Cut(out, "\nCOMMIT;\n")
	if !found {
		t.Fatalf("script has no COMMIT to anchor the AUTO_INCREMENT block:\n%s", out)
	}
	if !strings.Contains(after, autoIncSection) {
		t.Fatalf("opted-in script must emit the AUTO_INCREMENT block after COMMIT, got:\n%s", after)
	}
	for _, line := range strings.Split(after, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		t.Errorf("post-COMMIT line is not commented and would execute: %q\nblock:\n%s", line, after)
	}
}

// TestRestoreAutoIncrement_NoRowsEmitsNothing: with no matched events there is
// no table whose counter could need restoring, and the early return must stay a
// bare "no events" line rather than growing a checklist for an empty reversal.
func TestRestoreAutoIncrement_NoRowsEmitsNothing(t *testing.T) {
	g := New(nil, nil)
	g.SetRestoreAutoIncrement(true)
	if out := mustGenerate(t, g, nil); strings.Contains(out, autoIncSection) {
		t.Errorf("empty reversal must not emit the AUTO_INCREMENT checklist, got:\n%s", out)
	}
}
