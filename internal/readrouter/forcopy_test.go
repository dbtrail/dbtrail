package readrouter

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// What the copy is sent (#2081): backtick-quoted names become the same names
// in double quotes, and every other byte is the client's.
func TestForCopy_rewritesBacktickNames(t *testing.T) {
	long := strings.Repeat("n", 64)
	cases := map[string]string{
		"":                          "",
		"SELECT 1":                  "SELECT 1",
		"SELECT `id` FROM `orders`": `SELECT "id" FROM "orders"`,
		"SELECT `orders`.`id` FROM `shop`.`orders` WHERE `id` = 5":  `SELECT "orders"."id" FROM "shop"."orders" WHERE "id" = 5`,
		"SELECT `shop`.`orders`.`id` FROM `shop`.`orders`":          `SELECT "shop"."orders"."id" FROM "shop"."orders"`,
		"SELECT `o`.* FROM `orders` `o`":                            `SELECT "o".* FROM "orders" "o"`,
		"SELECT count(*) AS `total amount`, 1 AS `select` FROM `t`": `SELECT count(*) AS "total amount", 1 AS "select" FROM "t"`,
		"SELECT `año`, `名前`, `a-b`, `a.b`, `a b` FROM `t`":          `SELECT "año", "名前", "a-b", "a.b", "a b" FROM "t"`,
		"SELECT `" + long + "` FROM `t`":                            `SELECT "` + long + `" FROM "t"`,
		"SELECT `a`\nFROM `t`\nWHERE `b` = 1\n":                     "SELECT \"a\"\nFROM \"t\"\nWHERE \"b\" = 1\n",
		"SELECT `a\\b` FROM `t`":                                    `SELECT "a\b" FROM "t"`,
		"SELECT `it's` FROM `t`":                                    `SELECT "it's" FROM "t"`,
		"SELECT `a#b`, `a/*b`, `a - b` FROM `t`":                    `SELECT "a#b", "a/*b", "a - b" FROM "t"`,
		"SELECT `a\xffb` FROM `t`":                                  "SELECT \"a\xffb\" FROM \"t\"",
		// A NUL byte is not a reason to stay on MySQL: outside a string or a
		// comment both servers refuse it, inside one the copy refuses it.
		"SELECT `a\x00b` FROM `t` /* \x00 */": "SELECT \"a\x00b\" FROM \"t\" /* \x00 */",
		// MariaDB executes /*M! only with a capital M.
		"SELECT `a` /*m! +1 */ FROM `t`":                                  "SELECT \"a\" /*m! +1 */ FROM \"t\"",
		"SELECT `a`,`b`FROM`t`":                                           `SELECT "a","b"FROM"t"`,
		"SELECT `id` FROM `t` WHERE `id` IN (1,2) ORDER BY `id` LIMIT 20": `SELECT "id" FROM "t" WHERE "id" IN (1,2) ORDER BY "id" LIMIT 20`,
		// A backtick inside a string literal or a comment is the client's text.
		"SELECT `a` FROM `t` WHERE `b` = 'x`y`z'":       "SELECT \"a\" FROM \"t\" WHERE \"b\" = 'x`y`z'",
		"SELECT `a` FROM `t` WHERE `b` = 'it''s `q`'":   "SELECT \"a\" FROM \"t\" WHERE \"b\" = 'it''s `q`'",
		"SELECT `a` /* `c` 'd */ FROM `t`":              "SELECT \"a\" /* `c` 'd */ FROM \"t\"",
		"SELECT `a` -- `c` don't\nFROM `t`":             "SELECT \"a\" -- `c` don't\nFROM \"t\"",
		"SELECT `a` -- c\r\nFROM `t`\r\n":               "SELECT \"a\" -- c\r\nFROM \"t\"\r\n",
		"SELECT `a`, 'x' `b`, `c` + '1' FROM `t`":       "SELECT \"a\", 'x' \"b\", \"c\" + '1' FROM \"t\"",
		"SELECT `a` FROM `t` -- trailing `c`":           "SELECT \"a\" FROM \"t\" -- trailing `c`",
		"SELECT `a` FROM `t` WHERE `b` = '' AND `c`=''": `SELECT "a" FROM "t" WHERE "b" = '' AND "c"=''`,
		"SELECT `a` FROM `t` WHERE `b` = ?":             `SELECT "a" FROM "t" WHERE "b" = ?`,
		// No backtick: nothing changes, byte for byte.
		"SELECT id FROM orders WHERE note = 'a`b' -- x": "SELECT id FROM orders WHERE note = 'a`b' -- x",
	}
	for stmt, want := range cases {
		got, why := ForCopy(stmt)
		if why != "" {
			t.Errorf("ForCopy(%q) refused: %s", stmt, why)
			continue
		}
		if got != want {
			t.Errorf("ForCopy(%q)\n got %q\nwant %q", stmt, got, want)
		}
		if v := Veto(stmt); v != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, v)
		}
	}
}

// What is never guessed: the statement stays on MySQL, the copy is not tried,
// and ForCopy hands back the client's text untouched.
func TestForCopy_refusesWhatItCannotBeSureOf(t *testing.T) {
	cases := map[string]string{
		"SELECT `a``b` FROM t":        vetoNameQuote,
		"SELECT ```a` FROM t":         vetoNameQuote,
		"SELECT `a``` FROM t":         vetoNameQuote,
		"SELECT `a`` FROM t":          vetoNameQuote,
		"SELECT `a\"b` FROM t":        vetoNameQuote,
		"SELECT `\"` FROM t":          vetoNameQuote,
		"SELECT `a`.`b\"c` FROM t":    vetoNameQuote,
		"SELECT `` FROM t":            vetoNameEmpty,
		"SELECT a FROM t WHERE ``= 1": vetoNameEmpty,
		// A # comment is MySQL's alone (#2111): the copy reads #2 as a column
		// position. The backticks inside it would have been left alone.
		"SELECT `a` # `c` don't\nFROM `t`":                   vetoHash,
		"SELECT `a` FROM `t` # trailing `c`":                 vetoHash,
		"`":                                                  vetoUnterminated,
		"SELECT `a FROM t":                                   vetoUnterminated,
		"SELECT `a` FROM `t":                                 vetoUnterminated,
		"SELECT `a` FROM t WHERE b = 'x":                     vetoUnterminated,
		"SELECT `a` FROM t WHERE b = 'x''":                   vetoUnterminated,
		"SELECT `a` FROM t /* open `b`":                      vetoUnterminated,
		"SELECT a FROM t WHERE b = 'x":                       vetoUnterminated,
		"SELECT `a` FROM t /* a /* b */ WHERE `a` = 1 -- */": vetoNestedComment,
		"SELECT 1 /* a /* b */ + 1 -- */":                    vetoNestedComment,
		"SELECT `count`(*) FROM t":                           vetoNameCall,
		"WITH `p` (`a`) AS (SELECT 1) SELECT `a` FROM `p`":   vetoNameCall,
		"SELECT `abs` (-1)":                                  vetoNameCall,
		"SELECT `abs`\n\t(-1)":                               vetoNameCall,
		"SELECT `abs`\f(-1)":                                 vetoNameCall,
		"SELECT `text` 'Label' FROM `t`":                     vetoNameString,
		"SELECT `int`'5' FROM `t`":                           vetoNameString,
		"SELECT `date`\n\t '2024-01-01' FROM `t`":            vetoNameString,
		"SELECT `json`/* c */'{}' FROM `t`":                  vetoNameString,
		"SELECT `a`, `text` -- c\n 'Label' FROM `t`":         vetoNameString,
		"SELECT `a` -- x\r+1\n FROM `t`":                     vetoCommentCR,
		"SELECT `a` FROM `t` -- x\r WHERE `a` = 1":           vetoCommentCR,
		"SELECT `a` /*M! +1 */ FROM `t`":                     vetoHintComment,
		"SELECT `a` /*M!100100 +1 */ FROM `t`":               vetoHintComment,
		"SELECT `abs`\v\r(-1)":                               vetoNameCall,
		"SELECT `abs`/* c */(-1)":                            vetoNameCall,
		"SELECT `db`.`f`(1)":                                 vetoNameCall,
		"SELECT U&`d` FROM t":                                vetoNamePrefix,
		"SELECT u&`d` FROM t":                                vetoNamePrefix,
		"SELECT `a` FROM t WHERE c = U&`d`":                  vetoNamePrefix,
	}
	for stmt, want := range cases {
		got, why := ForCopy(stmt)
		if why != want {
			t.Errorf("ForCopy(%q) refusal = %q, want %q", stmt, why, want)
		}
		if got != stmt {
			t.Errorf("ForCopy(%q) returned %q with a refusal: the text must be the client's", stmt, got)
		}
		if v := Veto(stmt); v != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, v, want)
		}
	}
}

// A double-quoted string and a backslash in a string stay vetoed under their
// own names, with backtick names around them or without; ForCopy refuses
// them too, so no caller can send the copy a text whose literals it would
// read another way.
func TestForCopy_doubleQuotesAndBackslashesStayOnMySQL(t *testing.T) {
	const dq = "double-quoted string literal"
	const bs = "backslash in a string literal (an escape on MySQL, a plain character on the copy)"
	cases := map[string]string{
		"SELECT `a` FROM `t` WHERE `b` = \"x\"":     dq,
		"SELECT `a`\"b\" FROM `t`":                  dq,
		"SELECT \"a\"`b` FROM `t`":                  dq,
		"SELECT `a` FROM `t` WHERE `b` = \"x`y\"":   dq,
		"SELECT `a` FROM `t` WHERE `b` = '\"' ":     "",
		"SELECT `a` FROM `t` WHERE `b` = 'a\\'b'":   bs,
		"SELECT `a` FROM `t` WHERE `b` = 'a\\\\'":   bs,
		"SELECT `a` FROM `t` WHERE `b` = 'a\\' `c`": bs,
		// ANSI_QUOTES on the source makes "x" a name there. It is not
		// guessed: any double quote in the client's text is a veto.
		"SELECT \"a\" FROM `t`": dq,
		"SELECT \"a`b\" FROM t": dq,
	}
	for stmt, want := range cases {
		if v := Veto(stmt); v != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, v, want)
		}
		got, why := ForCopy(stmt)
		if (why != "") != (want != "") {
			t.Errorf("ForCopy(%q) refusal = %q, veto = %q: both or neither", stmt, why, want)
		}
		if want != "" && got != stmt {
			t.Errorf("ForCopy(%q) returned %q with a refusal", stmt, got)
		}
	}
}

// Every veto, with the statement written the way a driver writes it: backtick
// names everywhere. The veto list runs on the rewritten text, and each one
// must still fire.
func TestVeto_everyVetoStillFiresOnBacktickStatements(t *testing.T) {
	cases := map[string]string{
		"SELECT GROUP_CONCAT(`name`) FROM `t`":                     "GROUP_CONCAT",
		"SELECT * FROM `t` WHERE `ts` > NOW() - INTERVAL 1 DAY":    "NOW/CURDATE/CURTIME/CURRENT_TIMESTAMP",
		"SELECT STR_TO_DATE(`d`, '%Y') FROM `t`":                   "STR_TO_DATE",
		"SELECT TIMESTAMPDIFF(DAY, `a`, `b`) FROM `t`":             "TIMESTAMPDIFF/DATEDIFF",
		"SELECT * FROM `t` WHERE `name` = 'x' COLLATE utf8mb4_bin": "COLLATE",
		"SELECT CAST(`a` AS UNSIGNED) FROM `t`":                    "CAST AS UNSIGNED/SIGNED",
		"SELECT `a` DIV 2 FROM `t`":                                "DIV",
		"SELECT RAND(), `a` FROM `t`":                              "RAND/UUID",
		"SELECT FOUND_ROWS(), `a` FROM `t`":                        "FOUND_ROWS/LAST_INSERT_ID/ROW_COUNT",
		"SELECT DATABASE(), `a` FROM `t`":                          "CONNECTION_ID/USER/DATABASE/VERSION",
		"SELECT @@version, `a` FROM `t`":                           "user or system variable",
		"SELECT @`my var`":                                         "user or system variable",
		"SELECT * FROM `t` FOR UPDATE":                             "locking read",
		"SELECT * FROM `t` LOCK IN SHARE MODE":                     "locking read",
		"SELECT `a` FROM `t` INTO OUTFILE '/tmp/x'":                "INTO (OUTFILE/DUMPFILE/variables)",
		"SELECT * FROM information_schema.`tables`":                "system schema",
		"SELECT * FROM `information_schema`.`tables`":              "system schema",
		"SELECT * FROM `mysql` . `user`":                           "system schema",
		"SELECT * FROM `t` WHERE MATCH(`body`) AGAINST ('x')":      "MATCH AGAINST",
		"SELECT * FROM `t` WHERE BINARY `name` = 'A'":              "binary string comparison",
		"SELECT /*+ NO_INDEX(`t`) */ * FROM `t`":                   "optimizer hint or MySQL comment",
		"SELECT /*!80000 `a`, */ `b` FROM `t`":                     "optimizer hint or MySQL comment",
		"SELECT WEEK(`d`) FROM `t`":                                "WEEK/YEARWEEK",
		"SELECT EXTRACT(WEEK FROM `d`) FROM `t`":                   "EXTRACT(WEEK ...)",
		"WITH RECURSIVE `r` AS (SELECT 1 AS `n` UNION ALL SELECT `n` + 1 FROM `r` WHERE `n` < 3) SELECT `n` FROM `r`": "WITH RECURSIVE",
		"SELECT `a` AS $$, 2 AS $$ FROM `t`":                vetoDollar,
		"SELECT `a` AS $x$, 2 AS $x$ FROM `t`":              vetoDollar,
		"SELECT * FROM `t` WHERE `name` LIKE 'a%'":          "LIKE/REGEXP (case-sensitive on the copy, case-insensitive on MySQL)",
		"SELECT * FROM `t` WHERE `name` LIKE BINARY 'a%'":   "binary string comparison",
		"SELECT count(DISTINCT `status`) FROM `t`":          "DISTINCT inside an aggregate (not folded by the copy's collation)",
		"SELECT INSTR(`name`, 'x') FROM `t`":                "INSTR/LOCATE/POSITION/STRCMP (case-sensitive on the copy)",
		"SELECT `a` || `b` FROM `t`":                        "|| (string concatenation on the copy, logical OR on MySQL)",
		"SELECT JSON_EXTRACT(`v`, '$.tier') FROM `t`":       "JSON function or -> operator (missing or different on the copy)",
		"SELECT `v`->>'$.tier' FROM `t`":                    "JSON function or -> operator (missing or different on the copy)",
		"SELECT `a` ^ `b` FROM `t`":                         "^ (power on the copy, bitwise XOR on MySQL)",
		"SELECT `a`--1 FROM `t`":                            "-- without a space after it (two minus signs on MySQL, a comment on the copy)",
		"SELECT `a` FROM `t` UNION SELECT `a` FROM `u`":     vetoSetOps,
		"SELECT `a` FROM `t` UNION(SELECT `a` FROM `u`)":    vetoSetOps,
		"SELECT `a` FROM `t`union select `a` FROM `u`":      vetoSetOps,
		"SELECT `a` FROM `t`UNION(SELECT `a` FROM `u`)":     vetoSetOps,
		"SELECT `a` FROM `t`intersect select `a` FROM `u`":  vetoSetOps,
		"SELECT `a` FROM `t`except select `a` FROM `u`":     vetoSetOps,
		"SELECT `a` FROM `t` except`x`":                     vetoSetOps,
		"SELECT `a` FROM `t` INTERSECT SELECT `a` FROM `u`": vetoSetOps,
		"(SELECT `a` FROM `t`)EXCEPT(SELECT `a` FROM `u`)":  vetoSetOps,
		"SELECT `a` FROM `t` WHERE `b` = \"x\"":             "double-quoted string literal",
		"SELECT `a` FROM `t` WHERE `b` = 'a\\\\b'":          "backslash in a string literal (an escape on MySQL, a plain character on the copy)",
	}
	seen := map[string]bool{}
	for stmt, want := range cases {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
		seen[want] = true
	}
	// The table above must name every veto of the list: a veto added later
	// without a backtick statement here fails this test.
	for _, v := range vetoes {
		if !seen[v.name] {
			t.Errorf("veto %q has no backtick statement in this test", v.name)
		}
	}
}

const vetoDollar = "$...$ (a name on the source, a dollar-quoted string on the copy)"

const vetoSetOps = "UNION/INTERSECT/EXCEPT without ALL (duplicates removed by bytes on the copy, by collation on MySQL)"

// A quoted name that spells an operator is a name, in backticks on MySQL and
// in double quotes on the copy: the set-operation veto must not fire on it,
// before the rewrite or after.
func TestVeto_quotedNamesAreNotOperators(t *testing.T) {
	for _, stmt := range []string{
		"SELECT `union` FROM `t`",
		"SELECT `a` AS `except` FROM `t`",
		"SELECT `t`.`intersect` FROM `t`",
		"SELECT `a` FROM `t` UNION ALL SELECT `a` FROM `u`",
		"SELECT `binary` FROM `t`",
	} {
		if v := Veto(stmt); v != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, v)
		}
	}
}

// The veto regexes are written for the text the copy gets. Not one of them
// may depend on a backtick alone: where a pattern names a backtick it must
// name the double quote too, or it would read the rewritten name another way.
func TestVetoPatterns_treatBothQuotesAlike(t *testing.T) {
	class := regexp.MustCompile(`\[[^\]]*\]`)
	for _, v := range vetoes {
		src := v.re.String()
		rest := class.ReplaceAllStringFunc(src, func(c string) string {
			if strings.Contains(c, `\x60`) != strings.Contains(c, `"`) {
				t.Errorf("veto %q: character class %s names one kind of quote and not the other", v.name, c)
			}
			return ""
		})
		if strings.Contains(rest, `\x60`) || strings.Contains(rest, "`") {
			t.Errorf("veto %q: pattern %s names a backtick outside a character class", v.name, src)
		}
	}
}

// Veto reads the text the copy would get. A pattern that only a double-quoted
// name can match is added to the list for the length of the test: it must
// fire on the client's backtick-quoted name, which it can only do if the list
// is run on the rewritten text.
func TestVeto_runsOnTheRewrittenText(t *testing.T) {
	saved := vetoes
	defer func() { vetoes = saved }()
	vetoes = append(slices.Clone(saved), struct {
		name string
		re   *regexp.Regexp
	}{"probe", regexp.MustCompile(`"probe_name"`)})
	if got := Veto("SELECT `probe_name` FROM `t`"); got != "probe" {
		t.Errorf("Veto = %q, want the probe: the list did not read the names in double quotes", got)
	}
	// A backtick in a string or a comment is not a name and is not rewritten
	// in what the list reads either.
	for _, stmt := range []string{"SELECT 'x `probe_name` y' FROM `t`", "SELECT `a` /* `probe_name` */ FROM `t`"} {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
	// No backtick survives in the text the list reads, whatever is left open.
	for _, stmt := range []string{"SELECT `a` FROM `t", "SELECT `a` FROM `t` WHERE b = 'x`", "SELECT `a` /* `b", "SELECT `a` -- `b", "SELECT `a``b`", "`"} {
		if sc := scan(stmt); strings.Contains(sc.blankedCopy, "`") && sc.refusal == "" {
			t.Errorf("scan(%q): a backtick reaches the veto list with no refusal: %q", stmt, sc.blankedCopy)
		}
	}
}

// The scrubbed text other checks read (the LIMIT shapes, sql-compare's ORDER
// BY probe) keeps the client's backticks, as before.
func TestScrub_keepsBackticks(t *testing.T) {
	if got, want := Scrub("SELECT `a` FROM `t` WHERE b = 'x' -- c"), "SELECT `a` FROM `t` WHERE b = '' "; got != want {
		t.Errorf("Scrub = %q, want %q", got, want)
	}
	if _, ok := (Policy{}).Prejudge("SELECT `a` FROM `t` LIMIT 5"); !ok {
		t.Errorf("Prejudge: a bare LIMIT over a backtick table is no longer decided on its text")
	}
}
