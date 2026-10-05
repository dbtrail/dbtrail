package readrouter

import "testing"

// What the copy refuses rather than answers (#2114): the statement was sent
// to it, failed there, and MySQL answered after the failed attempt. Kept on
// MySQL from the text, the attempt is not paid. Measured on MySQL 8.4.9,
// MariaDB 11.4 and the copy; the fixtures in consoleapp pin the verdicts.

func TestVeto_limitWithAnOffsetBeforeTheComma(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT a FROM t ORDER BY a LIMIT 0, 20":                                    vetoLimitComma,
		"SELECT a FROM t ORDER BY a LIMIT 5,10":                                     vetoLimitComma,
		"SELECT a FROM t ORDER BY a LIMIT 5 ,10":                                    vetoLimitComma,
		"select a from t order by a limit 5 , 10;":                                  vetoLimitComma,
		"SELECT a FROM t ORDER BY a LIMIT\n\t0,\n20":                                vetoLimitComma,
		"SELECT a FROM t LIMIT 0 /* offset */ , 20":                                 vetoLimitComma,
		"SELECT `a` FROM `t` ORDER BY `a` LIMIT 0, 20":                              vetoLimitComma,
		"SELECT a FROM t WHERE b IN (SELECT c FROM (SELECT c FROM u LIMIT 1, 3) x)": vetoLimitComma, // in a subquery
		"SELECT a FROM t ORDER BY a LIMIT ?, ?":                                     vetoLimitComma, // a prepared statement's template
		"SELECT a FROM t ORDER BY a LIMIT ?,?":                                      vetoLimitComma,
		"SELECT a FROM t ORDER BY a LIMIT 0, ?":                                     vetoLimitComma,
		// The form both sides read.
		"SELECT a FROM t ORDER BY a LIMIT 20":            "",
		"SELECT a FROM t ORDER BY a LIMIT 20 OFFSET 0":   "",
		"SELECT a FROM t ORDER BY a LIMIT ? OFFSET ?":    "",
		"SELECT a FROM t ORDER BY a LIMIT 20\nOFFSET 40": "",
		// A comma after the LIMIT that belongs to a list around it.
		"SELECT (SELECT a FROM t ORDER BY a LIMIT 1), b FROM u":                                "",
		"SELECT x.a FROM (SELECT a FROM t LIMIT 5) , (SELECT a FROM u LIMIT 5) y":              "",
		"SELECT a FROM t WHERE b IN ((SELECT c FROM u LIMIT 1), (SELECT c FROM v LIMIT 1), 3)": "",
		// Not the keyword.
		"SELECT `limit`, 2 FROM t":                      "",
		"SELECT 1 AS `limit`, 2 FROM t":                 "",
		"SELECT t.`limit` + 5, 3 FROM t":                "",
		"SELECT rate_limit, 5, 6 FROM t":                "",
		"SELECT limits 5, 6 FROM t":                     "",
		"SELECT a FROM t WHERE note = 'LIMIT 0, 20'":    "",
		"SELECT a FROM t /* LIMIT 0, 20 */ ORDER BY a":  "",
		"SELECT a FROM t -- LIMIT 0, 20\nORDER BY a":    "",
		"SELECT a FROM t ORDER BY a LIMIT 3 -- 0, 20\n": "",
		"SELECT a FROM `limit 0, 20` ORDER BY a":        "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

func TestVeto_orderByNull(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY NULL":                                  vetoOrderByNull,
		"SELECT `a`, COUNT(*) FROM `t` GROUP BY `a` ORDER BY NULL":                            vetoOrderByNull,
		"select a, count(*) from t group by a order by null":                                  vetoOrderByNull,
		"SELECT a, COUNT(*) FROM t GROUP BY a ORDER\n BY\n\tNULL":                             vetoOrderByNull,
		"SELECT a, COUNT(*) FROM t GROUP BY a ORDER /* x */ BY NULL;":                         vetoOrderByNull,
		"SELECT a FROM t ORDER BY NULL":                                                       vetoOrderByNull,
		"SELECT a FROM t ORDER BY NULL DESC":                                                  vetoOrderByNull,
		"SELECT a FROM t ORDER BY NULL LIMIT 5":                                               vetoOrderByNull,
		"SELECT a FROM t ORDER BY NULL, a":                                                    vetoOrderByNull,
		"SELECT a FROM t ORDER BY (NULL)":                                                     vetoOrderByNull,
		"SELECT a FROM t WHERE b IN (SELECT c FROM u ORDER BY NULL)":                          vetoOrderByNull,
		"SELECT a FROM t WHERE a = ? GROUP BY a ORDER BY NULL":                                vetoOrderByNull, // a prepared statement's template
		"SELECT a, COUNT(*) OVER (ORDER BY a) FROM t GROUP BY a ORDER BY NULL":                vetoOrderByNull,
		"SELECT a, COUNT(*) OVER (ORDER BY NULL) FROM t GROUP BY a ORDER BY NULL":             vetoOrderByNull,
		"SELECT a, COUNT(*) OVER (PARTITION BY b ORDER BY a) FROM t GROUP BY a ORDER BY NULL": vetoOrderByNull,
		"SELECT a FROM t WHERE b IN (SELECT c FROM u WHERE d IN (1, 2) ORDER BY NULL)":        vetoOrderByNull,
		"SELECT GROUP_CONCAT(a ORDER BY NULL) FROM t":                                         "GROUP_CONCAT",
		// A window whose PARTITION BY list holds parentheses is not followed.
		"SELECT a, COUNT(*) OVER (PARTITION BY LOWER(b) ORDER BY NULL) FROM t": vetoOrderByNull,
		// A window ordered by NULL is answered by the copy, with the same
		// rows: not kept back.
		"SELECT a, COUNT(*) OVER (ORDER BY NULL) FROM t":                       "",
		"SELECT a, COUNT(*) OVER ( ORDER BY NULL ) FROM t":                     "",
		"SELECT a, COUNT(*) OVER (\n\tORDER BY NULL) FROM t":                   "",
		"SELECT a, COUNT(*) OVER (PARTITION BY b ORDER BY NULL) FROM t":        "",
		"SELECT a, COUNT(*) OVER (partition by b, `c` order by null) FROM t":   "",
		"SELECT a, COUNT(*) OVER w FROM t WINDOW w AS (ORDER BY NULL)":         "",
		"SELECT a, COUNT(*) OVER (ORDER BY NULL) FROM t GROUP BY a ORDER BY a": "",
		// Not the constant.
		"SELECT a FROM t ORDER BY NULLIF(a, b)":          "",
		"SELECT a FROM t ORDER BY nullable_col":          "",
		"SELECT a FROM t ORDER BY null_first, a":         "",
		"SELECT a FROM t ORDER BY a IS NULL":             "",
		"SELECT a FROM t ORDER BY a IS NULL, a":          "",
		"SELECT a FROM t ORDER BY `null`":                "",
		"SELECT a FROM t ORDER BY `NULL`, a":             "",
		"SELECT a FROM t ORDER BY t.`null`":              "",
		"SELECT a FROM t WHERE note = 'ORDER BY NULL'":   "",
		"SELECT a FROM t /* ORDER BY NULL */ ORDER BY a": "",
		"SELECT a FROM t -- ORDER BY NULL\nORDER BY a":   "",
		"SELECT a FROM t WHERE reorder BY_NULL":          "",
		"SELECT a FROM t WHERE b IS NULL ORDER BY a":     "",
		"SELECT a, b FROM t GROUP BY a, b ORDER BY a":    "",
		// Known and not kept back: NULL further down the list. The copy
		// refuses it too, so MySQL answers after the failed attempt.
		"SELECT a FROM t ORDER BY a, NULL": "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}

func TestVeto_binaryStringLiteral(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT a FROM t WHERE b = _binary'x'":       vetoBinaryIntroducer,
		"SELECT a FROM t WHERE b = _binary 'x'":      vetoBinaryIntroducer,
		"SELECT a FROM t WHERE b = _BINARY'x'":       vetoBinaryIntroducer,
		"SELECT a FROM t WHERE b IN (_binary'x', 1)": vetoBinaryIntroducer,
		// Already kept on MySQL by older rules, which name it first.
		"SELECT a FROM t WHERE b LIKE BINARY 'x%'":     "binary string comparison",
		"SELECT a FROM t WHERE b NOT LIKE BINARY 'x%'": "binary string comparison",
		"SELECT a FROM t WHERE BINARY b LIKE 'x%'":     "binary string comparison",
		"SELECT a FROM t WHERE b = BINARY 'x'":         "binary string comparison",
		"SELECT a FROM t WHERE b LIKE _binary'x%'":     "LIKE/REGEXP (case-sensitive on the copy, case-insensitive on MySQL)",
		// Not the introducer.
		"SELECT is_binary, x_binary FROM t":           "",
		"SELECT `_binary` FROM t":                     "",
		"SELECT a FROM t WHERE note = '_binary''x'''": "",
		"SELECT _binaryish FROM t":                    "",
		// A cast to BINARY is not kept back: the copy answers it the same
		// for text within ASCII and refuses the rest.
		"SELECT CAST(a AS BINARY) FROM t": "",
	} {
		if got := Veto(stmt); got != want {
			t.Errorf("Veto(%q) = %q, want %q", stmt, got, want)
		}
	}
}
