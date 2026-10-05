package readrouter

import "testing"

// UNION, INTERSECT and EXCEPT remove duplicates by comparing rows, and the
// copy compares them by bytes there where MySQL uses the collation: 'a' and
// 'A' are one row on MySQL and two on the copy, with no error (#2083). So a
// statement that asks for duplicate removal stays on MySQL. UNION ALL removes
// nothing and goes on being routed.
func TestVeto_setOperationsThatRemoveDuplicates(t *testing.T) {
	const name = "UNION/INTERSECT/EXCEPT without ALL (duplicates removed by bytes on the copy, by collation on MySQL)"
	vetoed := []string{
		"SELECT a FROM t UNION SELECT a FROM u",
		"SELECT a FROM t union select a FROM u",
		"SELECT a FROM t UNION DISTINCT SELECT a FROM u",
		"SELECT a FROM t\nUNION\nSELECT a FROM u",
		"SELECT a FROM t UNION\t(SELECT a FROM u)",
		"(SELECT a FROM t) UNION (SELECT a FROM u)",
		"(SELECT a FROM t)UNION(SELECT a FROM u)",
		"SELECT a FROM t UNION /* all */ SELECT a FROM u",
		"SELECT a FROM t UNION -- all\nSELECT a FROM u",
		"SELECT a FROM t UNION VALUES ROW(1)",
		"SELECT a FROM t UNION TABLE u",
		// One of several is enough.
		"SELECT a FROM t UNION ALL SELECT a FROM u UNION SELECT a FROM v",
		"SELECT a FROM t UNION SELECT a FROM u UNION ALL SELECT a FROM v",
		"SELECT * FROM (SELECT a FROM t UNION ALL SELECT a FROM u) x WHERE a IN (SELECT a FROM v UNION SELECT a FROM w)",
		"WITH c AS (SELECT a FROM t UNION SELECT a FROM u) SELECT * FROM c",
		"SELECT a FROM t INTERSECT SELECT a FROM u",
		"SELECT a FROM t intersect all SELECT a FROM u",
		"SELECT a FROM t EXCEPT SELECT a FROM u",
		"SELECT a FROM t EXCEPT ALL SELECT a FROM u",
		"(SELECT a FROM t)EXCEPT(SELECT a FROM u)",
	}
	for _, stmt := range vetoed {
		if got := Veto(stmt); got != name {
			t.Errorf("Veto(%q) = %q, want the set-operation veto", stmt, got)
		}
	}
	routed := []string{
		"SELECT a FROM t UNION ALL SELECT a FROM u",
		"SELECT a FROM t union all select a FROM u",
		"SELECT a FROM t UNION  ALL\nSELECT a FROM u",
		"SELECT a FROM t UNION/**/ALL SELECT a FROM u",
		"(SELECT a FROM t) UNION ALL (SELECT a FROM u)",
		"SELECT a FROM t UNION ALL SELECT a FROM u UNION ALL SELECT a FROM v",
		// The words inside a literal, a comment or a quoted name are not the
		// operator, and neither is a longer word that holds them.
		"SELECT a FROM t WHERE note = 'UNION SELECT' OR note = 'except' OR note = 'intersect'",
		"SELECT a /* UNION SELECT 1 */ FROM t",
		"SELECT a FROM t -- except\n WHERE a = 1",
		"SELECT `union`, `except`, `intersect` FROM t",
		"SELECT t.`union` FROM `except` t",
		"SELECT reunion, unions, exception, intersection, union_id FROM t",
		"SELECT x.except, x.intersect, x.union FROM x",
		"SELECT a FROM t WHERE a = 1",
	}
	for _, stmt := range routed {
		if got := Veto(stmt); got != "" {
			t.Errorf("Veto(%q) = %q, want none", stmt, got)
		}
	}
}
