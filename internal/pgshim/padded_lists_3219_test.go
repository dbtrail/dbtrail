package pgshim

import "testing"

// A table with a char(n) column as a later item of a FROM list written with
// commas, and under the star of an EXISTS (#3219). What the copy answers the
// same was measured by internal/pgcompare (charCases).

func TestPaddedLists_leftToTheCopy(t *testing.T) {
	for _, q := range []string{
		// The second table of a list, its char(n) column not used.
		`SELECT p.id FROM plain p, codes c WHERE c.id = p.id`,
		`SELECT plain.qty, codes.k FROM plain, codes WHERE codes.id = plain.id`,
		`SELECT p.id FROM plain AS p, codes AS c WHERE c.id = p.id AND c.k > 1`,
		`SELECT p.id FROM plain p, public.codes c WHERE c.id = p.id`,
		`SELECT p.qty, count(*) FROM plain p, codes c, plain q WHERE c.id = p.id AND q.id = p.id GROUP BY p.qty`,
		`SELECT p.id FROM codes c, plain p WHERE c.id = p.id`,
		`SELECT s.id FROM (SELECT id, qty FROM plain) s, codes c WHERE c.id = s.id`,
		`SELECT p.id FROM plain p, "codes" c WHERE c.id = p.id`,
		// The column returned whole, as from a join written with JOIN.
		`SELECT c.id, c.code FROM plain p, codes c WHERE c.id = p.id`,
		// In a subquery's list too.
		`SELECT id FROM plain WHERE id IN (SELECT c.id FROM plain q, codes c WHERE c.id = q.id)`,

		// EXISTS and NOT EXISTS with a star: the select list is not read.
		// (The first one was held to the source until #3219, as every star
		// outside a plain select list was.)
		`SELECT id FROM plain WHERE EXISTS (SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c.id = p.id)`,
		`SELECT p.id FROM plain p WHERE NOT EXISTS (SELECT * FROM codes c WHERE c.id = p.id AND c.k > 2)`,
		`SELECT p.qty, count(*) FROM plain p WHERE p.qty > 1 AND exists (select * from codes where codes.id = p.id) GROUP BY p.qty`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c, plain q WHERE c.id = p.id AND q.id = c.id)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes) AND NOT EXISTS (SELECT * FROM codes c WHERE c.k = p.qty)`,
	} {
		if veto := paddedScreen(q); veto != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, veto)
		}
	}
}

func TestPaddedLists_toTheSource(t *testing.T) {
	for _, q := range []string{
		// The join compares the char(n) column: its padding counts, with
		// commas as with JOIN ... ON.
		`SELECT p.id FROM plain p, codes c WHERE c.code = p.code`,
		`SELECT p.id FROM plain p, codes c WHERE p.code = c.code AND c.id = p.id`,
		`SELECT p.id FROM plain p, codes c WHERE c.id = p.id AND c.code = 'ab'`,
		`SELECT p.id FROM plain p, codes c WHERE c.id = p.id ORDER BY c.code`,
		`SELECT c.code, count(*) FROM plain p, codes c WHERE c.id = p.id GROUP BY c.code HAVING c.code > 'a'`,
		// NATURAL and USING compare columns without naming them, or name it.
		`SELECT p.id FROM plain p NATURAL JOIN codes`,
		`SELECT p.id FROM plain p, codes c NATURAL JOIN plain q`,
		`SELECT p.id FROM plain p JOIN codes c USING (code)`,
		// The table's name as a value, after a comma: the whole row.
		`SELECT k, codes FROM codes`,
		`SELECT id, c FROM codes c`,
		`SELECT count(DISTINCT c) FROM plain p, codes c WHERE c.id = p.id`,
		`SELECT p.id FROM plain p, codes c WHERE c = c`,
		`SELECT p.id FROM plain p, codes c WHERE (p.id, c) IS NOT NULL`,
		`SELECT greatest(id, codes) FROM codes`,
		`SELECT id FROM codes ORDER BY id, codes`,
		`SELECT id FROM codes GROUP BY id, codes`,
		`SELECT id IS DISTINCT FROM k, codes FROM codes`,
		// A list that mixes JOIN ... ON with commas is not followed.
		`SELECT p.id FROM plain p JOIN plain q ON q.id = p.id, codes c WHERE c.id = p.id`,
		// A star that is read: anything but EXISTS.
		`SELECT p.id FROM plain p WHERE p.id IN (SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE p.id = ANY (SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE (SELECT * FROM codes LIMIT 1) IS NOT NULL`,
		`SELECT p.id FROM plain p WHERE (p.id, p.code) IS DISTINCT FROM (SELECT * FROM codes LIMIT 1)`,
		`SELECT p.id, (SELECT * FROM codes LIMIT 1) FROM plain p`,
		`SELECT p.id FROM plain p WHERE p.id > ALL (SELECT * FROM codes)`,
		// EXISTS whose subquery compares the star's rows, or the column.
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes EXCEPT SELECT 1, 'ab', 'x', NULL, 'y', 'z', 'k', 1)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes UNION SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes INTERSECT SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c.code = p.code)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c.code = 'ab')`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c = c)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT DISTINCT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT *, code FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT c.* FROM codes c)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes NATURAL JOIN plain)`,
		`SELECT p.id FROM plain p WHERE EXISTS ((SELECT * FROM codes))`,
		`SELECT p.id FROM plain p WHERE s.exists (SELECT * FROM codes)`,
		// A bpchar without a length compared, as ever.
		`SELECT p.id FROM plain p, codes c WHERE c.free = p.code`,
	} {
		if veto := paddedScreen(q); !isPaddedVeto(veto) {
			t.Errorf("%s: %q, want it sent to the source for its padded column", q, veto)
		}
	}
}

func TestFromListItem(t *testing.T) {
	at := func(q, name string, nth int) (toks []rtToken, i int) {
		toks, _ = lexSQL(q)
		for j, tk := range toks {
			if tk.kind == rtWord && tk.text == name {
				if nth == 0 {
					return toks, j
				}
				nth--
			}
		}
		t.Fatalf("%s: no %q", q, name)
		return nil, 0
	}
	for _, tc := range []struct {
		q    string
		nth  int
		want bool
	}{
		{`SELECT 1 FROM a, codes`, 0, true},
		{`SELECT 1 FROM a x, b AS y, codes z`, 0, true},
		{`SELECT trim(both FROM a, codes) FROM t`, 0, false},
		{`SELECT f((SELECT 1 FROM a, codes)) FROM t`, 0, true},
		{`SELECT 1 FROM (SELECT 1 FROM t WHERE u > 1) s, codes`, 0, true},
		{`SELECT 1 FROM t WHERE EXISTS (SELECT 1 FROM a, codes)`, 0, true},
		{`SELECT 1 FROM codes`, 0, false},               // after FROM, not after a comma: the other rule's
		{`SELECT a, codes FROM t`, 0, false},            // a value
		{`SELECT 1 FROM t WHERE x IN (a, codes)`, 0, false},
		{`SELECT 1 FROM t ORDER BY a, codes`, 0, false},
		{`SELECT 1 FROM t GROUP BY a, codes`, 0, false},
		{`SELECT a IS DISTINCT FROM b, codes FROM t`, 0, false},
		{`SELECT f(a, codes) FROM t`, 0, false},
		{`SELECT 1 FROM a JOIN b ON b.i = a.i, codes`, 0, false},
		{`SELECT 1 FROM a, b WHERE a.i = b.i AND (a.i, codes) IS NULL`, 0, false},
		{`SELECT extract(year FROM d), codes FROM t`, 0, false},
		{`SELECT 1 FROM a, 2, codes`, 0, false},
		{`, codes`, 0, false},
		{`codes`, 0, false},
	} {
		toks, i := at(tc.q, "codes", tc.nth)
		if got := fromListItem(toks, i); got != tc.want {
			t.Errorf("%s: fromListItem = %v, want %v", tc.q, got, tc.want)
		}
	}
}
