package pgshim

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// #2985: "never read back after the source" holds for the tables a
// connection read from its source, not for the connection. These pin which
// statements' tables are told from their text, and that everything else
// marks every table.

func writtenRefs(r stmtReads) []string {
	out := []string{}
	for _, ref := range r.refs {
		w := ref.written()
		if r.isCTE(ref) {
			w = "with:" + w
		}
		out = append(out, w)
	}
	return out
}

func TestReadsOf_theTablesAStatementNames(t *testing.T) {
	for _, tc := range []struct {
		name, stmt string
		refs       []string
		funcs      []string
	}{
		{"no table", "SELECT 1", nil, nil},
		{"one table", "SELECT n FROM t WHERE id = 1", []string{"t"}, nil},
		{"a trailing semicolon and spaces", "  SELECT n\n\tFROM   t  ;  ", []string{"t"}, nil},
		{"comments", "/* a */ SELECT n -- FROM hidden\n FROM t /* , u */ WHERE id = 1", []string{"t"}, nil},
		{"uppercase folds", "SELECT N FROM Orders", []string{"orders"}, nil},
		{"a quoted name keeps its case", `SELECT n FROM "Orders"`, []string{`"Orders"`}, nil},
		{"a quote inside a quoted name", `SELECT n FROM "a""b"`, []string{`"a""b"`}, nil},
		{"with its schema", "SELECT n FROM public.t", []string{"public.t"}, nil},
		{"schema and table quoted", `SELECT n FROM "S"."T"`, []string{`"S"."T"`}, nil},
		{"three parts", "SELECT n FROM db.public.t", []string{"db.public.t"}, nil},
		{"a name that is a prefix of another's", "SELECT n FROM order_line, orders, order_lines2", []string{"order_line", "orders", "order_lines2"}, nil},
		{"an alias", "SELECT x.n FROM t x", []string{"t"}, nil},
		{"AS an alias", "SELECT x.n FROM t AS x WHERE x.id = 2", []string{"t"}, nil},
		{"a comma list", "SELECT 1 FROM a, b x, c AS y", []string{"a", "b", "c"}, nil},
		{"joins", "SELECT 1 FROM a JOIN b ON a.id = b.id LEFT OUTER JOIN c USING (id) CROSS JOIN d NATURAL JOIN e", []string{"a", "b", "c", "d", "e"}, nil},
		{"a join in parentheses", "SELECT 1 FROM a LEFT JOIN (b JOIN c ON b.id = c.id) ON a.id = b.id, d", []string{"a", "b", "c", "d"}, nil},
		{"a subquery in FROM", "SELECT 1 FROM (SELECT id FROM inner_t) s JOIN outer_t o ON o.id = s.id", []string{"inner_t", "outer_t"}, nil},
		{"a subquery in WHERE", "SELECT 1 FROM a WHERE id IN (SELECT id FROM b) AND EXISTS (SELECT 1 FROM c WHERE c.id = a.id)", []string{"a", "b", "c"}, nil},
		{"a subquery in the select list", "SELECT (SELECT max(n) FROM b), n FROM a", []string{"b", "a"}, []string{"max"}},
		{"a subquery in ON", "SELECT 1 FROM a JOIN b ON b.id = (SELECT min(id) FROM c)", []string{"a", "b", "c"}, []string{"min"}},
		{"a set operation", "SELECT n FROM a UNION ALL SELECT n FROM b EXCEPT SELECT n FROM c", []string{"a", "b", "c"}, nil},
		{"set operation in parentheses", "(SELECT n FROM a) UNION (SELECT n FROM b) ORDER BY 1", []string{"a", "b"}, nil},
		{"a WITH, its name not a table", "WITH w AS (SELECT n FROM a) SELECT n FROM w", []string{"a", "with:w"}, nil},
		{"a WITH with columns and two bodies", "WITH w (x, y) AS (SELECT 1, 2 FROM a), v AS MATERIALIZED (SELECT x FROM w) SELECT x FROM v JOIN b ON true", []string{"a", "with:w", "with:v", "b"}, nil},
		{"a WITH name read inside its own body is the table", "WITH t AS (SELECT n FROM t) SELECT n FROM t", []string{"t", "with:t"}, nil},
		{"a WITH name read in an earlier body is the table", "WITH a AS (SELECT n FROM b), b AS (SELECT 1 AS n) SELECT n FROM a", []string{"b", "with:a"}, nil},
		{"WITH RECURSIVE: the name is in scope in its body", "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r", []string{"with:r", "with:r"}, nil},
		{"a WITH name with its schema is the table", "WITH t AS (SELECT 1 AS n) SELECT n FROM public.t", []string{"public.t"}, nil},
		{"functions known to read no table", "SELECT count(*), sum(n), coalesce(max(n), 0), now() FROM t", []string{"t"}, []string{"count", "sum", "max", "now"}},
		{"FROM inside extract, substring and trim", "SELECT extract(year FROM d), substring(s FROM 2 FOR 3), trim(both 'x' FROM s) FROM t", []string{"t"}, []string{"extract", "substring", "trim"}},
		{"IS DISTINCT FROM", "SELECT a IS DISTINCT FROM b, a IS NOT DISTINCT FROM c FROM t WHERE x IS DISTINCT FROM y", []string{"t"}, nil},
		{"IS DISTINCT FROM in ON", "SELECT 1 FROM a JOIN b ON a.x IS DISTINCT FROM b.x, c", []string{"a", "b", "c"}, nil},
		{"a column named as a clause, after a dot", "SELECT 1 FROM a JOIN b ON a.id = b.for AND a.order = b.where, c", []string{"a", "b", "c"}, nil},
		{"a name after a dot may be a function's", `SELECT x.peek, (x.id).leak, x."Quoted" FROM t x`, []string{"t"}, []string{"peek", "leak", "Quoted"}},
		{"a word of the grammar that can name a function", "SELECT filter(id), is(id, 1) FROM t WHERE id IN (1, 2)", []string{"t"}, []string{"filter", "is", "in"}},
		{"a cast, a typed literal and a window", "SELECT n::numeric(10,2), DATE '2026-01-01', row_number() OVER (PARTITION BY g ORDER BY n) FROM t", []string{"t"}, []string{"numeric", "row_number"}},
		{"an aggregate with ORDER BY and FILTER", "SELECT string_agg(s, ',' ORDER BY s) FILTER (WHERE n > 1) FROM t GROUP BY g HAVING count(*) > 1 ORDER BY 1 LIMIT 5 OFFSET 2", []string{"t"}, []string{"string_agg", "count"}},
		{"a parameter", "SELECT n FROM t WHERE id = $1", []string{"t"}, nil},
		{"VALUES in FROM", "SELECT 1 FROM (VALUES (1), (2)) v JOIN t ON true", []string{"t"}, nil},
		{"timestamp with time zone", "SELECT CAST(d AS timestamp with time zone) FROM t", []string{"t"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, why := readsOf(tc.stmt, nil)
			if why != "" {
				t.Fatalf("not read: %s", why)
			}
			if got := writtenRefs(r); !slices.Equal(got, append([]string{}, tc.refs...)) {
				t.Errorf("tables %q, want %q", got, tc.refs)
			}
			for _, f := range tc.funcs {
				if !slices.Contains(r.funcs, f) {
					t.Errorf("the names the source is asked about are %q, without %s", r.funcs, f)
				}
			}
		})
	}
}

// Each of these marks every table: its tables cannot be told from its text.
func TestReadsOf_whatMarksEveryTable(t *testing.T) {
	for _, tc := range []struct{ name, stmt string }{
		{"an empty statement", " ; "},
		{"not lexable: a dollar quote", "SELECT $$x$$ FROM t"},
		{"not lexable: an escape string", "SELECT E'a\\n' FROM t"},
		{"not lexable: an unterminated string", "SELECT 'a FROM t"},
		{"not lexable: an unterminated comment", "SELECT 1 /* FROM t"},
		{"several statements", "SELECT 1 FROM a; SELECT 1 FROM b"},
		{"a user's function", "SELECT my_fn(n) FROM t"},
		{"a function that reads a table by its text", "SELECT query_to_xml('select * from secret', true, false, '') FROM t"},
		{"a sequence's value", "SELECT nextval('s') FROM t"},
		{"a function written with its schema", "SELECT public.f(n) FROM t"},
		{"a built-in one written with its schema", "SELECT pg_catalog.count(*) FROM t"},
		{"a quoted function name", `SELECT "count"(*) FROM t`},
		{"a function in FROM", "SELECT 1 FROM generate_series(1, 3) g"},
		{"a function in FROM, joined", "SELECT 1 FROM t JOIN my_srf(1) x ON true"},
		{"unnest in FROM", "SELECT 1 FROM t, unnest(t.arr) u"},
		{"ROWS FROM", "SELECT 1 FROM ROWS FROM (f(1), g(2)) x"},
		{"an alias with a column list", "SELECT 1 FROM t AS x (a, b)"},
		{"TABLE x", "TABLE t"},
		{"TABLE x in a subquery", "SELECT 1 WHERE EXISTS (TABLE t)"},
		{"LATERAL", "SELECT 1 FROM a, LATERAL (SELECT 1 FROM b WHERE b.id = a.id) x"},
		{"ONLY", "SELECT 1 FROM ONLY t"},
		{"TABLESAMPLE", "SELECT 1 FROM t TABLESAMPLE SYSTEM (10)"},
		{"SELECT INTO", "SELECT n INTO u FROM t"},
		{"a data-modifying WITH", "WITH d AS (DELETE FROM t RETURNING n) SELECT n FROM d"},
		{"a WITH inside a subquery", "SELECT 1 FROM (WITH w AS (SELECT 1 FROM a) SELECT 1 FROM w) s"},
		{"a WITH in a WITH's body", "WITH a AS (WITH b AS (SELECT 1 FROM t) SELECT 1 FROM b) SELECT 1 FROM a"},
		{"a WITH in parentheses", "(WITH w AS (SELECT 1 FROM a) SELECT 1 FROM w)"},
		{"FOR UPDATE", "SELECT n FROM t FOR UPDATE"},
		{"EXECUTE", "EXECUTE p(1)"},
		{"CALL", "CALL p(1)"},
		{"DO", "DO 'begin end'"},
		{"COPY", "COPY t TO STDOUT"},
		{"a cursor", "DECLARE c CURSOR FOR SELECT n FROM t"},
		{"FETCH", "FETCH 10 FROM c"},
		{"SHOW", "SHOW search_path"},
		{"VALUES alone", "VALUES (1)"},
		{"EXPLAIN", "EXPLAIN SELECT n FROM t"},
		{"OPERATOR()", "SELECT n OPERATOR(public.+) 1 FROM t"},
		{"a string in FROM", "SELECT 1 FROM 'file.parquet'"},
		{"FROM first, the copy's own syntax", "FROM t SELECT n"},
		{"FROM first in a subquery", "SELECT (FROM t SELECT count(*))"},
		{"FROM first after a set operation, the copy's own syntax", "SELECT 1 UNION ALL FROM t"},
		{"FROM after WHERE", "SELECT 1 FROM a WHERE true FROM t"},
		{"an alias before a table, the copy's own syntax", "WITH a AS (SELECT 1) SELECT 1 FROM a: t"},
		{"SELECT DISTINCT FROM", "SELECT DISTINCT FROM t"},
		{"a column named is, aliased distinct", "SELECT o.is distinct FROM t o"},
		{"a column named is, after NOT", "SELECT o.is not distinct FROM t o"},
		{"four parts", "SELECT 1 FROM a.b.c.d"},
		{"a name outside ASCII, which PostgreSQL folds otherwise", "SELECT 1 FROM TABLÉ_Ñ"},
		{"a trailing comma in FROM", "SELECT 1 FROM a, WHERE true"},
		{"more tables than are followed", "SELECT 1 FROM " + strings.TrimSuffix(strings.Repeat("t, ", readsMaxRefs+1), ", ")},
		{"an unbalanced parenthesis", "SELECT (1 FROM t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if r, why := readsOf(tc.stmt, nil); why == "" {
				t.Errorf("read as naming %q, calling %q: want every table", writtenRefs(r), r.funcs)
			}
		})
	}
}

// The copy's own functions are known only where the caller says so.
func TestReadsOf_theCopysMacros(t *testing.T) {
	stmt := "SELECT " + pgRoundAvg + "(n, 2) FROM t"
	if _, why := readsOf(stmt, nil); why == "" {
		t.Error("a statement for the source calling the copy's macro was read")
	}
	if r, why := readsOf(stmt, copyFuncs); why != "" || !slices.Equal(writtenRefs(r), []string{"t"}) {
		t.Errorf("for the copy: %q %q", writtenRefs(r), why)
	}
}

func TestProofSQL(t *testing.T) {
	r, why := readsOf(`WITH w AS (SELECT 1 FROM "it's") SELECT count(*) FROM w, public.t`, nil)
	if why != "" {
		t.Fatal(why)
	}
	got := proofSQL(r)
	for _, want := range []string{
		`('"it''s"', false)`, `('w', true)`, `('public.t', false)`,
		`p.proname = ANY (ARRAY['count']::pg_catalog.name[])`,
		`c.relkind = 'r' AND c.relpersistence = 'p' AND NOT c.relhassubclass AND NOT c.relrowsecurity AND NOT c.relispartition`,
		`pg_catalog.pg_inherits i WHERE i.inhrelid = c.oid`,
		`pg_catalog.to_regclass(v.ref)`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the proof lacks %s:\n%s", want, got)
		}
	}
	none, _ := readsOf("SELECT 1", nil)
	if got := proofSQL(none); !strings.HasSuffix(got, "THEN NULL ELSE '[]'::pg_catalog.text END") {
		t.Errorf("the proof of a statement that names nothing:\n%s", got)
	}
}

func TestParseProof(t *testing.T) {
	for _, tc := range []struct {
		cell string
		want [][2]string
		ok   bool
	}{
		{`[]`, nil, true},
		{`[["public", "t"], ["S", "Orders"]]`, [][2]string{{"public", "t"}, {"S", "Orders"}}, true},
		{``, nil, false},
		{`null`, nil, true},
		{`42`, nil, false},
		{`[["public"]]`, nil, false},
		{`[["public", ""]]`, nil, false},
		{`[[null, null]]`, nil, false},
		{`{"a": 1}`, nil, false},
	} {
		got, ok := parseProof([]byte(tc.cell))
		if ok != tc.ok || !slices.Equal(got, tc.want) {
			t.Errorf("%q: %v %v, want %v %v", tc.cell, got, ok, tc.want, tc.ok)
		}
	}
}

func TestSourceReads_theSetIsBounded(t *testing.T) {
	var r sourceReads
	if r.any() {
		t.Fatal("an empty set says the source answered")
	}
	r.add([][2]string{{"Public", "Orders"}})
	if !r.any() || !r.tables[tableKey("public", "orders")] || r.all {
		t.Fatalf("after one table: %+v", r)
	}
	var many [][2]string
	for i := range sourceReadsMax {
		many = append(many, [2]string{"public", fmt.Sprintf("t%d", i)})
	}
	r.add(many)
	if !r.all || r.tables != nil {
		t.Fatalf("past %d tables every table is marked: all=%v, %d kept", sourceReadsMax, r.all, len(r.tables))
	}
	r.add([][2]string{{"public", "late"}})
	if !r.all || r.tables != nil {
		t.Fatal("every table, once marked, stays marked")
	}
}

// tablesPort is a port over a fake source that confirms the tables in
// known, and a copy whose catalog holds the same tables.
type tablesPort struct {
	src  *fakeSource
	l    *costLogin
	knob *copyAgeKnob
	w    *copyWaitKnob
}

func catalogOf(names ...string) []CatalogTable {
	var out []CatalogTable
	for _, n := range names {
		schema, table, ok := strings.Cut(n, ".")
		if !ok {
			schema, table = "public", n
		}
		out = append(out, CatalogTable{Schema: schema, Name: table, Columns: []CatalogColumn{{Name: "id", Type: "INTEGER"}, {Name: "n", Type: "INTEGER"}}})
	}
	return out
}

// newTablesPort starts one; each opt changes the source before it serves.
func newTablesPort(t *testing.T, opts ...func(*fakeSource)) *tablesPort {
	t.Helper()
	p := &tablesPort{knob: &copyAgeKnob{}, w: &copyWaitKnob{reach: true}}
	plans := maps.Clone(costPlans)
	plans["FROM public.big"], plans["FROM other.big"] = costPlans["FROM big"], costPlans["FROM big"]
	src := &fakeSource{plans: plans, tables: map[string][2]string{
		"t": {"public", "t"}, "public.t": {"public", "t"}, "big": {"public", "big"}, "public.big": {"public", "big"},
		"hb": {"public", "hb"}, "wide": {"public", "wide"}, "other.t": {"other", "t"}, "other.big": {"other", "big"},
		`"Mixed"`: {"public", "Mixed"}, "mixed": {"public", "mixed"}, "rowsthenfail": {"public", "rowsthenfail"},
		"public.rowsthenfail": {"public", "rowsthenfail"},
	}}
	for _, o := range opts {
		o(src)
	}
	p.src = startFakeSourceWith(t, src)
	p.l = &costLogin{src: p.src, age: p.knob.age, wait: p.w.wait, catalog: func() []CatalogTable {
		return catalogOf("t", "big", "hb", "wide", "other.t", "other.big", "mixed", "bigger", "rowsthenfail")
	}}
	// The copy's answers read a snapshot alone: held back whenever the rule
	// applies, however the wait went.
	p.knob.set(snapshotAlone)
	return p
}

var (
	tablesOnCopy = costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}
	tablesTooOld = costOutcome{"42", "", RouteSource, ReasonCopyTooOld}
)

// The rule per table: after the source answered about t, a report on big is
// answered by the copy without waiting, even from a snapshot alone, and a
// report that reads t is held back as before.
func TestSourceTables_2985_onlyTheTablesTheSourceAnsweredAreHeldBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	// afterT is a connection the source answered about t, and nothing else.
	afterT := func() *pgconn.PgConn {
		t.Helper()
		c := costPortConn(t, ctx, p.l)
		proofs := len(p.src.proved())
		if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
			t.Fatalf("the read by key: %+v", got)
		}
		if n := len(p.src.proved()) - proofs; n != 1 {
			t.Fatalf("the source was asked to confirm %d statements, want 1", n)
		}
		return c
	}
	for _, tc := range []struct {
		name, stmt string
		want       costOutcome
		waits      int
	}{
		{"another table", "SELECT sum(n) FROM big", tablesOnCopy, 0},
		{"another table, with extra spaces and a comment", "SELECT  sum(n) /* t */\n FROM big -- t", tablesOnCopy, 0},
		{"the same table", "SELECT sum(n) FROM big b JOIN t ON t.id = b.id", tablesTooOld, 1},
		{"the same table, in a subquery", "SELECT sum(n) FROM big WHERE id IN (SELECT id FROM t)", tablesTooOld, 1},
		{"the same table, in a WITH", "WITH w AS (SELECT id FROM t) SELECT sum(n) FROM big JOIN w ON w.id = big.id", tablesTooOld, 1},
		{"the same table, with its schema", "SELECT sum(n) FROM big b JOIN public.t x ON x.id = b.id", tablesTooOld, 1},
		{"the same table, in capitals", "SELECT sum(n) FROM big b JOIN PUBLIC.T x ON x.id = b.id", tablesTooOld, 1},
		{"the same name in another schema, written with it", "SELECT sum(n) FROM big b JOIN other.t x ON x.id = b.id", tablesOnCopy, 0},
		{"a WITH named as the table the source read, when in doubt", "WITH t AS (SELECT id FROM big) SELECT sum(n) FROM big JOIN t ON t.id = big.id", tablesTooOld, 1},
		{"a table whose name the source's is a prefix of", "SELECT sum(n) FROM big JOIN bigger ON bigger.id = big.id", tablesOnCopy, 0},
		{"a table the copy's catalog does not hold", "SELECT sum(n) FROM big JOIN unknown_rel u ON u.id = big.id", tablesTooOld, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := afterT()
			before, _, _ := p.w.asked()
			runs := p.l.runs.Load()
			if got := costSimple(ctx, c, p.l, tc.stmt); got != tc.want {
				t.Errorf("%+v, want %+v", got, tc.want)
			}
			if after, _, _ := p.w.asked(); after-before != tc.waits {
				t.Errorf("waited %d times for the copy, want %d", after-before, tc.waits)
			}
			if d := p.l.runs.Load() - runs; d != 1 {
				t.Errorf("the copy ran %d statements, want 1", d)
			}
		})
	}
	// A statement of the copy that falls to the source is one more answer of
	// the source, about its own tables: after one on big and t, big is held
	// back too, and a third table is not.
	c := afterT()
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big b JOIN t ON t.id = b.id"); got != tablesTooOld {
		t.Fatalf("the report that reads t: %+v", got)
	}
	before, _, _ := p.w.asked()
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != tablesTooOld {
		t.Errorf("after the source answered a report on big: %+v, want %+v", got, tablesTooOld)
	}
	if after, _, _ := p.w.asked(); after != before+1 {
		t.Errorf("after the source answered a report on big: waited %d times, want once", after-before)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM wide"); got != tablesOnCopy {
		t.Errorf("a third table: %+v, want %+v", got, tablesOnCopy)
	}
}

// A bare name the source read marks the table the source resolved it to.
// The copy's statement is held back for that table written with its schema,
// and for the bare name, which the copy may resolve in any schema.
func TestSourceTables_2985_theSameNameInTwoSchemas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// On this source "big" resolves to other.big.
	p := newTablesPort(t, func(f *fakeSource) { f.tables["big"] = [2]string{"other", "big"} })
	const bySource = "SELECT n FROM big WHERE id = 1 AND now() IS NOT NULL"
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, bySource); got.side != RouteSource {
		t.Fatalf("the source's statement: %+v", got)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM public.big"); got != tablesOnCopy {
		t.Errorf("public.big, which the source did not read: %+v, want %+v", got, tablesOnCopy)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM other.big"); got != tablesTooOld {
		t.Errorf("other.big, which the source read: %+v, want %+v", got, tablesTooOld)
	}
	d := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, d, p.l, bySource); got.side != RouteSource {
		t.Fatalf("the source's statement: %+v", got)
	}
	if got := costSimple(ctx, d, p.l, "SELECT sum(n) FROM big"); got != tablesTooOld {
		t.Errorf("big without a schema: %+v, want %+v", got, tablesTooOld)
	}
}

// A table the source names in capitals is the copy's table of that name,
// which the copy matches without regard to case.
func TestSourceTables_2985_aQuotedNameInCapitals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	// Quoted capitals go to the source (the screen's veto).
	if got := costSimple(ctx, c, p.l, `SELECT n FROM "Mixed" WHERE id = 1`); got.side != RouteSource {
		t.Fatalf("the source's statement: %+v", got)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != tablesOnCopy {
		t.Errorf("another table: %+v, want %+v", got, tablesOnCopy)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big JOIN mixed m ON m.id = big.id"); got != tablesTooOld {
		t.Errorf(`the copy's "mixed", the source's "Mixed": %+v, want %+v`, got, tablesTooOld)
	}
}

// Every one of these, answered by the source, marks every table: the report
// on another table is held back, as before the rule was per table.
func TestSourceTables_2985_whatMarksEveryTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, tc := range []struct{ name, stmt string }{
		{"a user's function", "SELECT my_fn(n) FROM t WHERE id = 1"},
		{"a function in FROM", "SELECT g FROM generate_series(1, 3) g"},
		{"a table the source does not confirm (a view, a temporary table, a parent)", "SELECT n FROM some_view WHERE id = 1"},
		{"several statements", "SELECT n FROM t WHERE id = 1; SELECT n FROM t WHERE id = 2"},
		{"TABLE x", "TABLE t"},
		{"a cursor's rows", "FETCH 1 FROM cur"},
		{"ONLY", "SELECT n FROM ONLY t WHERE id = 1"},
		{"LATERAL", "SELECT n FROM t, LATERAL (SELECT 1) x WHERE id = 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTablesPort(t)
			c := costPortConn(t, ctx, p.l)
			if got := costSimple(ctx, c, p.l, tc.stmt); got.side != RouteSource || got.answer != "42" {
				t.Fatalf("the source's statement: %+v", got)
			}
			if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != tablesTooOld {
				t.Errorf("the report on another table: %+v, want %+v", got, tablesTooOld)
			}
			if n, _, _ := p.w.asked(); n != 1 {
				t.Errorf("waited %d times, want once", n)
			}
		})
	}
}

// A source that confirms nothing (it has an operator in SQL, or its answer
// is not the one expected) marks every table.
func TestSourceTables_2985_aSourceThatDoesNotConfirm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t, func(f *fakeSource) { f.tables = nil })
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != tablesTooOld {
		t.Errorf("the report on another table: %+v, want %+v", got, tablesTooOld)
	}
}

// A search_path that names pg_catalog or pg_temp puts a schema before them,
// where a function or an operator of the user's can stand for a built-in
// one: nothing is told from the text then.
func TestSourceTables_2985_aSearchPathThatNamesTheCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	if _, err := c.Exec(ctx, "SET search_path = public, pg_catalog").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}
	if n := len(p.src.proved()); n != 0 {
		t.Errorf("the source was asked to confirm %d statements, want none", n)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != tablesTooOld {
		t.Errorf("the report on another table: %+v, want %+v", got, tablesTooOld)
	}
}

// The extended protocol: a prepared statement the source answers marks its
// tables once its batch is over, at every execution; a statement of the
// copy in the same batch, before that, is held back.
func TestSourceTables_2985_preparedStatements(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	if _, err := c.Prepare(ctx, "byKey", "SELECT n FROM t WHERE id = $1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Prepare(ctx, "report", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Prepare(ctx, "reportT", "SELECT sum(n) FROM big JOIN t ON t.id = big.id", nil); err != nil {
		t.Fatal(err)
	}
	exec := func(name string, args ...string) (string, RouteNote) {
		t.Helper()
		var params [][]byte
		for _, a := range args {
			params = append(params, []byte(a))
		}
		res := c.ExecPrepared(ctx, name, params, nil, nil).Read()
		if res.Err != nil || len(res.Rows) != 1 {
			t.Fatalf("%s: %v %q", name, res.Err, res.Rows)
		}
		return string(res.Rows[0][0]), p.l.last()
	}
	for i := range 3 {
		if got, n := exec("byKey", "1"); got != "42" || n.Side != RouteSource {
			t.Fatalf("run %d, the read by key: %s %+v", i, got, n)
		}
		if n := len(p.src.proved()); n != i+1 {
			t.Fatalf("run %d: the source confirmed %d batches, want %d", i, n, i+1)
		}
		if got, n := exec("report"); got != "7" || n.Side != RouteCopy {
			t.Errorf("run %d, the report on another table: %s %s/%s, want the copy's 7", i, got, n.Side, n.Reason)
		}
	}
	if n, _, _ := p.w.asked(); n != 0 {
		t.Errorf("the reports on another table waited %d times, want none", n)
	}
	if got, n := exec("reportT"); got != "42" || n.Reason != ReasonCopyTooOld {
		t.Errorf("the report that reads t: %s %s/%s, want the source's, copy_too_old", got, n.Side, n.Reason)
	}
	// reportT fell to the source, which then read big too.
	if got, n := exec("report"); got != "42" || n.Reason != ReasonCopyTooOld {
		t.Errorf("the report on big after the source answered about big: %s %s/%s", got, n.Side, n.Reason)
	}
	if c.TxStatus() != 'I' {
		t.Errorf("status %c", c.TxStatus())
	}

	// A prepared statement whose tables are not told from its text.
	d := costPortConn(t, ctx, p.l)
	if _, err := d.Prepare(ctx, "fn", "SELECT my_fn(n) FROM t WHERE id = $1", nil); err != nil {
		t.Fatal(err)
	}
	if res := d.ExecPrepared(ctx, "fn", [][]byte{[]byte("1")}, nil, nil).Read(); res.Err != nil {
		t.Fatal(res.Err)
	}
	if got := costSimple(ctx, d, p.l, "SELECT sum(n) FROM big"); got != tablesTooOld {
		t.Errorf("after a prepared statement that calls a function: %+v, want %+v", got, tablesTooOld)
	}
}

// One batch, two statements and one Sync: the source answers the first, and
// the second, for the copy, runs before the source confirmed anything: it is
// held back.
func TestSourceTables_2985_aStatementOfTheCopyInTheSourcesBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	if _, err := c.Prepare(ctx, "byKey", "SELECT n FROM t WHERE id = $1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Prepare(ctx, "report", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatal(err)
	}
	batch := &pgconn.Batch{}
	batch.ExecPrepared("byKey", [][]byte{[]byte("1")}, nil, nil)
	batch.ExecPrepared("report", nil, nil, nil)
	res, err := c.ExecBatch(ctx, batch).ReadAll()
	if err != nil || len(res) != 2 || len(res[1].Rows) != 1 {
		t.Fatalf("the batch: %v %+v", err, res)
	}
	if got := string(res[1].Rows[0][0]); got != "42" {
		t.Errorf("the report in the source's batch: %s, want the source's 42: nothing was confirmed yet", got)
	}
}

// A batch whose statements for the source ran under two search_paths is not
// confirmed: the names of the first would be resolved under the second.
func TestSourceTables_2985_aBatchUnderTwoSearchPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	for name, stmt := range map[string]string{"byKey": "SELECT n FROM t WHERE id = $1", "path": "SET search_path = other"} {
		if _, err := c.Prepare(ctx, name, stmt, nil); err != nil {
			t.Fatalf("prepare %s: %v", name, err)
		}
	}
	batch := &pgconn.Batch{}
	batch.ExecPrepared("byKey", [][]byte{[]byte("1")}, nil, nil)
	batch.ExecPrepared("path", nil, nil, nil)
	batch.ExecPrepared("byKey", [][]byte{[]byte("2")}, nil, nil)
	if res, err := c.ExecBatch(ctx, batch).ReadAll(); err != nil || len(res) != 3 {
		t.Fatalf("the batch: %v %+v", err, res)
	}
	if n := len(p.src.proved()); n != 0 {
		t.Errorf("the source was asked to confirm %d batches, want none", n)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM public.big"); got != tablesTooOld {
		t.Errorf("a report on another table: %+v, want %+v: every table is marked", got, tablesTooOld)
	}
}

// A portal bound on the source under one search_path and run after the
// session set another: the source resolved its names when it was bound, so
// they are not confirmed under the new one.
func TestSourceTables_2985_aPortalBoundBeforeTheSearchPathChanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	fe := c.Frontend()
	// until reads to the next ReadyForQuery, and returns the first cell of
	// each row and any error's code.
	until := func() (cells []string, code string) {
		t.Helper()
		for {
			msg, err := c.ReceiveMessage(ctx)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			switch m := msg.(type) {
			case *pgproto3.DataRow:
				cells = append(cells, string(m.Values[0]))
			case *pgproto3.ErrorResponse:
				code = m.Code
			case *pgproto3.ReadyForQuery:
				return cells, code
			}
		}
	}
	// One batch: bind a under the first path, set another, bind b (which
	// brings the source to the new path), run a, run b.
	fe.Send(&pgproto3.Parse{Name: "k", Query: "SELECT n FROM t WHERE id = 1"})
	fe.Send(&pgproto3.Bind{DestinationPortal: "a", PreparedStatement: "k"})
	fe.Send(&pgproto3.Parse{Name: "sp", Query: "SET search_path = other"})
	fe.Send(&pgproto3.Bind{DestinationPortal: "s", PreparedStatement: "sp"})
	fe.Send(&pgproto3.Execute{Portal: "s"})
	fe.Send(&pgproto3.Bind{DestinationPortal: "b", PreparedStatement: "k"})
	fe.Send(&pgproto3.Execute{Portal: "a"})
	fe.Send(&pgproto3.Execute{Portal: "b"})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if cells, code := until(); code != "" || len(cells) != 2 {
		t.Fatalf("the batch: %q %s", cells, code)
	}
	fe.Send(&pgproto3.Query{String: "SELECT sum(n) FROM public.big"})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if cells, code := until(); code != "" || len(cells) != 1 || cells[0] != "42" {
		t.Errorf("a report on another table: %q %s, want the source's 42: every table is marked", cells, code)
	}
	if n := p.l.last(); n.Reason != ReasonCopyTooOld {
		t.Errorf("noted %s/%s, want copy_too_old", n.Side, n.Reason)
	}
}

// A batch that never reached its Sync, then a simple query: what the source
// sent in the batch was never confirmed, so every table is marked, whatever
// the simple query's own tables are.
func TestSourceTables_2985_aBatchLeftOpenThenASimpleQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	fe := c.Frontend()
	read := func(done func(pgproto3.BackendMessage) bool) (cells []string, code string) {
		t.Helper()
		for {
			msg, err := c.ReceiveMessage(ctx)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			switch m := msg.(type) {
			case *pgproto3.DataRow:
				cells = append(cells, string(m.Values[0]))
			case *pgproto3.ErrorResponse:
				code = m.Code
			}
			if done(msg) {
				return cells, code
			}
		}
	}
	ready := func(m pgproto3.BackendMessage) bool { _, ok := m.(*pgproto3.ReadyForQuery); return ok }
	fe.Send(&pgproto3.Parse{Name: "k", Query: "SELECT n FROM t WHERE id = 1"})
	fe.Send(&pgproto3.Bind{DestinationPortal: "a", PreparedStatement: "k"})
	fe.Send(&pgproto3.Execute{Portal: "a"})
	fe.Send(&pgproto3.Flush{})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if cells, code := read(func(m pgproto3.BackendMessage) bool { _, ok := m.(*pgproto3.CommandComplete); return ok }); code != "" || len(cells) != 1 {
		t.Fatalf("the batch's rows: %q %s", cells, code)
	}
	// No Sync: a simple query, which the source answers about hb.
	fe.Send(&pgproto3.Query{String: "SELECT n FROM hb WHERE id = 1"})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if cells, code := read(ready); code != "" || len(cells) != 1 || cells[0] != "42" {
		t.Fatalf("the simple query: %q %s", cells, code)
	}
	fe.Send(&pgproto3.Query{String: "SELECT sum(n) FROM big"})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if cells, code := read(ready); code != "" || len(cells) != 1 || cells[0] != "42" {
		t.Errorf("a report on a third table: %q %s, want the source's 42: the batch's tables were never confirmed", cells, code)
	}
	if n := p.l.last(); n.Reason != ReasonCopyTooOld {
		t.Errorf("noted %s/%s, want copy_too_old", n.Side, n.Reason)
	}
}

// A transaction block lives on the source: its statements mark their own
// tables, and after COMMIT a report on another table is answered by the
// copy.
func TestSourceTables_2985_aTransactionBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	for _, stmt := range []string{"BEGIN", "SELECT n FROM t WHERE id = 1", "COMMIT"} {
		if _, err := c.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if n := len(p.src.proved()); n != 1 {
		t.Fatalf("the source confirmed %d statements of the block, want 1", n)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != tablesOnCopy {
		t.Errorf("after the block, a report on another table: %+v, want %+v", got, tablesOnCopy)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big JOIN t ON t.id = big.id"); got != tablesTooOld {
		t.Errorf("after the block, a report that reads t: %+v, want %+v", got, tablesTooOld)
	}
}

// A connection used again after an error: an error alone marks nothing; rows
// and then an error mark the statement's tables.
func TestSourceTables_2985_afterAnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT count(*) FROM nosuch"); got.code != "42P01" {
		t.Fatalf("a table neither side has: %+v", got)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big JOIN t ON t.id = big.id"); got != tablesOnCopy {
		t.Errorf("after an error alone: %+v, want %+v", got, tablesOnCopy)
	}
	if got := costSimple(ctx, c, p.l, "SELECT n FROM rowsthenfail"); got.code != "22012" {
		t.Fatalf("rows then an error: %+v", got)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != tablesOnCopy {
		t.Errorf("after rows of another table and an error: %+v, want %+v", got, tablesOnCopy)
	}
	// The failed statement's own table is held back: its rows were sent.
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big WHERE id IN (SELECT id FROM public.rowsthenfail)"); got.reason != ReasonCopyTooOld {
		t.Errorf("a report on the table whose rows came before the error: %+v, want copy_too_old", got)
	}
}

// A table read from the source and then dropped from the publication is no
// longer in the copy's catalog: a statement that names it is not read as
// apart, and is held back.
func TestSourceTables_2985_aTableThatLeftTheCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.catalog = func() []CatalogTable { return catalogOf("big") }
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != tablesOnCopy {
		t.Errorf("a report on a table still there: %+v, want %+v", got, tablesOnCopy)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big b JOIN t ON t.id = b.id"); got != tablesTooOld {
		t.Errorf("a report that names the table the catalog no longer holds: %+v, want %+v", got, tablesTooOld)
	}
}

// A statement read as naming no table, when the copy says its run read one,
// was read wrong: it is held back.
func TestSourceTables_2985_theCopyReadATableTheTextDidNotName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p := newTablesPort(t)
	p.l.cost = &CostPolicy{}
	c := costPortConn(t, ctx, p.l)
	// Routing by plan off: the read with now() goes to the source by the
	// screen, the others to the copy.
	if got := costSimple(ctx, c, p.l, "SELECT n FROM t WHERE id = 1 AND now() IS NOT NULL"); got.side != RouteSource {
		t.Fatalf("the source's statement: %+v", got)
	}
	if got := costSimple(ctx, c, p.l, "SELECT 1 + 1"); got != tablesTooOld {
		t.Errorf("a statement that names no table, whose run read one: %+v, want %+v", got, tablesTooOld)
	}
	if n, _, _ := p.w.asked(); n != 0 {
		t.Errorf("it waited %d times; nothing in its text said it read a table", n)
	}
	p.knob.set(noTable)
	if got := costSimple(ctx, c, p.l, "SELECT 1 + 1"); got != tablesOnCopy {
		t.Errorf("a statement that names no table and read none: %+v, want %+v", got, tablesOnCopy)
	}
}

// The wait is 6 seconds unless a test shortens it.
func TestCopyWaitLimit_default(t *testing.T) {
	if DefaultCopyWaitLimit != 6*time.Second || copyWaitLimit != DefaultCopyWaitLimit {
		t.Errorf("the copy waits %v (default %v), want 6s", copyWaitLimit, DefaultCopyWaitLimit)
	}
}

// A copy that reaches the source's position after 4 seconds answers: the
// wait of 6 covers it.
func TestSourceTables_2985_theCopyReachesThePositionAfterFourSeconds(t *testing.T) {
	if testing.Short() {
		t.Skip("waits four seconds")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	knob := &copyAgeKnob{}
	knob.at(time.Now())
	var deadline atomic.Int64
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src, age: knob.age, wait: func(ctx context.Context, _ uint64) (CopyWaitOutcome, string) {
		if d, ok := ctx.Deadline(); ok {
			deadline.Store(int64(time.Until(d)))
		}
		select {
		case <-time.After(4 * time.Second):
			return CopyReached, ""
		case <-ctx.Done():
			return CopyNotReached, "ran out"
		}
	}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("the read by key: %+v", got)
	}
	start := time.Now()
	if got := costSimple(ctx, c, l, "SELECT sum(n) FROM big"); got != tablesOnCopy {
		t.Errorf("the copy reached the position after 4 s: %+v, want %+v", got, tablesOnCopy)
	}
	if took := time.Since(start); took < 4*time.Second || took > 5500*time.Millisecond {
		t.Errorf("answered after %v, want a little over 4 s", took)
	}
	if d := time.Duration(deadline.Load()); d < 5*time.Second || d > 6*time.Second {
		t.Errorf("the wait was given %v, want just under 6 s", d)
	}
}
