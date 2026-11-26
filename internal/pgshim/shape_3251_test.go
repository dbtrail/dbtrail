package pgshim

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A table changed on the source since its snapshot (#3251). PostgreSQL's
// change stream announces no ALTER TABLE: capture learns a table's new shape
// from the next write to it, and the copy keeps the snapshot's columns until
// the table's next snapshot. A statement that reads such a table is answered
// by the source, before the copy runs anything; one that reads only other
// tables stays on the copy.

// shapeKnob is a host's Binding.SourceShapes: what the source's catalog
// says of each table now, or an error, and what it was asked.
type shapeKnob struct {
	mu    sync.Mutex
	now   map[[2]string]SourceShape
	err   error
	asked atomic.Int32
	last  [][2]string
}

func (k *shapeKnob) shapes(_ context.Context, tables [][2]string) (map[[2]string]SourceShape, error) {
	k.asked.Add(1)
	k.mu.Lock()
	defer k.mu.Unlock()
	k.last = append([][2]string(nil), tables...)
	if k.err != nil {
		return nil, k.err
	}
	out := map[[2]string]SourceShape{}
	for _, t := range tables {
		if s, ok := k.now[t]; ok {
			out[t] = s
		}
	}
	return out, nil
}

func (k *shapeKnob) edit(f func(now map[[2]string]SourceShape)) {
	k.mu.Lock()
	defer k.mu.Unlock()
	f(k.now)
}

func (k *shapeKnob) fail(err error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.err = err
}

func (k *shapeKnob) lastAsked() [][2]string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([][2]string(nil), k.last...)
}

// shapedCatalogOf is catalogOf with the shape each table's snapshot
// recorded: its OID and its two columns, id and n, both int.
func shapedCatalogOf(names ...string) []CatalogTable {
	tables := catalogOf(names...)
	for i := range tables {
		tables[i].OID = uint32(16400 + i)
		tables[i].Recorded = &baseline.PGRelation{OID: tables[i].OID, Columns: []baseline.PGColumn{
			{Name: "id", TypeOID: 23, TypeMod: -1, Ordinal: 1},
			{Name: "n", TypeOID: 23, TypeMod: -1, Ordinal: 2},
		}}
	}
	return tables
}

// sameShapes is what a source whose tables are all as their snapshots
// recorded them answers.
func sameShapes(tables []CatalogTable) map[[2]string]SourceShape {
	out := map[[2]string]SourceShape{}
	for _, t := range tables {
		out[[2]string{t.Schema, t.Name}] = SourceShape{OID: t.Recorded.OID, Columns: slices.Clone(t.Recorded.Columns)}
	}
	return out
}

var shapePortTables = []string{"t", "big", "hb", "wide", "other.t", "other.big", "mixed", "bigger", "rowsthenfail"}

// newShapePort is newTablesPort with snapshots that record each table's
// shape, and a host that says the source's.
func newShapePort(t *testing.T) (*tablesPort, *shapeKnob) {
	t.Helper()
	p := newTablesPort(t)
	p.l.catalog = func() []CatalogTable { return shapedCatalogOf(shapePortTables...) }
	k := &shapeKnob{now: sameShapes(shapedCatalogOf(shapePortTables...))}
	p.l.shapes = k.shapes
	return p, k
}

var (
	shapeDiffers   = costOutcome{"42", "", RouteSource, ReasonCopyShapeDiffers}
	shapeUnchecked = costOutcome{"", "55000", RouteRefused, ReasonCopyShapeUnchecked}
)

func addColumn(table string) func(map[[2]string]SourceShape) {
	return func(now map[[2]string]SourceShape) {
		s := now[[2]string{"public", table}]
		s.Columns = append(slices.Clone(s.Columns), baseline.PGColumn{Name: "e", TypeOID: 23, TypeMod: -1, Ordinal: 3})
		now[[2]string{"public", table}] = s
	}
}

func TestCopyShape_3251_simple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newShapePort(t)
	c := costPortConn(t, ctx, p.l)
	// A statement the copy should answer runs on a connection of its own:
	// one the source already answered about a table holds the copy's answer
	// from a snapshot back (copy_too_old, #2985), which is not this rule.
	expect := func(name, stmt string, want costOutcome, runs int32) {
		t.Helper()
		on := c
		if want.side == RouteCopy {
			on = costPortConn(t, ctx, p.l)
		}
		before := p.l.runs.Load()
		if got := costSimple(ctx, on, p.l, stmt); got != want {
			t.Errorf("%s: %s: %+v, want %+v", name, stmt, got, want)
		}
		if d := p.l.runs.Load() - before; d != runs {
			t.Errorf("%s: the copy ran %d statements, want %d", name, d, runs)
		}
	}
	expect("every shape as its snapshot", "SELECT sum(n) FROM big", tablesOnCopy, 1)
	// big without a schema is big in every schema the copy holds.
	if got := k.lastAsked(); !slices.Equal(got, [][2]string{{"public", "big"}, {"other", "big"}}) {
		t.Errorf("the source was asked about %v, want public.big and other.big: the tables the statement may read", got)
	}

	k.edit(addColumn("big"))
	for _, tc := range []struct{ name, stmt string }{
		{"named columns", "SELECT sum(n) FROM big"},
		{"SELECT *", "SELECT * FROM big"},
		{"with its schema", "SELECT sum(n) FROM public.big"},
		{"in a join with a table that did not change", "SELECT sum(n) FROM wide JOIN big ON big.id = wide.id"},
		{"in a subquery", "SELECT sum(n) FROM wide WHERE id IN (SELECT id FROM big)"},
		// Heavy on the source, with a veto the copy would answer it in spite
		// of (heavy_on_copy): a table of other columns is not a value written
		// a little otherwise, and the source answers.
		{"heavy, with a veto heavy statements stay on the copy in spite of", "SELECT sum(n) FROM big WHERE now() > '2000-01-01'"},
	} {
		expect(tc.name, tc.stmt, shapeDiffers, 0)
	}
	expect("a table that did not change", "SELECT sum(n) FROM wide", tablesOnCopy, 1)
	expect("the same name in another schema, which did not change", "SELECT sum(n) FROM other.big", tablesOnCopy, 1)
	// A name without a schema is that name in every schema the copy holds:
	// in doubt, the source.
	if _, err := c.Exec(ctx, "SET search_path TO other").ReadAll(); err != nil {
		t.Fatal(err)
	}
	expect("under another search_path, an unqualified name of the changed table", "SELECT sum(n) FROM big", shapeDiffers, 0)
	if _, err := c.Exec(ctx, "SET search_path TO public").ReadAll(); err != nil {
		t.Fatal(err)
	}
	// The copy's own commands are not the source's: asked of nothing.
	asked := k.asked.Load()
	own := costPortConn(t, ctx, p.l)
	for _, stmt := range []string{"SHOW TABLES", "DESCRIBE big"} {
		if got := costSimple(ctx, own, p.l, stmt); got.code != "" || got.side != RouteCopy || got.reason != ReasonCopyCommand {
			t.Errorf("%s: %+v, want the copy's", stmt, got)
		}
	}
	if k.asked.Load() != asked {
		t.Error("the source was asked about the shapes for a command of the copy's own")
	}

	// Each other way the source's table is not the snapshot's.
	for _, tc := range []struct {
		name string
		edit func(map[[2]string]SourceShape)
	}{
		{"retyped", func(now map[[2]string]SourceShape) {
			s := now[[2]string{"public", "wide"}]
			s.Columns = slices.Clone(s.Columns)
			s.Columns[1].TypeOID = 20
			now[[2]string{"public", "wide"}] = s
		}},
		{"dropped and created again", func(now map[[2]string]SourceShape) {
			s := now[[2]string{"public", "wide"}]
			s.OID++
			now[[2]string{"public", "wide"}] = s
		}},
		{"dropped or renamed", func(now map[[2]string]SourceShape) { delete(now, [2]string{"public", "wide"}) }},
	} {
		k.edit(func(now map[[2]string]SourceShape) {
			now[[2]string{"public", "wide"}] = sameShapes(shapedCatalogOf(shapePortTables...))[[2]string{"public", "wide"}]
		})
		expect(tc.name+", before", "SELECT sum(n) FROM wide", tablesOnCopy, 1)
		k.edit(tc.edit)
		expect(tc.name, "SELECT sum(n) FROM wide", shapeDiffers, 0)
	}

	// The table's next snapshot records the source's shape: the copy again,
	// on the same connection.
	k.edit(func(now map[[2]string]SourceShape) {
		clear(now)
		putShapes(now, sameShapes(shapedCatalogOf(shapePortTables...)))
	})
	expect("healed", "SELECT sum(n) FROM big", tablesOnCopy, 1)
}

func putShapes(dst, src map[[2]string]SourceShape) {
	for k, v := range src {
		dst[k] = v
	}
}

// A snapshot that does not record a table's shape cannot be compared with
// the source's: the source answers.
func TestCopyShape_3251_noShapeRecorded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, _ := newShapePort(t)
	p.l.catalog = func() []CatalogTable {
		tables := shapedCatalogOf(shapePortTables...)
		tables[1].Recorded = nil // big
		return tables
	}
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != shapeDiffers {
		t.Errorf("no shape recorded: %+v, want %+v", got, shapeDiffers)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM wide"); got != tablesOnCopy {
		t.Errorf("another table: %+v, want the copy", got)
	}
}

// The source cannot be asked (down, refusing the account, slower than the
// question's time): the statement fails closed. Nothing is run on the copy,
// whose columns may be the old ones, and nothing is sent to a source that
// just did not answer: it is refused, and can be run again.
func TestCopyShape_3251_sourceCannotBeAsked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newShapePort(t)
	k.fail(errors.New("dial tcp 10.0.3.7:5432: connect: connection refused"))
	c := costPortConn(t, ctx, p.l)
	runs := p.l.runs.Load()
	res, err := c.Exec(ctx, "SELECT sum(n) FROM big").ReadAll()
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55000" || len(res) > 0 && len(res[0].Rows) > 0 {
		t.Fatalf("simple: %v %v, want 55000", err, res)
	}
	if strings.Contains(pe.Message, "10.0.3.7") || !strings.Contains(pe.Message, "could not check") {
		t.Errorf("the client's message: %q: it says what happened, and the address stays in the log", pe.Message)
	}
	if n := p.l.last(); n.Side != RouteRefused || n.Reason != ReasonCopyShapeUnchecked {
		t.Errorf("noted %s/%s, want refused/%s", n.Side, n.Reason, ReasonCopyShapeUnchecked)
	}
	// Extended: Prepare describes the statement, which is where it is
	// refused; an Execute of one prepared earlier is refused too.
	if _, err := c.Prepare(ctx, "s", "SELECT sum(n) FROM big", nil); !errors.As(err, &pe) || pe.Code != "55000" {
		t.Errorf("prepare: %v, want 55000", err)
	}
	if d := p.l.runs.Load() - runs; d != 0 {
		t.Errorf("the copy ran %d statements, want none", d)
	}
	k.fail(nil)
	if _, err := c.Prepare(ctx, "early", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatal(err)
	}
	// Its Describe ran on the copy; its execution must not.
	runs = p.l.runs.Load()
	k.fail(errors.New("timeout: the source did not answer within 2s"))
	if r := c.ExecPrepared(ctx, "early", nil, nil, nil).Read(); !errors.As(r.Err, &pe) || pe.Code != "55000" {
		t.Errorf("execute: %v, want 55000", r.Err)
	}
	if n := p.l.last(); n.Side != RouteRefused || n.Reason != ReasonCopyShapeUnchecked {
		t.Errorf("execute noted %s/%s, want refused/%s", n.Side, n.Reason, ReasonCopyShapeUnchecked)
	}
	if d := p.l.runs.Load() - runs; d != 0 {
		t.Errorf("the copy ran %d statements, want none", d)
	}
	// A statement that reads no table of the copy asks nothing.
	asked := k.asked.Load()
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM nosuch_on_copy"); got.side == RouteRefused {
		t.Errorf("a table the copy does not hold: %+v, not refused for its shape", got)
	}
	if k.asked.Load() != asked {
		t.Error("the source was asked about the shape of no table of the copy")
	}
}

// Prepared statements: one described on the copy before the change, whose
// execution meets the change, goes to the source then, and back to the copy
// once healed; one described after the change is described by the source
// and stays there, as PostgreSQL keeps a prepared statement to the shape it
// was described with.
func TestCopyShape_3251_preparedStatements(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newShapePort(t)
	c := costPortConn(t, ctx, p.l)
	if _, err := c.Prepare(ctx, "before", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatal(err)
	}
	exec := func(name string) (string, RouteNote) {
		t.Helper()
		res := c.ExecPrepared(ctx, name, nil, nil, nil).Read()
		if res.Err != nil || len(res.Rows) != 1 {
			t.Fatalf("%s: %v %q", name, res.Err, res.Rows)
		}
		return string(res.Rows[0][0]), p.l.last()
	}
	if got, n := exec("before"); got != "7" || n.Side != RouteCopy {
		t.Fatalf("before the change: %s %s/%s, want the copy's 7", got, n.Side, n.Reason)
	}
	k.edit(addColumn("big"))
	for i := range 2 {
		if got, n := exec("before"); got != "42" || n.Side != RouteSource || n.Reason != ReasonCopyShapeDiffers {
			t.Errorf("run %d after the change: %s %s/%s, want the source's 42, %s", i, got, n.Side, n.Reason, ReasonCopyShapeDiffers)
		}
	}
	if _, err := c.Prepare(ctx, "after", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatal(err)
	}
	if got, n := exec("after"); got != "42" || n.Side != RouteSource || n.Reason != ReasonCopyShapeDiffers {
		t.Errorf("prepared after the change: %s %s/%s, want the source's 42", got, n.Side, n.Reason)
	}
	k.edit(func(now map[[2]string]SourceShape) { putShapes(now, sameShapes(shapedCatalogOf(shapePortTables...))) })
	// Healed, the shape rule holds it no more. The source answered this
	// connection about big meanwhile, so the copy's snapshot answer is held
	// back by the rule of #2985 (copy_too_old), as for any statement.
	if _, n := exec("before"); n.Reason == ReasonCopyShapeDiffers {
		t.Errorf("described before, healed: %s/%s: the reason is the moment's, and must not keep the statement", n.Side, n.Reason)
	}
	d := costPortConn(t, ctx, p.l)
	if _, err := d.Prepare(ctx, "again", "SELECT sum(n) FROM big", nil); err != nil {
		t.Fatal(err)
	}
	if res := d.ExecPrepared(ctx, "again", nil, nil, nil).Read(); res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "7" {
		t.Errorf("healed, a new connection: %v %q, want the copy's 7", res.Err, res.Rows)
	}
	if got, n := exec("after"); got != "42" || n.Side != RouteSource {
		t.Errorf("described by the source, healed: %s %s/%s, want the source still", got, n.Side, n.Reason)
	}
	if !transientFallback(ReasonCopyShapeDiffers) {
		t.Error("copy_shape_differs must not keep a statement described on the copy on the source for good")
	}
}

// A connection that sends nothing to its source (the server hides tables or
// columns): the copy would answer with the old columns, and nothing else can
// answer, so the statement is refused, as a char(n) or a nondeterministic
// collation the copy cannot compare is refused there. A table that did not
// change is the copy's.
func TestCopyShape_3251_copyOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newShapePort(t)
	p.l.barred = func() string { return "the server hides tables or columns" }
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != (costOutcome{"7", "", RouteCopy, ReasonHiddenRules}) {
		t.Errorf("unchanged: %+v, want the copy's", got)
	}
	k.edit(addColumn("big"))
	refused := costOutcome{"", "55000", RouteRefused, ReasonCopyShapeDiffers}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != refused {
		t.Errorf("changed: %+v, want %+v", got, refused)
	}
	var pe *pgconn.PgError
	if _, err := c.Prepare(ctx, "s", "SELECT sum(n) FROM big", nil); !errors.As(err, &pe) || pe.Code != "55000" || !strings.Contains(pe.Message, "public.big") {
		t.Errorf("prepare: %v, want 55000 naming public.big", err)
	}
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM wide"); got != (costOutcome{"7", "", RouteCopy, ReasonHiddenRules}) {
		t.Errorf("another table: %+v, want the copy's", got)
	}
	k.fail(errors.New("boom"))
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM wide"); got != shapeUnchecked {
		t.Errorf("the source cannot be asked: %+v, want %+v", got, shapeUnchecked)
	}
}

func TestCopyShapeWhy_3251(t *testing.T) {
	tables := shapedCatalogOf("t")
	tbl := &tables[0]
	now := sameShapes(tables)[[2]string{"public", "t"}]
	if got, _ := copyShapeWhy(tbl, now, true); got != "" {
		t.Errorf("the same: %q", got)
	}
	for _, tc := range []struct {
		name  string
		tbl   func(CatalogTable) CatalogTable
		now   func(SourceShape) SourceShape
		found bool
		want  string
	}{
		{"not on the source", nil, nil, false, "public.t is not on your PostgreSQL under that name"},
		{"no shape recorded", func(c CatalogTable) CatalogTable { c.Recorded = nil; return c }, nil, true, "does not record the columns of public.t"},
		{"another OID", nil, func(s SourceShape) SourceShape { s.OID = 1; return s }, true, "dropped and created again"},
		{"a column added", nil, func(s SourceShape) SourceShape {
			s.Columns = append(slices.Clone(s.Columns), baseline.PGColumn{Name: "e", TypeOID: 25, TypeMod: -1, Ordinal: 3})
			return s
		}, true, "the columns of public.t on your PostgreSQL are not the ones"},
	} {
		tt, nn := *tbl, now
		if tc.tbl != nil {
			tt = tc.tbl(tt)
		}
		if tc.now != nil {
			nn = tc.now(nn)
		}
		if got, _ := copyShapeWhy(&tt, nn, tc.found); !strings.Contains(got, tc.want) {
			t.Errorf("%s: %q, want it to say %q", tc.name, got, tc.want)
		}
	}
	// Which columns changed goes to the log, never to the client.
	added := now
	added.Columns = append(slices.Clone(now.Columns), baseline.PGColumn{Name: "e", TypeOID: 25, TypeMod: -1, Ordinal: 3})
	if why, detail := copyShapeWhy(tbl, added, true); strings.Contains(why, `e`+")") || strings.Contains(why, "added") || detail != "added e" {
		t.Errorf("a column added: client %q, log %q", why, detail)
	}
}

// A column the server hides from the client changes type on the source: the
// statement is refused, and the client is not told the column's name.
func TestCopyShape_3251_hiddenColumnNotNamed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, k := newShapePort(t)
	// big's snapshot records a column secret_salary the server hides: it is
	// not among the catalog's columns, which are the ones a statement reads.
	withHidden := func() []CatalogTable {
		tables := shapedCatalogOf(shapePortTables...)
		r := *tables[1].Recorded
		r.Columns = append(slices.Clone(r.Columns), baseline.PGColumn{Name: "secret_salary", TypeOID: 1700, TypeMod: -1, Ordinal: 3})
		tables[1].Recorded = &r
		return tables
	}
	p.l.catalog = withHidden
	k.edit(func(now map[[2]string]SourceShape) { putShapes(now, sameShapes(withHidden())) })
	p.l.barred = func() string { return "the server hides tables or columns" }
	c := costPortConn(t, ctx, p.l)
	if got := costSimple(ctx, c, p.l, "SELECT sum(n) FROM big"); got != (costOutcome{"7", "", RouteCopy, ReasonHiddenRules}) {
		t.Fatalf("unchanged: %+v, want the copy's", got)
	}
	k.edit(func(now map[[2]string]SourceShape) {
		s := now[[2]string{"public", "big"}]
		s.Columns = slices.Clone(s.Columns)
		s.Columns[2].TypeOID = 25
		now[[2]string{"public", "big"}] = s
	})
	var pe *pgconn.PgError
	_, err := c.Exec(ctx, "SELECT sum(n) FROM big").ReadAll()
	if !errors.As(err, &pe) || pe.Code != "55000" {
		t.Fatalf("simple: %v, want 55000", err)
	}
	if strings.Contains(strings.ToLower(pe.Message+pe.Detail+pe.Hint), "secret") {
		t.Errorf("simple: the client was told the hidden column's name: %q", pe.Message)
	}
	if _, err := c.Prepare(ctx, "s", "SELECT sum(n) FROM big", nil); !errors.As(err, &pe) || pe.Code != "55000" || strings.Contains(strings.ToLower(pe.Message), "secret") {
		t.Errorf("prepare: %v, want 55000 without the hidden column's name", err)
	}
}

func TestShapeTablesRead_3251(t *testing.T) {
	tables := shapedCatalogOf("t", "other.t", "big", "sales.Mixed", "p_2026")
	name := func(idx []int) []string {
		var out []string
		for _, i := range idx {
			out = append(out, tables[i].Schema+"."+tables[i].Name)
		}
		return out
	}
	all := []string{"public.t", "other.t", "public.big", "sales.Mixed", "public.p_2026"}
	for _, tc := range []struct {
		stmt string
		want []string
	}{
		{"SELECT 1", nil},
		{"SELECT sum(n) FROM big", []string{"public.big"}},
		// A name without a schema is that name in any schema.
		{"SELECT * FROM t", []string{"public.t", "other.t"}},
		{"SELECT * FROM other.t", []string{"other.t"}},
		{`SELECT * FROM "public"."t"`, []string{"public.t"}},
		// Case is not compared, as the copy matches names.
		{"SELECT * FROM mixed", []string{"sales.Mixed"}},
		{`SELECT * FROM SALES."MIXED"`, []string{"sales.Mixed"}},
		{"SELECT * FROM t JOIN big ON t.id = big.id", []string{"public.t", "other.t", "public.big"}},
		{"SELECT * FROM big WHERE id IN (SELECT id FROM other.t)", []string{"other.t", "public.big"}},
		// A partition is a table of its own on the copy.
		{"SELECT * FROM p_2026", []string{"public.p_2026"}},
		{"SELECT * FROM nosuch", nil},
		// A WITH named as a table counts as that table: in doubt, the source.
		{"WITH big AS (SELECT 1 AS n) SELECT n FROM big", []string{"public.big"}},
		// What the text does not say: every table.
		{"SELECT * FROM db.public.t", all},
		{"SELECT 1; SELECT 2", all},
		{"SELECT sum(n) FROM generate_series(1, 3) g(n)", all},
	} {
		got := name(shapeTablesRead(tc.stmt, tables))
		slices.Sort(got)
		want := slices.Clone(tc.want)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: %v, want %v", tc.stmt, got, want)
		}
	}
}

func TestCopyShapeReasons_3251(t *testing.T) {
	for _, r := range []string{ReasonCopyShapeDiffers, ReasonCopyShapeUnchecked} {
		if reasonMeans[r] == "" {
			t.Errorf("%s has no sentence in the port's status", r)
		}
	}
}
