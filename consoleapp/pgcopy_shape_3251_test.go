package consoleapp

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The source's shape of each table, as the PostgreSQL port asks it (#3251):
// kept a moment per table, so a burst of statements asks once, and asked
// again soon after, so an ALTER TABLE is seen within that moment.

// shapeAskLog is a fake source: what it was asked, and what it answers.
type shapeAskLog struct {
	mu    sync.Mutex
	asked [][]string
	now   map[[2]string]pgshim.SourceShape
	err   error
}

func (a *shapeAskLog) ask(_ context.Context, schemas, names []string) (map[[2]string]pgshim.SourceShape, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var q []string
	for i := range schemas {
		q = append(q, schemas[i]+"."+names[i])
	}
	a.asked = append(a.asked, q)
	if a.err != nil {
		return nil, a.err
	}
	out := map[[2]string]pgshim.SourceShape{}
	for i := range schemas {
		if s, ok := a.now[[2]string{schemas[i], names[i]}]; ok {
			out[[2]string{schemas[i], names[i]}] = s
		}
	}
	return out, nil
}

func (a *shapeAskLog) log() [][]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.asked)
}

func TestPGShapeCache_3251(t *testing.T) {
	at := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	src := &shapeAskLog{now: map[[2]string]pgshim.SourceShape{
		{"public", "a"}: {OID: 1, Columns: []baseline.PGColumn{{Name: "id", TypeOID: 23, TypeMod: -1, Ordinal: 1}}},
		{"public", "b"}: {OID: 2},
	}}
	c := &pgShapeCache{ttl: time.Second, failTTL: 2 * time.Second, now: func() time.Time { return at }, ask: src.ask}
	ctx := context.Background()
	get := func(tables ...[2]string) map[[2]string]pgshim.SourceShape {
		t.Helper()
		got, err := c.shapes(ctx, tables)
		if err != nil {
			t.Fatalf("%v: %v", tables, err)
		}
		return got
	}
	if got := get([2]string{"public", "a"}); got[[2]string{"public", "a"}].OID != 1 || len(got) != 1 {
		t.Fatalf("a: %v", got)
	}
	// Within the moment: not asked again; a table not known yet is asked
	// alone.
	get([2]string{"public", "a"})
	if got := get([2]string{"public", "a"}, [2]string{"public", "b"}); len(got) != 2 {
		t.Errorf("a and b: %v", got)
	}
	if got := src.log(); !slices.EqualFunc(got, [][]string{{"public.a"}, {"public.b"}}, slices.Equal) {
		t.Errorf("asked %q, want a, then b alone", got)
	}
	// A table the source does not have is an answer too, kept as long.
	if got := get([2]string{"public", "gone"}); len(got) != 0 {
		t.Errorf("gone: %v, want nothing", got)
	}
	get([2]string{"public", "gone"})
	if n := len(src.log()); n != 3 {
		t.Errorf("asked %d times, want 3: a table not found is not asked again at once", n)
	}
	// Past the moment, asked again: an ALTER TABLE since is seen.
	src.mu.Lock()
	src.now[[2]string{"public", "a"}] = pgshim.SourceShape{OID: 1, Columns: []baseline.PGColumn{{Name: "id", TypeOID: 20, TypeMod: -1, Ordinal: 1}}}
	src.mu.Unlock()
	at = at.Add(1100 * time.Millisecond)
	if got := get([2]string{"public", "a"}); got[[2]string{"public", "a"}].Columns[0].TypeOID != 20 {
		t.Errorf("after the moment: %v, want the source's new type", got)
	}
	if got := src.log(); !slices.Equal(got[len(got)-1], []string{"public.a"}) {
		t.Errorf("last asked %q, want public.a", got[len(got)-1])
	}

	// A failure is the answer for a while: a table that must be asked gets
	// it, and the source is not asked again meanwhile.
	src.mu.Lock()
	src.err = errors.New("connection refused")
	src.mu.Unlock()
	at = at.Add(1100 * time.Millisecond)
	if _, err := c.shapes(ctx, [][2]string{{"public", "a"}}); err == nil {
		t.Fatal("the source failed and the cache answered")
	}
	n := len(src.log())
	if _, err := c.shapes(ctx, [][2]string{{"public", "b"}}); err == nil {
		t.Error("asked again during the failure, and it answered")
	}
	if len(src.log()) != n {
		t.Error("the source was asked again during the failure's time")
	}
	src.mu.Lock()
	src.err = nil
	src.mu.Unlock()
	at = at.Add(2100 * time.Millisecond)
	if _, err := c.shapes(ctx, [][2]string{{"public", "a"}}); err != nil {
		t.Errorf("after the failure's time: %v, want the source asked again", err)
	}
	// No table, no question.
	n = len(src.log())
	if got, err := c.shapes(ctx, nil); err != nil || len(got) != 0 || len(src.log()) != n {
		t.Errorf("no table: %v %v, asked %d times more", got, err, len(src.log())-n)
	}
}

// The questions that meet ask once.
func TestPGShapeCache_concurrentQuestionsAskOnce_3251(t *testing.T) {
	var mu sync.Mutex
	asked := 0
	gate := make(chan struct{})
	c := &pgShapeCache{ttl: time.Minute, failTTL: time.Minute, now: time.Now, ask: func(context.Context, []string, []string) (map[[2]string]pgshim.SourceShape, error) {
		mu.Lock()
		asked++
		mu.Unlock()
		<-gate
		return map[[2]string]pgshim.SourceShape{{"public", "a"}: {OID: 1}}, nil
	}}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := c.shapes(context.Background(), [][2]string{{"public", "a"}}); err != nil || got[[2]string{"public", "a"}].OID != 1 {
				t.Errorf("%v %v", got, err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()
	if asked != 1 {
		t.Errorf("asked %d times, want once", asked)
	}
}

// fakeCatalogSource is a copy whose snapshot records one table's shape and
// not the other's.
type fakeCatalogSource struct{ tables []console.CopyTable }

func (f fakeCatalogSource) PGCopyStampFresh(context.Context) (string, error) { return "s1", nil }
func (f fakeCatalogSource) CopyTablesStamped(context.Context) ([]console.CopyTable, string, error) {
	return f.tables, "s1", nil
}

// The port's catalog carries the shape each table's snapshot recorded,
// whole, the columns the server hides included: what the source's columns
// are compared with.
func TestPGCopyCatalog_recordedShape_3251(t *testing.T) {
	rel := &baseline.PGRelation{OID: 16401, Columns: []baseline.PGColumn{
		{Name: "id", TypeOID: 23, TypeMod: -1, Ordinal: 1},
		{Name: "secret", TypeOID: 25, TypeMod: -1, Ordinal: 2},
	}}
	src := fakeCatalogSource{tables: []console.CopyTable{
		{Schema: "public", Table: "shaped", Relation: rel, Hidden: []string{"secret"}},
		{Schema: "public", Table: "old"},
	}}
	run := func(context.Context, string) (sqlsandbox.Result, error) {
		return sqlsandbox.Result{Rows: [][]any{{"public", "old", "id", "INTEGER"}, {"public", "shaped", "id", "INTEGER"}}}, nil
	}
	got, err := pgCopyCatalog(context.Background(), src, run, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "old" || got[1].Name != "shaped" {
		t.Fatalf("%+v", got)
	}
	if got[0].Recorded != nil {
		t.Errorf("a snapshot that records no shape: %+v, want none", got[0].Recorded)
	}
	if r := got[1].Recorded; r == nil || r.OID != 16401 || len(r.Columns) != 2 || r.Columns[1].Name != "secret" {
		t.Errorf("the recorded shape: %+v, want the snapshot's, its hidden column included", r)
	}
}
