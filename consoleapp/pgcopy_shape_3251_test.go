package consoleapp

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

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

// A question takes one of the port's connections to its sources before it
// connects, and gives it back: the port's bound on them holds for it. With
// none free it does not connect at all.
func TestPGAskShapes_takesASourceSlot_3251(t *testing.T) {
	unreachable := "postgres://nobody@127.0.0.1:1/none?connect_timeout=1&sslmode=disable"
	var taken, given int
	acquire := func(context.Context) (func(), bool) {
		taken++
		return func() { given++ }, true
	}
	if _, err := pgAskShapes(context.Background(), unreachable, acquire, []string{"public"}, []string{"t"}); err == nil {
		t.Fatal("a source nothing listens for answered")
	}
	if taken != 1 || given != 1 {
		t.Errorf("took %d connections and gave back %d, want 1 and 1", taken, given)
	}
	none := func(context.Context) (func(), bool) { return nil, false }
	started := time.Now()
	if _, err := pgAskShapes(context.Background(), unreachable, none, []string{"public"}, []string{"t"}); !errors.Is(err, errNoSourceSlot) {
		t.Errorf("no connection free: %v, want %v", err, errNoSourceSlot)
	}
	if d := time.Since(started); d > 500*time.Millisecond {
		t.Errorf("no connection free took %v: it must not try to connect", d)
	}
	// Through the port's own slots: every one taken, the question waits no
	// longer than pgShapeWait and fails.
	slots := newPGSourceSlots(1)
	slots.wait = time.Minute
	hold, ok := slots.acquire(context.Background())
	if !ok {
		t.Fatal("the one slot")
	}
	defer hold()
	ask := pgPortSourceShapes(unreachable, "test", slots)
	started = time.Now()
	if _, err := ask(context.Background(), [][2]string{{"public", "t"}}); !errors.Is(err, errNoSourceSlot) {
		t.Errorf("the port's slots all taken: %v, want %v", err, errNoSourceSlot)
	}
	if d := time.Since(started); d < pgShapeWait-100*time.Millisecond || d > pgShapeWait+time.Second {
		t.Errorf("waited %v for a slot, want about %v", d, pgShapeWait)
	}
}

// A source that answers slowly (or a port whose connections to it are all
// taken): its answer, and its failure, are dated when they arrive, so the
// statements that follow within ttl, or failTTL, are not asked again.
func TestPGShapeCache_datedOnArrival_3251(t *testing.T) {
	var mu sync.Mutex
	at := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return at }
	asked := 0
	var fail error
	c := &pgShapeCache{ttl: time.Second, failTTL: 2 * time.Second, now: clock, ask: func(context.Context, []string, []string) (map[[2]string]pgshim.SourceShape, error) {
		mu.Lock()
		defer mu.Unlock()
		asked++
		at = at.Add(1500 * time.Millisecond) // longer than ttl
		if fail != nil {
			return nil, fail
		}
		return map[[2]string]pgshim.SourceShape{{"public", "a"}: {OID: 1}}, nil
	}}
	ctx := context.Background()
	for range 3 {
		if _, err := c.shapes(ctx, [][2]string{{"public", "a"}}); err != nil {
			t.Fatal(err)
		}
	}
	if asked != 1 {
		t.Errorf("asked %d times, want once: an answer slower than ttl is dated when it came", asked)
	}
	mu.Lock()
	at = at.Add(1100 * time.Millisecond)
	fail = errors.New("slow")
	mu.Unlock()
	for range 3 {
		if _, err := c.shapes(ctx, [][2]string{{"public", "a"}}); err == nil {
			t.Fatal("the failure was not the answer")
		}
	}
	if asked != 2 {
		t.Errorf("asked %d times, want twice: a failure slower than failTTL is dated when it came", asked)
	}
}

// A question on its way holds nobody else up: a statement whose tables are
// known is answered at once, and one that waits for the question stops
// waiting when its own context ends, without failing it for the others.
func TestPGShapeCache_aQuestionHoldsNobodyUp_3251(t *testing.T) {
	gate := make(chan struct{})
	var mu sync.Mutex
	asked := 0
	c := &pgShapeCache{ttl: time.Minute, failTTL: time.Minute, now: time.Now, ask: func(ctx context.Context, _, names []string) (map[[2]string]pgshim.SourceShape, error) {
		mu.Lock()
		asked++
		mu.Unlock()
		if names[0] == "slow" {
			select {
			case <-gate:
			case <-ctx.Done():
				// A question tied to the statement that asked it would end
				// here when that statement is cancelled.
				return nil, ctx.Err()
			}
		}
		out := map[[2]string]pgshim.SourceShape{}
		for _, n := range names {
			out[[2]string{"public", n}] = pgshim.SourceShape{OID: 7}
		}
		return out, nil
	}}
	if _, err := c.shapes(context.Background(), [][2]string{{"public", "fast"}}); err != nil {
		t.Fatal(err)
	}
	// The first waiter's context ends while the question is on its way.
	cctx, stop := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := c.shapes(cctx, [][2]string{{"public", "slow"}})
		first <- err
	}()
	time.Sleep(50 * time.Millisecond)
	second := make(chan map[[2]string]pgshim.SourceShape, 1)
	go func() {
		got, _ := c.shapes(context.Background(), [][2]string{{"public", "slow"}})
		second <- got
	}()
	started := time.Now()
	if got, err := c.shapes(context.Background(), [][2]string{{"public", "fast"}}); err != nil || got[[2]string{"public", "fast"}].OID != 7 {
		t.Errorf("a known table during a question: %v %v", got, err)
	}
	if d := time.Since(started); d > 100*time.Millisecond {
		t.Errorf("a known table waited %v behind a question", d)
	}
	stop()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the cancelled waiter: %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the cancelled waiter is still waiting")
	}
	close(gate)
	select {
	case got := <-second:
		if got[[2]string{"public", "slow"}].OID != 7 {
			t.Errorf("the other waiter: %v, want the question's answer", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the other waiter never got the answer")
	}
	mu.Lock()
	defer mu.Unlock()
	if asked != 2 {
		t.Errorf("asked %d times, want 2 (fast once, slow once)", asked)
	}
}

// What the client is told of a failure is its cause in words, never the
// error's text.
func TestPGAskShapes_causes_3251(t *testing.T) {
	var ce *pgshim.ShapeCheckError
	none := func(context.Context) (func(), bool) { return nil, false }
	if _, err := pgAskShapes(context.Background(), "postgres://x@127.0.0.1:1/x?sslmode=disable", none, []string{"public"}, []string{"t"}); !errors.As(err, &ce) || ce.Cause != "every connection of the port to your PostgreSQL stayed taken" {
		t.Errorf("no slot: %v", err)
	}
	one := func(context.Context) (func(), bool) { return func() {}, true }
	_, err := pgAskShapes(context.Background(), "postgres://nobody:secret@127.0.0.1:1/none?connect_timeout=1&sslmode=disable", one, []string{"public"}, []string{"t"})
	if !errors.As(err, &ce) || ce.Cause != "the port could not connect to your PostgreSQL" || strings.Contains(ce.Cause, "127.0.0.1") || strings.Contains(ce.Cause, "secret") {
		t.Errorf("unreachable: %v (%+v)", err, ce)
	}
	if got := pgRefusedCause(&pgconn.PgError{Code: "42501", Message: "permission denied for column secret_salary"}); got != "your PostgreSQL refused the question (SQLSTATE 42501)" {
		t.Errorf("refused: %q", got)
	}
}

// Counted in /status apart from the reasons, by why.
func TestPGRouteStatsCountsShapeUnchecked_3251(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy, ShapeUnchecked: pgshim.ShapeUncheckedSourceNotAsked})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, ShapeUnchecked: pgshim.ShapeUncheckedNoShapeRecorded})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	n := r.doc("").Servers["s1"]
	if n.ShapeUnchecked[pgshim.ShapeUncheckedSourceNotAsked] != 1 || n.ShapeUnchecked[pgshim.ShapeUncheckedNoShapeRecorded] != 1 || len(n.ShapeUnchecked) != 2 {
		t.Errorf("copy_shape_unchecked %v", n.ShapeUnchecked)
	}
	if n.Reasons[pgshim.ReasonHeavyOnCopy] != 1 || n.Reasons[pgshim.ReasonAnswersTheSame] != 2 {
		t.Errorf("reasons %v: the count apart must not change them", n.Reasons)
	}
}
