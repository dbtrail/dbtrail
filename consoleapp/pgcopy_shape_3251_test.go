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
// by OID, kept a moment per table from when it was asked, so a burst of
// statements asks once and a change is seen within that moment.

// fakeClock is a clock a test moves.
type fakeClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// shapeAskLog is a fake source: what it was asked, and what it answers.
type shapeAskLog struct {
	mu    sync.Mutex
	asked [][]uint32
	now   map[uint32]pgshim.SourceShape
	err   error
}

func (a *shapeAskLog) ask(_ context.Context, oids []uint32) (map[uint32]pgshim.SourceShape, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.asked = append(a.asked, slices.Clone(oids))
	if a.err != nil {
		return nil, a.err
	}
	out := map[uint32]pgshim.SourceShape{}
	for _, o := range oids {
		if sh, ok := a.now[o]; ok {
			out[o] = sh
		}
	}
	return out, nil
}

func (a *shapeAskLog) log() [][]uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.asked)
}

func (a *shapeAskLog) set(f func()) { a.mu.Lock(); defer a.mu.Unlock(); f() }

func TestPGShapeCache_3251(t *testing.T) {
	clock := &fakeClock{at: time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)}
	src := &shapeAskLog{now: map[uint32]pgshim.SourceShape{
		1: {Schema: "public", Name: "a", OID: 1, Columns: []baseline.PGColumn{{Name: "id", TypeOID: 23, TypeMod: -1, Ordinal: 1}}},
		2: {Schema: "public", Name: "b", OID: 2},
	}}
	c := &pgShapeCache{ttl: time.Second, failTTL: 2 * time.Second, now: clock.now, ask: src.ask}
	ctx := context.Background()
	get := func(oids ...uint32) map[uint32]pgshim.SourceShape {
		t.Helper()
		got, err := c.shapes(ctx, oids)
		if err != nil {
			t.Fatalf("%v: %v", oids, err)
		}
		return got
	}
	if got := get(1); got[1].Name != "a" || len(got) != 1 {
		t.Fatalf("1: %v", got)
	}
	// Within the moment: not asked again; a table not known yet is asked
	// alone.
	get(1)
	if got := get(1, 2); len(got) != 2 {
		t.Errorf("1 and 2: %v", got)
	}
	if got := src.log(); !slices.EqualFunc(got, [][]uint32{{1}, {2}}, slices.Equal) {
		t.Errorf("asked %v, want 1, then 2 alone", got)
	}
	// An OID the source does not have is an answer too, kept as long.
	if got := get(99); len(got) != 0 {
		t.Errorf("99: %v, want nothing", got)
	}
	get(99)
	if n := len(src.log()); n != 3 {
		t.Errorf("asked %d times, want 3: an OID not found is not asked again at once", n)
	}
	// Past the moment, asked again: an ALTER TABLE since is seen.
	src.set(func() { src.now[1] = pgshim.SourceShape{Schema: "public", Name: "a", OID: 1, Columns: []baseline.PGColumn{{Name: "id", TypeOID: 20, TypeMod: -1, Ordinal: 1}}} })
	clock.add(1100 * time.Millisecond)
	if got := get(1); got[1].Columns[0].TypeOID != 20 {
		t.Errorf("after the moment: %v, want the source's new type", got)
	}
	// A failure is the answer for a while: the source is not asked again.
	src.set(func() { src.err = errors.New("connection refused") })
	clock.add(1100 * time.Millisecond)
	if _, err := c.shapes(ctx, []uint32{1}); err == nil {
		t.Fatal("the source failed and the cache answered")
	}
	n := len(src.log())
	if _, err := c.shapes(ctx, []uint32{2}); err == nil {
		t.Error("asked again during the failure, and it answered")
	}
	if len(src.log()) != n {
		t.Error("the source was asked again during the failure's time")
	}
	src.set(func() { src.err = nil })
	clock.add(2100 * time.Millisecond)
	if _, err := c.shapes(ctx, []uint32{1}); err != nil {
		t.Errorf("after the failure's time: %v, want the source asked again", err)
	}
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
	c := &pgShapeCache{ttl: time.Minute, failTTL: time.Minute, now: time.Now, ask: func(context.Context, []uint32) (map[uint32]pgshim.SourceShape, error) {
		mu.Lock()
		asked++
		mu.Unlock()
		<-gate
		return map[uint32]pgshim.SourceShape{1: {OID: 1}}, nil
	}}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := c.shapes(context.Background(), []uint32{1}); err != nil || got[1].OID != 1 {
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

// Two questions overlap: P1 for {a, b} reads the catalog before an ALTER of
// b and is slow; P2 for {b, c}, asked after the ALTER, comes back first.
// P1's old b, coming back last, does not overwrite P2's: answers are dated
// by when they were asked.
func TestPGShapeCache_aLateOldAnswerDoesNotOverwrite_3251(t *testing.T) {
	clock := &fakeClock{at: time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)}
	oldB := pgshim.SourceShape{Schema: "public", Name: "b", OID: 2, Columns: []baseline.PGColumn{{Name: "x", TypeOID: 25, TypeMod: -1, Ordinal: 1}}}
	newB := pgshim.SourceShape{Schema: "public", Name: "b", OID: 2, Columns: []baseline.PGColumn{{Name: "x", TypeOID: 1042, TypeMod: 7, Ordinal: 1}}}
	p1 := make(chan struct{})
	c := &pgShapeCache{ttl: time.Second, failTTL: 2 * time.Second, now: clock.now, ask: func(_ context.Context, oids []uint32) (map[uint32]pgshim.SourceShape, error) {
		if slices.Contains(oids, 1) {
			// P1: read before the ALTER, answered late.
			<-p1
			return map[uint32]pgshim.SourceShape{1: {OID: 1, Name: "a", Schema: "public"}, 2: oldB}, nil
		}
		return map[uint32]pgshim.SourceShape{2: newB, 3: {OID: 3, Name: "c", Schema: "public"}}, nil
	}}
	first := make(chan map[uint32]pgshim.SourceShape, 1)
	go func() {
		got, _ := c.shapes(context.Background(), []uint32{1, 2})
		first <- got
	}()
	time.Sleep(50 * time.Millisecond)
	clock.add(100 * time.Millisecond)
	if got, err := c.shapes(context.Background(), []uint32{2, 3}); err != nil || got[2].Columns[0].TypeOID != 1042 {
		t.Fatalf("P2: %v %v", got, err)
	}
	close(p1)
	<-first
	if got, err := c.shapes(context.Background(), []uint32{2}); err != nil || got[2].Columns[0].TypeOID != 1042 {
		t.Errorf("after P1 came back: %v %v, want P2's b, asked after the ALTER", got, err)
	}
}

// Answers are dated by when they were asked: one slower than ttl answers the
// statements that waited for it, and the next statement asks again (each
// waits for one question, never for a line of them). A failure is dated when
// it came: a source slower than failTTL is not asked by every statement.
func TestPGShapeCache_slowSource_3251(t *testing.T) {
	clock := &fakeClock{at: time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)}
	var mu sync.Mutex
	asked := 0
	var fail error
	c := &pgShapeCache{ttl: time.Second, failTTL: 2 * time.Second, now: clock.now, ask: func(context.Context, []uint32) (map[uint32]pgshim.SourceShape, error) {
		mu.Lock()
		asked++
		f := fail
		mu.Unlock()
		clock.add(1500 * time.Millisecond) // longer than ttl
		if f != nil {
			return nil, f
		}
		return map[uint32]pgshim.SourceShape{1: {OID: 1}}, nil
	}}
	ctx := context.Background()
	if got, err := c.shapes(ctx, []uint32{1}); err != nil || got[1].OID != 1 {
		t.Fatalf("a slow answer, to the statement that waited for it: %v %v", got, err)
	}
	if _, err := c.shapes(ctx, []uint32{1}); err != nil {
		t.Fatal(err)
	}
	if asked != 2 {
		t.Errorf("asked %d times, want 2: an answer older than ttl when it came is not kept for later statements", asked)
	}
	mu.Lock()
	fail = errors.New("slow")
	mu.Unlock()
	clock.add(1100 * time.Millisecond)
	for range 3 {
		if _, err := c.shapes(ctx, []uint32{1}); err == nil {
			t.Fatal("the failure was not the answer")
		}
	}
	if asked != 3 {
		t.Errorf("asked %d times, want 3: a failure slower than failTTL is dated when it came", asked)
	}
}

// A question on its way holds nobody else up: a statement whose tables are
// known is answered at once, and one that waits for the question stops
// waiting when its own context ends, without failing it for the others.
func TestPGShapeCache_aQuestionHoldsNobodyUp_3251(t *testing.T) {
	gate := make(chan struct{})
	var mu sync.Mutex
	asked := 0
	c := &pgShapeCache{ttl: time.Minute, failTTL: time.Minute, now: time.Now, ask: func(ctx context.Context, oids []uint32) (map[uint32]pgshim.SourceShape, error) {
		mu.Lock()
		asked++
		mu.Unlock()
		if oids[0] == 2 {
			select {
			case <-gate:
			case <-ctx.Done():
				// A question tied to the statement that asked it would end
				// here when that statement is cancelled.
				return nil, ctx.Err()
			}
		}
		out := map[uint32]pgshim.SourceShape{}
		for _, o := range oids {
			out[o] = pgshim.SourceShape{OID: o}
		}
		return out, nil
	}}
	if _, err := c.shapes(context.Background(), []uint32{1}); err != nil {
		t.Fatal(err)
	}
	cctx, stop := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := c.shapes(cctx, []uint32{2})
		first <- err
	}()
	time.Sleep(50 * time.Millisecond)
	second := make(chan map[uint32]pgshim.SourceShape, 1)
	go func() {
		got, _ := c.shapes(context.Background(), []uint32{2})
		second <- got
	}()
	started := time.Now()
	if got, err := c.shapes(context.Background(), []uint32{1}); err != nil || got[1].OID != 1 {
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
		if got[2].OID != 2 {
			t.Errorf("the other waiter: %v, want the question's answer", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the other waiter never got the answer")
	}
	mu.Lock()
	defer mu.Unlock()
	if asked != 2 {
		t.Errorf("asked %d times, want 2", asked)
	}
}

// A server's cache goes when it is asked for with another account.
func TestPGPortSourceShapes_aChangedAccountDropsTheOldCache_3251(t *testing.T) {
	slots := newPGSourceSlots(1)
	pgPortSourceShapes("postgres://a@h/db", "srv-3251", slots)
	pgPortSourceShapes("postgres://b@h/db", "srv-3251", slots)
	pgShapeCaches.mu.Lock()
	defer pgShapeCaches.mu.Unlock()
	if _, ok := pgShapeCaches.byDSN[pgShapeCacheKey{"postgres://a@h/db", slots}]; ok {
		t.Error("the old account's cache is still kept")
	}
	if _, ok := pgShapeCaches.byDSN[pgShapeCacheKey{"postgres://b@h/db", slots}]; !ok {
		t.Error("the new account's cache is not kept")
	}
}

// A question takes one of the port's connections to its sources before it
// connects, and gives it back; with none free it does not connect at all.
func TestPGAskShapes_takesASourceSlot_3251(t *testing.T) {
	unreachable := "postgres://nobody@127.0.0.1:1/none?connect_timeout=1&sslmode=disable"
	var taken, given int
	acquire := func(context.Context) (func(), bool) {
		taken++
		return func() { given++ }, true
	}
	if _, err := pgAskShapes(context.Background(), unreachable, acquire, []uint32{1}); err == nil {
		t.Fatal("a source nothing listens for answered")
	}
	if taken != 1 || given != 1 {
		t.Errorf("took %d connections and gave back %d, want 1 and 1", taken, given)
	}
	none := func(context.Context) (func(), bool) { return nil, false }
	started := time.Now()
	if _, err := pgAskShapes(context.Background(), unreachable, none, []uint32{1}); !errors.Is(err, errNoSourceSlot) {
		t.Errorf("no connection free: %v, want %v", err, errNoSourceSlot)
	}
	if d := time.Since(started); d > 500*time.Millisecond {
		t.Errorf("no connection free took %v: it must not try to connect", d)
	}
	slots := newPGSourceSlots(1)
	slots.wait = time.Minute
	hold, ok := slots.acquire(context.Background())
	if !ok {
		t.Fatal("the one slot")
	}
	defer hold()
	ask := pgPortSourceShapes(unreachable, "test-slot-3251", slots)
	started = time.Now()
	if _, err := ask(context.Background(), []uint32{1}); !errors.Is(err, errNoSourceSlot) {
		t.Errorf("the port's slots all taken: %v, want %v", err, errNoSourceSlot)
	}
	if d := time.Since(started); d < pgShapeWait-100*time.Millisecond || d > pgShapeWait+time.Second {
		t.Errorf("waited %v for a slot, want about %v", d, pgShapeWait)
	}
}

// What the client is told of a failure is its cause in words, never the
// error's text.
func TestPGAskShapes_causes_3251(t *testing.T) {
	var ce *pgshim.ShapeCheckError
	none := func(context.Context) (func(), bool) { return nil, false }
	if _, err := pgAskShapes(context.Background(), "postgres://x@127.0.0.1:1/x?sslmode=disable", none, []uint32{1}); !errors.As(err, &ce) || ce.Cause != "every connection of the port to your PostgreSQL stayed taken" {
		t.Errorf("no slot: %v", err)
	}
	one := func(context.Context) (func(), bool) { return func() {}, true }
	_, err := pgAskShapes(context.Background(), "postgres://nobody:secret@127.0.0.1:1/none?connect_timeout=1&sslmode=disable", one, []uint32{1})
	if !errors.As(err, &ce) || ce.Cause != "the port could not connect to your PostgreSQL" || strings.Contains(ce.Cause, "127.0.0.1") || strings.Contains(ce.Cause, "secret") {
		t.Errorf("unreachable: %v (%+v)", err, ce)
	}
	if got := pgRefusedCause(&pgconn.PgError{Code: "42501", Message: "permission denied for column secret_salary"}); got != "your PostgreSQL refused the question (SQLSTATE 42501)" {
		t.Errorf("refused: %q", got)
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
	if got[1].RecordedAs != [2]string{"public", "shaped"} {
		t.Errorf("the recorded name: %v, want the snapshot's", got[1].RecordedAs)
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
