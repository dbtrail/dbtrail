package pgshim

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The port's result budget (#2804): the results held for clients that have
// not read them yet are counted in one budget for the port, a statement there
// is no room for is refused, and what a connection counted is given back when
// its answer is written or the connection ends.

// oneCell is what a result of one row of one cell costs before its bytes.
const oneCell = rowOverheadBytes + cellOverheadBytes

// cellsOf is a result of one row of one cell that costs n bytes in the
// budget, its fixed cost included; 0 is a result of no rows.
func cellsOf(n int) [][][]byte {
	if n == 0 {
		return nil
	}
	if n < oneCell {
		panic("a result of one cell costs at least its fixed cost")
	}
	return [][][]byte{{make([]byte, n-oneCell)}}
}

// What a result costs: its cells' bytes, and a fixed cost for every row and
// every cell, so a result of NULLs or of empty strings is not free. Each case
// is held on a port whose budget the cells' bytes alone would stay under, and
// the next statement must be refused.
func TestResultBudget_aResultCostsItsRowsAndCellsNotOnlyTheirBytes(t *testing.T) {
	grid := func(rows, cells int, cell func() []byte) [][][]byte {
		out := make([][][]byte, rows)
		for i := range out {
			out[i] = make([][]byte, cells)
			for j := range out[i] {
				out[i][j] = cell()
			}
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		rows    [][][]byte
		payload int64
		want    int64
		budget  int64
	}{
		{"1000 rows of 10 NULL cells", grid(1000, 10, func() []byte { return nil }), 0, 1000*31 + 10000*28, 1000},
		{"1000 rows of 10 empty strings", grid(1000, 10, func() []byte { return []byte{} }), 0, 1000*31 + 10000*28, 1000},
		{"one large cell", grid(1, 1, func() []byte { return make([]byte, 1000) }), 1000, 1000 + 31 + 28, 1050},
		{"one row of no cells", [][][]byte{{}}, 0, 31, 31},
		{"no rows", nil, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resultBytes(tc.rows); got != tc.want {
				t.Fatalf("the result costs %d bytes, want %d", got, tc.want)
			}
			if tc.budget == 0 {
				return
			}
			if tc.payload >= tc.budget {
				t.Fatalf("the cells' bytes alone (%d) reach the budget of %d: this would prove nothing", tc.payload, tc.budget)
			}
			b := NewResultBudget(tc.budget)
			if err := budgetSession(b).holdResult(tc.rows); err != nil {
				t.Fatalf("refused with nothing held: %s", err.msg)
			}
			if b.Held() != tc.want {
				t.Fatalf("held %d, want %d", b.Held(), tc.want)
			}
			if err := budgetSession(b).admitResult(); err == nil {
				t.Fatalf("with a result of %d bytes held on a port of %d, the next statement was let in", b.Held(), tc.budget)
			}
		})
	}
}

func budgetSession(b *ResultBudget) *session {
	return &session{budget: b, logger: discardLogger()}
}

func TestResultBudget_noBudgetAdmitsEverything(t *testing.T) {
	if b := NewResultBudget(0); b != nil {
		t.Fatalf("a budget of 0 bytes is %v, want none", b)
	}
	if b := NewResultBudget(-1); b != nil {
		t.Fatalf("a budget of -1 bytes is %v, want none", b)
	}
	var none *ResultBudget
	if none.Max() != 0 || none.Held() != 0 || none.Charged() != 0 || none.Refused() != 0 {
		t.Fatal("no budget reports numbers")
	}
	s := budgetSession(nil)
	if err := s.admitResult(); err != nil {
		t.Fatalf("admitted with no budget: %v", err)
	}
	if err := s.holdResult(cellsOf(1 << 20)); err != nil {
		t.Fatalf("held with no budget: %v", err)
	}
	s.releaseResult()
	s.releaseResult()
}

func TestResultBudget_refusesAtTheBudgetNotBelowIt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		first   int
		refused bool
	}{
		{"nothing held", 0, false},
		{"one byte under the budget", 999, false},
		{"exactly the budget", 1000, true},
		{"one byte over the budget", 1001, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewResultBudget(1000)
			first := budgetSession(b)
			// With nothing held the first result is always taken, whatever
			// its size: the port holds under its budget plus one result.
			if err := first.holdResult(cellsOf(tc.first)); err != nil {
				t.Fatalf("the first result of %d bytes was refused with nothing held: %s", tc.first, err.msg)
			}
			if b.Held() != int64(tc.first) {
				t.Fatalf("held %d, want %d", b.Held(), tc.first)
			}
			next := budgetSession(b)
			before, after := next.admitResult(), next.holdResult(cellsOf(300))
			if (before != nil) != tc.refused || (after != nil) != tc.refused {
				t.Fatalf("with %d of 1000 bytes held: before the statement %v, with its result %v; want refused=%v at both", tc.first, before, after, tc.refused)
			}
			if !tc.refused {
				if b.Held() != int64(tc.first+300) {
					t.Fatalf("held %d, want %d", b.Held(), tc.first+300)
				}
				return
			}
			if before.code != "53000" || !strings.Contains(before.msg, "still sending") || !strings.Contains(before.msg, "run the statement again") {
				t.Fatalf("the refusal is %s %q, want 53000 saying the port is still sending results and to run it again", before.code, before.msg)
			}
			if b.Held() != int64(tc.first) || b.Charged() != int64(tc.first) {
				t.Fatalf("a refused result changed the count: held %d, charged %d, want %d", b.Held(), b.Charged(), tc.first)
			}
			if b.Refused() != 2 {
				t.Fatalf("%d refusals counted, want 2", b.Refused())
			}
			// A result with no rows holds nothing, and is refused all the
			// same while the port is full: it is asked for like any other.
			if err := next.holdResult(nil); err == nil {
				t.Fatal("a result of no rows was taken by a full port")
			}
		})
	}
}

func TestResultBudget_aResultIsGivenBackOnce(t *testing.T) {
	b := NewResultBudget(10000)
	one, two := budgetSession(b), budgetSession(b)
	if err := one.holdResult(cellsOf(4000)); err != nil {
		t.Fatal(err.msg)
	}
	if err := two.holdResult(cellsOf(3000)); err != nil {
		t.Fatal(err.msg)
	}
	one.releaseResult()
	if b.Held() != 3000 {
		t.Fatalf("held %d after one of two results was given back, want 3000", b.Held())
	}
	// Twice: when the answer is written and again when the connection ends.
	one.releaseResult()
	one.releaseResult()
	if b.Held() != 3000 {
		t.Fatalf("held %d after the same result was given back again, want 3000: it was taken from another connection's count", b.Held())
	}
	// A connection holds one result: its next one replaces the last.
	if err := two.holdResult(cellsOf(500)); err != nil {
		t.Fatal(err.msg)
	}
	if b.Held() != 500 {
		t.Fatalf("held %d, want the 500 bytes of the connection's present result", b.Held())
	}
	// No rows: nothing counted, nothing to give back.
	if err := one.holdResult(nil); err != nil {
		t.Fatal(err.msg)
	}
	one.releaseResult()
	two.releaseResult()
	if b.Held() != 0 {
		t.Fatalf("held %d with every result given back, want 0", b.Held())
	}
	if b.Charged() != 7500 {
		t.Fatalf("charged %d, want the 7500 bytes ever counted", b.Charged())
	}
}

// Many small results on many connections: none is near the budget alone,
// together they pass it, and the port never holds more than its budget plus
// one result however the connections interleave.
func TestResultBudget_manySmallResultsTogetherAreBounded(t *testing.T) {
	const max, size, conns, rounds = 640, 80, 32, 2000
	b := NewResultBudget(max)

	// In order first: eight results of 80 bytes fill 640, the ninth is refused,
	// and one given back makes room for it.
	var sessions []*session
	for i := 0; i < max/size; i++ {
		s := budgetSession(b)
		if err := s.admitResult(); err != nil {
			t.Fatalf("result %d refused under the budget: %s", i, err.msg)
		}
		if err := s.holdResult(cellsOf(size)); err != nil {
			t.Fatalf("result %d refused under the budget: %s", i, err.msg)
		}
		sessions = append(sessions, s)
	}
	ninth := budgetSession(b)
	if err := ninth.admitResult(); err == nil {
		t.Fatalf("a ninth result was let in with %d of %d bytes held", b.Held(), max)
	}
	sessions[0].releaseResult()
	if err := ninth.holdResult(cellsOf(size)); err != nil {
		t.Fatalf("no room after a result was given back: %s", err.msg)
	}
	ninth.releaseResult()
	for _, s := range sessions {
		s.releaseResult()
	}
	if b.Held() != 0 {
		t.Fatalf("held %d, want 0", b.Held())
	}

	charged := b.Charged()
	var held atomic.Int64
	var over atomic.Int64
	var wg sync.WaitGroup
	for range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := budgetSession(b)
			for range rounds {
				if s.admitResult() != nil {
					continue
				}
				if s.holdResult(cellsOf(size)) != nil {
					continue
				}
				held.Add(1)
				if h := b.Held(); h > max-1+size {
					over.Store(h)
				}
				s.releaseResult()
			}
		}()
	}
	wg.Wait()
	if over.Load() != 0 {
		t.Fatalf("the port held %d bytes, more than its budget of %d plus one result of %d", over.Load(), max, size)
	}
	if b.Held() != 0 {
		t.Fatalf("held %d after every connection gave its result back, want 0", b.Held())
	}
	if got, want := b.Charged()-charged, held.Load()*size; got != want {
		t.Fatalf("charged %d, want %d: the bytes of the results taken", got, want)
	}
}

// rawClient is a PostgreSQL client that reads only when the test says so.
type rawClient struct {
	t  *testing.T
	c  net.Conn
	fe *pgproto3.Frontend
}

// rawLogin connects to the port and logs in with the port's password.
func rawLogin(t *testing.T, addr string) *rawClient {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(60 * time.Second))
	r := &rawClient{t: t, c: c, fe: pgproto3.NewFrontend(c, c)}
	r.fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{"user": "app", "database": "orders"}})
	if err := r.fe.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := scramClientLogin(r.fe, portPassword, nil); err != nil {
		t.Fatalf("login: %v", err)
	}
	for {
		msg, err := r.fe.Receive()
		if err != nil {
			t.Fatalf("login: %v", err)
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			t.Fatalf("login refused: %s %s", m.Code, m.Message)
		case *pgproto3.ReadyForQuery:
			return r
		}
	}
}

// send sends a statement and reads nothing.
func (r *rawClient) send(stmt string) {
	r.t.Helper()
	r.fe.Send(&pgproto3.Query{String: stmt})
	if err := r.fe.Flush(); err != nil {
		r.t.Fatalf("send %s: %v", stmt, err)
	}
}

// rawAnswer is what a statement was answered with.
type rawAnswer struct {
	rows    int
	bytes   int64
	notices int
	code    string // the error's SQLSTATE, "" for none
	message string
	err     error // the connection ended before the answer did
}

// read reads one statement's answer to its end, waiting pause after each row.
func (r *rawClient) read(pause time.Duration) rawAnswer {
	var a rawAnswer
	for {
		msg, err := r.fe.Receive()
		if err != nil {
			a.err = err
			return a
		}
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			a.rows++
			for _, v := range m.Values {
				a.bytes += int64(len(v))
			}
			if pause > 0 {
				time.Sleep(pause)
			}
		case *pgproto3.NoticeResponse:
			a.notices++
		case *pgproto3.ErrorResponse:
			a.code, a.message = m.Code, m.Message
		case *pgproto3.ReadyForQuery:
			return a
		}
	}
}

func waitBudget(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const (
	bigRows  = 24
	bigCell  = 1 << 20
	bigBytes = bigRows * bigCell
	// bigHeld is what the port counts for it: the bytes, and the fixed cost
	// of its 24 rows of one cell.
	bigHeld = bigBytes + bigRows*oneCell
)

// bigOrSmall answers a statement that names "big" with 24 MiB in 24 rows,
// far more than the sockets between a client and the port take in, and any
// other with one row of one byte. ran counts the statements it was given.
func bigOrSmall(ran *atomic.Int64) func(context.Context, string, RunOptions) (sqlsandbox.Result, error) {
	cell := strings.Repeat("x", bigCell)
	return func(_ context.Context, statement string, _ RunOptions) (sqlsandbox.Result, error) {
		ran.Add(1)
		if !strings.Contains(statement, "big") {
			return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{json.Number("1")}}}, nil
		}
		rows := make([][]any, bigRows)
		for i := range rows {
			rows[i] = []any{cell}
		}
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "v", Type: "VARCHAR"}}, Rows: rows}, nil
	}
}

func lowerWriteTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := portWriteTimeout.Load()
	portWriteTimeout.Store(int64(d))
	t.Cleanup(func() { portWriteTimeout.Store(old) })
}

func sqlRuns(rec *audittest.Recorder) int {
	n := 0
	for _, ev := range rec.Events() {
		if ev.Surface == "shim" && ev.Action == "sql.run" {
			n++
		}
	}
	return n
}

// The issue's case: a port with a small budget, a client that does not read
// its first result, and a second statement that is refused with the "results
// held" error instead of being answered. Once the first client has read, the
// statement is answered.
func TestPort_aResultHeldForASlowClientRefusesTheNextStatement(t *testing.T) {
	var ran atomic.Int64
	p := &portLogin{run: bigOrSmall(&ran)}
	b := NewResultBudget(1 << 20)
	rec := audittest.Install(t)
	addr := serveAddrCfg(t, Config{Login: p.login, ResultBudget: b})

	slow := rawLogin(t, addr)
	slow.send("SELECT v FROM big")
	waitBudget(t, "the port to hold the first result", func() bool { return b.Held() == bigHeld })
	// Held while it is being sent, not counted and given back at once.
	time.Sleep(200 * time.Millisecond)
	if b.Held() != bigHeld {
		t.Fatalf("held %d a moment later, want the %d bytes of the result nobody read", b.Held(), bigHeld)
	}

	other := rawLogin(t, addr)
	other.send("SELECT 1 AS n")
	a := other.read(0)
	if a.code != "53000" || !strings.Contains(a.message, "still sending") {
		t.Fatalf("the second statement got %d rows, error %q %q; want 53000 saying the port is still sending results", a.rows, a.code, a.message)
	}
	if a.rows != 0 {
		t.Fatalf("a refused statement returned %d rows: refused means no answer, not a short one", a.rows)
	}
	if ran.Load() != 1 {
		t.Fatalf("%d statements reached the copy, want 1: a statement there is no room for is refused before it runs", ran.Load())
	}
	if b.Refused() != 1 {
		t.Fatalf("%d refusals counted, want 1", b.Refused())
	}
	if n := sqlRuns(rec); n != 1 {
		t.Fatalf("%d audit records, want 1: the refused statement served nobody", n)
	}
	// What returns no rows is not the budget's: the connection goes on.
	other.send("SET application_name = 'x'")
	if a := other.read(0); a.code != "" || a.err != nil {
		t.Fatalf("SET on a full port: %q %q %v", a.code, a.message, a.err)
	}

	// The first client reads: every row, whole.
	if a := slow.read(0); a.rows != bigRows || a.bytes != bigBytes || a.code != "" || a.err != nil {
		t.Fatalf("the slow client read %d rows, %d bytes, error %q %v; want %d rows, %d bytes", a.rows, a.bytes, a.code, a.err, bigRows, bigBytes)
	}
	waitBudget(t, "the result to be given back once it was read", func() bool { return b.Held() == 0 })
	other.send("SELECT 1 AS n")
	if a := other.read(0); a.rows != 1 || a.code != "" {
		t.Fatalf("with nothing held the statement got %d rows, error %q %q; want its row", a.rows, a.code, a.message)
	}
	waitBudget(t, "the small result to be given back", func() bool { return b.Held() == 0 })
	if want := int64(bigHeld + oneCell + 1); b.Charged() != want {
		t.Fatalf("charged %d, want %d: the two results that were sent", b.Charged(), want)
	}
}

// A client that hangs up in the middle of its result: the bytes come back
// when its connection is torn down.
func TestPort_aClientThatLeavesMidResultGivesItBack(t *testing.T) {
	var ran atomic.Int64
	p := &portLogin{run: bigOrSmall(&ran)}
	b := NewResultBudget(1 << 20)
	addr := serveAddrCfg(t, Config{Login: p.login, ResultBudget: b})
	gone := rawLogin(t, addr)
	gone.send("SELECT v FROM big")
	waitBudget(t, "the port to hold the result", func() bool { return b.Held() == bigHeld })
	_ = gone.c.Close()
	waitBudget(t, "the result of a client that left to be given back", func() bool { return b.Held() == 0 })
	waitBudget(t, "the connection to end", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.closed == 1
	})
	next := rawLogin(t, addr)
	next.send("SELECT 1 AS n")
	if a := next.read(0); a.rows != 1 || a.code != "" {
		t.Fatalf("after the client left: %d rows, error %q %q", a.rows, a.code, a.message)
	}
}

// A client that asked, stopped reading and stays connected is cut after the
// write timeout, and what the port held for it comes back: it cannot keep a
// part of the budget for as long as it likes.
func TestPort_aClientThatStopsReadingIsCut(t *testing.T) {
	lowerWriteTimeout(t, 300*time.Millisecond)
	var ran atomic.Int64
	p := &portLogin{run: bigOrSmall(&ran)}
	b := NewResultBudget(1 << 20)
	addr := serveAddrCfg(t, Config{Login: p.login, ResultBudget: b})
	stuck := rawLogin(t, addr)
	started := time.Now()
	stuck.send("SELECT v FROM big")
	waitBudget(t, "the port to hold the result", func() bool { return b.Held() == bigHeld })
	waitBudget(t, "the result of a client that reads nothing to be given back", func() bool { return b.Held() == 0 })
	if took := time.Since(started); took < 250*time.Millisecond {
		t.Fatalf("given back after %v, before the write timeout: the client was not waited for", took)
	}
	// Its connection is closed in the middle of the answer: what it reads
	// now ends in an error, never in a whole answer.
	if a := stuck.read(0); a.err == nil {
		t.Fatalf("the client read a whole answer (%d rows) from a connection that was cut", a.rows)
	}
}

// A client that reads its large result slowly, for longer than the write
// timeout in all, is not cut: the timeout is for a client that reads nothing.
func TestPort_aSlowReaderIsNotCut(t *testing.T) {
	lowerWriteTimeout(t, time.Second)
	var ran atomic.Int64
	p := &portLogin{run: bigOrSmall(&ran)}
	b := NewResultBudget(1 << 20)
	addr := serveAddrCfg(t, Config{Login: p.login, ResultBudget: b})
	slow := rawLogin(t, addr)
	started := time.Now()
	slow.send("SELECT v FROM big")
	a := slow.read(100 * time.Millisecond)
	if a.rows != bigRows || a.bytes != bigBytes || a.err != nil {
		t.Fatalf("a slow reader got %d rows, %d bytes, %v; want the whole result", a.rows, a.bytes, a.err)
	}
	if took := time.Since(started); took < 2*time.Second {
		t.Fatalf("the read took %v: not longer than the write timeout, so this proved nothing", took)
	}
	waitBudget(t, "the result to be given back", func() bool { return b.Held() == 0 })
}

// A statement that fails after its result was built, or part of it, counts
// nothing, and the connection's next statement is counted as usual.
func TestPort_aFailedStatementHoldsNothing(t *testing.T) {
	col := []sqlsandbox.Column{{Name: "a", Type: "INTEGER"}, {Name: "b", Type: "VARCHAR"}}
	var mu sync.Mutex
	var next sqlsandbox.Result
	var nextErr error
	set := func(res sqlsandbox.Result, err error) {
		mu.Lock()
		defer mu.Unlock()
		next, nextErr = res, err
	}
	p := &portLogin{run: func(context.Context, string, RunOptions) (sqlsandbox.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		return next, nextErr
	}}
	b := NewResultBudget(1 << 20)
	addr := serveAddrCfg(t, Config{Login: p.login, ResultBudget: b})
	c := rawLogin(t, addr)
	for _, tc := range []struct {
		name string
		res  sqlsandbox.Result
		err  error
		code string
	}{
		{"cut at the row cap", sqlsandbox.Result{Columns: col, Rows: [][]any{{json.Number("1"), "x"}}, Truncated: true}, nil, "54000"},
		{"a cut cell", sqlsandbox.Result{Columns: col, Rows: [][]any{{json.Number("1"), "x"}}, TruncatedCells: 1}, nil, "54000"},
		{"a second row of the wrong width", sqlsandbox.Result{Columns: col, Rows: [][]any{{json.Number("1"), "x"}, {json.Number("2")}}}, nil, "XX000"},
		{"the copy failed with rows in hand", sqlsandbox.Result{Columns: col, Rows: [][]any{{json.Number("1"), "x"}}}, &sqlsandbox.QueryError{Message: "Conversion Error: no"}, "22000"},
		{"too large", sqlsandbox.Result{}, sqlsandbox.ErrResultTooLarge, "54000"},
	} {
		set(tc.res, tc.err)
		c.send("SELECT a, b FROM t")
		if a := c.read(0); a.code != tc.code || a.rows != 0 {
			t.Fatalf("%s: %d rows, error %q %q; want %s and no rows", tc.name, a.rows, a.code, a.message, tc.code)
		}
		if b.Held() != 0 || b.Charged() != 0 {
			t.Fatalf("%s: held %d, charged %d; a statement that failed holds nothing", tc.name, b.Held(), b.Charged())
		}
	}
	// A result with its columns and no row is a result: nothing to count.
	set(sqlsandbox.Result{Columns: col}, nil)
	c.send("SELECT a, b FROM t WHERE false")
	if a := c.read(0); a.code != "" || a.rows != 0 {
		t.Fatalf("no rows: %d rows, error %q", a.rows, a.code)
	}
	if b.Charged() != 0 {
		t.Fatalf("a result of no rows was charged %d bytes", b.Charged())
	}
	// And one with rows is counted by its cells: "12" + "abc", and the fixed
	// cost of two rows of two cells, the NULL ones included.
	set(sqlsandbox.Result{Columns: col, Rows: [][]any{{json.Number("12"), "abc"}, {nil, nil}}}, nil)
	c.send("SELECT a, b FROM t")
	if a := c.read(0); a.code != "" || a.rows != 2 {
		t.Fatalf("%d rows, error %q %q; want 2 rows", a.rows, a.code, a.message)
	}
	waitBudget(t, "the result to be given back", func() bool { return b.Held() == 0 })
	if want := int64(5 + 2*rowOverheadBytes + 4*cellOverheadBytes); b.Charged() != want {
		t.Fatalf("charged %d, want %d: the 5 bytes of the cells and the cost of 2 rows and 4 cells", b.Charged(), want)
	}
}

// The budget taken while the statement ran (many pass the first question and
// then wait for a worker): the result is refused when it is ready, whole, with
// no notice and no audit record, and the count is left as it was.
func TestPort_aResultReadyOnAFullPortIsRefusedWhole(t *testing.T) {
	b := NewResultBudget(100)
	p := &portLogin{run: func(context.Context, string, RunOptions) (sqlsandbox.Result, error) {
		// Another connection's results fill the port meanwhile.
		b.held.Store(100)
		return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{json.Number("1")}, {json.Number("2")}},
			Note: "answered from an earlier copy"}, nil
	}}
	rec := audittest.Install(t)
	addr := serveAddrCfg(t, Config{Login: p.login, ResultBudget: b})
	c := rawLogin(t, addr)
	c.send("SELECT n FROM t")
	a := c.read(0)
	if a.code != "53000" || a.rows != 0 {
		t.Fatalf("%d rows, error %q %q; want 53000 and no rows", a.rows, a.code, a.message)
	}
	if a.notices != 0 {
		t.Fatalf("%d notices beside a refused answer", a.notices)
	}
	if n := sqlRuns(rec); n != 0 {
		t.Fatalf("%d audit records for an answer nobody got", n)
	}
	if b.Held() != 100 {
		t.Fatalf("held %d after the refusal, want the 100 bytes of the other connections left alone", b.Held())
	}
	p.mu.Lock()
	answered := append([]bool{}, p.answered...)
	p.mu.Unlock()
	if len(answered) != 1 || !answered[0] {
		t.Fatalf("the statement was counted as %v, want one refused statement", answered)
	}
}

// Time travel's answers are held like the copy's, so the budget is asked for
// them too: on a full port the statement is refused before the engine reads
// anything, and with room it goes to the engine.
func TestPort_timeTravelAsksTheBudget(t *testing.T) {
	b := NewResultBudget(100)
	login := func(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
		if !password.Matches([]string{portPassword}) {
			return nil, &Error{Code: "28P01", Message: "no"}
		}
		return &Binding{
			Run: func(context.Context, string, RunOptions) (sqlsandbox.Result, error) {
				return sqlsandbox.Result{}, errors.New("time travel reached the copy")
			},
			Actor:  "server:orders",
			Schema: "public",
			// An index nobody answers on: a statement that reaches the
			// engine fails there, which tells "reached the engine" from
			// "refused before it".
			TimeTravel: shim.NewHandlerWithConfig(deadDB(t), shim.Config{NoArchive: true, IndexDBName: "idx"}, discardLogger()),
		}, nil
	}
	addr := serveAddrCfg(t, Config{Login: login, ResultBudget: b})
	c := rawLogin(t, addr)
	const stmt = "SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 1"

	c.send(stmt)
	if a := c.read(0); a.code == "" || a.code == "53000" {
		t.Fatalf("with room, time travel got %q %q; want the engine's own failure on an index that is not there", a.code, a.message)
	}
	if b.Refused() != 0 {
		t.Fatalf("%d refusals with nothing held", b.Refused())
	}

	b.held.Store(100)
	for _, s := range []string{stmt, "SELECT * FROM _snapshot.orders AS OF '5 minutes ago' WHERE id = 1"} {
		c.send(s)
		if a := c.read(0); a.code != "53000" || !strings.Contains(a.message, "still sending") {
			t.Fatalf("%s on a full port: %q %q; want 53000", s, a.code, a.message)
		}
	}
	if b.Refused() != 2 {
		t.Fatalf("%d refusals, want 2", b.Refused())
	}
	// A whole table as of a moment is refused for what it is, full or not:
	// the budget is asked only for what would be answered.
	c.send("SELECT * FROM _flashback.orders AS OF '5 minutes ago'")
	if a := c.read(0); a.code != "0A000" {
		t.Fatalf("full-table time travel on a full port: %q %q, want 0A000", a.code, a.message)
	}
	if b.Held() != 100 {
		t.Fatalf("held %d, want 100: refusals count nothing", b.Held())
	}
}

// With no budget given the port bounds nothing, as before this existed: the
// time-travel front-end of `bintrail-pg flashback` gives none.
func TestPort_withNoBudgetNothingIsRefused(t *testing.T) {
	var ran atomic.Int64
	p := &portLogin{run: bigOrSmall(&ran)}
	addr := serveAddrCfg(t, Config{Login: p.login})
	slow := rawLogin(t, addr)
	slow.send("SELECT v FROM big")
	waitBudget(t, "the statement to run", func() bool { return ran.Load() == 1 })
	other := rawLogin(t, addr)
	other.send("SELECT 1 AS n")
	if a := other.read(0); a.rows != 1 || a.code != "" {
		t.Fatalf("with no budget: %d rows, error %q %q", a.rows, a.code, a.message)
	}
	if a := slow.read(0); a.rows != bigRows || a.err != nil {
		t.Fatalf("the slow client read %d rows, %v", a.rows, a.err)
	}
}
