package pgshim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// Routing by the source's plan: only a statement that is expensive on the
// source goes to the copy, which has no index.

// plan is an EXPLAIN (FORMAT JSON) answer with the given top node.
func plan(top string) string { return `[{"Plan": ` + top + `}]` }

func TestParsePGPlan(t *testing.T) {
	for _, tc := range []struct {
		name       string
		raw        string
		cost, rows float64
		err        string
	}{
		{"a read by the primary key", plan(`{"Node Type": "Index Scan", "Total Cost": 8.45, "Plan Rows": 1}`), 8.45, 0, ""},
		{"a sequential scan at the top", plan(`{"Node Type": "Seq Scan", "Total Cost": 4202, "Plan Rows": 201680}`), 4202, 201680, ""},
		{"a sequential scan under an aggregate and a join", plan(`{"Node Type": "Aggregate", "Total Cost": 900, "Plan Rows": 1, "Plans": [
			{"Node Type": "Hash Join", "Total Cost": 800, "Plan Rows": 10, "Plans": [
				{"Node Type": "Index Scan", "Total Cost": 8, "Plan Rows": 2400000},
				{"Node Type": "Hash", "Total Cost": 700, "Plan Rows": 10, "Plans": [
					{"Node Type": "Seq Scan", "Total Cost": 600, "Plan Rows": 1500000}]}]}]}`), 900, 1500000, ""},
		{"the largest of several", plan(`{"Node Type": "Append", "Total Cost": 10, "Plan Rows": 3, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 1, "Plan Rows": 30},
			{"Node Type": "Seq Scan", "Total Cost": 1, "Plan Rows": 3000},
			{"Node Type": "Seq Scan", "Total Cost": 1, "Plan Rows": 300}]}`), 10, 3000, ""},
		{"a CTE, an init plan and a subplan", plan(`{"Node Type": "CTE Scan", "Total Cost": 50, "Plan Rows": 5, "Plans": [
			{"Node Type": "Seq Scan", "Parent Relationship": "InitPlan", "Subplan Name": "CTE w", "Total Cost": 20, "Plan Rows": 70000},
			{"Node Type": "Result", "Parent Relationship": "SubPlan", "Total Cost": 1, "Plan Rows": 1, "Plans": [
				{"Node Type": "Seq Scan", "Total Cost": 1, "Plan Rows": 2000000}]}]}`), 50, 2000000, ""},
		// 625,000 rows for each of 2.4 processes (two workers and the
		// leader), as PostgreSQL 16 plans a scan of 1,500,000 rows.
		{"a parallel scan counts every process", plan(`{"Node Type": "Aggregate", "Total Cost": 25858.72, "Plan Rows": 1, "Plans": [
			{"Node Type": "Gather", "Total Cost": 25858.71, "Plan Rows": 2, "Workers Planned": 2, "Plans": [
				{"Node Type": "Aggregate", "Total Cost": 24858.51, "Plan Rows": 1, "Plans": [
					{"Node Type": "Seq Scan", "Parallel Aware": true, "Total Cost": 23296, "Plan Rows": 625000}]}]}]}`), 25858.72, 1500000, ""},
		{"a sequential scan that is not parallel under a Gather", plan(`{"Node Type": "Gather", "Total Cost": 5, "Plan Rows": 2, "Workers Planned": 4, "Plans": [
			{"Node Type": "Seq Scan", "Parallel Aware": false, "Total Cost": 1, "Plan Rows": 100}]}`), 5, 100, ""},
		{"no sequential scan", plan(`{"Node Type": "Bitmap Heap Scan", "Total Cost": 15059, "Plan Rows": 30000, "Plans": [
			{"Node Type": "Bitmap Index Scan", "Total Cost": 300, "Plan Rows": 30000}]}`), 15059, 0, ""},

		{"not JSON", `QUERY PLAN`, 0, 0, "not EXPLAIN's JSON"},
		{"an object, not a list", `{"Plan": {"Node Type": "Result", "Total Cost": 1}}`, 0, 0, "not EXPLAIN's JSON"},
		{"an empty list", `[]`, 0, 0, "not one EXPLAIN plan"},
		{"two plans", `[{"Plan": {"Total Cost": 1}}, {"Plan": {"Total Cost": 2}}]`, 0, 0, "not one EXPLAIN plan"},
		{"no Plan", `[{"Planning Time": 1}]`, 0, 0, "not one EXPLAIN plan"},
		{"no cost", plan(`{"Node Type": "Result", "Plan Rows": 1}`), 0, 0, "no Total Cost"},
		{"a cost written as text", plan(`{"Node Type": "Result", "Total Cost": "9"}`), 0, 0, "no Total Cost"},
		{"a Seq Scan without rows", plan(`{"Node Type": "Seq Scan", "Total Cost": 9}`), 0, 0, "no Plan Rows"},
		{"Plans not a list", plan(`{"Node Type": "Result", "Total Cost": 9, "Plans": {"Node Type": "Seq Scan"}}`), 0, 0, "not a list"},
		{"a node not an object", plan(`{"Node Type": "Result", "Total Cost": 9, "Plans": [3]}`), 0, 0, "not an object"},
	} {
		got, err := parsePGPlan([]byte(tc.raw))
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("%s: %+v %v, want an error saying %q", tc.name, got, err, tc.err)
			}
			continue
		}
		if err != nil || got.cost != tc.cost || got.maxSeqScanRows != tc.rows {
			t.Errorf("%s: %+v %v, want cost %v and rows %v", tc.name, got, err, tc.cost, tc.rows)
		}
	}
	// Nested deeper than any plan: refused, not walked for ever.
	deep := `{"Node Type": "Result", "Total Cost": 1, "Plans": [`
	raw := plan(strings.Repeat(deep, maxPlanDepth+2) + `{"Node Type": "Result", "Total Cost": 1}` + strings.Repeat("]}", maxPlanDepth+2))
	if _, err := parsePGPlan([]byte(raw)); err == nil || !strings.Contains(err.Error(), "too deep") {
		t.Errorf("a plan nested too deep: %v", err)
	}
}

func TestParallelDivisor(t *testing.T) {
	for w, want := range map[float64]float64{0: 1, 1: 1.7, 2: 2.4, 3: 3.1, 4: 4, 8: 8} {
		if got := parallelDivisor(w); fmt.Sprintf("%.4f", got) != fmt.Sprintf("%.4f", want) {
			t.Errorf("parallelDivisor(%v) = %v, want %v", w, got, want)
		}
	}
}

func TestCostPolicy_toCopy(t *testing.T) {
	def := DefaultCostPolicy()
	if def.CostThreshold != 50000 || def.ScanRows != 1000000 {
		t.Fatalf("defaults %+v", def)
	}
	for _, tc := range []struct {
		pol  CostPolicy
		pl   pgPlan
		want bool
	}{
		// The measured statements (TPC-C, PostgreSQL 16).
		{def, pgPlan{cost: 1}, false},                                                // a read by the primary key
		{def, pgPlan{cost: 12}, false},                                               // an order's lines by an index
		{def, pgPlan{cost: 712}, false},                                              // the stock level join
		{def, pgPlan{cost: 15059}, false},                                            // count(*) of one warehouse's orders by an index
		{def, pgPlan{cost: 4202, maxSeqScanRows: 201680}, false},                     // max(ts) of a small table
		{def, pgPlan{cost: 177}, false},                                              // a report cheap on PostgreSQL too
		{def, pgPlan{cost: 98367, maxSeqScanRows: 2400000}, true},                    // the cheapest expensive report
		{def, pgPlan{cost: 13351258, maxSeqScanRows: 61000000}, true},                // the most expensive one
		{def, pgPlan{cost: 50000}, true},                                             // the cost exactly at the threshold
		{def, pgPlan{cost: 49999.99}, false},                                         // just under it
		{def, pgPlan{cost: 30000, maxSeqScanRows: 1000000}, true},                    // a scan exactly at its threshold, whatever the cost
		{def, pgPlan{cost: 30000, maxSeqScanRows: 999999}, false},                    // just under it
		{CostPolicy{ScanRows: 10}, pgPlan{cost: 1e9}, false},                         // the cost rule off
		{CostPolicy{CostThreshold: 10}, pgPlan{cost: 1, maxSeqScanRows: 1e9}, false}, // the scan rule off
		{CostPolicy{CostThreshold: 10}, pgPlan{cost: 10}, true},
	} {
		if got, why := tc.pol.toCopy(tc.pl); got != tc.want || why == "" {
			t.Errorf("%+v %+v: %v (%s), want %v", tc.pol, tc.pl, got, why, tc.want)
		}
	}
	if !(CostPolicy{}).off() || (CostPolicy{ScanRows: 1}).off() || def.off() {
		t.Error("off() is wrong")
	}
}

func TestMultiStatement(t *testing.T) {
	for q, want := range map[string]bool{
		"SELECT 1":                       false,
		"SELECT 1;":                      false,
		"SELECT 1 ;; ;":                  false,
		"SELECT 1; SELECT 2":             true,
		"SELECT 1;SELECT 2;":             true,
		"SELECT ';' FROM t":              false,
		`SELECT ";" FROM t`:              false,
		"SELECT 1 -- ; SELECT 2":         false,
		"SELECT 1 /* ; SELECT 2 */":      false,
		"SELECT 1; -- a comment":         false,
		"SELECT 1; /* only a comment */": false,
		"SELECT 'unterminated; SELECT 2": true,
	} {
		if got := multiStatement(q); got != want {
			t.Errorf("multiStatement(%q) = %v, want %v", q, got, want)
		}
	}
}

// fakeSource is a PostgreSQL the port's forwarding account logs in to. It
// answers EXPLAIN with the plan of the first entry of plans whose key the
// statement contains (an error when the value starts with "error "), and any
// other statement with one row: 42, which the copy never answers.
type fakeSource struct {
	plans map[string]string
	addr  string
	// delay, when set, is how long it takes to answer each simple query;
	// noPosition, when set, refuses the read of its WAL position.
	delay      time.Duration
	noPosition bool
	// blockReadWrite, when set, answers that a block the port opened is not
	// read-only; begins are the texts that opened a block.
	blockReadWrite bool
	begins         []string
	// tables, when set, are the plain tables it confirms a statement read
	// (proofSQL), by the name as written, each with the [schema, table] it
	// answers; a name it does not know is not confirmed. nil: it confirms
	// nothing, and every table counts as read.
	tables map[string][2]string

	mu       sync.Mutex
	explains []string
	queries  []string
	// positions counts the reads of its WAL position, proofs the
	// statements it was asked to confirm the tables of.
	positions int
	proofs    []string
	// discards counts its DISCARD ALL; refuseDiscard answers one with an
	// error.
	discards      int
	refuseDiscard bool
	// hangDiscard answers a DISCARD ALL with nothing.
	hangDiscard bool
	// realDSN, when set, is a real PostgreSQL the port forwards to.
	realDSN string
	// emptyRows, when set, answers a statement that names "manyempty" with
	// that many rows of one NULL cell, then waits for emptyGate to close
	// before it ends the answer.
	emptyRows int
	emptyGate chan struct{}
	// conns are the connections it serves now (dropConns).
	conns map[net.Conn]bool
}

// sayToConns writes a FATAL to every connection it serves, without closing
// them yet, as a source ending an idle one says why first.
func (f *fakeSource) sayToConns() {
	msg, _ := (&pgproto3.ErrorResponse{Severity: "FATAL", Code: "25P04", Message: "terminating connection due to idle-session timeout"}).Encode(nil)
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.conns {
		_, _ = c.Write(msg)
	}
}

// dropConns ends every connection it serves, as a source does for an idle
// one past idle_session_timeout.
func (f *fakeSource) dropConns() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.conns {
		_ = c.Close()
	}
}

// proofRef is one name of a proofSQL statement and whether it may stay
// unresolved.
var proofRef = regexp.MustCompile(`\('((?:[^']|'')*)', (true|false)\)`)

// proved are the statements the source was asked to confirm the tables of.
func (f *fakeSource) proved() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.proofs...)
}

// proofCell answers a proofSQL statement as a PostgreSQL holding f.tables
// would: nil for NULL.
func (f *fakeSource) proofCell(q string) []byte {
	if f.tables == nil {
		return nil
	}
	_, values, ok := strings.Cut(q, "FROM (VALUES ")
	if !ok {
		return []byte("[]")
	}
	out := [][]string{}
	for _, m := range proofRef.FindAllStringSubmatch(values, -1) {
		t, known := f.tables[strings.ReplaceAll(m[1], "''", "'")]
		switch {
		case known:
			out = append(out, []string{t[0], t[1]})
		case m[2] == "false":
			return nil
		}
	}
	cell, _ := json.Marshal(out)
	return cell
}

// fakeWALPosition is the WAL position a fakeSource says it stands at.
const fakeWALPosition = "16/B374D848"

func (f *fakeSource) positionsRead() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.positions
}

func startFakeSource(t *testing.T, plans map[string]string) *fakeSource {
	t.Helper()
	return startFakeSourceSlow(t, plans, 0)
}

// startFakeSourceSlow is startFakeSource answering each simple query after
// delay.
func startFakeSourceSlow(t *testing.T, plans map[string]string, delay time.Duration) *fakeSource {
	t.Helper()
	return startFakeSourceWith(t, &fakeSource{plans: plans, delay: delay})
}

// startFakeSourceWith serves f, its fields set before any connection.
func startFakeSourceWith(t *testing.T, f *fakeSource) *fakeSource {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.addr = ln.Addr().String()
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return f
}

func (f *fakeSource) dsn() string {
	if f.realDSN != "" {
		return f.realDSN
	}
	return fmt.Sprintf("postgres://fwd:pw@%s/db?sslmode=disable&connect_timeout=5", f.addr)
}

func (f *fakeSource) explained() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.explains...)
}

func (f *fakeSource) ran() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

var dollarParam = regexp.MustCompile(`\$(\d+)`)

func (f *fakeSource) serve(c net.Conn) {
	f.mu.Lock()
	if f.conns == nil {
		f.conns = map[net.Conn]bool{}
	}
	f.conns[c] = true
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.conns, c)
		f.mu.Unlock()
	}()
	defer c.Close()
	be := pgproto3.NewBackend(c, c)
	if _, err := be.ReceiveStartupMessage(); err != nil {
		return
	}
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "default_transaction_read_only", Value: "on"})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 7, SecretKey: []byte{1, 2, 3, 4}})
	tx := byte('I')
	be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
	if be.Flush() != nil {
		return
	}
	row := func() {
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("42")}})
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
	}
	rowShape := &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("n"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1}}}
	params := 0
	failed := false
	for {
		msg, err := be.Receive()
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			q := m.String
			upper := strings.ToUpper(strings.TrimSpace(q))
			if f.delay > 0 {
				time.Sleep(f.delay)
			}
			switch {
			case strings.HasPrefix(upper, "EXPLAIN"):
				f.mu.Lock()
				f.explains = append(f.explains, q)
				f.mu.Unlock()
				answer := `[{"Plan": {"Node Type": "Result", "Total Cost": 0.01, "Plan Rows": 1}}]`
				// The longest key the statement holds: the most specific, the
				// same one every time.
				best := ""
				for k, v := range f.plans {
					if strings.Contains(q, k) && len(k) > len(best) {
						best, answer = k, v
					}
				}
				if code, ok := strings.CutPrefix(answer, "error "); ok {
					be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: "refused by the fake source"})
				} else {
					be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("QUERY PLAN"), DataTypeOID: 114, DataTypeSize: -1, TypeModifier: -1}}})
					be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(answer)}})
					be.Send(&pgproto3.CommandComplete{CommandTag: []byte("EXPLAIN")})
				}
			case strings.Contains(q, "FROM pg_catalog.pg_operator o"):
				f.mu.Lock()
				f.proofs = append(f.proofs, q)
				f.mu.Unlock()
				if tx == 'E' {
					be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02", Message: "current transaction is aborted"})
					break
				}
				be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("case"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}})
				be.Send(&pgproto3.DataRow{Values: [][]byte{f.proofCell(q)}})
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case strings.Contains(q, "pg_current_wal_lsn()"):
				f.mu.Lock()
				f.positions++
				f.mu.Unlock()
				if f.noPosition {
					be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "permission denied for function pg_current_wal_lsn"})
					break
				}
				be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("lsn"), DataTypeOID: 3220, DataTypeSize: 8, TypeModifier: -1}}})
				be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(fakeWALPosition)}})
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case upper == "DISCARD ALL":
				f.mu.Lock()
				f.discards++
				refuse, hang := f.refuseDiscard, f.hangDiscard
				f.mu.Unlock()
				if hang {
					// Answers nothing, until the port closes the connection.
					_, _ = be.Receive()
					return
				}
				if refuse {
					be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25006", Message: "refused by the fake source"})
				} else {
					be.Send(&pgproto3.CommandComplete{CommandTag: []byte("DISCARD ALL")})
				}
				be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
				if be.Flush() != nil {
					return
				}
				continue
			case strings.HasPrefix(upper, "SET"):
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SET")})
			case upper == "BEGIN" || strings.HasPrefix(upper, "BEGIN "):
				tx = 'T'
				f.mu.Lock()
				f.begins = append(f.begins, q)
				f.mu.Unlock()
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
				if strings.HasSuffix(upper, "; SHOW TRANSACTION_READ_ONLY") {
					// The port's question behind its BEGIN.
					answer := "on"
					if f.blockReadWrite {
						answer = "off"
					}
					be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("transaction_read_only"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}})
					be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(answer)}})
					be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SHOW")})
				}
			case upper == "COMMIT" || upper == "ROLLBACK":
				tx = 'I'
				be.Send(&pgproto3.CommandComplete{CommandTag: []byte(upper)})
			default:
				f.mu.Lock()
				f.queries = append(f.queries, q)
				f.mu.Unlock()
				if strings.Contains(q, "rowsthenfail") {
					// Rows, then an error, as SELECT c FROM t; SELECT 1/0.
					be.Send(rowShape)
					row()
					be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "22012", Message: "division by zero"})
					break
				}
				if strings.Contains(q, "manyempty") && f.emptyRows > 0 {
					be.Send(rowShape)
					for range f.emptyRows {
						be.Send(&pgproto3.DataRow{Values: [][]byte{nil}})
					}
					if be.Flush() != nil {
						return
					}
					select {
					case <-f.emptyGate:
					case <-time.After(30 * time.Second):
					}
					be.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", f.emptyRows))})
					break
				}
				if strings.Contains(q, "nosuch") {
					be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P01", Message: `relation "nosuch" does not exist`})
					break
				}
				be.Send(rowShape)
				row()
			}
			be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
		case *pgproto3.Parse:
			if failed {
				continue
			}
			f.mu.Lock()
			f.queries = append(f.queries, m.Query)
			f.mu.Unlock()
			params = 0
			for _, p := range dollarParam.FindAllStringSubmatch(m.Query, -1) {
				var n int
				fmt.Sscan(p[1], &n)
				params = max(params, n)
			}
			be.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Describe:
			if failed {
				continue
			}
			if m.ObjectType == 'S' {
				oids := make([]uint32, params)
				for i := range oids {
					oids[i] = 23
				}
				be.Send(&pgproto3.ParameterDescription{ParameterOIDs: oids})
			}
			be.Send(rowShape)
		case *pgproto3.Bind:
			if !failed {
				be.Send(&pgproto3.BindComplete{})
			}
		case *pgproto3.Execute:
			if !failed {
				row()
			}
		case *pgproto3.Close:
			if !failed {
				be.Send(&pgproto3.CloseComplete{})
			}
		case *pgproto3.Sync:
			failed = false
			be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
		case *pgproto3.Terminate:
			return
		}
		if be.Flush() != nil {
			return
		}
	}
}

// costLogin is a port whose copy answers every statement with 7 and whose
// source is a fakeSource (nil: the server has no forwarding account).
type costLogin struct {
	src  *fakeSource
	cost *CostPolicy
	// held counts the connections to the source held now, and heldAtCopy
	// what it was each time the copy ran a statement; opened how many were
	// asked for.
	held       atomic.Int32
	opened     atomic.Int32
	mu         sync.Mutex
	heldAtCopy []int32
	notes      []RouteNote
	// slots, when set, bounds the connections to the source as the port
	// does: a statement waits for one while its context lasts. maxHeld is
	// the most held at once.
	slots   chan struct{}
	maxHeld atomic.Int32
	// refuse makes every Acquire fail, as a port whose every connection to
	// the source stayed taken for the whole wait.
	refuse bool
	// age, when set, is the copy's RunAge; wait its CopyWait. runs counts
	// the statements the copy ran.
	age  func() CopyAge
	wait func(ctx context.Context, lsn uint64) (CopyWaitOutcome, string)
	runs atomic.Int32
	// catalog, when set, is the copy's tables (Binding.Catalog).
	catalog func() []CatalogTable
	// copyErr, when set, is what the copy answers a statement with instead
	// of 7 when it is not nil; catalogErr the same for its catalog.
	copyErr    func(st string) error
	catalogErr func() error
	// captureAccount is Forward.CaptureAccount, names Binding.CopyTableNames.
	captureAccount bool
	names          func() [][2]string
	// tailsPending is a copy whose tables' recent changes are not ready: a
	// statement asked to have them (RunOptions.NeedTails) is refused before
	// it runs, as the host's copy refuses it (#3086); any other runs from
	// the snapshot. needTails records what each statement asked.
	tailsPending atomic.Bool
	needTails    []bool
	// olderThanSeen is a copy whose recent changes are older than an
	// earlier answer of the copy on the connection (#3116): a statement
	// asked to have them (RunOptions.NeedTails) is refused before it runs,
	// as the host's copy refuses it; any other is answered with the host's
	// note that says so.
	olderThanSeen atomic.Bool
	// fastLane records RunOptions.FastLane of each statement the copy was
	// given (fastlane.go).
	fastLane []bool
	// keep, when set, is the port's pool of connections kept idle between
	// statements (Forward.Park).
	keep *keptSources
	// barred, when set, is the server's Forward.Barred.
	barred func() string
	// sides, when set, is the server's Forward.Sides.
	sides *CostSides
	// notCaptured, when set, is Binding.NotCaptured.
	notCaptured func(context.Context) ([]TableNotCaptured, error)
	// joinStats, when set, is the server's Forward.JoinStats; joinNDV
	// records RunOptions.JoinNDV of each statement the copy was given.
	joinStats *JoinStats
	joinRun   []joinRun
	// orders, when set, is Binding.WrittenOrders; with retried the copy
	// answers every statement it runs in its own join order after its retry
	// in the order written (sqlsandbox.WrittenOrderRetryOK).
	orders  *WrittenOrders
	retried bool
}

// joinRun is one statement the copy was given, its RunOptions.JoinNDV and
// KeepJoinOrder.
type joinRun struct {
	statement string
	ndv       map[string]float64
	keepOrder bool
}

func (l *costLogin) joinRuns() []joinRun {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]joinRun(nil), l.joinRun...)
}

// keptSources is a port's pool of connections kept idle by their clients, as
// the pg-router's is: reclaimAll ends them all, as clients waiting for a
// slot would.
type keptSources struct {
	mu   sync.Mutex
	idle map[*func()]bool
}

func (k *keptSources) park(reclaim func()) func() bool {
	e := &reclaim
	k.mu.Lock()
	if k.idle == nil {
		k.idle = map[*func()]bool{}
	}
	k.idle[e] = true
	k.mu.Unlock()
	return func() bool {
		k.mu.Lock()
		defer k.mu.Unlock()
		ok := k.idle[e]
		delete(k.idle, e)
		return ok
	}
}

func (k *keptSources) reclaimAll() int {
	k.mu.Lock()
	var all []*func()
	for e := range k.idle {
		all = append(all, e)
	}
	clear(k.idle)
	k.mu.Unlock()
	for _, e := range all {
		(*e)()
	}
	return len(all)
}

func (k *keptSources) parked() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.idle)
}

func (l *costLogin) run(_ context.Context, st string, opts RunOptions) (sqlsandbox.Result, error) {
	l.mu.Lock()
	l.needTails = append(l.needTails, opts.NeedTails)
	l.fastLane = append(l.fastLane, opts.FastLane)
	l.joinRun = append(l.joinRun, joinRun{st, opts.JoinNDV, opts.KeepJoinOrder})
	l.mu.Unlock()
	if opts.NeedTails && l.tailsPending.Load() {
		return sqlsandbox.Result{}, &sqlsandbox.MayHaveChangedError{Reason: "fresh mode: the newest captured changes of public.t are not ready yet"}
	}
	if opts.NeedTails && l.olderThanSeen.Load() {
		return sqlsandbox.Result{}, &sqlsandbox.MayHaveChangedError{Reason: "fresh mode: " + olderThanSeenReason}
	}
	l.runs.Add(1)
	if l.copyErr != nil {
		if err := l.copyErr(st); err != nil {
			return sqlsandbox.Result{}, err
		}
	}
	l.mu.Lock()
	l.heldAtCopy = append(l.heldAtCopy, l.held.Load())
	l.mu.Unlock()
	res := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{int32(7)}}}
	if l.olderThanSeen.Load() {
		res.Note = olderThanSeenNote
	}
	if l.retried && !opts.KeepJoinOrder {
		res.WrittenOrderRetry = sqlsandbox.WrittenOrderRetryOK
	}
	return res, nil
}

func (l *costLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	b := &Binding{Run: l.run, Actor: "server:orders", Schema: "public", WrittenOrders: l.orders, Routed: func(n RouteNote) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.notes = append(l.notes, n)
	}}
	if l.src != nil {
		b.Forward = &Forward{DSN: l.src.dsn(), Limit: 4, CaptureAccount: l.captureAccount, Cost: l.cost, Barred: l.barred, Sides: l.sides, JoinStats: l.joinStats, Acquire: func(ctx context.Context) (func(), bool) {
			if l.refuse {
				return nil, false
			}
			if l.slots != nil {
				select {
				case l.slots <- struct{}{}:
				case <-ctx.Done():
					return nil, false
				}
			}
			l.opened.Add(1)
			n := l.held.Add(1)
			for m := l.maxHeld.Load(); n > m && !l.maxHeld.CompareAndSwap(m, n); m = l.maxHeld.Load() {
			}
			var once sync.Once
			return func() {
				once.Do(func() {
					l.held.Add(-1)
					if l.slots != nil {
						<-l.slots
					}
				})
			}, true
		}}
	}
	if l.src != nil && l.keep != nil {
		b.Forward.Park = l.keep.park
	}
	if l.names != nil {
		b.CopyTableNames = func(context.Context) ([][2]string, error) { return l.names(), nil }
	}
	b.RunAge = l.age
	if l.catalog != nil {
		b.Catalog = func(context.Context) ([]CatalogTable, error) {
			if l.catalogErr != nil {
				if err := l.catalogErr(); err != nil {
					return nil, err
				}
			}
			return l.catalog(), nil
		}
	}
	if l.wait != nil {
		b.CopyWait = l.wait
	}
	b.NotCaptured = l.notCaptured
	return b, nil
}

// lastNeedTails is what the copy's last statement asked of its tails.
func (l *costLogin) lastNeedTails() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.needTails) > 0 && l.needTails[len(l.needTails)-1]
}

func (l *costLogin) last() RouteNote {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.notes) == 0 {
		return RouteNote{}
	}
	return l.notes[len(l.notes)-1]
}

func costPortConn(t *testing.T, ctx context.Context, l *costLogin) *pgconn.PgConn {
	t.Helper()
	addr := serveAddrCfg(t, Config{Login: l.login})
	c, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

// costPlans are the source's plans for the statements of the routing tests.
var costPlans = map[string]string{
	"WHERE id = ":   plan(`{"Node Type": "Index Scan", "Total Cost": 8.45, "Plan Rows": 1}`),
	"FROM big":      plan(`{"Node Type": "Aggregate", "Total Cost": 98367, "Plan Rows": 1, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`),
	"FROM wide":     plan(`{"Node Type": "Aggregate", "Total Cost": 30000, "Plan Rows": 1, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 29000, "Plan Rows": 1200000}]}`),
	"FROM hb":       plan(`{"Node Type": "Aggregate", "Total Cost": 4202, "Plan Rows": 1, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 3700, "Plan Rows": 201680}]}`),
	"FROM nosuch":   "error 42P01",
	"FROM garbled":  `[{"Plan": "not a node"}]`,
	"FROM canceled": "error 57014",
	"FROM locked":   "error 55P03",
}

type costOutcome struct{ answer, code, side, reason string }

func costSimple(ctx context.Context, c *pgconn.PgConn, l *costLogin, stmt string) costOutcome {
	res, err := c.Exec(ctx, stmt).ReadAll()
	o := costOutcome{}
	var pe *pgconn.PgError
	switch {
	case errors.As(err, &pe):
		o.code = pe.Code
	case err != nil:
		o.code = err.Error()
	}
	if len(res) > 0 && len(res[0].Rows) > 0 {
		o.answer = string(res[0].Rows[0][0])
	}
	n := l.last()
	o.side, o.reason = n.Side, n.Reason
	return o
}

func TestRouteByCost_simple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src}
	c := costPortConn(t, ctx, l)
	for _, tc := range []struct {
		name, stmt string
		want       costOutcome
		explain    string // the EXPLAIN the source was asked, "" for none
	}{
		{"a read by the primary key", "SELECT n FROM t WHERE id = 1", costOutcome{"42", "", RouteSource, ReasonCheapPlan}, "EXPLAIN (FORMAT JSON) SELECT n FROM t WHERE id = 1"},
		{"a report", "SELECT sum(n) FROM big", costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}, "EXPLAIN (FORMAT JSON) SELECT sum(n) FROM big"},
		{"a scan over the row threshold that costs little", "SELECT sum(n) FROM wide", costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}, "EXPLAIN (FORMAT JSON) SELECT sum(n) FROM wide"},
		{"a scan of a small table", "SELECT sum(n) FROM hb", costOutcome{"42", "", RouteSource, ReasonCheapPlan}, "EXPLAIN (FORMAT JSON) SELECT sum(n) FROM hb"},
		{"EXPLAIN refused: the source answers, with its error", "SELECT count(*) FROM nosuch", costOutcome{"", "42P01", RouteSource, ReasonExplainFailed}, "EXPLAIN (FORMAT JSON) SELECT count(*) FROM nosuch"},
		{"a plan that cannot be read", "SELECT count(*) FROM garbled", costOutcome{"42", "", RouteSource, ReasonExplainFailed}, "EXPLAIN (FORMAT JSON) SELECT count(*) FROM garbled"},
		{"a plan refused for the moment", "SELECT count(*) FROM locked", costOutcome{"42", "", RouteSource, ReasonExplainFailed}, "EXPLAIN (FORMAT JSON) SELECT count(*) FROM locked"},
		{"cancelled while its plan was asked for: not run anywhere", "SELECT count(*) FROM canceled", costOutcome{"", "57014", RouteSource, ReasonExplainFailed}, "EXPLAIN (FORMAT JSON) SELECT count(*) FROM canceled"},
		// Asked of nothing: the screen sends it to the source already.
		// Its plan is asked (heavy.go): cheap, the screen's veto stands.
		{"a veto", "SELECT now()", costOutcome{"42", "", RouteSource, ReasonVeto}, "EXPLAIN (FORMAT JSON) SELECT now()"},
		// EXPLAIN of the first would run the second on the source.
		{"two statements", "SELECT sum(n) FROM big; SELECT sum(n) FROM big", costOutcome{"42", "", RouteSource, ReasonExplainFailed}, ""},
	} {
		before := len(src.explained())
		ranBefore := len(src.ran())
		got := costSimple(ctx, c, l, tc.stmt)
		if got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
		asked := src.explained()[before:]
		switch {
		case tc.explain == "" && len(asked) != 0:
			t.Errorf("%s: the source was asked %q", tc.name, asked)
		case tc.explain != "" && (len(asked) != 1 || asked[0] != tc.explain):
			t.Errorf("%s: the source was asked %q, want %q once", tc.name, asked, tc.explain)
		}
		ran := src.ran()[ranBefore:]
		if onSource := len(ran) > 0; onSource != (tc.want.side == RouteSource && tc.want.code != "57014") {
			t.Errorf("%s: ran on the source %q", tc.name, ran)
		}
	}
	// The copy ran with no connection held to the source: the one its plan
	// was asked on was given back first.
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.heldAtCopy) == 0 {
		t.Fatal("the copy answered nothing")
	}
	for i, h := range l.heldAtCopy {
		if h != 0 {
			t.Errorf("copy statement %d ran with %d connections held to the source", i, h)
		}
	}
	if c.TxStatus() != 'I' {
		t.Errorf("status %c", c.TxStatus())
	}
}

func TestRouteByCost_notAsked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Without a forwarding account the copy answers everything, and no
	// source is asked.
	noFwd := &costLogin{}
	c := costPortConn(t, ctx, noFwd)
	if got := costSimple(ctx, c, noFwd, "SELECT n FROM t WHERE id = 1"); got != (costOutcome{"7", "", RouteCopy, ReasonNoForwardAccount}) {
		t.Errorf("without a forwarding account: %+v", got)
	}

	// Inside a transaction block the source answers, without being asked
	// for a plan.
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src}
	d := costPortConn(t, ctx, l)
	if _, err := d.Exec(ctx, "BEGIN").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if got := costSimple(ctx, d, l, "SELECT sum(n) FROM big"); got != (costOutcome{"42", "", RouteSource, ReasonInTransaction}) {
		t.Errorf("in a block: %+v", got)
	}
	if _, err := d.Exec(ctx, "COMMIT").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if asked := src.explained(); len(asked) != 0 {
		t.Errorf("asked in a block: %q", asked)
	}

	// Routing by plan turned off: the copy answers what the screen lets
	// through, as before, and nothing is asked.
	off := &costLogin{src: startFakeSource(t, costPlans), cost: &CostPolicy{}}
	e := costPortConn(t, ctx, off)
	if got := costSimple(ctx, e, off, "SELECT n FROM t WHERE id = 1"); got != (costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}) {
		t.Errorf("turned off: %+v", got)
	}
	if asked := off.src.explained(); len(asked) != 0 {
		t.Errorf("asked with routing by plan off: %q", asked)
	}

	// Thresholds of the server's own: a low one sends the read by key to the
	// copy.
	low := &costLogin{src: startFakeSource(t, costPlans), cost: &CostPolicy{CostThreshold: 5}}
	g := costPortConn(t, ctx, low)
	if got := costSimple(ctx, g, low, "SELECT n FROM t WHERE id = 1"); got != (costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}) {
		t.Errorf("a threshold of 5: %+v", got)
	}
}

// A prepared statement is asked about once, before any argument exists
// (GENERIC_PLAN when it has parameters), and its Describe and every
// execution are answered by the same side.
func TestRouteByCost_prepared(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src}
	c := costPortConn(t, ctx, l)
	for i, tc := range []struct {
		name, stmt string
		args       [][]byte
		answer     string
		side       string
		reason     string
		explain    string
	}{
		{"a read by the primary key", "SELECT n FROM t WHERE id = $1", [][]byte{[]byte("1")}, "42", RouteSource, ReasonCheapPlan, "EXPLAIN (GENERIC_PLAN, FORMAT JSON) SELECT n FROM t WHERE id = $1"},
		{"a report with a parameter", "SELECT sum(n) FROM big WHERE n = $1", [][]byte{[]byte("1")}, "7", RouteCopy, ReasonAnswersTheSame, "EXPLAIN (GENERIC_PLAN, FORMAT JSON) SELECT sum(n) FROM big WHERE n = $1"},
		{"a report without a parameter", "SELECT sum(n) FROM big", nil, "7", RouteCopy, ReasonAnswersTheSame, "EXPLAIN (FORMAT JSON) SELECT sum(n) FROM big"},
	} {
		name := fmt.Sprintf("s%d", i)
		before := len(src.explained())
		sd, err := c.Prepare(ctx, name, tc.stmt, nil)
		if err != nil {
			t.Fatalf("%s: prepare: %v", tc.name, err)
		}
		if len(sd.Fields) != 1 || string(sd.Fields[0].Name) != "n" {
			t.Errorf("%s: described %+v", tc.name, sd.Fields)
		}
		for i := range 3 {
			res := c.ExecPrepared(ctx, name, tc.args, nil, nil).Read()
			if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != tc.answer {
				t.Errorf("%s, run %d: %v %q, want %s", tc.name, i, res.Err, res.Rows, tc.answer)
			}
			if n := l.last(); n.Side != tc.side || n.Reason != tc.reason {
				t.Errorf("%s, run %d: noted %s/%s, want %s/%s", tc.name, i, n.Side, n.Reason, tc.side, tc.reason)
			}
		}
		if asked := src.explained()[before:]; len(asked) != 1 || asked[0] != tc.explain {
			t.Errorf("%s: the source was asked %q, want %q once", tc.name, asked, tc.explain)
		}
	}
	// The unnamed statement of ExecParams: decided at its Bind, the same way.
	before := len(src.explained())
	res := c.ExecParams(ctx, "SELECT n FROM t WHERE id = $1", [][]byte{[]byte("1")}, nil, nil, nil).Read()
	if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "42" {
		t.Errorf("ExecParams of a read by key: %v %q", res.Err, res.Rows)
	}
	if asked := src.explained()[before:]; len(asked) != 1 {
		t.Errorf("ExecParams: asked %q, want once", asked)
	}
	if c.TxStatus() != 'I' {
		t.Errorf("status %c", c.TxStatus())
	}
}

// A source that cannot be reached is tried once for a simple query, and the
// copy answers it (#2985): routing by plan is off while the source cannot be
// asked.
func TestRouteByCost_unreachableSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	l := &costLogin{src: &fakeSource{addr: ln.Addr().String()}}
	c := costPortConn(t, ctx, l)
	// The copy answers it, as before routing by plan, with a notice (#2985).
	got := costSimple(ctx, c, l, "SELECT sum(n) FROM big")
	if got != (costOutcome{"7", "", RouteCopy, ReasonSourceUnreachable}) {
		t.Errorf("an unreachable source: %+v, want the copy's 7 as source_unreachable", got)
	}
	if n := l.opened.Load(); n != 1 {
		t.Errorf("%d connections to the source were opened for one statement, want 1: it was sent there again", n)
	}
}

// A statement first described in the middle of a batch that already went to
// the source is not asked about there (a simple query cannot be sent in the
// middle of it, and an error of the port's own would discard the client's
// batch): it goes to the source for that batch, and is asked the next time.
func TestRouteByCost_inAnOpenBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src}
	c := costPortConn(t, ctx, l)
	p := c.StartPipeline(ctx)
	// A veto first: its batch is open on the source from its Bind on.
	p.SendQueryParams("SELECT now()", nil, nil, nil, nil)
	p.SendPrepare("report", "SELECT sum(n) FROM big", nil)
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	var described *pgconn.StatementDescription
	for done := false; !done; {
		r, err := p.GetResults()
		if err != nil {
			t.Fatalf("the batch: %v", err)
		}
		switch x := r.(type) {
		case *pgconn.ResultReader:
			if res := x.Read(); res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "42" {
				t.Errorf("the veto: %v %q", res.Err, res.Rows)
			}
		case *pgconn.StatementDescription:
			described = x
		case *pgconn.PipelineSync:
			done = true
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if described == nil || len(described.Fields) != 1 {
		t.Fatalf("the report was described as %+v", described)
	}
	if asked := src.explained(); len(asked) != 0 {
		t.Errorf("asked in the middle of a batch on the source: %q", asked)
	}
	// Its own batch: asked once, and answered by the copy.
	for i := range 2 {
		res := c.ExecPrepared(ctx, "report", nil, nil, nil).Read()
		if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "7" {
			t.Errorf("run %d: %v %q, want the copy's 7", i, res.Err, res.Rows)
		}
	}
	if asked := src.explained(); len(asked) != 1 || asked[0] != "EXPLAIN (FORMAT JSON) SELECT sum(n) FROM big" {
		t.Errorf("asked %q, want once in a batch of its own", asked)
	}
	if c.TxStatus() != 'I' {
		t.Errorf("status %c", c.TxStatus())
	}
}
