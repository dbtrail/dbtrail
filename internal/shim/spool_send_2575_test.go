package shim

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// spoolStmt is an expensive SELECT the ladder sends to the copy.
const spoolStmt = "SELECT id, s, amount, created FROM t"

// spoolCase is a copy answer with every kind of cell the wire renders
// differently: a number, text, an empty string (not NULL), a DECIMAL (text
// with its scale), a TIMESTAMP (RFC 3339 in, MySQL's spelling out), NULL.
func spoolCase() (sqlsandbox.Result, [][]any) {
	cols := []sqlsandbox.Column{{Name: "id", Type: "BIGINT"}, {Name: "s", Type: "VARCHAR"},
		{Name: "d", Type: "DECIMAL(10,2)"}, {Name: "at", Type: "TIMESTAMP"}}
	rows := [][]any{
		{json.Number("1"), "one", "2.50", "2026-01-02T03:04:05Z"},
		{json.Number("2"), "", nil, nil},
		{nil, "three", "0.00", "2026-12-31T23:59:59.5Z"},
	}
	return sqlsandbox.Result{Columns: cols}, rows
}

// spoolRouting is a routing handler whose copy answers expensive statements
// from a file when asked to, its client a packet capture.
func spoolRouting(t *testing.T, f *fakeFreeSQL, r *fakeRouter, pw *capturePW) *Handler {
	t.Helper()
	f.updatedAt = time.Now()
	f.spoolDir = t.TempDir()
	h := routingHandler(t, r, f, time.Minute)
	if pw != nil {
		h.BindConn(pw)
	}
	return h
}

func gone(t *testing.T, dirs []string) {
	t.Helper()
	for _, d := range dirs {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			t.Errorf("the result file %s was left behind (%v)", d, err)
		}
	}
}

// A routed text statement asks the copy for a file, and a result that comes
// back in one is sent to the client row by row: the same header (the
// source's names, the copy's types) and the same row bytes as the result in
// memory would have been, then the file is removed. The audit record counts
// the file's rows.
func TestRoute_resultFromAFileIsTheSameAnswer(t *testing.T) {
	base, rows := spoolCase()
	names := []string{"ID", "S", "D", "AT"}

	// The answer in memory: the reference.
	memF := &fakeFreeSQL{res: base}
	memF.res.Rows = rows
	mem := spoolRouting(t, memF, &fakeRouter{toCopy: true, reason: "expensive", namesDefault: names}, nil)
	want, err := mem.HandleQuery(spoolStmt)
	if err != nil || want.Resultset == nil || len(want.Resultset.RowDatas) != 3 {
		t.Fatalf("in memory: (%v, %v)", want, err)
	}
	if memF.gotSess.ResultToFile {
		t.Error("a connection that cannot stream asked for a file")
	}

	f := &fakeFreeSQL{res: base, spoolRows: rows}
	pw := &capturePW{}
	h := spoolRouting(t, f, &fakeRouter{toCopy: true, reason: "expensive", namesDefault: names}, pw)
	rec := audittest.Install(t)
	got, err := h.HandleQuery(spoolStmt)
	if err != nil {
		t.Fatal(err)
	}
	if !f.gotSess.ResultToFile || len(f.spools) != 1 {
		t.Fatalf("a routed text statement did not ask for a file (ResultToFile %v, %d files)", f.gotSess.ResultToFile, len(f.spools))
	}
	if got == nil || got.Resultset == nil || !got.Resultset.StreamingDone {
		t.Fatalf("not a streamed answer: %+v", got)
	}
	cols := len(names)
	if len(pw.packets) != headerPackets(names)+3 {
		t.Fatalf("%d packets, want the header (%d) and 3 rows", len(pw.packets), headerPackets(names))
	}
	for i, f := range want.Resultset.Fields {
		if !bytes.Equal(pw.packets[1+i], f.Dump()) {
			t.Errorf("column %d: the file's header %q, in memory %q", i, pw.packets[1+i], f.Dump())
		}
	}
	for i, rd := range want.Resultset.RowDatas {
		if !bytes.Equal(pw.packets[cols+2+i], rd) {
			t.Errorf("row %d: from the file %q, in memory %q", i, pw.packets[cols+2+i], rd)
		}
	}
	gone(t, f.spools)
	if ev := rec.Events(); len(ev) != 1 || ev[0].Detail["rows"] != "3" {
		t.Errorf("audit: %+v, want one record of 3 rows", ev)
	}
	if h.spooled != nil {
		t.Error("the answer stayed pending after it was sent")
	}
}

// A result over the port's row cap is no longer sent to MySQL on the plan's
// estimate when it can come from a file: the copy has no row cap then. A
// connection that cannot stream keeps the cap, and so does one whose copy
// has no disk for a file now (it would compute the result and refuse it).
func TestRoute_rowCapDoesNotApplyToAFile(t *testing.T) {
	base, rows := spoolCase()
	f := &fakeFreeSQL{res: base, spoolRows: rows, rowCap: 1000}
	r := &fakeRouter{toCopy: true, reason: "expensive", resultRows: 3_000_000}
	h := spoolRouting(t, f, r, &capturePW{})
	if _, err := h.HandleQuery(spoolStmt); err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 || len(r.forwarded) != 0 {
		t.Errorf("with a file: copy ran %d time(s), forwarded %v; want the copy", f.calls, r.forwarded)
	}

	f2 := &fakeFreeSQL{res: base, spoolRows: rows, rowCap: 1000}
	r2 := &fakeRouter{toCopy: true, reason: "expensive", resultRows: 3_000_000}
	h2 := spoolRouting(t, f2, r2, nil)
	if _, err := h2.HandleQuery(spoolStmt); err != nil {
		t.Fatal(err)
	}
	if f2.calls != 0 || len(r2.forwarded) != 1 {
		t.Errorf("without a file: copy ran %d time(s), forwarded %v; want MySQL", f2.calls, r2.forwarded)
	}

	f3 := &fakeFreeSQL{res: base, spoolRows: rows, rowCap: 1000, noFileRoom: true}
	r3 := &fakeRouter{toCopy: true, reason: "expensive", resultRows: 3_000_000}
	h3 := spoolRouting(t, f3, r3, &capturePW{})
	if _, err := h3.HandleQuery(spoolStmt); err != nil {
		t.Fatal(err)
	}
	if f3.calls != 0 || len(r3.forwarded) != 1 {
		t.Errorf("no disk for a file: copy ran %d time(s), forwarded %v; want MySQL", f3.calls, r3.forwarded)
	}
}

// A result in a file that the copy cut (a cell at the cell cap) is refused
// like one in memory and goes to MySQL; the file is removed.
func TestRoute_cutResultFromAFileGoesToMySQL(t *testing.T) {
	base, rows := spoolCase()
	base.TruncatedCells = 1
	f := &fakeFreeSQL{res: base, spoolRows: rows}
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	pw := &capturePW{}
	h := spoolRouting(t, f, r, pw)
	res, err := h.HandleQuery(spoolStmt)
	if err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 {
		t.Fatalf("the copy ran %d time(s), want once", f.calls)
	}
	if len(r.forwarded) != 1 || firstCell(t, res) != "mysql" {
		t.Errorf("a cut result: forwarded %v, answer %+v; want MySQL's", r.forwarded, res)
	}
	gone(t, f.spools)
	if h.spooled != nil {
		t.Error("a refused answer stayed pending")
	}
}

// Once rows are on the wire a failure is the client's error in the middle
// of the resultset, never a forward to MySQL (a second answer on the same
// connection); the file is removed either way.
func TestRoute_failureWhileSendingAFileIsNotForwarded(t *testing.T) {
	base, rows := spoolCase()
	names := []string{"id", "s", "d", "at"}
	f := &fakeFreeSQL{res: base, spoolRows: rows}
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	pw := &capturePW{failAt: headerPackets(names) + 2} // the second row
	h := spoolRouting(t, f, r, pw)
	res, err := h.HandleQuery(spoolStmt)
	if err == nil || res != nil {
		t.Fatalf("a failed send answered (%v, %v)", res, err)
	}
	wantMyError(t, err, mysql.ER_UNKNOWN_ERROR, "after 1 of its 3 rows")
	if f.calls != 1 || len(r.forwarded) != 0 {
		t.Errorf("copy ran %d time(s), forwarded %v after rows were sent", f.calls, r.forwarded)
	}
	gone(t, f.spools)
}

// A connection closed with an answer still pending removes its file.
func TestHandlerClose_removesAPendingFile(t *testing.T) {
	base, rows := spoolCase()
	f := &fakeFreeSQL{res: base, spoolRows: rows, spoolDir: t.TempDir()}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	h.BindConn(&capturePW{})
	res, err := h.runFreeSQLRouted("", "SELECT 1", nil, false, true, "expensive", 0)
	if err != nil || res == nil || h.spooled == nil {
		t.Fatalf("(%v, %v), pending %v", res, err, h.spooled)
	}
	h.Close()
	gone(t, f.spools)
	if h.spooled != nil {
		t.Error("Close left the answer pending")
	}
}

// A prepared statement's answer goes back in the binary encoding, buffered:
// it never asks the copy for a file, and keeps the row cap.
func TestRoute_preparedStatementAsksForNoFile(t *testing.T) {
	base, rows := spoolCase()
	f := &fakeFreeSQL{res: base, spoolRows: rows}
	f.res.Rows = rows
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	h := spoolRouting(t, f, r, &capturePW{})
	_, _, ctx, err := h.HandleStmtPrepare("SELECT id, s, amount, created FROM t WHERE id > ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(0)}); err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 {
		t.Fatalf("the prepared statement reached the copy %d time(s)", f.calls)
	}
	if f.gotSess.ResultToFile || len(f.spools) != 0 {
		t.Errorf("a prepared statement asked for a file (%v, %d made)", f.gotSess.ResultToFile, len(f.spools))
	}
}
