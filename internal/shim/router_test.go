package shim

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// fakeRouter records what the handler asked of the source and answers with
// canned verdicts; forwarded statements come back as a one-cell "mysql" row
// so a test can tell which side answered.
type fakeRouter struct {
	toCopy    bool
	reason    string
	decideErr error
	inTxn     bool
	useDBErr  error
	forwarded []string
	explained []string
	useDBs    []string
	closed    int
}

func (r *fakeRouter) Decide(_ context.Context, stmt string) (bool, string, error) {
	r.explained = append(r.explained, stmt)
	return r.toCopy, r.reason, r.decideErr
}

func (r *fakeRouter) Forward(_ context.Context, stmt string, _ readrouter.RowSink) (*mysql.Result, error) {
	r.forwarded = append(r.forwarded, stmt)
	rs, err := mysql.BuildSimpleTextResultset([]string{"side"}, [][]any{{"mysql"}})
	if err != nil {
		return nil, err
	}
	return &mysql.Result{Status: mysql.SERVER_STATUS_AUTOCOMMIT, Resultset: rs}, nil
}

func (r *fakeRouter) UseDB(_ context.Context, db string) error {
	r.useDBs = append(r.useDBs, db)
	return r.useDBErr
}
func (r *fakeRouter) InTransaction() bool { return r.inTxn }
func (r *fakeRouter) Close()              { r.closed++ }

func routingHandler(t *testing.T, r *fakeRouter, f *fakeFreeSQL, maxAge time.Duration) *Handler {
	t.Helper()
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	h.BindRouter(r, RouterConfig{MaxCopyAge: maxAge})
	return h
}

func firstCell(t *testing.T, res *mysql.Result) string {
	t.Helper()
	if res == nil || res.Resultset == nil {
		t.Fatal("no resultset")
	}
	return textRows(t, res.Resultset)[0][0]
}

// The ladder, one rung per case: everything that is not an expensive SELECT
// on a fresh copy is MySQL's.
func TestRouter_forwardsEverythingButExpensiveSelects(t *testing.T) {
	fresh := time.Now().Add(-10 * time.Second)
	cases := []struct {
		name       string
		stmt       string
		router     fakeRouter
		copyAge    time.Time
		maxAge     time.Duration
		wantSide   string
		wantNoEXPL bool // the plan was never asked for (freshness is checked AFTER the plan)
	}{
		{"not a select", "SHOW TABLES", fakeRouter{toCopy: true}, fresh, time.Minute, "mysql", true},
		{"a write", "UPDATE t SET a = 1", fakeRouter{toCopy: true}, fresh, time.Minute, "mysql", true},
		{"handshake noise", "SELECT @@version_comment LIMIT 1", fakeRouter{toCopy: true}, fresh, time.Minute, "mysql", true},
		{"vetoed", "SELECT NOW()", fakeRouter{toCopy: true}, fresh, time.Minute, "mysql", true},
		{"in transaction", "SELECT count(*) FROM t", fakeRouter{toCopy: true, inTxn: true}, fresh, time.Minute, "mysql", true},
		{"copy age unknown", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, time.Time{}, time.Minute, "mysql", false},
		{"copy too old", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, time.Now().Add(-time.Hour), time.Minute, "mysql", false},
		{"routing off", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, fresh, 0, "mysql", true},
		{"cheap plan", "SELECT * FROM t WHERE id = 1", fakeRouter{toCopy: false, reason: "cheap"}, fresh, time.Minute, "mysql", false},
		{"explain failed", "SELECT * FROM t WHERE id = 1", fakeRouter{decideErr: errors.New("boom")}, fresh, time.Minute, "mysql", false},
		{"expensive plan", "SELECT status, count(*) FROM t GROUP BY status", fakeRouter{toCopy: true, reason: "plan cost 20146 >= 10000"}, fresh, time.Minute, "copy", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.router
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: tc.copyAge}
			h := routingHandler(t, &r, f, tc.maxAge)
			res, err := h.HandleQuery(tc.stmt)
			if err != nil {
				t.Fatalf("HandleQuery: %v", err)
			}
			if got := firstCell(t, res); got != tc.wantSide {
				t.Errorf("answered by %s, want %s", got, tc.wantSide)
			}
			if tc.wantNoEXPL && len(r.explained) != 0 {
				t.Errorf("EXPLAIN ran (%v) on a statement decided before the plan", r.explained)
			}
			if tc.wantSide == "mysql" && (len(r.forwarded) != 1 || r.forwarded[0] != tc.stmt) {
				t.Errorf("forwarded = %v, want the statement verbatim", r.forwarded)
			}
			if tc.wantSide == "copy" && f.calls != 1 {
				t.Errorf("copy ran %d times, want 1", f.calls)
			}
		})
	}
}

// A SET stops this connection's routing for good: the copy does not honour
// session settings, and what the client set must apply to every later read.
func TestRouter_sessionSetPinsTheConnectionToMySQL(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	if res, _ := h.HandleQuery("SELECT status, count(*) FROM t GROUP BY status"); firstCell(t, res) != "copy" {
		t.Fatal("the expensive select did not go to the copy before the SET")
	}
	if _, err := h.HandleQuery("SET time_zone = '+00:00'"); err != nil {
		t.Fatal(err)
	}
	res, err := h.HandleQuery("SELECT status, count(*) FROM t GROUP BY status")
	if err != nil {
		t.Fatal(err)
	}
	if firstCell(t, res) != "mysql" {
		t.Error("after a SET the expensive select still went to the copy")
	}
	if want := []string{"SET time_zone = '+00:00'", "SELECT status, count(*) FROM t GROUP BY status"}; strings.Join(r.forwarded, "|") != strings.Join(want, "|") {
		t.Errorf("forwarded = %v, want %v", r.forwarded, want)
	}
}

// The copy refusing is a fallback, not an error: MySQL runs the statement and
// the client never sees the copy's error.
func TestRouter_copyRefusalFallsBackToMySQL(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{err: errors.New("Binder Error: function group_concat does not exist"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	res, err := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a")
	if err != nil {
		t.Fatalf("the copy's error reached the client: %v", err)
	}
	if firstCell(t, res) != "mysql" {
		t.Error("the refused statement was not forwarded")
	}
	if f.calls != 1 || len(r.forwarded) != 1 {
		t.Errorf("copy calls %d, forwarded %d; want 1 and 1", f.calls, len(r.forwarded))
	}
}

// The copy gets the statement exactly as the client wrote it, backticks
// included: nothing is translated on the way, and what the copy then refuses
// goes to MySQL. Both USE paths reach the router.
func TestRouter_sendsTheCopyTheClientsTextAndFollowsUSE(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	if err := h.UseDB("shop"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleQuery("USE `shop2`"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(r.useDBs, ",") != "shop,shop2" {
		t.Errorf("router saw USE %v, want shop then shop2", r.useDBs)
	}
	if _, err := h.HandleQuery("SELECT `status`, count(*) FROM `orders` GROUP BY `status`"); err != nil {
		t.Fatal(err)
	}
	if want := "SELECT `status`, count(*) FROM `orders` GROUP BY `status`"; f.gotStmt != want {
		t.Errorf("copy got %q, want the client's text %q", f.gotStmt, want)
	}
	if f.gotSchema != "shop2" {
		t.Errorf("copy schema = %q, want shop2", f.gotSchema)
	}
	if r.explained[0] != "SELECT `status`, count(*) FROM `orders` GROUP BY `status`" {
		t.Errorf("EXPLAIN did not get the client's text: %q", r.explained[0])
	}
	h.Close()
	if r.closed != 1 {
		t.Errorf("Close closed the router %d times, want 1", r.closed)
	}
	// Close on a handler with no router is a no-op.
	NewHandler(nil, nil).Close()
}

// Without a router nothing changes: the copy answers, as since #2025.
func TestRouter_unboundKeepsTheCopyPath(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("x", "INTEGER", json.Number("1"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	if _, err := h.HandleQuery("SELECT x FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if f.calls != 1 {
		t.Errorf("copy calls = %d, want 1", f.calls)
	}
}

// The copy cutting a result at the cap is a refusal under routing: MySQL
// answers whole, and the client never sees a warning-counted partial result.
func TestRouter_truncatedCopyResultFallsBackToMySQL(t *testing.T) {
	for name, res := range map[string]sqlsandbox.Result{
		"rows":  {Columns: oneCell("side", "VARCHAR", "copy").Columns, Rows: oneCell("side", "VARCHAR", "copy").Rows, Truncated: true},
		"cells": {Columns: oneCell("side", "VARCHAR", "copy").Columns, Rows: oneCell("side", "VARCHAR", "copy").Rows, TruncatedCells: 1},
	} {
		r := &fakeRouter{toCopy: true, reason: "expensive"}
		f := &fakeFreeSQL{res: res, updatedAt: time.Now()}
		h := routingHandler(t, r, f, time.Minute)
		out, err := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if firstCell(t, out) != "mysql" || len(r.forwarded) != 1 {
			t.Errorf("%s: cut copy result was served (forwarded %d)", name, len(r.forwarded))
		}
		if out.Warnings != 0 {
			t.Errorf("%s: the copy's truncation warning leaked onto MySQL's result", name)
		}
	}
}

// SHOW WARNINGS follows the side that answered the last statement: the
// copy's after a copy-served one (MySQL's would be the EXPLAIN's note), and
// MySQL's after a forwarded one.
func TestRouter_showWarningsFollowsTheLastSide(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	if _, err := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleQuery("SHOW WARNINGS"); err != nil {
		t.Fatal(err)
	}
	if len(r.forwarded) != 0 {
		t.Errorf("SHOW WARNINGS after a copy-served statement went to MySQL: %v", r.forwarded)
	}
	r.toCopy = false
	if _, err := h.HandleQuery("SELECT * FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleQuery("SHOW WARNINGS"); err != nil {
		t.Fatal(err)
	}
	if n := len(r.forwarded); n == 0 || r.forwarded[n-1] != "SHOW WARNINGS" {
		t.Errorf("SHOW WARNINGS after a forwarded statement stayed local: %v", r.forwarded)
	}
}

// A USE that MySQL refuses changes nothing: the copy and MySQL stay on the
// same schema.
func TestRouter_refusedUSEKeepsTheSchema(t *testing.T) {
	r := &fakeRouter{useDBErr: mysql.NewError(mysql.ER_BAD_DB_ERROR, "Unknown database 'nope'")}
	f := &fakeFreeSQL{res: oneCell("x", "INTEGER", json.Number("1"))}
	h := routingHandler(t, r, f, time.Minute)
	r.useDBErr = nil
	if err := h.UseDB("shop"); err != nil {
		t.Fatal(err)
	}
	r.useDBErr = mysql.NewError(mysql.ER_BAD_DB_ERROR, "Unknown database 'nope'")
	if err := h.UseDB("nope"); err == nil {
		t.Fatal("USE nope: MySQL's refusal did not reach the client")
	}
	h.mu.Lock()
	db := h.db
	h.mu.Unlock()
	if db != "shop" {
		t.Errorf("after a refused USE the schema is %q, want shop", db)
	}
}

// Connect-time settings a driver sends do not pin the connection; a SET
// inside MySQL's executable comment does, like a bare one.
func TestRouter_harmlessAndCommentedSets(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	for _, s := range []string{"SET NAMES utf8mb4", "SET autocommit=1"} {
		if _, err := h.HandleQuery(s); err != nil {
			t.Fatal(err)
		}
	}
	if res, _ := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a"); firstCell(t, res) != "copy" {
		t.Error("a harmless connect-time SET pinned the connection to MySQL")
	}
	if _, err := h.HandleQuery("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE */"); err != nil {
		t.Fatal(err)
	}
	if res, _ := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a"); firstCell(t, res) != "mysql" {
		t.Error("a SET inside an executable comment did not pin the connection")
	}
}

// Freshness is checked only for a statement the plan sends to the copy: a
// cheap read never pays the snapshot listing.
func TestRouter_freshnessOnlyAfterThePlan(t *testing.T) {
	r := &fakeRouter{toCopy: false, reason: "cheap"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := routingHandler(t, r, f, time.Minute)
	if _, err := h.HandleQuery("SELECT * FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if f.ageCalls != 0 {
		t.Errorf("CopyUpdatedAt called %d times for a cheap plan, want 0", f.ageCalls)
	}
	r.toCopy = true
	if _, err := h.HandleQuery("SELECT a, count(*) FROM t GROUP BY a"); err != nil {
		t.Fatal(err)
	}
	if f.ageCalls != 1 {
		t.Errorf("CopyUpdatedAt called %d times for an expensive plan, want 1", f.ageCalls)
	}
}
