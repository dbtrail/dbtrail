package console

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The port has its own row cap: a result larger than the SQL page's 1,000
// rows comes back whole on the port, is still cut on the page, and the port
// reports a result past ITS cap as cut.
func TestSQLOnCopy_portRowCapIsNotThePagesCap(t *testing.T) {
	f := newSQLFixture(t, sandboxRunner{sqlsandbox.New(sqlsandbox.Config{})}, false)
	ctx := context.Background()
	q := &SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}

	if got := q.RowCap(); got != DefaultSQLPortMaxRows {
		t.Fatalf("RowCap with none configured = %d, want %d", got, DefaultSQLPortMaxRows)
	}
	res, err := q.Run(ctx, "SELECT * FROM range(1500)", "", sqlsandbox.Session{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1500 || res.Truncated {
		t.Errorf("port: %d rows, truncated=%v; want all 1500", len(res.Rows), res.Truncated)
	}
	// sql_select_limit above the page's cap and under the port's is the
	// client's own cut, not an error.
	res, err = q.Run(ctx, "SELECT * FROM range(5000)", "", sqlsandbox.Session{SelectLimit: 1200})
	if err != nil || len(res.Rows) != 1200 || res.Truncated {
		t.Errorf("port under sql_select_limit 1200: %d rows, truncated=%v, err=%v; want 1200 rows, not truncated", len(res.Rows), res.Truncated, err)
	}

	out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT * FROM range(1500)", "", 0, sqlsandbox.Session{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Result.Rows) != 1000 || !out.Result.Truncated {
		t.Errorf("page: %d rows, truncated=%v; want it cut at 1000", len(out.Result.Rows), out.Result.Truncated)
	}

	f.s.sqlPortMaxRows = 40
	if got := q.RowCap(); got != 40 {
		t.Fatalf("RowCap = %d, want the configured 40", got)
	}
	res, err = q.Run(ctx, "SELECT * FROM range(41)", "", sqlsandbox.Session{})
	if err != nil || len(res.Rows) != 40 || !res.Truncated {
		t.Errorf("port at cap 40: %d rows, truncated=%v, err=%v; want 40 rows reported cut", len(res.Rows), res.Truncated, err)
	}
	if res, err = q.Run(ctx, "SELECT * FROM range(40)", "", sqlsandbox.Session{}); err != nil || len(res.Rows) != 40 || res.Truncated {
		t.Errorf("port at exactly the cap: %d rows, truncated=%v, err=%v; want it whole", len(res.Rows), res.Truncated, err)
	}
}

// New carries Config.SQLPortMaxRows to the port, and leaves the page's cap alone.
func TestNew_sqlPortMaxRows(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", SQLPortMaxRows: 77})
	if err != nil {
		t.Fatal(err)
	}
	if got := (&SQLOnCopy{s: srv}).RowCap(); got != 77 {
		t.Errorf("RowCap = %d, want 77", got)
	}
	if srv.sqlLimits.MaxRows != sqlsandbox.DefaultLimits().MaxRows {
		t.Errorf("page cap = %d, want the default", srv.sqlLimits.MaxRows)
	}
}
