package sqlsandbox

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// The port's USE names a schema; the worker applies it only when the views
// created a schema of that name, and otherwise leaves the default, so a wrong
// name costs a hint and not every statement on the connection.
//
// "Of that name" is how DuckDB resolves names: ASCII case folded, nothing
// else. The copy's session collation equates far more than that ('ß' with
// 'ss', full-width with ASCII, accents, a zero-width space), so a probe that
// compared names in SQL answered "exists" for a name DuckDB then refuses, and
// SET search_path failed every statement on the connection with a session
// error (#2083).
func TestRun_useOfANameTheCollationEquatesIsNotApplied(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	for _, schema := range []string{"ｓｈｏｐ", "shóp", "sh​op", "SHOP", "shop"} {
		job := f.job("SELECT count(*) FROM shop.orders")
		job.Schema = schema
		res, err := r.Run(context.Background(), job)
		if err != nil {
			t.Errorf("USE %q: %v", schema, err)
			continue
		}
		if len(res.Rows) != 1 {
			t.Errorf("USE %q: %d rows", schema, len(res.Rows))
		}
	}
	// The real name, in another case, IS applied: unqualified names resolve.
	job := f.job("SELECT count(*) FROM orders")
	job.Schema = "SHOP"
	if _, err := r.Run(context.Background(), job); err != nil {
		t.Errorf("USE SHOP then an unqualified name: %v", err)
	}
	// A name that only the collation equates is not: the default stays, and
	// the unqualified name fails with the hint, not with a session error.
	job.Schema = "ｓｈｏｐ"
	_, err := r.Run(context.Background(), job)
	if err == nil {
		t.Fatal("an unqualified name resolved under a schema that does not exist")
	}
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Errorf("USE of a full-width name: got %T %v, want the query error with the hint", err, err)
	}
}

// The probe and SET search_path must never disagree. DuckDB resolves a schema
// name folding ASCII case and nothing else, so with a schema named "Été",
// USE été and USE ÉTÉ name no schema; a probe that lower-cased both sides
// said they did (lower() folds all of Unicode), and SET search_path then
// failed every statement on the connection.
func TestRun_useFoldsASCIICaseOnly(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	views := `CREATE SCHEMA "Été"; CREATE VIEW "Été".t AS SELECT 1 AS x; CREATE SCHEMA "İX"; CREATE VIEW "İX".t AS SELECT 2 AS x;`
	run := func(schema, sqlText string) (Result, error) {
		job := f.job(sqlText)
		job.ViewsSQL = views
		job.Schema = schema
		return r.Run(context.Background(), job)
	}
	// Applied: the exact name, and a name that differs in ASCII case only.
	for schema, want := range map[string]string{"Été": "1", "İX": "2", "İx": "2"} {
		res, err := run(schema, "SELECT x FROM t")
		if err != nil {
			t.Errorf("USE %q then an unqualified name: %v", schema, err)
			continue
		}
		if got := fmt.Sprint(res.Rows[0][0]); got != want {
			t.Errorf("USE %q: x = %s, want %s", schema, got, want)
		}
	}
	// Not applied: every statement still runs, and the unqualified name fails
	// as a query error with the hint, never as a session error.
	for _, schema := range []string{"été", "ÉTÉ", "éTé", "ete", "ix", "i̇x"} {
		if _, err := run(schema, "SELECT 1"); err != nil {
			t.Errorf("USE %q: a statement that needs no schema failed: %v", schema, err)
		}
		_, err := run(schema, "SELECT x FROM t")
		var qe *QueryError
		if !errors.As(err, &qe) {
			t.Errorf("USE %q then an unqualified name: got %T %v, want the query error with the hint", schema, err, err)
		}
	}
}
