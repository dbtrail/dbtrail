package sqlsandbox

import (
	"context"
	"errors"
	"testing"
)

// The port's USE names a schema; the worker applies it only when the views
// created a schema of that name, and otherwise leaves the default, so a wrong
// name costs a hint and not every statement on the connection.
//
// "Of that name" is how DuckDB resolves names: ASCII case folded, nothing
// else. The probe runs in the copy's session, whose default collation equates
// far more than that ('ß' with 'ss', full-width with ASCII, accents, a
// zero-width space), so without a byte comparison it answered "exists" for a
// name DuckDB then refuses, and SET search_path failed every statement on the
// connection with a session error (#2083).
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
