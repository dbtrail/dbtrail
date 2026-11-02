package sqlsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// A views script that ends with DeclineSQL (#2533): the check runs over the
// views the statement reads, in the same worker and session, before the
// statement. When it holds, the statement does not run and the copy declines
// it as a value it would not answer exactly (DeclinedValue), not as a fault;
// when it does not, the statement answers as without it.
func TestDeclineSQL_endOfViews(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	run := func(cond string) (Result, error) {
		t.Helper()
		job := f.job("SELECT id, status FROM shop.orders ORDER BY id")
		job.ViewsSQL += DeclineSQL(cond, "shop.orders.status holds text the copy compares another way than MySQL")
		return r.Run(context.Background(), job)
	}

	res, err := run(`EXISTS (SELECT 1 FROM "shop"."orders" WHERE regexp_matches("status", '[^ -~]| $'))`)
	if err != nil {
		t.Fatalf("a check that does not hold: %v", err)
	}
	if want := [][]any{{json.Number("1"), "new"}, {json.Number("2"), "paid"}}; !reflect.DeepEqual(res.Rows, want) {
		t.Errorf("rows = %#v, want %#v", res.Rows, want)
	}

	_, err = run(`EXISTS (SELECT 1 FROM "shop"."orders" WHERE "status" = 'paid')`)
	var q *QueryError
	if !errors.As(err, &q) || !DeclinedValue(err) {
		t.Fatalf("a check that holds: %v (%T), want a QueryError the copy declined", err, err)
	}
	if !strings.Contains(q.Message, "shop.orders.status holds text") {
		t.Errorf("message %q does not say what was found", q.Message)
	}

	// A check that cannot run is the views' failure, as any other line of
	// the script: not a decline, which would hide it behind a route to MySQL.
	_, err = run(`EXISTS (SELECT 1 FROM "shop"."orders" WHERE regexp_matches("no_such", 'x'))`)
	if err == nil || DeclinedValue(err) {
		t.Errorf("a check over a column the view lacks: %v, want a failure that is not a decline", err)
	}
}

// What a statement or a value writes into the check is a literal of its own:
// a quote in what is found does not end the error's text.
func TestDeclineSQL_quotes(t *testing.T) {
	got := DeclineSQL("true", "o'brien.name")
	if !strings.Contains(got, "'o''brien.name ") || !strings.HasSuffix(got, ";\n") {
		t.Errorf("DeclineSQL = %q", got)
	}
}
