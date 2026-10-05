package sqlsandbox

import (
	"context"
	"encoding/json"
	"testing"
)

// Division and modulo by zero are NULL on the copy, as on MySQL (#2083).
// DuckDB's default answers +Infinity, -Infinity or NaN, with no error, so
// `amount / qty` returned a value on the copy for the rows where MySQL
// returns NULL, and COUNT, SUM, AVG and WHERE over it answered differently.
func TestRun_divisionByZeroIsNull(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), f.job(`SELECT
		1 / 0 AS int_div,
		0 / 0 AS zero_div,
		-7.10 / 0 AS dec_div,
		1.5e0 / 0 AS dbl_div,
		-1e0 / 0 AS neg_dbl_div,
		1 / 0.0 AS by_dec_zero,
		5 % 0 AS int_mod,
		5.5 % 0 AS dec_mod,
		1e0 % 0 AS dbl_mod,
		1 // 0 AS int_floor_div,
		(SELECT count(x / y) FROM (VALUES (1, 0), (4, 2), (NULL, 1)) t(x, y)) AS counted,
		(SELECT sum(x / y) FROM (VALUES (1, 0), (4, 2)) t(x, y)) AS summed,
		(SELECT avg(x / y) FROM (VALUES (1, 0), (4, 2)) t(x, y)) AS averaged,
		(SELECT count(*) FROM (VALUES (1, 0), (4, 2)) t(x, y) WHERE x / y IS NULL) AS filtered,
		6 / 3 AS plain,
		7 % 4 AS plain_mod`))
	if err != nil {
		t.Fatal(err)
	}
	want := []any{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		json.Number("1"), json.Number("2"), json.Number("2"), json.Number("1"), json.Number("2"), json.Number("3")}
	for i, w := range want {
		if res.Rows[0][i] != w {
			t.Errorf("%s = %#v, want %#v", res.Columns[i].Name, res.Rows[0][i], w)
		}
	}
}
