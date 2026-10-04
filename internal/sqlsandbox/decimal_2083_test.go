package sqlsandbox

import (
	"context"
	"math/big"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

// A DECIMAL cell is printed with exactly its scale, trailing zeros kept, as
// MySQL prints one (#2083). The driver's own Decimal.String trims them: 10.00
// came out as "10", and a program comparing the returned text saw a different
// answer.
func TestCell_decimalKeepsItsScale(t *testing.T) {
	big38, _ := new(big.Int).SetString("12345678901234567890123456789012345678", 10)
	cases := []struct {
		name  string
		value *big.Int
		scale uint8
		want  string
	}{
		{"trailing zeros", big.NewInt(1000), 2, "10.00"},
		{"one trailing zero", big.NewInt(250), 2, "2.50"},
		{"negative", big.NewInt(-710), 2, "-7.10"},
		{"zero", big.NewInt(0), 2, "0.00"},
		{"zero, scale 0", big.NewInt(0), 0, "0"},
		{"scale 0", big.NewInt(42), 0, "42"},
		{"negative, scale 0", big.NewInt(-42), 0, "-42"},
		{"below one", big.NewInt(5), 2, "0.05"},
		{"negative below one", big.NewInt(-5), 2, "-0.05"},
		{"exactly the scale's digits", big.NewInt(50), 2, "0.50"},
		{"scale wider than the digits", big.NewInt(1), 10, "0.0000000001"},
		{"whole number ending in zeros", big.NewInt(11732955000), 2, "117329550.00"},
		{"38 digits, scale 0", big38, 0, "12345678901234567890123456789012345678"},
		{"38 digits, scale 10", big38, 10, "1234567890123456789012345678.9012345678"},
		{"38 digits, scale 38", big38, 38, "0.12345678901234567890123456789012345678"},
		{"negative 38 digits, scale 38", new(big.Int).Neg(big38), 38, "-0.12345678901234567890123456789012345678"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var r renderer
			got := r.cell(duckdb.Decimal{Width: 38, Scale: c.scale, Value: c.value}, "DECIMAL")
			if got != c.want {
				t.Errorf("cell = %#v, want %q", got, c.want)
			}
		})
	}
	// Inside a LIST the element is a Decimal too.
	var r renderer
	got := r.cell([]any{duckdb.Decimal{Width: 10, Scale: 2, Value: big.NewInt(1000)}}, "DECIMAL(10,2)[]")
	if l, ok := got.([]any); !ok || len(l) != 1 || l[0] != "10.00" {
		t.Errorf("list cell = %#v, want [10.00]", got)
	}
}

// The same through the real engine: every expression whose DuckDB type is
// DECIMAL(p,s) prints s decimals, whatever built it.
func TestRun_decimalsPrintTheirScale(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), f.job(`
		WITH t(id, amount, qty) AS (VALUES
			(1, 10.00::DECIMAL(12,2), 3), (2, 2.50::DECIMAL(12,2), 4), (3, -7.10::DECIMAL(12,2), 0),
			(4, 117329550.00::DECIMAL(12,2), 9), (5, NULL, NULL))
		SELECT
			round(sum(amount), 2) AS round_sum,
			sum(amount) AS total,
			max(amount) AS top,
			min(amount) AS low,
			sum(amount) FILTER (WHERE id > 100) AS none,
			sum(CASE WHEN id = 1 THEN amount ELSE qty END) AS mixed,
			sum(coalesce(amount, 0)) AS filled,
			max(round(amount, 1)) AS one_decimal,
			max(amount * qty) AS times,
			max(CAST(qty AS DECIMAL(10,3))) AS cast3,
			max(amount::DECIMAL(38,10)) AS wide,
			sum(0::DECIMAL(10,2)) AS zero,
			max(amount::DECIMAL(12,0)) AS whole
		FROM t`))
	if err != nil {
		t.Fatal(err)
	}
	want := []struct{ typ, text string }{
		{"DECIMAL(38,2)", "117329555.40"},
		{"DECIMAL(38,2)", "117329555.40"},
		{"DECIMAL(12,2)", "117329550.00"},
		{"DECIMAL(12,2)", "-7.10"},
		{"DECIMAL(38,2)", ""},
		{"DECIMAL(38,2)", "23.00"},
		{"DECIMAL(38,2)", "117329555.40"},
		{"DECIMAL(12,1)", "117329550.0"},
		{"", "1055965950.00"},
		{"DECIMAL(10,3)", "9.000"},
		{"DECIMAL(38,10)", "117329550.0000000000"},
		{"DECIMAL(38,2)", "0.00"},
		{"DECIMAL(12,0)", "117329550"},
	}
	row := res.Rows[0]
	for i, w := range want {
		if w.typ != "" && res.Columns[i].Type != w.typ {
			t.Errorf("%s type = %s, want %s", res.Columns[i].Name, res.Columns[i].Type, w.typ)
		}
		if w.text == "" {
			if row[i] != nil {
				t.Errorf("%s = %#v, want NULL", res.Columns[i].Name, row[i])
			}
			continue
		}
		if row[i] != w.text {
			t.Errorf("%s = %#v, want %q", res.Columns[i].Name, row[i], w.text)
		}
	}
	// A set operation widens to the larger scale, and a result with no rows
	// still reports the type.
	res, err = r.Run(context.Background(), f.job(
		`SELECT 10.00::DECIMAL(12,2) AS x UNION ALL SELECT 4 UNION ALL SELECT 2.25::DECIMAL(38,10) ORDER BY x`))
	if err != nil {
		t.Fatal(err)
	}
	for i, w := range []string{"2.2500000000", "4.0000000000", "10.0000000000"} {
		if res.Rows[i][0] != w {
			t.Errorf("union row %d = %#v, want %q", i, res.Rows[i][0], w)
		}
	}
	res, err = r.Run(context.Background(), f.job(
		`SELECT id, sum(amount) AS s FROM (VALUES (1, 1.50::DECIMAL(12,2))) t(id, amount) WHERE id > 100 GROUP BY id`))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 0 || res.Columns[1].Type != "DECIMAL(38,2)" {
		t.Errorf("empty GROUP BY: %d rows, type %s; want 0 rows and DECIMAL(38,2)", len(res.Rows), res.Columns[1].Type)
	}
}
