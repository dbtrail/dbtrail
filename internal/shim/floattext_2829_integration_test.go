//go:build integration

package shim

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2829 against MySQL itself: for thousands of FLOAT and DOUBLE values, the
// text the copy prints (mysqlFloatText, mysqlDoubleText) is byte for byte
// the text MySQL's text protocol sends. Each value is the one MySQL stored,
// read back from its binary protocol (the exact bytes), so the comparison
// does not depend on how the value was written in.
func TestIntegrationFloatTextIsMySQLs(t *testing.T) {
	_, dbName := testutil.CreateTestDB(t)
	cfg, err := drivermysql.ParseDSN(testutil.IntegrationDSN(dbName))
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.Connect(cfg.Addr, cfg.User, cfg.Passwd, dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Execute("CREATE TABLE ft (id INT PRIMARY KEY, f FLOAT, d DOUBLE)"); err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(2829))
	var floats []float32
	var doubles []float64
	for len(floats) < 4000 {
		f := math.Float32frombits(rng.Uint32())
		if !math.IsNaN(float64(f)) && !math.IsInf(float64(f), 0) {
			floats = append(floats, f)
		}
	}
	for len(doubles) < 4000 {
		d := math.Float64frombits(rng.Uint64())
		if !math.IsNaN(d) && !math.IsInf(d, 0) {
			doubles = append(doubles, d)
		}
	}
	// Where the choice between the fixed form and the exponent turns:
	// around 1e-14 and 1e15, with one digit and with many.
	for e := -20; e <= 20; e++ {
		for _, m := range []float64{1, 1.5, 9.999999, 1.2345678901234567, 123456789, 0.5} {
			v := m * math.Pow(10, float64(e))
			floats = append(floats, float32(v), -float32(v))
			doubles = append(doubles, v, -v)
		}
	}
	doubles = append(doubles, 1234567890123456.8, 12345678901234567, 123456789012345.6, 0.1+0.2, 5e-324, math.MaxFloat64, -math.SmallestNonzeroFloat64)
	floats = append(floats, math.MaxFloat32, math.SmallestNonzeroFloat32, 16777217, 100000.5, 100001.5, 999999.5)

	n := max(len(floats), len(doubles))
	for start := 0; start < n; start += 500 {
		var vals []string
		for i := start; i < min(start+500, n); i++ {
			fs, ds := "NULL", "NULL"
			if i < len(floats) {
				// The widened double, every digit: exactly the float.
				fs = strconv.FormatFloat(float64(floats[i]), 'g', -1, 64)
			}
			if i < len(doubles) {
				ds = strconv.FormatFloat(doubles[i], 'g', -1, 64)
			}
			vals = append(vals, fmt.Sprintf("(%d, %s, %s)", i, fs, ds))
		}
		if _, err := c.Execute("INSERT INTO ft VALUES " + strings.Join(vals, ",")); err != nil {
			t.Fatal(err)
		}
	}

	const sel = "SELECT f, d FROM ft ORDER BY id"
	text, err := c.Execute(sel)
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.Prepare(sel)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	bin, err := st.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(text.RowDatas) != n || len(bin.Values) != n {
		t.Fatalf("%d text rows and %d binary rows, want %d", len(text.RowDatas), len(bin.Values), n)
	}
	compared, bad := 0, 0
	for r, row := range text.RowDatas {
		pos := 0
		for c := 0; c < 2; c++ {
			v, isNull, w, err := mysql.LengthEncodedString(row[pos:])
			if err != nil {
				t.Fatal(err)
			}
			pos += w
			fv := bin.Values[r][c]
			if isNull != (fv.Type == mysql.FieldValueTypeNull) {
				t.Fatalf("row %d column %d: NULL on one protocol only", r, c)
			}
			if isNull {
				continue
			}
			x := fv.AsFloat64()
			got := mysqlDoubleText(x)
			if c == 0 {
				got = mysqlFloatText(float32(x))
			}
			compared++
			if got != string(v) {
				bad++
				if bad <= 20 {
					t.Errorf("row %d %s %v (bits %x): the copy prints %q, MySQL sends %q", r, []string{"FLOAT", "DOUBLE"}[c], x, math.Float64bits(x), got, v)
				}
			}
		}
	}
	if bad > 0 {
		t.Errorf("%d of %d cells printed otherwise than MySQL", bad, compared)
	}
	t.Logf("%d cells compared with MySQL's text", compared)
}
