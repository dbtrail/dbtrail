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

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// connectFloatDB opens a go-mysql client on a fresh test database, so the
// text and the binary protocol of one SELECT can be read side by side.
func connectFloatDB(t *testing.T) (*client.Conn, string) {
	t.Helper()
	_, dbName := testutil.CreateTestDB(t)
	cfg, err := drivermysql.ParseDSN(testutil.IntegrationDSN(dbName))
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.Connect(cfg.Addr, cfg.User, cfg.Passwd, dbName)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, dbName
}

// columnMetas reads DATA_TYPE and COLUMN_TYPE of table's columns from
// information_schema, the two fields a schema snapshot stores.
func columnMetas(t *testing.T, c *client.Conn, dbName, table string) []metadata.ColumnMeta {
	t.Helper()
	res, err := c.Execute(`SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, dbName, table)
	if err != nil {
		t.Fatal(err)
	}
	var out []metadata.ColumnMeta
	for r := range res.Values {
		name, _ := res.GetString(r, 0)
		dt, _ := res.GetString(r, 1)
		ct, _ := res.GetString(r, 2)
		out = append(out, metadata.ColumnMeta{Name: name, DataType: dt, ColumnType: ct})
	}
	return out
}

// compareFloatText runs sel on both protocols and checks every non-NULL
// FLOAT or DOUBLE cell: floatPrintOf(the column's declaration).text(the
// exact value MySQL sends on the binary protocol) must be the bytes of its
// text protocol. It returns the number of cells compared.
func compareFloatText(t *testing.T, c *client.Conn, sel string, metas []metadata.ColumnMeta) int {
	t.Helper()
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
	if len(text.RowDatas) != len(bin.Values) {
		t.Fatalf("%d text rows, %d binary rows", len(text.RowDatas), len(bin.Values))
	}
	prints := make([]floatPrint, len(metas))
	isFloat := make([]bool, len(metas))
	for i, m := range metas {
		prints[i], isFloat[i] = floatPrintOf(m)
		if isFloat[i] && !prints[i].declared {
			t.Fatalf("column %s: COLUMN_TYPE %q not read as a declaration", m.Name, m.ColumnType)
		}
	}
	compared, bad := 0, 0
	for r, row := range text.RowDatas {
		pos := 0
		for col := range metas {
			v, isNull, w, err := mysql.LengthEncodedString(row[pos:])
			if err != nil {
				t.Fatal(err)
			}
			pos += w
			if isNull || !isFloat[col] {
				continue
			}
			x := bin.Values[r][col].AsFloat64()
			got := prints[col].text(x, false)
			compared++
			if got != string(v) {
				bad++
				if bad <= 25 {
					t.Errorf("row %d %s (%s) %v (bits %x): time travel prints %q, MySQL sends %q",
						r, metas[col].Name, metas[col].ColumnType, x, math.Float64bits(x), got, v)
				}
			}
		}
	}
	if bad > 0 {
		t.Errorf("%d of %d cells printed otherwise than MySQL", bad, compared)
	}
	return compared
}

// #2851 against MySQL itself: a FLOAT or DOUBLE column declared with digits
// (M,D), with ZEROFILL, or with neither prints on the time-travel port as
// MySQL's text protocol sends it, for each declaration a schema snapshot can
// record. Each value is read back from the binary protocol (the exact bytes
// MySQL holds), so the comparison does not depend on how it was written in.
func TestIntegrationTimeTravelFloatTextIsMySQLs(t *testing.T) {
	c, dbName := connectFloatDB(t)
	type colDef struct {
		name, decl string
		m, d       int // M and D, 0 and -1 when not declared
		unsigned   bool
	}
	defs := []colDef{
		{"f", "FLOAT", 0, -1, false},
		{"d", "DOUBLE", 0, -1, false},
		{"r", "REAL", 0, -1, false},
		{"f72", "FLOAT(7,2)", 7, 2, false},
		{"f2010", "FLOAT(20,10)", 20, 10, false},
		{"f50", "FLOAT(5,0)", 5, 0, false},
		{"f128", "FLOAT(12,8)", 12, 8, false},
		{"d102", "DOUBLE(10,2)", 10, 2, false},
		{"d3020", "DOUBLE(30,20)", 30, 20, false},
		{"d255", "DOUBLE(255,30)", 255, 30, false},
		{"r83", "REAL(8,3)", 8, 3, false},
		{"fz", "FLOAT ZEROFILL", 0, -1, true},
		{"dz", "DOUBLE ZEROFILL", 0, -1, true},
		{"d102z", "DOUBLE(10,2) ZEROFILL", 10, 2, true},
		{"f72uz", "FLOAT(7,2) UNSIGNED ZEROFILL", 7, 2, true},
		{"du", "DOUBLE UNSIGNED", 0, -1, true},
	}
	var ddl []string
	for _, d := range defs {
		ddl = append(ddl, d.name+" "+d.decl)
	}
	if _, err := c.Execute("CREATE TABLE fx (id INT PRIMARY KEY, " + strings.Join(ddl, ", ") + ")"); err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(2851))
	// values builds the literals one column is filled with.
	values := func(d colDef) []string {
		var out []string
		if d.d >= 0 {
			intDigits := d.m - d.d
			for range 300 {
				// A whole part of up to M-D digits and three more decimals
				// than D, which MySQL rounds on the way in.
				ip := ""
				if intDigits > 0 {
					n := 1 + rng.Intn(min(intDigits, 15))
					for i := range n {
						dig := rng.Intn(10)
						if i == 0 && n > 1 && dig == 0 {
							dig = 1
						}
						ip += strconv.Itoa(dig)
					}
				} else {
					ip = "0"
				}
				fp := ""
				for range d.d + 3 {
					fp += strconv.Itoa(rng.Intn(10))
				}
				lit := ip
				if fp != "" {
					lit += "." + fp
				}
				if !d.unsigned && rng.Intn(2) == 0 {
					lit = "-" + lit
				}
				out = append(out, lit)
			}
			// The largest value the column holds, the smallest step, and a tie.
			if intDigits > 0 && intDigits <= 15 {
				out = append(out, strings.Repeat("9", intDigits)+"."+strings.Repeat("9", d.d))
			}
			if d.d > 0 {
				out = append(out, "0."+strings.Repeat("0", d.d-1)+"1", "0."+strings.Repeat("0", d.d)+"5", "0.125", "1.005", "2.675")
			}
			out = append(out, "0", "1", "10")
			if !d.unsigned {
				out = append(out, "-0e0", "-1.5")
			}
			return out
		}
		single := strings.HasPrefix(d.decl, "FLOAT")
		for range 400 {
			var x float64
			if single {
				f := math.Float32frombits(rng.Uint32())
				if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
					continue
				}
				x = float64(f)
			} else {
				x = math.Float64frombits(rng.Uint64())
				if math.IsNaN(x) || math.IsInf(x, 0) {
					continue
				}
			}
			if d.unsigned {
				x = math.Abs(x)
			}
			out = append(out, strconv.FormatFloat(x, 'g', -1, 64))
		}
		out = append(out, "0", "1.5", "123", "0.1", "1e-10", "1e15", "1e16", "1e21", "123456789012345678", "3.14159265")
		if !d.unsigned {
			out = append(out, "-0e0", "-1.5", "-1e21")
		}
		return out
	}

	id := 0
	for _, d := range defs {
		vals := values(d)
		for start := 0; start < len(vals); start += 200 {
			var tuples []string
			for _, v := range vals[start:min(start+200, len(vals))] {
				id++
				tuples = append(tuples, fmt.Sprintf("(%d, %s)", id, v))
			}
			if _, err := c.Execute("INSERT INTO fx (id, " + d.name + ") VALUES " + strings.Join(tuples, ",")); err != nil {
				t.Fatalf("insert into %s (%s): %v", d.name, d.decl, err)
			}
		}
	}

	metas := columnMetas(t, c, dbName, "fx")
	for _, m := range metas {
		t.Logf("%s: DATA_TYPE %q COLUMN_TYPE %q", m.Name, m.DataType, m.ColumnType)
	}
	n := compareFloatText(t, c, "SELECT * FROM fx ORDER BY id", metas)
	t.Logf("%d cells compared with MySQL's text", n)
}

// #2851, a value from an earlier epoch of the table: after ALTER ... MODIFY
// changes a column between FLOAT and DOUBLE, MySQL holds the value converted
// (a FLOAT widened, a DOUBLE rounded to the nearest float) and prints it by
// the new type. The time-travel port reads an event's value at the type it
// had when captured (typeEventFloats) and prints it by the type at AS OF;
// this pins that the pair is what MySQL sends.
func TestIntegrationTimeTravelFloatTextAcrossAlter(t *testing.T) {
	c, dbName := connectFloatDB(t)
	if _, err := c.Execute("CREATE TABLE ea (id INT PRIMARY KEY, widened FLOAT, narrowed DOUBLE, unfixed DOUBLE(10,2))"); err != nil {
		t.Fatal(err)
	}
	type pair struct {
		lit string
		f   float32
		d   float64
		dd  float64
	}
	pairs := []pair{
		{"0.1", 0.1, 0.1, 0.1},
		{"0.3333333", 0.3333333, 0.3333333, 0.33},
		{"3.14159265", 3.14159265, 3.14159265, 3.14},
		{"123456.7", 123456.7, 123456.7, 123456.7},
		{"-2.5", -2.5, -2.5, -2.5},
	}
	for i, p := range pairs {
		if _, err := c.Execute(fmt.Sprintf("INSERT INTO ea VALUES (%d, %s, %s, %s)", i, p.lit, p.lit, p.lit)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Execute("ALTER TABLE ea MODIFY widened DOUBLE, MODIFY narrowed FLOAT, MODIFY unfixed DOUBLE"); err != nil {
		t.Fatal(err)
	}
	metas := columnMetas(t, c, dbName, "ea")
	res, err := c.Execute("SELECT widened, narrowed, unfixed FROM ea ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	asOf := map[string]floatPrint{}
	for _, m := range metas {
		if fp, ok := floatPrintOf(m); ok {
			asOf[m.Name] = fp
		}
	}
	for r, p := range pairs {
		// What the event captured before the ALTER holds, read as
		// typeEventFloats reads it: a float32 for the FLOAT column, a float64
		// for the DOUBLE ones.
		want := []struct {
			col string
			v   float64
		}{{"widened", float64(p.f)}, {"narrowed", p.d}, {"unfixed", p.dd}}
		for j, w := range want {
			mysqlText, _ := res.GetString(r, j)
			got := asOf[w.col].text(w.v, false)
			if got != mysqlText {
				t.Errorf("row %d %s after the ALTER: time travel prints %q, MySQL sends %q", r, w.col, got, mysqlText)
			}
		}
	}
}
