//go:build integration

package readrouter

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2828 against a real MySQL: a forwarded text-protocol row carries the
// bytes MySQL sent for every cell. The expected texts were read with the
// mysql client from MySQL 8.0.46 (--raw -B); before the fix the forwarder
// handed on Go's reprint of a parsed float64 (1e38 as 1 and 38 zeros, a
// FLOAT(7,2) 0.10 as 0.1) and of a parsed int64 (an INT ZEROFILL 00042 as
// 42). The prepared form answers in the binary protocol, where the values
// are MySQL's own bits.
func TestIntegrationForwarder_floatTextIsMySQLs_2828(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db := fmt.Sprintf("bt_fl2828_%d", time.Now().UnixNano()%1_000_000)
	root, err := sql.Open("mysql", testutil.BaseDSN()+"/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for _, q := range []string{
		"CREATE DATABASE " + db,
		"CREATE TABLE " + db + ".fl (id INT PRIMARY KEY, f FLOAT, f72 FLOAT(7,2), fu FLOAT UNSIGNED, d DOUBLE, d103 DOUBLE(10,3), z INT(5) ZEROFILL, dz DOUBLE(8,2) ZEROFILL)",
		"INSERT INTO " + db + ".fl VALUES (1,1e38,0.1,1e-38,-3.4e38,1.5,42,3.1),(2,-0.0,1.00,0,-0.0,-0.0,0,-0.0)," +
			"(3,NULL,NULL,NULL,1e38,NULL,NULL,NULL),(4,3.14159,12345.67,1.17549e-38,0.1,123.456,7,0.5)," +
			"(5,-1.5e-7,-0.05,3.40282e38,1.7976931348623157e308,0,99999,12345.678),(6,123456789,99999.99,0.000001,2.2250738585072014e-308,1e6,1,1)",
	} {
		if _, err := root.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = root.Exec("DROP DATABASE " + db) })

	fw, err := NewForwarder(testutil.BaseDSN()+"/"+db, config.SSL{Mode: "preferred"}, DefaultPolicy(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()
	ctx := context.Background()

	const null = "NULL"
	check := func(stmt string, want [][]string) {
		t.Helper()
		var buf BufferSink
		if _, err := fw.Forward(ctx, stmt, &buf); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		if len(buf.Rows) != len(want) {
			t.Fatalf("%s: %d rows, want %d", stmt, len(buf.Rows), len(want))
		}
		for i, row := range want {
			for c, w := range row {
				got := buf.Rows[i][c]
				if w == null {
					if got != nil {
						t.Errorf("%s: row %d, column %d = %#v, want NULL", stmt, i+1, c+1, got)
					}
					continue
				}
				if b, ok := got.([]byte); !ok || string(b) != w {
					t.Errorf("%s: row %d, column %s = %#v, want %q", stmt, i+1, buf.Fields[c].Name, got, w)
				}
			}
		}
	}
	check("SELECT f, f72, fu, d, d103, z, dz FROM fl ORDER BY id", [][]string{
		{"1e38", "0.10", "1e-38", "-3.4e38", "1.500", "00042", "00003.10"},
		{"0", "1.00", "0", "0", "0.000", "00000", "00000.00"},
		{null, null, null, "1e38", null, null, null},
		{"3.14159", "12345.67", "1.17549e-38", "0.1", "123.456", "00007", "00000.50"},
		{"-0.00000015", "-0.05", "3.40282e38", "1.7976931348623157e308", "0.000", "99999", "12345.68"},
		{"123457000", "99999.99", "0.000001", "2.2250738585072014e-308", "1000000.000", "00001", "00001.00"},
	})
	// Expressions: a DOUBLE result, a FLOAT computed as a double, a fixed
	// scale carried through +0, and a negative zero, which MySQL prints.
	check("SELECT d, d*1, f*1, f72+0, -d FROM fl WHERE id IN (1,2,3) ORDER BY id", [][]string{
		{"-3.4e38", "-3.4e38", "9.999999680285692e37", "0.10", "3.4e38"},
		{"0", "0", "0", "1.00", "-0"},
		{"1e38", "1e38", null, null, "-1e38"},
	})
	check("SELECT -0e0, 1/3e0, 100000000000000000.0e0, 123456789012345678e0", [][]string{
		{"-0", "0.3333333333333333", "1e17", "1.2345678901234568e17"},
	})

	// The column definitions handed on are MySQL's, not the string-typed
	// copies the rows were read under.
	var buf BufferSink
	if _, err := fw.Forward(ctx, "SELECT f, f72, d, z FROM fl WHERE id = 1", &buf); err != nil {
		t.Fatal(err)
	}
	for i, want := range []byte{mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_DOUBLE, mysql.MYSQL_TYPE_LONG} {
		if buf.Fields[i].Type != want {
			t.Errorf("column %s: type %d, want %d", buf.Fields[i].Name, buf.Fields[i].Type, want)
		}
	}
	if buf.Fields[1].Decimals != 2 || buf.Fields[3].Flag&mysql.ZEROFILL_FLAG == 0 {
		t.Errorf("FLOAT(7,2) decimals %d, ZEROFILL flag %v: the definitions were not MySQL's", buf.Fields[1].Decimals, buf.Fields[3].Flag&mysql.ZEROFILL_FLAG != 0)
	}

	// The prepared form: binary rows, MySQL's own bits.
	st, err := fw.Prepare(ctx, "SELECT f, d, z FROM fl WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var pbuf BufferSink
	if _, err := st.Execute(ctx, []any{1}, &pbuf); err != nil {
		t.Fatal(err)
	}
	if len(pbuf.Rows) != 1 {
		t.Fatalf("prepared: %d rows, want 1", len(pbuf.Rows))
	}
	f, fok := pbuf.Rows[0][0].(float64)
	d, dok := pbuf.Rows[0][1].(float64)
	if !fok || math.Float32bits(float32(f)) != math.Float32bits(float32(1e38)) || !dok || math.Float64bits(d) != math.Float64bits(-3.4e38) {
		t.Errorf("prepared: f %#v, d %#v, want MySQL's float32 1e38 and float64 -3.4e38", pbuf.Rows[0][0], pbuf.Rows[0][1])
	}
	if z := fmt.Sprint(pbuf.Rows[0][2]); z != "42" {
		t.Errorf("prepared: z = %s, want 42 (the client applies ZEROFILL from the column's flag)", z)
	}
}
