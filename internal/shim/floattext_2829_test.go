package shim

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// What MySQL 8.0.46 sent for each value, measured with a raw client on the
// text protocol (#2829). A FLOAT is keyed by its four bytes and a DOUBLE by
// its eight, as the binary protocol sent them for the same rows.
var (
	measuredFloat = map[uint32]string{
		0x00000000: "0", 0x80000000: "-0", 0x3f800000: "1", 0xbf800000: "-1",
		0x4048f5c3: "3.14", 0x3dcccccd: "0.1", 0xbdcccccd: "-0.1",
		0x3eaaaaaa: "0.333333", 0x3eaaaaad: "0.333333", 0x3e99999a: "0.3",
		0x7e967699: "1e38", 0xff7fc99e: "-3.4e38", 0x7f7fffff: "3.40282e38",
		0x2edbe6ff: "0.0000000001", 0x006ce3ee: "1e-38", 0x00800000: "1.17549e-38", 0x00000001: "1.4013e-45",
		0x4ceb79a3: "123457000", 0x4b800000: "16777200", 0x47c35040: "100000", 0x47c350c0: "100002",
		0x497423f8: "1000000", 0x4b18967b: "10000000", 0x47c34fff: "100000", 0x40200000: "2.5",
		0x58635fa9: "1e15", 0x5a0e1bca: "1e16", 0x56b5e621: "100000000000000", 0x56e0910c: "123457000000000",
		0x588c5aa8: "1.23457e15", 0x5bdb4da6: "1.23457e17",
		0x38d1b717: "0.0001", 0x3727c5ac: "0.00001", 0x374f07e5: "0.00001234", 0x3901742e: "0.000123457",
		0x283424dc: "0.00000000000001", 0x26901d7d: "0.000000000000001", 0x24e69595: "1e-16",
		0x4996b438: "1234570", 0x4b3c614e: "12345700", 0x42f6e979: "123.456",
	}
	measuredDouble = map[uint64]string{
		0x0000000000000000: "0", 0x8000000000000000: "-0",
		0x40091eb851eb851f: "3.14", 0x3fb999999999999a: "0.1", 0xbfb999999999999a: "-0.1", 0x3fd55555318abc87: "0.3333333",
		0x3fd3333333333334: "0.30000000000000004",
		0x47d2ced32a16a1b1: "1e38", 0xc7eff933c78cdfad: "-3.4e38", 0x7e37e43c8800759c: "1e300", 0x81a56e1fc2f8f359: "-1e-300",
		0x0000000000000001: "5e-324", 0x7fefffffffffffff: "1.7976931348623157e308",
		0x3ddb7cdfd9d7bdbb: "0.0000000001", 0x3cd203af9ee75616: "0.000000000000001", 0x3c9cd2b297d889bc: "1e-16",
		0x42d6bcc41e900000: "100000000000000", 0x430c6bf526340000: "1e15", 0x42dc12218377de40: "123456789012345",
		0x43118b54f22aeb00: "1.234567890123456e15", 0x437b69b4ba630f35: "1.2345678901234568e17",
		// A FLOAT widened to a DOUBLE (f + 0 on MySQL).
		0x40091eb860000000: "3.140000104904175", 0x3ddb7cdfe0000000: "0.0000000001000000013351432",
		0x3d06849b80000000: "0.0000000000000099999998245167", 0x380b38fb80000000: "9.999999350456404e-39",
		0x47d2ced320000000: "9.999999680285692e37", 0x42dc122180000000: "123456788103168",
		0x4341c37940000000: "1.0000000272564224e16", 0xc7eff933c0000000: "-3.3999999521443642e38",
	}
)

func TestMySQLFloatText_asMeasured(t *testing.T) {
	for bits, want := range measuredFloat {
		f := math.Float32frombits(bits)
		if got := mysqlFloatText(f); got != want {
			t.Errorf("FLOAT %08x (%v): %q, MySQL sends %q", bits, f, got, want)
		}
	}
	for bits, want := range measuredDouble {
		d := math.Float64frombits(bits)
		if got := mysqlDoubleText(d); got != want {
			t.Errorf("DOUBLE %016x (%v): %q, MySQL sends %q", bits, d, got, want)
		}
	}
}

// The text a prepared statement's answer is encoded from reads back as the
// same bits, where the six digits MySQL prints would not.
func TestExactFloatText_readsBackTheSameBits(t *testing.T) {
	for bits := range measuredFloat {
		f := math.Float32frombits(bits)
		back, err := strconv.ParseFloat(exactFloatText(float64(f), true), 32)
		if err != nil || math.Float32bits(float32(back)) != bits {
			t.Errorf("FLOAT %08x: %q reads back as %08x (%v)", bits, exactFloatText(float64(f), true), math.Float32bits(float32(back)), err)
		}
	}
	for bits := range measuredDouble {
		d := math.Float64frombits(bits)
		back, err := strconv.ParseFloat(exactFloatText(d, false), 64)
		if err != nil || math.Float64bits(back) != bits {
			t.Errorf("DOUBLE %016x: %q reads back as %016x (%v)", bits, exactFloatText(d, false), math.Float64bits(back), err)
		}
	}
}

// floatNumber is a FLOAT cell as the worker sends it: the widened double.
func floatNumber(bits uint32) json.Number {
	return json.Number(strconv.FormatFloat(float64(math.Float32frombits(bits)), 'g', -1, 64))
}

func doubleNumber(bits uint64) json.Number {
	return json.Number(strconv.FormatFloat(math.Float64frombits(bits), 'g', -1, 64))
}

// floatPair is two FLOATs MySQL prints alike, 0.3333333 and 0.3333334, and a
// DOUBLE: what tells an answer encoded from the printed text from one
// encoded from the value.
func floatPair() sqlsandbox.Result {
	return sqlsandbox.Result{
		Columns: []sqlsandbox.Column{{Name: "f", Type: "FLOAT"}, {Name: "d", Type: "DOUBLE"}},
		Rows: [][]any{
			{floatNumber(0x3eaaaaaa), doubleNumber(0x40091eb851eb851f)},
			{floatNumber(0x3eaaaaad), doubleNumber(0x47d2ced32a16a1b1)},
			{nil, nil},
		},
	}
}

func TestFreeSQLResultset_floatTextAndHeader(t *testing.T) {
	rs, err := freeSQLResultset(floatPair(), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Fields[0].Type != mysql.MYSQL_TYPE_FLOAT || rs.Fields[1].Type != mysql.MYSQL_TYPE_DOUBLE {
		t.Errorf("types %d/%d, want FLOAT/DOUBLE", rs.Fields[0].Type, rs.Fields[1].Type)
	}
	for i, f := range rs.Fields {
		if f.Decimal != 31 {
			t.Errorf("column %d decimals %d, MySQL sends 31 for a FLOAT or DOUBLE without digits", i, f.Decimal)
		}
	}
	got := rawTextRows(t, rs)
	want := [][]string{{"0.333333", "3.14"}, {"0.333333", "1e38"}, {"<NULL>", "<NULL>"}}
	for i := range want {
		if strings.Join(got[i], "|") != strings.Join(want[i], "|") {
			t.Errorf("row %d: %q, MySQL sends %q", i, got[i], want[i])
		}
	}
}

// rawTextRows is every cell of a text resultset as the bytes the wire
// carries, "<NULL>" for NULL. Not textRows: that parses a FLOAT or DOUBLE
// cell into a number and prints it again, the very difference measured here.
func rawTextRows(t *testing.T, rs *mysql.Resultset) [][]string {
	t.Helper()
	var out [][]string
	for _, rd := range rs.RowDatas {
		var cells []string
		for pos := 0; pos < len(rd); {
			v, isNull, n, err := mysql.LengthEncodedString(rd[pos:])
			if err != nil {
				t.Fatal(err)
			}
			pos += n
			if isNull {
				cells = append(cells, "<NULL>")
			} else {
				cells = append(cells, string(v))
			}
		}
		out = append(out, cells)
	}
	return out
}

// binaryFloatRows reads a binary resultset whose columns are all FLOAT or
// DOUBLE: each cell its bits in hex, or NULL.
func binaryFloatRows(t *testing.T, fields []*mysql.Field, rows []mysql.RowData) [][]string {
	t.Helper()
	nf := len(fields)
	bitmap := (nf + 7 + 2) >> 3
	var out [][]string
	for r, data := range rows {
		pos := 1 + bitmap
		var cells []string
		for c, f := range fields {
			if data[1+(c+2)/8]&(1<<(uint(c+2)%8)) != 0 {
				cells = append(cells, "NULL")
				continue
			}
			switch f.Type {
			case mysql.MYSQL_TYPE_FLOAT:
				cells = append(cells, strconv.FormatUint(uint64(binary.LittleEndian.Uint32(data[pos:])), 16))
				pos += 4
			case mysql.MYSQL_TYPE_DOUBLE:
				cells = append(cells, strconv.FormatUint(binary.LittleEndian.Uint64(data[pos:]), 16))
				pos += 8
			default:
				t.Fatalf("row %d column %d is type %d, want FLOAT or DOUBLE", r, c, f.Type)
			}
		}
		out = append(out, cells)
	}
	return out
}

// What MySQL's binary protocol sent for floatPair's rows.
const floatPairBits = "3eaaaaaa|40091eb851eb851f / 3eaaaaad|47d2ced32a16a1b1 / NULL|NULL"

func joinRows(rows [][]string) string {
	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = strings.Join(r, "|")
	}
	return strings.Join(parts, " / ")
}

func TestFreeSQLResultset_binaryKeepsTheExactValue(t *testing.T) {
	rs, err := freeSQLResultset(floatPair(), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	bin, err := binaryResultset(rs)
	if err != nil {
		t.Fatal(err)
	}
	if got := joinRows(binaryFloatRows(t, bin.Fields, bin.RowDatas)); got != floatPairBits {
		t.Errorf("binary rows %s, MySQL sends %s", got, floatPairBits)
	}
}

// Every way a prepared statement's answer leaves the copy sends the exact
// bits: the port without routing (the statement run as text, then
// re-encoded), and under routing a result in memory and one from a file.
func TestPrepared_floatBitsOnEveryPath(t *testing.T) {
	const stmt = "SELECT f, d FROM fl WHERE id > ?"
	check := func(name string, fields []*mysql.Field, rows []mysql.RowData) {
		t.Helper()
		if got := joinRows(binaryFloatRows(t, fields, rows)); got != floatPairBits {
			t.Errorf("%s: binary rows %s, MySQL sends %s", name, got, floatPairBits)
		}
	}
	execute := func(h *Handler) *mysql.Result {
		t.Helper()
		_, _, ctx, err := h.HandleStmtPrepare(stmt)
		if err != nil {
			t.Fatal(err)
		}
		res, err := h.HandleStmtExecute(ctx, "", []any{int64(0)})
		if err != nil || res == nil || res.Resultset == nil {
			t.Fatalf("(%v, %v)", res, err)
		}
		return res
	}

	plain := &fakeFreeSQL{res: floatPair()}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(plain)
	res := execute(h)
	check("without routing", res.Resultset.Fields, res.Resultset.RowDatas)

	mem := &fakeFreeSQL{res: floatPair()}
	res = execute(spoolRouting(t, mem, &fakeRouter{toCopy: true, reason: "expensive"}, nil))
	check("routed, in memory", res.Resultset.Fields, res.Resultset.RowDatas)

	pair := floatPair()
	file := &fakeFreeSQL{res: sqlsandbox.Result{Columns: pair.Columns}, spoolRows: pair.Rows}
	pw := &capturePW{}
	res = execute(spoolRouting(t, file, &fakeRouter{toCopy: true, reason: "expensive"}, pw))
	if len(file.spools) != 1 || !res.Resultset.StreamingDone {
		t.Fatalf("not sent from a file (%d files, streamed %v)", len(file.spools), res.Resultset.StreamingDone)
	}
	cols := len(res.Resultset.Fields)
	var rows []mysql.RowData
	for _, p := range pw.packets[cols+2:] {
		if len(p) > 0 && p[0] == 0x00 {
			rows = append(rows, p)
		}
	}
	check("routed, from a file", res.Resultset.Fields, rows)
}

// The text protocol prints MySQL's text on the port without routing, and the
// same from a file under routing.
func TestFloatText_onTheWire(t *testing.T) {
	want := "0.333333|3.14 / 0.333333|1e38 / <NULL>|<NULL>"
	plain := &fakeFreeSQL{res: floatPair()}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(plain)
	res, err := h.HandleQuery("SELECT f, d FROM fl")
	if err != nil {
		t.Fatal(err)
	}
	if got := joinRows(rawTextRows(t, res.Resultset)); got != want {
		t.Errorf("without routing: %s, MySQL sends %s", got, want)
	}

	pair := floatPair()
	file := &fakeFreeSQL{res: sqlsandbox.Result{Columns: pair.Columns}, spoolRows: pair.Rows}
	pw := &capturePW{}
	hr := spoolRouting(t, file, &fakeRouter{toCopy: true, reason: "expensive"}, pw)
	if _, err := hr.HandleQuery("SELECT f, d FROM fl"); err != nil {
		t.Fatal(err)
	}
	if len(file.spools) != 1 {
		t.Fatalf("%d files, want the answer from one", len(file.spools))
	}
	var got []string
	for _, p := range pw.packets[2+2:] {
		if len(p) == 0 || p[0] == 0xfe {
			continue
		}
		var cells []string
		for pos := 0; pos < len(p); {
			v, isNull, n, err := mysql.LengthEncodedString(p[pos:])
			if err != nil {
				t.Fatal(err)
			}
			pos += n
			if isNull {
				cells = append(cells, "<NULL>")
			} else {
				cells = append(cells, string(v))
			}
		}
		got = append(got, strings.Join(cells, "|"))
	}
	if strings.Join(got, " / ") != want {
		t.Errorf("routed, from a file: %s, MySQL sends %s", strings.Join(got, " / "), want)
	}
}

// NaN and the infinities, which MySQL never sends: the copy's answer is
// refused before anything is written, without routing as an error, under
// routing sent to MySQL. Both from memory and from a file.
func TestFloatNotFinite_refused(t *testing.T) {
	for _, cell := range []any{"NaN", "Infinity", "-Infinity", math.Inf(1), json.Number("3.5e38")} {
		res := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "f", Type: "FLOAT"}}, Rows: [][]any{{json.Number("1")}, {cell}}}
		if _, err := freeSQLResultset(res, nil, false); !errors.Is(err, errNotFinite) || !strings.Contains(err.Error(), "row 2") {
			t.Errorf("%v: err %v, want errNotFinite naming row 2", cell, err)
		}
	}
	bad := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "d", Type: "DOUBLE"}}, Rows: [][]any{{"-Infinity"}}}

	plain := &fakeFreeSQL{res: bad}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(plain)
	_, err := h.HandleQuery("SELECT d FROM fl")
	wantMyError(t, err, mysql.ER_DATA_OUT_OF_RANGE, "does not send this result")

	for _, fromFile := range []bool{false, true} {
		f := &fakeFreeSQL{res: bad}
		if fromFile {
			f = &fakeFreeSQL{res: sqlsandbox.Result{Columns: bad.Columns}, spoolRows: bad.Rows}
		}
		r := &fakeRouter{toCopy: true, reason: "expensive"}
		pw := &capturePW{}
		hr := spoolRouting(t, f, r, pw)
		if _, err := hr.HandleQuery("SELECT d FROM fl"); err != nil {
			t.Fatalf("file %v: %v", fromFile, err)
		}
		if f.calls != 1 || len(r.forwarded) != 1 {
			t.Errorf("file %v: copy ran %d time(s), forwarded %v; want it sent to MySQL", fromFile, f.calls, r.forwarded)
		}
		gone(t, f.spools)
	}
}

// A FLOAT or DOUBLE inside a LIST is JSON, as before: only a column of the
// type itself is printed as MySQL prints one.
func TestFloatType_onlyTheTypeItself(t *testing.T) {
	for typ, want := range map[string]bool{"FLOAT": true, "REAL": true, "DOUBLE": true, " double ": true,
		"FLOAT[]": false, "DOUBLE[2]": false, "STRUCT(a DOUBLE)": false, "DECIMAL(10,2)": false, "INTEGER": false, "": false} {
		if _, ok := floatType(typ); ok != want {
			t.Errorf("floatType(%q) = %v, want %v", typ, ok, want)
		}
	}
}

// A column the copy prints without the decimals MySQL gives it (#2829): on
// the port without routing the answer goes out with a warning, 1105 (the
// port's own code), that SHOW WARNINGS reads back; beside the older-copy
// note both are there.
func TestFreeSQL_printNoteIsAWarning(t *testing.T) {
	const note = "shop.m.d102: declared with fixed decimals or ZEROFILL, which the copy does not print"
	for _, c := range []struct {
		name  string
		older string
		count uint16
	}{{"alone", "", 1}, {"beside the older-copy note", "Answered from the copy of ...", 2}} {
		res := oneCell("d102", "DOUBLE", json.Number("0.1"))
		res.PrintNote = note
		res.Note = c.older
		h := NewHandler(nil, nil)
		h.BindFreeSQL(&fakeFreeSQL{res: res})
		conn := &warningsConn{}
		h.BindConn(conn)
		got, err := h.HandleQuery("SELECT d102 FROM m")
		if err != nil {
			t.Fatal(err)
		}
		if cells := joinRows(rawTextRows(t, got.Resultset)); cells != "0.1" {
			t.Errorf("%s: answer %q, want the copy's 0.1", c.name, cells)
		}
		if conn.warnings != c.count {
			t.Errorf("%s: connection warnings = %d, want %d", c.name, conn.warnings, c.count)
		}
		w, err := h.HandleQuery("SHOW WARNINGS")
		if err != nil {
			t.Fatal(err)
		}
		rows := rawTextRows(t, w.Resultset)
		last := rows[len(rows)-1]
		if len(rows) != int(c.count) || last[0] != "Warning" || last[1] != "1105" || last[2] != note {
			t.Errorf("%s: SHOW WARNINGS rows = %v", c.name, rows)
		}
	}
}
