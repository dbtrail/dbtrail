package readrouter

import (
	"bytes"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// textRow is one text-protocol row as MySQL sends it: a length-encoded
// string per cell, 0xfb for NULL.
func textRow(cells ...*string) []byte {
	var b []byte
	for _, c := range cells {
		if c == nil {
			b = append(b, 0xfb)
			continue
		}
		b = append(b, mysql.PutLengthEncodedString([]byte(*c))...)
	}
	return b
}

func sent2828(s string) *string { return &s }

// fakeTextRead stands in for go-mysql's ExecuteSelectStreaming: it sets the
// result's fields, calls perRes, then parses each raw row under res.Fields
// as they stand at that moment (what readResultRowsStreaming does) and
// hands it to perRow.
func fakeTextRead(fields []*mysql.Field, rows [][]byte, binary bool) func(*mysql.Result, client.SelectPerRowCallback, client.SelectPerResultCallback) error {
	return func(res *mysql.Result, perRow client.SelectPerRowCallback, perRes client.SelectPerResultCallback) error {
		res.Resultset = mysql.NewResultset(len(fields))
		res.Fields = fields
		if err := perRes(res); err != nil {
			return err
		}
		var dst []mysql.FieldValue
		for _, r := range rows {
			var err error
			if dst, err = mysql.RowData(r).Parse(res.Fields, binary, dst); err != nil {
				return err
			}
			if err := perRow(dst); err != nil {
				return err
			}
		}
		res.StreamingDone = true
		return nil
	}
}

// #2828: a forwarded text row reaches the sink as the bytes MySQL sent, for
// every column type. go-mysql parses a FLOAT or DOUBLE cell into a float64
// and an integer into an int64, and printing those again gave Go's form:
// 1e38 as 1 and 38 zeros, a FLOAT(7,2) 0.10 as 0.1, an INT ZEROFILL 00042 as
// 42. The texts below are what MySQL 8.0.46 sent for these columns.
func TestForwarderStream_textCellsAreTheSourceBytes_2828(t *testing.T) {
	f, err := NewForwarder("u:p@tcp(127.0.0.1:1)/db", config.SSL{Mode: "disabled"}, DefaultPolicy(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fields := []*mysql.Field{
		{Name: []byte("f"), Type: mysql.MYSQL_TYPE_FLOAT, Decimal: 31},
		{Name: []byte("f72"), Type: mysql.MYSQL_TYPE_FLOAT, Decimal: 2},
		{Name: []byte("fu"), Type: mysql.MYSQL_TYPE_FLOAT, Flag: mysql.UNSIGNED_FLAG, Decimal: 31},
		{Name: []byte("d"), Type: mysql.MYSQL_TYPE_DOUBLE, Decimal: 31},
		{Name: []byte("z"), Type: mysql.MYSQL_TYPE_LONG, Flag: mysql.UNSIGNED_FLAG | mysql.ZEROFILL_FLAG},
		{Name: []byte("big"), Type: mysql.MYSQL_TYPE_LONGLONG, Flag: mysql.UNSIGNED_FLAG},
		{Name: []byte("s"), Type: mysql.MYSQL_TYPE_VAR_STRING},
	}
	sent := [][]*string{
		{sent2828("1e38"), sent2828("0.10"), sent2828("1e-38"), sent2828("-3.4e38"), sent2828("00042"), sent2828("18446744073709551615"), sent2828("a")},
		{sent2828("-0"), sent2828("1.00"), sent2828("0"), sent2828("1.7976931348623157e308"), sent2828("00000"), sent2828("0"), sent2828("")},
		{nil, nil, nil, sent2828("-0.00000015"), nil, nil, nil},
		{sent2828("123457000"), sent2828("-0.05"), sent2828("3.40282e38"), sent2828("2.2250738585072014e-308"), sent2828("99999"), sent2828("1"), sent2828("b")},
	}
	raw := make([][]byte, len(sent))
	for i, r := range sent {
		raw[i] = textRow(r...)
	}
	var sink BufferSink
	res, err := f.stream(&sink, true, fakeTextRead(fields, raw, false))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range sent {
		for c, w := range want {
			got := sink.Rows[i][c]
			switch {
			case w == nil:
				if got != nil {
					t.Errorf("row %d, %s: %#v, want NULL", i+1, fields[c].Name, got)
				}
			default:
				b, ok := got.([]byte)
				if !ok || !bytes.Equal(b, []byte(*w)) || b == nil {
					t.Errorf("row %d, %s: %#v, want the bytes %q", i+1, fields[c].Name, got, *w)
				}
			}
		}
	}
	// The sink and the returned result carry the source's own definitions,
	// not the string-typed copies the rows were parsed under.
	for c, fld := range fields {
		if sink.Fields[c] != fld || res.Fields[c] != fld {
			t.Errorf("column %s: the definition handed on is not the source's", fld.Name)
		}
	}
	if fields[0].Type != mysql.MYSQL_TYPE_FLOAT || fields[3].Type != mysql.MYSQL_TYPE_DOUBLE || fields[4].Type != mysql.MYSQL_TYPE_LONG {
		t.Errorf("the source's definitions were changed: %d %d %d", fields[0].Type, fields[3].Type, fields[4].Type)
	}
}

// A prepared statement's rows are binary: they are decoded under the
// source's own types and re-encoded under them, so nothing is reparsed from
// text and the fields are left as they are.
func TestForwarderStream_binaryKeepsTypedCells_2828(t *testing.T) {
	f, err := NewForwarder("u:p@tcp(127.0.0.1:1)/db", config.SSL{Mode: "disabled"}, DefaultPolicy(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fields := []*mysql.Field{{Name: []byte("d"), Type: mysql.MYSQL_TYPE_DOUBLE}}
	enc, err := mysql.FormatBinaryValue(1e38)
	if err != nil {
		t.Fatal(err)
	}
	row := append([]byte{mysql.OK_HEADER, 0x00}, enc...)
	var sink BufferSink
	res, err := f.stream(&sink, false, fakeTextRead(fields, [][]byte{row}, true))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := sink.Rows[0][0].(float64); !ok || got != 1e38 {
		t.Errorf("binary DOUBLE cell = %#v, want float64 1e38", sink.Rows[0][0])
	}
	if res.Fields[0] != fields[0] {
		t.Error("the binary path replaced the source's definition")
	}
}
