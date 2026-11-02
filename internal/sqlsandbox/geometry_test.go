package sqlsandbox

import (
	"context"
	"testing"
)

// GEOMETRY is built into the engine since DuckDB 1.5 and arrives as its WKB
// bytes. POINT(0 0) is all 0x00 and 0x01, valid UTF-8, so without the type
// check the cell read as a string of control characters with no error. It
// is hex, like any other binary value, through the engine and directly.
func TestRun_geometryIsHex(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), f.job(`SELECT 'POINT(0 0)'::GEOMETRY AS g`))
	if err != nil {
		t.Fatal(err)
	}
	const want = "0x010100000000000000000000000000000000000000"
	if res.Columns[0].Type != "GEOMETRY" || res.Rows[0][0] != want {
		t.Errorf("g = %s %#v, want GEOMETRY %q", res.Columns[0].Type, res.Rows[0][0], want)
	}

	var rd renderer
	if got := rd.cell([]byte("text"), "GEOMETRY"); got != "0x74657874" {
		t.Errorf("GEOMETRY cell of valid UTF-8 = %#v, want hex", got)
	}
	if got := rd.cell([]byte("text"), "BLOB"); got != "text" {
		t.Errorf("BLOB cell of valid UTF-8 = %#v, want the text", got)
	}
}
