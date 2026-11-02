package sqlsandbox

import (
	"context"
	"reflect"
	"testing"
)

// GEOMETRY is built into the engine since DuckDB 1.5 and arrives as its WKB
// bytes. POINT(0 0) is all 0x00 and 0x01, valid UTF-8, so without the type
// check the cell read as a string of control characters with no error. It
// is always hex, unlike a BLOB, which prints as text when its bytes are
// valid UTF-8; checked through the engine and directly.
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

	// Inside a LIST, a STRUCT and a MAP the element is a GEOMETRY too, and
	// the column type is what names it (GEOMETRY[], STRUCT("a" GEOMETRY),
	// MAP(VARCHAR, GEOMETRY)).
	res, err = r.Run(context.Background(), f.job(`SELECT
		['POINT(0 0)'::GEOMETRY] AS l,
		{'a': 'POINT(0 0)'::GEOMETRY} AS s,
		MAP {'k': 'POINT(0 0)'::GEOMETRY} AS m`))
	if err != nil {
		t.Fatal(err)
	}
	wantNested := []any{[]any{want}, map[string]any{"a": want}, map[string]any{"k": want}}
	for i, w := range wantNested {
		if !reflect.DeepEqual(res.Rows[0][i], w) {
			t.Errorf("%s (%s) = %#v, want %#v", res.Columns[i].Name, res.Columns[i].Type, res.Rows[0][i], w)
		}
	}

	var rd renderer
	if got := rd.cell([]byte("text"), "GEOMETRY"); got != "0x74657874" {
		t.Errorf("GEOMETRY cell of valid UTF-8 = %#v, want hex", got)
	}
	if got := rd.cell([]byte("text"), "BLOB"); got != "text" {
		t.Errorf("BLOB cell of valid UTF-8 = %#v, want the text", got)
	}
}
