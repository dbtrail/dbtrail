package pgshim

import (
	"encoding/binary"
	"testing"
)

// A statement described by the source as char(n) and answered by the copy
// keeps its description: its value is sent as that type.
func TestConformToDescribed_bpchar(t *testing.T) {
	described := []colType{{oid: oidBpchar}}
	got, perr := conformToDescribed(described, []colType{{oid: oidVarchar}}, [][][]byte{{[]byte("ab")}})
	if perr != nil {
		t.Fatalf("refused: %+v", perr)
	}
	if got[0].oid != oidBpchar {
		t.Errorf("announced %d", got[0].oid)
	}
	cell, err := binaryCell(colType{oid: oidBpchar}, []byte("ab"))
	if err != nil || string(cell) != "ab" {
		t.Errorf("binary cell %q, %v", cell, err)
	}
}

// The same for an array of char(n) or of varchar: the source describes it
// as 1014 or 1015, which the copy never announces.
func TestConformToDescribed_bpcharArray(t *testing.T) {
	for _, oid := range []uint32{1014, 1015} {
		described := []colType{{oid: oid}}
		got, perr := conformToDescribed(described, []colType{{oid: 1009}}, [][][]byte{{[]byte(`{ab,"a b",NULL}`)}})
		if perr != nil {
			t.Fatalf("%d refused: %+v", oid, perr)
		}
		if got[0].oid != oid {
			t.Errorf("%d announced %d", oid, got[0].oid)
		}
		text, _ := binaryCell(colType{oid: 1009}, []byte(`{ab,"a b",NULL}`))
		cell, err := binaryCell(colType{oid: oid}, []byte(`{ab,"a b",NULL}`))
		if err != nil || len(cell) != len(text) {
			t.Fatalf("%d binary cell %x, %v", oid, cell, err)
		}
		// The array's header names its element type: the one described.
		if elem := binary.BigEndian.Uint32(cell[8:12]); elem != arrayElement[oid] {
			t.Errorf("%d: element type %d in the header", oid, elem)
		}
	}
}
