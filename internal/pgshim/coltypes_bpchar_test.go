package pgshim

import "testing"

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
