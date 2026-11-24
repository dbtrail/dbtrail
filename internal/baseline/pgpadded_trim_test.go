package baseline

import (
	"reflect"
	"testing"
)

func TestPGPaddedToTrim(t *testing.T) {
	if got := PGPaddedToTrim(nil); got != nil {
		t.Errorf("no relation: %v", got)
	}
	rel := &PGRelation{Columns: []PGColumn{
		{Name: "code", TypeOID: pgOIDBpchar, TypeMod: 9},
		{Name: "free", TypeOID: pgOIDBpchar, TypeMod: -1},
		{Name: "word", TypeOID: pgOIDText, TypeMod: -1},
		{Name: "v", TypeOID: pgOIDVarchar, TypeMod: 9},
		{Name: "list", TypeOID: 1014, TypeMod: 9},
		{Name: "dom", TypeOID: 90001, TypeMod: 9},
		{Name: "Twin", TypeOID: pgOIDBpchar, TypeMod: 9},
		{Name: "twin", TypeOID: pgOIDText, TypeMod: -1},
		{Name: "Name", TypeOID: pgOIDBpchar, TypeMod: 9},
	}}
	want := map[string]bool{"code": true, "Name": true}
	if got := PGPaddedToTrim(rel); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
