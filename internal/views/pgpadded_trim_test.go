package views

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func paddedTable() BaselineTable {
	rel := &baseline.PGRelation{Columns: []baseline.PGColumn{
		{Name: "id", TypeOID: 23, TypeMod: -1},
		{Name: "code", TypeOID: 1042, TypeMod: 9},
		{Name: "free", TypeOID: 1042, TypeMod: -1},
		{Name: "word", TypeOID: 25, TypeMod: -1},
	}}
	return BaselineTable{Schema: "public", Table: "t", Path: "/x/public.t.parquet", PGRelation: rel, PGCasts: baseline.PGCasts(*rel)}
}

func TestTrimPadded_onlyWhenAskedAndOnlyCharN(t *testing.T) {
	plain := paddedTable()
	if got := replaceClause(plain); strings.Contains(got, "rtrim") {
		t.Errorf("trimmed without being asked: %s", got)
	}
	tables := []BaselineTable{plain}
	trimmed := WithPaddingTrimmed(tables)
	if tables[0].TrimPadded {
		t.Error("the list it was given was changed")
	}
	got := replaceClause(trimmed[0])
	if !strings.Contains(got, `rtrim("code", ' ') COLLATE C AS "code"`) {
		t.Errorf("code not trimmed: %s", got)
	}
	for _, c := range []string{"free", "word"} {
		if strings.Contains(got, `rtrim("`+c+`"`) {
			t.Errorf("%s trimmed: %s", c, got)
		}
	}
}
