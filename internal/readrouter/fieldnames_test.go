package readrouter

import (
	"reflect"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// FieldNames reads the names out of column definitions as a source sends
// them: an expression's text, a name with spaces, an empty name, and one in
// another alphabet come back as they are.
func TestFieldNames(t *testing.T) {
	want := []string{"COUNT(*)", "SUM(ol_amount)", "id", "a b", "", "ñandú", "1+1"}
	var defs [][]byte
	for _, n := range want {
		defs = append(defs, (&mysql.Field{Schema: []byte("shop"), Table: []byte("t"), OrgName: []byte("other"), Name: []byte(n), Type: mysql.MYSQL_TYPE_LONGLONG}).Dump())
	}
	got, err := FieldNames(defs)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("FieldNames = %q, %v; want %q", got, err, want)
	}
	if got, err := FieldNames(nil); err != nil || len(got) != 0 {
		t.Errorf("no definitions: %q, %v", got, err)
	}
	_, err = FieldNames([][]byte{defs[0], []byte("not a definition")})
	if err == nil || !strings.Contains(err.Error(), "column 2 of 2") {
		t.Errorf("a definition that cannot be read: %v; want an error that says which", err)
	}
}
