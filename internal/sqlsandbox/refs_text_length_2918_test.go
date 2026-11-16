package sqlsandbox

import (
	"reflect"
	"testing"
)

// #2918: the length functions return a number, which cannot end with a space
// their argument held inside, so the column they count is not one a call can
// cut (TextUse.Wrapped). BIT_LENGTH reached the copy as written and was read
// as such a call: under a PAD SPACE collation every inner space then kept
// the statement from the copy.
func TestCollectRefs_lengthFunctionsDoNotCutText_2918(t *testing.T) {
	c := func(n string) ColumnRef { return ColumnRef{Name: n} }
	for _, tc := range []struct {
		sql  string
		want TextUse
	}{
		{"SELECT bit_length(a), strlen(a), char_length(a), character_length(a), length(a) FROM t", TextUse{}},
		// A comparison still puts the counted column in order, as before.
		{"SELECT sum(bit_length(a)) FROM t WHERE strlen(b) > 2", TextUse{Ordered: []ColumnRef{c("b")}}},
		{"SELECT left(a, 2) FROM t", TextUse{Wrapped: []ColumnRef{c("a")}}},
	} {
		got := parseForRefs(t, tc.sql).Text
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s\n  got  %+v\n  want %+v", tc.sql, got, tc.want)
		}
	}
}
