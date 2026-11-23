package pgcapture

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// The blocker ends up on a screen and in the log, so it has no field for free
// text the application chose (a statement, a prepared transaction's name). The
// only strings are the user and database of a session of the captured database.
func TestSlotBlockerHoldsNoFreeText(t *testing.T) {
	typ := reflect.TypeFor[slotBlocker]()
	var got []string
	for i := range typ.NumField() {
		if typ.Field(i).Type.Kind() == reflect.String {
			got = append(got, typ.Field(i).Name)
		}
	}
	if !slices.Equal(got, []string{"user", "database"}) {
		t.Fatalf("string fields of slotBlocker: %q; want only user and database", got)
	}
}

func TestSlotBlockerDescribe(t *testing.T) {
	cases := []struct {
		name string
		b    slotBlocker
		want []string
	}{
		{"a session", slotBlocker{pid: 4242, user: "app", database: "shop", age: 125 * time.Second},
			[]string{"session 4242", "user app", "database shop", "for 2m5s"}},
		{"age hidden from this role", slotBlocker{pid: 7, age: -1},
			[]string{"session 7", "cannot see for how long"}},
		{"prepared transaction", slotBlocker{prepared: true, age: 5 * time.Minute},
			[]string{"a prepared transaction", "for 5m0s", "pg_prepared_xacts"}},
		{"another database", slotBlocker{pid: 11, otherDatabase: true, age: time.Minute},
			[]string{"session 11, in another database", "for 1m0s"}},
		{"no user or database visible", slotBlocker{pid: 9, age: 0},
			[]string{"session 9", "for 0s"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.b.describe()
			t.Log(got)
			for _, w := range c.want {
				if !strings.Contains(got, w) {
					t.Errorf("%q lacks %q", got, w)
				}
			}
			if strings.Contains(got, "()") || strings.Contains(got, "user ,") {
				t.Errorf("empty field in %q", got)
			}
		})
	}
}
