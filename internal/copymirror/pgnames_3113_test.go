package copymirror

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
)

// Every file name a PostgreSQL snapshot can hold has to be one the mirror
// takes: a single key it refuses discards the server's whole snapshot (#3113).
func TestPGStorableNamesAreSafeKeys_3113(t *testing.T) {
	names := []string{
		"orders", "Orders", " spaced ", ".", "..", "...", "..x", "x..", ".hidden", "a.b", "Ñandú", "日本語",
		"a\nb", "a∕b", "a／b", "_SUCCESS", "t.000000.pgnew", strings.Repeat("x", 63),
		"/", `\`, "a/b", `a\b`, "../x", `..\x`, "/etc/x", "a\x00b", "", "views.sql", "_MANIFEST",
	}
	for _, schema := range names {
		for _, table := range names {
			if pgbaseline.UnstorableTable(schema, table) != nil {
				continue
			}
			for _, suffix := range []string{".parquet", ".000000.pgdead", ".000000.pgnew"} {
				if rel := schema + "/" + table + suffix; !safeRel(rel) {
					t.Errorf("a snapshot can hold %q and the mirror refuses it", rel)
				}
			}
		}
	}
	for _, c := range [][2]string{{"public", `a\b`}, {`a\b`, "t"}, {"public", "a/b"}, {"..", "t"}} {
		if pgbaseline.UnstorableTable(c[0], c[1]) == nil {
			t.Errorf("%q.%q is accepted", c[0], c[1])
		}
	}
}
