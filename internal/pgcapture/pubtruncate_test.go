package pgcapture

import (
	"strings"
	"testing"
)

// #3108: the statement that makes a publication publish TRUNCATE, for every
// shape a publication name can take. The name is quoted the way PostgreSQL
// quotes an identifier, so a name with capitals or a double quote is pasted
// as it is, and a control character cannot break the statement onto a
// second line.
func TestTruncateFixStatement(t *testing.T) {
	for _, c := range []struct{ name, want string }{
		{"dbtrail_pub", `ALTER PUBLICATION "dbtrail_pub" SET (publish = 'insert, update, delete, truncate');`},
		{"MyPub", `ALTER PUBLICATION "MyPub" SET (publish = 'insert, update, delete, truncate');`},
		{"my pub", `ALTER PUBLICATION "my pub" SET (publish = 'insert, update, delete, truncate');`},
		{`a"b`, `ALTER PUBLICATION "a""b" SET (publish = 'insert, update, delete, truncate');`},
		{"a\nb", `ALTER PUBLICATION "a?b" SET (publish = 'insert, update, delete, truncate');`},
	} {
		if got := TruncateFixStatement(c.name); got != c.want {
			t.Errorf("TruncateFixStatement(%q) = %s, want %s", c.name, got, c.want)
		}
	}
}

func TestTruncateNotPublishedProblem(t *testing.T) {
	got := TruncateNotPublishedProblem("MyPub")
	const want = `publication "MyPub" does not publish TRUNCATE, so a TRUNCATE on the source is not recorded: the rows it removed stay in the copy and come back in every rebuild of the table, with nothing to say they are gone`
	if got != want {
		t.Errorf("problem =\n%s\nwant\n%s", got, want)
	}
	if strings.Contains(TruncateNotPublishedProblem("a\nb"), "\n") {
		t.Errorf("a name with a newline breaks the line: %q", TruncateNotPublishedProblem("a\nb"))
	}
}
