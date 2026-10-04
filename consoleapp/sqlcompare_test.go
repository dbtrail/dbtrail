package consoleapp

import (
	"strings"
	"testing"
)

// The command refuses to start without both sides and exactly one statement
// source, naming what is missing; nothing is dialled before that.
func TestSQLCompare_flagValidation(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"sql-compare", "--copy-dsn", "x@tcp(h)/"}, "--source-dsn is required"},
		{[]string{"sql-compare", "--source-dsn", "u:p@tcp(h)/d"}, "--copy-dsn is required"},
		{[]string{"sql-compare", "--source-dsn", "u:p@tcp(h)/d", "--copy-dsn", "x@tcp(h)/"}, "exactly one of --statements"},
		{[]string{"sql-compare", "--source-dsn", "u:p@tcp(h)/d", "--copy-dsn", "x@tcp(h)/", "--sample", "5", "--statements", "f"}, "exactly one of --statements"},
		{[]string{"sql-compare", "--source-dsn", "u:p@tcp(h)/d", "--copy-dsn", "x@tcp(h)/", "--sample", "5", "--format", "yaml"}, "invalid format"},
		{[]string{"sql-compare", "--source-dsn", "u:p@tcp(h)/d", "--copy-dsn", "x@tcp(h)/", "--statements", "/nonexistent/stmts.sql"}, "no such file"},
	}
	for _, tc := range cases {
		t.Setenv("BINTRAIL_SOURCE_DSN", "")
		scSourceDSN, scCopyDSN, scStatements, scSample, scFormat = "", "", "", 0, "text"
		rootCmd.SetArgs(tc.args)
		err := rootCmd.Execute()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: err = %v, want %q", tc.args, err, tc.want)
		}
	}
}
