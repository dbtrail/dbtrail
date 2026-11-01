package shim

import (
	"errors"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// #2565: the mysql client (8.1 and later) sends "select $$" on every
// connection to learn whether the server reads $$ as a dollar quote, and
// ignores the error it gets (client/mysql.cc, server_supports_dollar_quote):
// 1064 from a server that reads dollar quotes, 1054 (unknown column '$$') from
// MySQL 8.0. That error is the client's own probe, not a statement that
// failed for the user: the answer is counted, not as an error. The error
// still reaches the client unchanged. Anything else, a byte apart or another
// error, is still an error.
func TestRouterAnswered_clientProbeIsNotAFailure(t *testing.T) {
	parse := mysql.NewError(mysql.ER_PARSE_ERROR, "You have an error in your SQL syntax")
	unknown := mysql.NewError(mysql.ER_BAD_FIELD_ERROR, "Unknown column '$$' in 'field list'")
	for _, tc := range []struct {
		name, stmt string
		err        error
		want       string
	}{
		{"the probe, a dollar-quote server", "select $$", parse, "mysql/ok"},
		{"the probe, MySQL 8.0", "select $$", unknown, "mysql/ok"},
		{"upper case: typed by a person", "SELECT $$", parse, "mysql/error"},
		{"a trailing blank", "select $$ ", parse, "mysql/error"},
		{"a semicolon", "select $$;", parse, "mysql/error"},
		{"a newline", "select $$\n", parse, "mysql/error"},
		{"another error", "select $$", mysql.NewError(mysql.ER_NO_SUCH_TABLE, "no such table"), "mysql/error"},
		{"not a MySQL error", "select $$", errors.New("ERROR 1064: syntax"), "mysql/error"},
		{"another statement with a parse error", "select $", parse, "mysql/error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeRouter{toCopy: false, reason: "cheap", forwardErr: tc.err}
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
			var decided, answered []string
			h := answeredHandler(r, f, RouterConfig{MaxCopyAge: time.Minute}, &decided, &answered)
			_, err := h.HandleQuery(tc.stmt)
			if err == nil {
				t.Fatal("the error did not reach the client")
			}
			var me *mysql.MyError
			if want := (*mysql.MyError)(nil); errors.As(tc.err, &want) && (!errors.As(err, &me) || me.Code != want.Code) {
				t.Errorf("the client got %v, want MySQL's own error %v", err, tc.err)
			}
			if len(answered) != 1 || answered[0] != tc.want {
				t.Errorf("answered %v, want [%s] (decided %v)", answered, tc.want, decided)
			}
		})
	}
}
