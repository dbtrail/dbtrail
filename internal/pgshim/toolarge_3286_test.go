package pgshim

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A result the copy ran and the port refused for its size is told to the
// host as such (#3286), so /status can count them apart from other errors:
// rows past the row cap, or bytes past what one statement returns.
type tooLargeLogin struct {
	mu    sync.Mutex
	res   sqlsandbox.Result
	err   error
	notes []RouteNote
}

func (l *tooLargeLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	return &Binding{
		Run: func(context.Context, string, RunOptions) (sqlsandbox.Result, error) {
			l.mu.Lock()
			defer l.mu.Unlock()
			return l.res, l.err
		},
		Actor: "server:orders", Schema: "public",
		Routed: func(n RouteNote) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.notes = append(l.notes, n)
		},
	}, nil
}

func (l *tooLargeLogin) set(res sqlsandbox.Result, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.res, l.err = res, err
}

// note waits for the n-th note.
func (l *tooLargeLogin) note(t *testing.T, n int) RouteNote {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		if len(l.notes) > n {
			got := l.notes[n]
			l.mu.Unlock()
			return got
		}
		l.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("no note %d", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRouteNote_tooLarge(t *testing.T) {
	col := []sqlsandbox.Column{{Name: "a", Type: "INTEGER"}}
	l := &tooLargeLogin{}
	c := rawLogin(t, serveAddrCfg(t, Config{Login: l.login}))
	for i, tc := range []struct {
		name string
		res  sqlsandbox.Result
		err  error
		code string
		want string
	}{
		{"over the row cap", sqlsandbox.Result{Columns: col, Rows: [][]any{{json.Number("1")}}, Truncated: true}, nil, "54000", TooLargeRows},
		{"over the bytes", sqlsandbox.Result{}, sqlsandbox.ErrResultTooLarge, "54000", TooLargeBytes},
		{"answered", sqlsandbox.Result{Columns: col, Rows: [][]any{{json.Number("1")}}}, nil, "", ""},
		{"another error", sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Conversion Error: no"}, "22000", ""},
	} {
		l.set(tc.res, tc.err)
		c.send("SELECT a FROM t")
		if a := c.read(0); a.code != tc.code {
			t.Fatalf("%s: error %q %q, want %q", tc.name, a.code, a.message, tc.code)
		}
		n := l.note(t, i)
		if n.TooLarge != tc.want || n.Side != RouteCopy {
			t.Errorf("%s: note %+v, want TooLarge %q on the copy", tc.name, n, tc.want)
		}
	}
}
