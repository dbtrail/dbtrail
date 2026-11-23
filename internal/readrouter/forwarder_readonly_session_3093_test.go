package readrouter

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// refusingReadOnlySource refuses the statement that marks a session
// read-only.
type refusingReadOnlySource struct{ *collationSource }

func (h *refusingReadOnlySource) HandleQuery(q string) (*mysql.Result, error) {
	if q == ReadOnlySessionSet {
		h.mu.Lock()
		h.seen = append(h.seen, q)
		h.mu.Unlock()
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "You have an error in your SQL syntax")
	}
	return h.collationSource.HandleQuery(q)
}

// #3093: a read-only port marks each new session read-only on the source
// before anything of the client's, and only a read-only port does.
func TestForwarder_readOnlySessionIsMarkedFirst_3093(t *testing.T) {
	ctx := context.Background()
	for _, readOnly := range []bool{true, false} {
		f, src := newCollationForwarder(t, "8.0.46", "utf8mb4_unicode_ci")
		f.ReadOnlySession = readOnly
		if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err != nil {
			t.Fatal(err)
		}
		want := []string{"DO 1"}
		if readOnly {
			want = []string{ReadOnlySessionSet, "DO 1"}
		}
		if got := src.statements(); !reflect.DeepEqual(got, want) {
			t.Errorf("read-only %v: the source saw %q, want %q", readOnly, got, want)
		}
	}
}

// A source that refuses the mark leaves a connection that does not open:
// the client's statement is not sent on a session that is not protected,
// and the error says what was refused.
func TestForwarder_readOnlySessionRefusedDoesNotOpen_3093(t *testing.T) {
	ctx := context.Background()
	var src *refusingReadOnlySource
	f, _ := newWrappedCollationForwarder(t, "8.0.46", "utf8mb4_unicode_ci", func(inner *collationSource) server.Handler {
		src = &refusingReadOnlySource{collationSource: inner}
		return src
	})
	f.ReadOnlySession = true
	var opened []error
	f.OnConnect = func(err error) { opened = append(opened, err) }
	_, err := f.Forward(ctx, "DELETE FROM t", &BufferSink{})
	if err == nil {
		t.Fatal("a statement was forwarded on a session the source refused to mark read-only")
	}
	for _, q := range src.statements() {
		if q == "DELETE FROM t" {
			t.Fatalf("the client's statement reached the source: %q", src.statements())
		}
	}
	if len(opened) != 1 || opened[0] == nil || !strings.Contains(opened[0].Error(), "refused to mark the session read-only") || !strings.Contains(opened[0].Error(), ReadOnlySessionSet) {
		t.Errorf("OnConnect was told %v, want once, with what the source refused", opened)
	}
	// It stays lost: no later statement goes out on another, unmarked, session.
	if _, err := f.Forward(ctx, "DO 1", &BufferSink{}); err == nil {
		t.Error("a later statement was forwarded")
	}
	for _, q := range src.statements() {
		if q == "DO 1" {
			t.Errorf("a later statement reached the source: %q", src.statements())
		}
	}
}
