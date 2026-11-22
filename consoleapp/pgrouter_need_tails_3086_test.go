package consoleapp

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// #3086: what the port asks of a statement's tails reaches the copy's run.
func TestPGRunContext_3086(t *testing.T) {
	type key struct{}
	base := context.WithValue(context.Background(), key{}, "kept")
	if got := pgRunContext(base, pgshim.RunOptions{}); got != base {
		t.Error("a statement that does not need its tails runs under another context")
	}
	got := pgRunContext(base, pgshim.RunOptions{NeedTails: true})
	if !console.PGNeedsTails(got) {
		t.Error("a statement that needs its tails is not marked so")
	}
	if got.Value(key{}) != "kept" {
		t.Error("the statement's own context was lost")
	}
}
