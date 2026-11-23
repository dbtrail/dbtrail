package console

import (
	"context"
	"testing"
	"time"
)

func TestPGNeedTails(t *testing.T) {
	if pgNeedTails(context.Background()) {
		t.Error("an unmarked context needs its tails")
	}
	ctx := WithPGNeedTails(context.Background())
	if !pgNeedTails(ctx) {
		t.Error("a marked context does not")
	}
	child, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if !pgNeedTails(child) {
		t.Error("a context derived from a marked one does not")
	}
}
