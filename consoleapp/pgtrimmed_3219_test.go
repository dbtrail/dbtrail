package consoleapp

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// The port's "which ones could not be read" is the console's.
func TestPGTrimmedUnknownIsTheConsoles(t *testing.T) {
	if pgshim.TrimmedUnknown != console.PGTrimmedUnknown {
		t.Fatalf("%q and %q", pgshim.TrimmedUnknown, console.PGTrimmedUnknown)
	}
}

// What the screen took as trimmed reaches the copy's run.
func TestPGRunContextCarriesTrimmed(t *testing.T) {
	ctx := pgRunContext(context.Background(), pgshim.RunOptions{Trimmed: []string{"c"}, NeedTails: true})
	if !console.PGNeedsTails(ctx) {
		t.Error("NeedTails lost")
	}
	if got := console.PGTrimmedNames(ctx); len(got) != 1 || got[0] != "c" {
		t.Errorf("trimmed %q", got)
	}
}
