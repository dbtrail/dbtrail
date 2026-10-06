package pgshim

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/shim"
)

// #2174: a _snapshot refused because the source's binary log started again
// reaches a PostgreSQL client as 22023, the code of a coverage gap, not as an
// internal error; other raw errors stay XX000.
func TestPgResolveError_renumbered_2174(t *testing.T) {
	refusal := fmt.Errorf("resolve _snapshot: %w: the source's binary log started again", reconstruct.ErrBinlogRenumbered)
	if got := pgResolveError(refusal); got.code != "22023" || got.msg != refusal.Error() {
		t.Errorf("renumbered = %+v; want 22023 carrying %q", got, refusal)
	}
	if got := pgResolveError(errors.New("a baseline read failed")); got.code != "XX000" {
		t.Errorf("another error = %+v; want XX000", got)
	}
	gap := &shim.ResolveError{Class: shim.ResolveTimeout, QType: shim.TypeSnapshot, Err: errors.New("deadline")}
	if got := pgResolveError(gap); got.code != "57014" {
		t.Errorf("a timeout = %+v; want 57014", got)
	}
}
