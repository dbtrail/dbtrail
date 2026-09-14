package reconstruct

import (
	"context"
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// StubLinkFileForTest swaps the hard-link primitive carryForward tries first,
// so an external test can force the copy fallback through the REAL fold. Every
// test machine has one filesystem, so without the stub os.Link always succeeds
// and the fold-level copy arm never executes anywhere (the same blindness that
// let `return nil` replace the copy and pass both tiers — see linkFile's doc).
// Test-only by construction: _test.go files never build into the shipped
// package.
func StubLinkFileForTest(fn func(oldname, newname string) error) (restore func()) {
	prev := linkFile
	linkFile = fn
	return func() { linkFile = prev }
}

// CountSnapshotCutsForTest wraps the cut resolver the fold calls, so an
// external test can assert one fold resolves exactly one cut (#1635). A second
// resolution against a quiet index returns the same coordinate and is
// otherwise invisible.
//
// The counter is atomic: tables fold concurrently, and a regression that
// resolved once per table would bump it from several goroutines at once.
func CountSnapshotCutsForTest(calls *atomic.Int32) (restore func()) {
	prev := resolveSnapshotCut
	resolveSnapshotCut = func(ctx context.Context, db *sql.DB, at time.Time) (*query.BinlogPos, error) {
		calls.Add(1)
		return prev(ctx, db, at)
	}
	return func() { resolveSnapshotCut = prev }
}
