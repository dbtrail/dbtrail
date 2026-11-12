package console

import (
	"context"
	"time"
)

// SetBcryptCostForTest sets the cost the console hashes passwords at and
// returns a function that puts back the previous one. Test helper ONLY: a test
// binary lowers the cost so password logins stop dominating its run time. No
// shipped code may call it; TestBcryptCostHookIsTestOnly fails the build if
// any non-test file does, and TestShippedBcryptCost pins the shipped value.
func SetBcryptCostForTest(cost int) (restore func()) {
	prev := bcryptCost
	bcryptCost = cost
	return func() { bcryptCost = prev }
}

// SetNewestSnapshotForTest replaces how the method choice finds a server's
// newest snapshot (reconstruct.NewestSnapshot, which reaches the network for
// an s3:// source) and returns a function that puts the real one back. Test
// helper ONLY, for another package's tests that drive a scheduled slot of a
// server whose snapshots go to S3; no shipped code may call it.
func SetNewestSnapshotForTest(fn func(ctx context.Context, source string) (time.Time, []string, error)) (restore func()) {
	prev := newestSnapshot
	newestSnapshot = fn
	return func() { newestSnapshot = prev }
}
