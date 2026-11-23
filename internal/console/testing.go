package console

import (
	"context"
	"sync"
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

// HoldTailKeeperForTest takes the lock of server id's tail keeper, as an
// update of its tails holds it for most of its length, until release is called.
// ok is false when the server has no keeper.
// Test helper ONLY, for another package's tests; no shipped code may call it.
func (s *Server) HoldTailKeeperForTest(id string) (release func(), ok bool) {
	s.tails.mu.Lock()
	k := s.tails.by[id]
	s.tails.mu.Unlock()
	if k == nil {
		return nil, false
	}
	k.mu.Lock()
	return sync.OnceFunc(k.mu.Unlock), true
}
