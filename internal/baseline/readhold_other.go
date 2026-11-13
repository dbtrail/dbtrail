//go:build !unix

package baseline

// HoldSnapshot is a no-op where flock is unavailable. bintrail ships on Linux
// and macOS; see readhold_unix.go.
func HoldSnapshot(string) (release func(), gone bool) { return func() {}, false }

// lockForRemoval is a no-op where flock is unavailable.
func lockForRemoval(string) (unlock func(), held bool) { return func() {}, false }
