//go:build !unix

package copymirror

// lockMirror has no cross-process lock off unix; one process per mirror is
// the operator's to keep there.
func lockMirror(string) (func(), bool, error) { return func() {}, false, nil }
