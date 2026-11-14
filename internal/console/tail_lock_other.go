//go:build !unix

package console

import (
	"errors"
	"os"
)

// lockTailDir cannot lock without flock: no directory is ever proved unused,
// so a sweep keeps every one (the safe direction), and a keeper says once
// that its directory is not locked.
func lockTailDir(string) (*os.File, error) {
	return nil, errors.New("locking the tails' directory needs flock, which this platform does not have")
}
