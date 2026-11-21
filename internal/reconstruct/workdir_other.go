//go:build !unix

package reconstruct

import (
	"errors"
	"os"
)

// lockWorkDir cannot lock without flock: no directory is ever proved unused,
// so nothing is swept (the safe direction), and a directory is made under the
// name that is not locked by design. What a killed process leaves here stays
// until it is removed by hand.
func lockWorkDir(string, bool) (*os.File, error) {
	return nil, errors.New("locking a working directory needs flock, which this platform does not have")
}

// ownedByThisUser cannot be told here, so nothing is swept.
func ownedByThisUser(os.FileInfo) bool { return false }
