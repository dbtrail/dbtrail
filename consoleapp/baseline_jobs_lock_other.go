//go:build !unix

package consoleapp

import (
	"errors"
	"os"
)

// Without flock nothing can prove a job's owner is dead, so no job is
// journaled and nothing is ever reclaimed: the safe direction. dbtrail ships
// on Linux and macOS; this keeps the package compiling elsewhere.

type jobLockState int

const (
	jobLockHeld jobLockState = iota
	jobLockTaken
	jobLockMissing
)

func createJobLock(string) (*os.File, error) {
	return nil, errors.New("job locks need flock, which this platform does not have")
}

func tryJobLock(string) (*os.File, jobLockState, error) { return nil, jobLockHeld, nil }
