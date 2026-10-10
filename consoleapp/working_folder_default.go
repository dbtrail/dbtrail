package consoleapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// workingFolderName is the working folder's name beside the servers file. It
// is the name the compose stack has always set by hand
// (/var/lib/bintrail/baseline-staging), so an install that sets nothing and
// one that runs the compose file write to the same place.
const workingFolderName = "baseline-staging"

// tempWorkingFolder is the working folder under the system temp folder: the
// default before #2255, and still the one when DBTrail's own data folder
// cannot hold a full read's dump.
func tempWorkingFolder() string {
	return filepath.Join(os.TempDir(), "bintrail-baseline-staging")
}

// workingFolderDefault is the working folder a daemon uses when none was set,
// and, when that is the temp folder, why it is not beside DBTrail's data.
//
// InMemory is the one case that is a hazard rather than a fact: the temp
// folder is RAM on this host, so a dump written there is taken from the
// memory the capture process runs in.
//
// Unusable is the other one worth a warning: the folder beside the data
// exists as a choice and could not be written (a full disk, a read-only
// mount), which an operator will want to hear about.
type workingFolderDefault struct {
	Dir      string
	Why      string
	InMemory bool
	Unusable bool
}

// fsClass is what kind of storage holds a path, as far as the working folder
// cares.
type fsClass int

const (
	fsLocal fsClass = iota
	fsNetwork
	fsMemory
)

// fsKindFn reports what kind of storage holds an existing path, and its name.
// A variable so a test can stand in for the kernel.
var fsKindFn = fsKind

// inTemp is the answer "the temp folder", with whether that folder is memory.
func inTemp(why string) workingFolderDefault {
	dir := tempWorkingFolder()
	class, _, err := fsKindFn(existingParent(dir))
	return workingFolderDefault{Dir: dir, Why: why, InMemory: err == nil && class == fsMemory}
}

// defaultWorkingFolder chooses the working folder for a daemon that was given
// none (#2255). It is a folder beside the servers file, on the disk that
// already holds what DBTrail keeps: the system temp folder is on many hosts a
// small filesystem or memory, and a full read writes a whole uncompressed dump
// there.
//
// Three cases keep the temp folder, each with its reason for the page:
//
//   - DBTrail's data is on network storage (the documented ECS setup keeps it
//     on EFS). A dump there is slow, billed, and on a filesystem that never
//     reports itself full, so the disk check before a read would always pass.
//   - DBTrail's data is held in memory (a tmpfs mount): no place for a dump.
//   - The folder cannot be created or written (a config directory mounted
//     read-only). A read that worked before the default moved must not start
//     failing because of it.
//
// It creates the folder when it chooses it, which is what makes the write
// check real. It creates nothing in the first two cases above.
func defaultWorkingFolder(serversPath string) workingFolderDefault {
	if strings.TrimSpace(serversPath) == "" {
		return inTemp("This console keeps no settings on disk, so there is no data folder to put it in.")
	}
	abs, err := filepath.Abs(serversPath)
	if err != nil {
		return inTemp(fmt.Sprintf("DBTrail's data folder could not be located: %s.", firstLineOf(err.Error())))
	}
	state := filepath.Dir(abs)
	dir := filepath.Join(state, workingFolderName)

	// Asked of the nearest folder that exists: on a fresh install the data
	// folder is not there yet, and its parent says what disk it will be on.
	// A kind that cannot be read is not a reason to stay in the temp folder:
	// the write check below still has to pass.
	switch class, kind, err := fsKindFn(existingParent(state)); {
	case err != nil:
	case class == fsNetwork:
		return inTemp(fmt.Sprintf(
			"DBTrail's data folder %s is on network storage (%s), where a full read's dump would be slow and its size never checked.", state, kind))
	case class == fsMemory:
		return inTemp(fmt.Sprintf(
			"DBTrail's data folder %s is held in memory (%s), and a full read's dump does not belong in RAM.", state, kind))
	}
	if err := writableDir(state, dir); err != nil {
		d := inTemp(fmt.Sprintf("%s, beside DBTrail's data, cannot be used: %s.", dir, firstLineOf(err.Error())))
		d.Unusable = true
		return d
	}
	return workingFolderDefault{Dir: dir}
}

// writableDir creates dir if it is missing and proves a file can be written
// in it. The file is removed again.
//
// The data folder is made first and private (0700), the mode every other
// writer gives it (the auth file, the run histories, the job locks): on a
// fresh install this can be the first thing to create it, and MkdirAll on the
// working folder alone would leave the folder that holds the credential files
// listable by every user of the host, for good. The working folder is private
// too: nothing but DBTrail reads a dump in progress.
func writableDir(state, dir string) error {
	if err := os.MkdirAll(state, 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".write-check-*")
	if err != nil {
		return err
	}
	name := f.Name()
	closeErr := f.Close()
	if err := os.Remove(name); err != nil {
		return err
	}
	return closeErr
}

// workingFolderBoot is the working folder this process resolved, kept so
// that every reader at startup (the page, the sweep, the supervisor) gets ONE
// answer. Worked out again per call, a disk error between two calls would
// have the page name one folder while reads write to another. Keyed by what
// the answer depends on, so a process that is configured once resolves once
// and a test that changes the inputs gets its own answer.
var workingFolderBoot struct {
	sync.Mutex
	key string
	def workingFolderDefault
}

func defaultWorkingFolderOnce(serversPath string) workingFolderDefault {
	key := serversPath + "\x00" + os.TempDir()
	workingFolderBoot.Lock()
	defer workingFolderBoot.Unlock()
	if workingFolderBoot.key != key || workingFolderBoot.def.Dir == "" {
		workingFolderBoot.key, workingFolderBoot.def = key, defaultWorkingFolder(serversPath)
	}
	return workingFolderBoot.def
}
