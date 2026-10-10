//go:build darwin || linux

package consoleapp

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// ownFolder reports whether path is a real folder (not a link) that belongs
// to this process's user and that no other user can write into. "" means
// yes; anything else is why not.
//
// It is the condition for deleting inside a folder DBTrail did not just
// create itself: in a folder another user can write into, an entry can be
// swapped for a link between the moment it is checked and the moment it is
// removed, and the removal then lands wherever the link points.
func ownFolder(path string) string {
	fi, err := os.Lstat(path)
	if err != nil {
		return firstLineOf(err.Error())
	}
	if fi.Mode()&fs.ModeSymlink != 0 || !fi.IsDir() {
		return "it is not a folder"
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "its owner cannot be read"
	}
	if int(st.Uid) != os.Geteuid() {
		return fmt.Sprintf("it belongs to another user (uid %d)", st.Uid)
	}
	if fi.Mode().Perm()&0o022 != 0 {
		return fmt.Sprintf("other users can write into it (mode %o)", fi.Mode().Perm())
	}
	return ""
}

// heldInPlace reports whether no other user can rename or replace path:
// the folder that holds it is either closed to other users' writes or has
// the sticky bit (the shape of /tmp, where only an entry's owner may rename
// or remove it). "" means yes.
func heldInPlace(path string) string {
	parent := filepath.Dir(filepath.Clean(path))
	fi, err := os.Stat(parent)
	if err != nil {
		return firstLineOf(err.Error())
	}
	if fi.Mode().Perm()&0o022 != 0 && fi.Mode()&fs.ModeSticky == 0 {
		return fmt.Sprintf("the folder that holds it, %s, lets other users rename it (mode %o, no sticky bit)", parent, fi.Mode().Perm())
	}
	return ""
}
