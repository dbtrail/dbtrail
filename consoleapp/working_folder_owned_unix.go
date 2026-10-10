//go:build darwin || linux

package consoleapp

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// currentUID is this process's user. A variable so a test can stand in
// another user without being root.
var currentUID = os.Geteuid

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
	if int(st.Uid) != currentUID() {
		return fmt.Sprintf("it belongs to another user (uid %d)", st.Uid)
	}
	if fi.Mode().Perm()&0o022 != 0 {
		return fmt.Sprintf("other users can write into it (mode %o)", fi.Mode().Perm())
	}
	return ""
}

// heldInPlace reports whether no other unprivileged user can rename or
// replace path or any folder above it. "" means yes.
//
// Every folder from the one that holds path up to the root has to belong to
// this process's user or to root, and either be closed to other users'
// writes or carry the sticky bit (the shape of /tmp, where only an entry's
// owner may rename or remove it). The owner matters as much as the mode: a
// folder's owner can rename its entries whatever the mode says, so a 0755
// folder of another user holds nothing in place.
//
// The chain is that of the real path: a link on the way (/tmp on macOS) is
// resolved first, and what is checked is where it leads.
func heldInPlace(path string) string {
	real, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return firstLineOf(err.Error())
	}
	for dir := filepath.Dir(real); ; dir = filepath.Dir(dir) {
		fi, err := os.Lstat(dir)
		if err != nil {
			return firstLineOf(err.Error())
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok || !fi.IsDir() {
			return fmt.Sprintf("%s, above it, cannot be checked", dir)
		}
		if uid := int(st.Uid); uid != 0 && uid != currentUID() {
			return fmt.Sprintf("%s, above it, belongs to another user (uid %d), who can rename what it holds", dir, uid)
		}
		if fi.Mode().Perm()&0o022 != 0 && fi.Mode()&fs.ModeSticky == 0 {
			return fmt.Sprintf("%s, above it, lets other users rename what it holds (mode %o, no sticky bit)", dir, fi.Mode().Perm())
		}
		if filepath.Dir(dir) == dir {
			return ""
		}
	}
}
