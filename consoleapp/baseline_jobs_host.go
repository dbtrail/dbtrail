package consoleapp

import (
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
)

// hostIdentity names the kernel this process runs on, for the journal
// (#2180). An flock is only seen by processes of the kernel that granted it:
// on NFS mounted with local locks (`nolock`, `local_lock=flock|all`), after
// an NFSv4 lease loss, or on a folder a host and Docker Desktop's VM both
// mount, another host's live job holds a lock this host cannot see. So a job
// is reclaimed only by the host that journaled it.
//
// The kernel boot id where there is one: every container on a host shares
// it, and it changes on reboot (which ends every process of the old boot,
// at the cost that what a reboot interrupted is kept, not reclaimed). The
// host name otherwise. "" when neither can be read: such a job is never
// reclaimed by anyone.
func hostIdentity() string {
	if id := strings.TrimSpace(bootID()); id != "" {
		return "boot:" + id
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return "host:" + h
	}
	return ""
}

// jobsDirOnNetworkFS reports whether a folder is on a network or FUSE file
// system, and which. Indirected for tests.
var jobsDirOnNetworkFS = networkFS

// networkFSMagic classifies a Linux statfs f_type.
func networkFSMagic(magic int64) (string, bool) {
	switch magic {
	case 0x6969:
		return "nfs", true
	case 0xFF534D42:
		return "cifs", true
	case 0xFE534D42:
		return "smb2", true
	case 0x517B:
		return "smb", true
	case 0x65735546:
		return "fuse (virtiofs and Docker Desktop shares among them)", true
	case 0x01021997:
		return "9p", true
	case 0x00C36400:
		return "ceph", true
	case 0x5346414F:
		return "afs", true
	}
	return "", false
}

// networkFSName classifies a macOS/BSD statfs f_fstypename.
func networkFSName(name string) bool {
	switch {
	case name == "nfs", name == "smbfs", name == "afpfs", name == "webdav", name == "cifs":
		return true
	case strings.Contains(name, "fuse"):
		return true
	}
	return false
}

var networkWarned atomic.Bool

// noteForeignJob says why a job journaled by another host (or by no known
// host) is kept: once per process at Warn on a network file system, where
// this is the shape a cross-host state folder takes, and at Info otherwise.
func noteForeignJob(jobsDir, jobHost, server, kind string) {
	if fs, ok := jobsDirOnNetworkFS(jobsDir); ok {
		if networkWarned.CompareAndSwap(false, true) {
			slog.Warn("snapshot jobs: the state folder is on a network file system ("+fs+") and holds jobs another host "+
				"journaled, whose locks this host cannot see; they are never cleaned up from here. A state folder shared "+
				"between hosts needs a lock every host sees (Amazon EFS and NFSv4 without local locks do); see "+
				"docs/deployment.md", "dir", jobsDir, "job_host", jobHost, "this_host", hostIdentity())
		}
		return
	}
	slog.Info("snapshot jobs: a job journaled by another host (or one whose host is not known) is left alone",
		"server", server, "kind", kind, "job_host", jobHost, "this_host", hostIdentity())
}
