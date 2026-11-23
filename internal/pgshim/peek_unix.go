//go:build unix

package pgshim

import "syscall"

// peekIdle looks at a socket without waiting and without taking anything
// from it (idleEnded): ok false when it cannot be looked at so.
func peekIdle(sc syscall.Conn) (why string, ok bool) {
	rc, err := sc.SyscallConn()
	if err != nil {
		return "", false
	}
	var b [1]byte
	var n int
	var rerr error
	if err := rc.Read(func(fd uintptr) bool {
		n, _, rerr = syscall.Recvfrom(int(fd), b[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		return true
	}); err != nil {
		return err.Error(), true
	}
	switch {
	case rerr == syscall.EAGAIN || rerr == syscall.EWOULDBLOCK:
		return "", true
	case rerr != nil:
		return rerr.Error(), true
	case n == 0:
		return "the source closed the connection", true
	}
	return idleSentSomething, true
}
