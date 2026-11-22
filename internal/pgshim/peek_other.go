//go:build !unix

package pgshim

import "syscall"

// peekIdle cannot look at a socket without reading it here: idleEnded reads
// with a short deadline instead.
func peekIdle(syscall.Conn) (string, bool) { return "", false }
