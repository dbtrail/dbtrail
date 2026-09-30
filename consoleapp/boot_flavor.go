package consoleapp

import (
	"strings"
	"sync"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// sourceFlavorCell holds the flavor watch's own capture (--source-dsn) runs
// as: the declared --source-flavor at start, then what the source reports each
// time the stream asks it. The boot entry has no saved Source type, so the
// server list and the capture status read take its flavor from here. Only
// "mysql" and "mariadb" are ever held; "" means not known yet.
type sourceFlavorCell struct {
	mu sync.Mutex
	v  string
}

func newSourceFlavorCell(declared string) *sourceFlavorCell {
	c := &sourceFlavorCell{}
	c.set(declared)
	return c
}

// set records f when it is mysql or mariadb, in any case and spacing, and
// ignores anything else: a value that is not a flavor a MySQL-family stream
// runs as is never shown as one.
func (c *sourceFlavorCell) set(f string) {
	f = strings.ToLower(strings.TrimSpace(f))
	if f != console.FlavorMySQL && f != console.FlavorMariaDB {
		return
	}
	c.mu.Lock()
	c.v = f
	c.mu.Unlock()
}

// get is the flavor held, "" for none or a nil cell.
func (c *sourceFlavorCell) get() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v
}

// mainStreamFlavorHook is the OnFlavorResolved of watch's main stream: it
// records every resolution in cell (a restart asks the source again) and
// starts the source jobs once.
func mainStreamFlavorHook(cell *sourceFlavorCell, startJobs func(string)) func(string) {
	once := streamrun.FlavorOnce(startJobs)
	return func(flavor string) {
		cell.set(flavor)
		once(flavor)
	}
}
