package console

import (
	"context"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

const (
	// folderUsageFresh is how long a measured size is served before a request
	// starts the next measurement.
	folderUsageFresh = time.Minute
	// folderUsageRetry is how long a folder that could not be measured waits
	// for its next try: one past the entry limit costs a long walk each time,
	// and it will not have shrunk a minute later.
	folderUsageRetry = 30 * time.Minute
	// folderUsageMaxAge is how old a size may be and still be reported. A
	// measurement stuck on a disk that stopped answering must not leave an old
	// number on the page for as long as the disk is gone.
	folderUsageMaxAge = 10 * time.Minute
	// folderUsageBudget bounds one walk. A folder that takes longer has no
	// size to show, which is better than a walk that never ends.
	folderUsageBudget = time.Minute
	// folderUsageMaxEntries bounds what one walk visits, and with it the set
	// of hard-linked files it has to remember.
	folderUsageMaxEntries = 500_000
	// folderUsageWait is how long one request waits for measurements, for all
	// its folders together. A small folder answers in this request; a large
	// one in a later one.
	folderUsageWait = 250 * time.Millisecond
	// folderUsageConcurrent is how many measurements run at once in this
	// process, which is also the one that captures changes under watch.
	folderUsageConcurrent = 2
)

// folderUsageCache measures snapshot folders for the pages that report them
// (#2475). It only reads: nothing here decides or removes anything.
//
// Everything that touches a snapshot folder runs in the background job
// (run). A request reads what the job stored and never calls the filesystem
// on the folder, because a network disk that stopped answering blocks such a
// call with no timeout, and a request stuck there would never return. The
// zero value is ready to use.
type folderUsageCache struct {
	mu    sync.Mutex
	byDir map[string]*folderUsageEntry
	slots chan struct{}
	// wait overrides folderUsageWait; tests set it.
	wait time.Duration
	// Seams over the three filesystem reads, for a test's folder that does
	// not answer. Nil means the real one.
	resolve func(string) string
	diskOf  func(string) (uint64, bool)
	walk    func(context.Context, string, int) (uint64, bool)
}

type folderUsageEntry struct {
	bytes uint64
	ok    bool
	at    time.Time
	// canonical and disk are what the job found out about the folder;
	// resolvedAt is when canonical was read.
	canonical  string
	resolvedAt time.Time
	disk       uint64
	diskOK     bool
	// done is non-nil while a job runs, and closes when it ends.
	done chan struct{}
}

// folderReading is what is known about a folder without asking its disk.
type folderReading struct {
	Bytes      uint64
	Measured   bool
	KeepsEvery bool
	Canonical  string
	Disk       uint64
	DiskKnown  bool
}

// folderPlan is the input of the "no cleanup reaches this folder" answer, all
// of it from memory.
type folderPlan struct {
	entries   []ServerEntry
	globalDir string
	globalS3  string
	ageSet    bool
	loop      bool
}

// keepsEveryResolved answers for dir from paths already resolved, without
// touching a disk. It is true only when it knows: a process that runs no
// cleanup cannot say (another one may prune the folder), and neither can one
// missing the resolved path of any configured folder, since that folder may
// be this one under another spelling.
func (p folderPlan) keepsEveryResolved(dir string, resolved func(string) (string, bool)) bool {
	if !p.loop || dir == "" {
		return false
	}
	known := true
	canon := func(path string) string {
		c, ok := resolved(path)
		if !ok {
			known = false
			return path
		}
		return c
	}
	keeps := folderKeepsEveryWith(canon, p.entries, dir, p.globalDir, p.globalS3, p.ageSet)
	return keeps && known
}

// dirs are the folders the answer needs resolved.
func (p folderPlan) dirs() []string {
	out := []string{p.globalDir}
	for _, e := range p.entries {
		out = append(out, e.BaselineDir, e.LocalKeepHeldDir)
	}
	return out
}

// folderPlan is this process's plan, as it stands now.
func (s *Server) folderPlan() folderPlan {
	p := folderPlan{globalDir: s.cm.defaultBaselineDir, globalS3: s.cm.defaultBaselineS3, ageSet: s.pruneRetainMinutes() > 0, loop: s.localPruneLoop}
	if s.cm.reg != nil {
		p.entries = s.cm.reg.List()
	}
	return p
}

// readAll returns what is known of each folder, keyed as given, after waiting
// at most one shared deadline for the jobs it started.
func (c *folderUsageCache) readAll(dirs []string, plan folderPlan) map[string]folderReading {
	wait := c.waitTime()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		pending := c.startDue(dirs, plan.dirs())
		if len(pending) == 0 {
			break
		}
		for _, done := range pending {
			left := time.Until(deadline)
			if left <= 0 {
				break
			}
			timer := time.NewTimer(left)
			select {
			case <-done:
			case <-timer.C:
			}
			timer.Stop()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	resolved := func(path string) (string, bool) {
		e := c.byDir[path]
		if e == nil || e.canonical == "" || time.Since(e.resolvedAt) >= folderUsageMaxAge {
			return "", false
		}
		return e.canonical, true
	}
	out := make(map[string]folderReading, len(dirs))
	for _, d := range dirs {
		e := c.byDir[d]
		if d == "" || e == nil {
			continue
		}
		out[d] = folderReading{
			Bytes:      e.bytes,
			Measured:   e.ok && time.Since(e.at) < folderUsageMaxAge,
			KeepsEvery: plan.keepsEveryResolved(d, resolved),
			Canonical:  e.canonical,
			Disk:       e.disk,
			DiskKnown:  e.diskOK,
		}
	}
	return out
}

func (c *folderUsageCache) waitTime() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wait > 0 {
		return c.wait
	}
	return folderUsageWait
}

// startDue starts the job of each folder that needs one, and returns the jobs
// running for these folders. dirs are measured; resolveOnly are only resolved,
// since the plan needs their path and not their size. A job already running,
// even one stuck on a dead disk, is not started again.
func (c *folderUsageCache) startDue(dirs, resolveOnly []string) []chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byDir == nil {
		c.byDir = map[string]*folderUsageEntry{}
		c.slots = make(chan struct{}, folderUsageConcurrent)
	}
	measure := make(map[string]bool, len(dirs))
	for _, d := range dirs {
		measure[d] = true
	}
	seen := map[string]bool{"": true}
	var pending []chan struct{}
	for _, d := range append(append([]string{}, dirs...), resolveOnly...) {
		if seen[d] {
			continue
		}
		seen[d] = true
		e := c.byDir[d]
		if e == nil {
			e = &folderUsageEntry{}
			c.byDir[d] = e
		}
		if e.done == nil {
			interval := folderUsageFresh
			if !e.ok {
				interval = folderUsageRetry
			}
			walkDue := measure[d] && (e.at.IsZero() || time.Since(e.at) >= interval)
			resolveDue := e.resolvedAt.IsZero() || time.Since(e.resolvedAt) >= folderUsageFresh
			if walkDue || resolveDue {
				e.done = make(chan struct{})
				go c.run(d, e, e.done, walkDue)
			}
		}
		if e.done != nil {
			pending = append(pending, e.done)
		}
	}
	return pending
}

// run is the background job: the only code that touches the folder, and only
// this one folder. walk is false when only its path is due, so a folder that
// cannot be measured is not walked again for it. This goroutine lives in the
// process that also captures changes, so a panic in it ends as "no size",
// never as an outage.
func (c *folderUsageCache) run(dir string, e *folderUsageEntry, done chan struct{}, walk bool) {
	var (
		n      uint64
		ok     bool
		disk   uint64
		diskOK bool
	)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("console: measuring a snapshot folder panicked; its size is not shown", "dir", dir, "panic", r, "stack", string(debug.Stack()))
			n, ok, walk = 0, false, true
		}
		c.mu.Lock()
		if walk {
			e.bytes, e.ok, e.at = n, ok, time.Now()
		}
		e.disk, e.diskOK = disk, diskOK
		if e.resolvedAt.IsZero() {
			// Nothing was resolved (a panic): not asked again every request.
			e.resolvedAt = time.Now()
		}
		e.done = nil
		c.mu.Unlock()
		close(done)
	}()
	c.slots <- struct{}{}
	defer func() { <-c.slots }()
	var canonical string
	if c.resolve != nil {
		canonical = c.resolve(dir)
	} else {
		canonical = canonicalDir(dir)
	}
	// Stored now: the other folders' answers need it even if this folder's
	// disk stops answering in the steps below.
	c.mu.Lock()
	e.canonical, e.resolvedAt = canonical, time.Now()
	c.mu.Unlock()
	disk, diskOK = c.disk(canonical)
	if walk {
		ctx, cancel := context.WithTimeout(context.Background(), folderUsageBudget)
		defer cancel()
		if c.walk != nil {
			n, ok = c.walk(ctx, canonical, folderUsageMaxEntries)
		} else {
			n, ok = measureFolder(ctx, canonical, folderUsageMaxEntries)
		}
	}
}

// disk is the id of the filesystem holding path.
func (c *folderUsageCache) disk(path string) (uint64, bool) {
	if c.diskOf != nil {
		return c.diskOf(path)
	}
	return folderDisk(path)
}
