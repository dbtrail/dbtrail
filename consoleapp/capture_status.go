package consoleapp

import (
	"context"
	"database/sql"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/status"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// Whether capture is caught up, for the Overview (#1794).
//
// The snapshot schedule asks the source this before a full read on age
// (#1791, capture_probe.go). The Overview asks the same question for a
// different reason: a server nobody wrote to and a capture that fell behind
// look the same from the index, and the page can only tell them apart by
// asking. The read is the schedule's (compareWithSource); what is added here
// is what a page needs and a schedule does not:
//
//   - the answer is kept for a short while and one read runs at a time per
//     server, because this process is the capture too and the source is the
//     operator's production server: a page that redraws, or ten tabs, ask
//     the source once;
//   - "behind" is said only on a second read. The two positions are read at
//     different instants and the capture saves its own every few seconds,
//     so on a first read a healthy capture of a busy source looks behind;
//   - anything that does not settle it is unknown. Unknown is never shown
//     as up to date and never as behind.

const (
	// captureStatusTTL is how long an answer is reused.
	captureStatusTTL = 30 * time.Second
	// captureStatusPendingTTL is the same for a first read that found the
	// source ahead: shorter than captureStatusRetry, so the page's second
	// ask reads the source again.
	captureStatusPendingTTL = 20 * time.Second
	// captureStatusRetry is when the page is told to ask again.
	captureStatusRetry = 25 * time.Second
	// captureConfirmGap is how long after the first read the capture must
	// have saved its position for "behind" to be said: a position saved
	// before then may simply predate what the source had written.
	captureConfirmGap = 10 * time.Second
)

// captureSample is what the source had executed at one read.
type captureSample struct {
	executed string
	// purged is what the source had purged at the same read.
	purged string
	at     time.Time
}

// captureSlot is one server's last answer, the read in flight for it, and
// the sample a later read is compared with.
type captureSlot struct {
	// key is the pair of DSNs the slot was read from, and the source's
	// flavor: an edited server starts over, its previous sample was another
	// source's, or another read's.
	key    string
	answer console.CaptureStatus
	at     time.Time
	ttl    time.Duration
	has    bool
	prev   *captureSample
	flight chan struct{} // closed when the read in flight has stored its answer
	logged string
}

// captureStatusReporter is console.CaptureStatusReporter for the watch
// daemon.
type captureStatusReporter struct {
	// bootSourceDSN is the source of the daemon's own capture (--source-dsn),
	// empty on a daemon started without one.
	bootSourceDSN string
	// bootFlavor reports the flavor the daemon's own capture runs as (its
	// sourceFlavorCell: the declared --source-flavor, then what the source
	// reported); nil or "" reads as MySQL. The boot entry carries no flavor
	// of its own.
	bootFlavor func() string

	mu    sync.Mutex
	slots map[string]*captureSlot
	// read and now are replaced by tests; nil means the real ones.
	read func(ctx context.Context, indexDSN, sourceDSN string) captureProbeResult
	// readMariaDB is read for a MariaDB source; nil means the real one.
	readMariaDB func(ctx context.Context, indexDSN, sourceDSN string) captureProbeResult
	now         func() time.Time
}

func newCaptureStatusReporter(bootSourceDSN string) *captureStatusReporter {
	return &captureStatusReporter{bootSourceDSN: bootSourceDSN}
}

// withBootFlavor sets where the daemon's own capture flavor is read from.
func (c *captureStatusReporter) withBootFlavor(flavor func() string) *captureStatusReporter {
	c.bootFlavor = flavor
	return c
}

// CaptureStatus answers for e, from the last answer while it is fresh.
func (c *captureStatusReporter) CaptureStatus(ctx context.Context, e console.ServerEntry) console.CaptureStatus {
	unknown := func(detail string) console.CaptureStatus {
		return console.CaptureStatus{ServerID: e.ID, State: console.CaptureStateUnknown, Detail: detail}
	}
	source, flavor := e.SourceDSN, e.SourceFlavor()
	if e.ID == bootCaptureServerID {
		source = c.bootSourceDSN
		if c.bootFlavor != nil && c.bootFlavor() == console.FlavorMariaDB {
			flavor = console.FlavorMariaDB
		}
	}
	// What is known without asking anyone.
	switch {
	case source == "":
		return unknown("this server has no source to ask")
	case e.IsPostgres():
		return unknown("PostgreSQL sources are not compared yet")
	}
	now, read := time.Now, captureHeadFromDBs
	if c.now != nil {
		now = c.now
	}
	if c.read != nil {
		read = c.read
	}
	if flavor == console.FlavorMariaDB {
		// Up to date on an exact match, else unknown (capture_status_mariadb.go).
		read = captureHeadFromDBsMariaDB
		if c.readMariaDB != nil {
			read = c.readMariaDB
		}
	}
	// The flavor too: an edit of the flavor alone changes which read runs.
	key := e.DSN + "\x00" + source + "\x00" + flavor

	c.mu.Lock()
	if c.slots == nil {
		c.slots = map[string]*captureSlot{}
	}
	slot := c.slots[e.ID]
	if slot == nil || slot.key != key {
		slot = &captureSlot{key: key}
		c.slots[e.ID] = slot
	}
	if slot.has && now().Sub(slot.at) < slot.ttl {
		a := slot.answer
		c.mu.Unlock()
		return a
	}
	if wait := slot.flight; wait != nil {
		// One read per server at a time: this request takes the answer of
		// the read already on its way.
		c.mu.Unlock()
		select {
		case <-wait:
			c.mu.Lock()
			a, has := slot.answer, slot.has
			c.mu.Unlock()
			if has {
				return a
			}
			return unknown("the source was not read")
		case <-ctx.Done():
			return unknown("the request that asked was cancelled")
		}
	}
	done := make(chan struct{})
	slot.flight = done
	prev := slot.prev
	c.mu.Unlock()
	// Deferred, so that whatever happens below, the next load reads again
	// instead of waiting on a read that is over.
	defer func() {
		c.mu.Lock()
		if slot.flight == done {
			slot.flight = nil
		}
		c.mu.Unlock()
		close(done)
	}()

	// Detached from the request: a tab that closes mid-read must not leave
	// "the request was cancelled" behind as the state of the capture.
	r := boundedCaptureProbe(context.WithoutCancel(ctx), windowProbeTimeout, func(ctx context.Context) (r captureProbeResult) {
		// This process is the capture too, and the read runs on a goroutine
		// of its own: a panic here would end the daemon over a line on a
		// page. It is an answer instead, unknown like any read that failed.
		defer func() {
			if p := recover(); p != nil {
				slog.Error("capture status: the read of the source panicked", "server", e.Name, "id", e.ID, "panic", p, "stack", string(debug.Stack()))
				r = captureProbeResult{detail: "the source could not be read", cause: "panic"}
			}
		}()
		return read(ctx, e.DSN, source)
	})
	at := now()
	answer, next := captureStatusFrom(r, prev, at)
	answer.ServerID = e.ID

	c.mu.Lock()
	slot.answer, slot.at, slot.has, slot.prev = answer, at, true, next
	slot.ttl = captureStatusTTL
	if answer.RetryInSeconds > 0 {
		slot.ttl = captureStatusPendingTTL
	}
	// Once per change of answer, not once per read: the page asks for as
	// long as it is open.
	said := answer.State + "|" + answer.Detail + "|" + r.cause
	report := slot.logged != said
	slot.logged = said
	c.mu.Unlock()

	if report {
		reportCaptureStatus(e, answer, r.cause)
	}
	return answer
}

// bootCaptureServerID is the id the console gives the daemon's own index
// entry.
const bootCaptureServerID = "default"

// reportCaptureStatus says in the log what the page was told. Warn when a
// read failed or the source is ahead; Debug for the rest, which is either
// good news or holds for the server's lifetime.
func reportCaptureStatus(e console.ServerEntry, a console.CaptureStatus, cause string) {
	args := []any{"server", e.Name, "id", e.ID, "state", a.State}
	if a.Detail != "" {
		args = append(args, "reason", a.Detail)
	}
	if cause != "" {
		args = append(args, "error", cause)
	}
	if cause != "" || a.State == console.CaptureStateBehind {
		slog.Warn("capture status: the source was asked whether capture is caught up", args...)
		return
	}
	slog.Debug("capture status: the source was asked whether capture is caught up", args...)
}

// captureStatusFrom is the answer for one read r, pure: prev is the sample
// an earlier read left (nil: none), and the sample returned is the one the
// next read is compared with.
//
// Up to date is compareGTIDSets' caught up and nothing else: the two sets
// EQUAL (#1791). Behind needs every one of these, and anything short of
// them is unknown:
//
//   - the source holds only transactions of the kind capture records. A
//     tagged GTID (MySQL 8.3 and later) is never recorded, so a source with
//     one stays ahead of a capture that has read everything;
//   - the source is ahead by transactions capture can read. Its purged set
//     (@@GLOBAL.gtid_purged) counts as executed and no binlog carries it:
//     a dump from another server loaded with its SET @@GLOBAL.gtid_purged
//     leaves the source ahead forever. Those are left out of the count;
//     they never make anything up to date;
//   - the source is ahead by more than one transaction. A statement that
//     commits on its own and carries no rows (a GRANT, CREATE VIEW, an empty
//     transaction) is recorded when the NEXT transaction arrives, so after
//     one of those a quiet source stays one transaction ahead for as long
//     as it is quiet;
//   - an earlier read exists, the capture has saved its position since
//     (captureConfirmGap after it), and that position still does not hold
//     what the source had at the earlier read. A capture that has reached
//     it is reading, however far ahead the source is by now.
func captureStatusFrom(r captureProbeResult, prev *captureSample, now time.Time) (console.CaptureStatus, *captureSample) {
	unknown := func(detail string) console.CaptureStatus {
		return console.CaptureStatus{State: console.CaptureStateUnknown, Detail: detail}
	}
	switch r.verdict {
	case console.CaptureCaughtUp:
		return console.CaptureStatus{State: console.CaptureStateUpToDate}, &captureSample{executed: r.executed, at: now}
	case console.CaptureBehind:
	default:
		// Nothing was learned about the source: the earlier sample stays.
		detail := r.detail
		if detail == "" {
			detail = "the source was not read"
		}
		return unknown(detail), prev
	}
	have, wrote, ok := parseGTIDPair(r.captured, r.executed)
	if !ok {
		return unknown("the GTID sets do not parse"), prev
	}
	if hasTaggedGTIDs(wrote) {
		return unknown("the source has tagged GTIDs, which capture does not record"), prev
	}
	// What capture can ever hold: what it has, and what the source purged
	// without a binlog to read it from. Only "behind" is judged against it;
	// up to date was settled above on the two sets as they are.
	reachable, ok := withPurged(have, r.purged)
	if !ok || !wrote.Contain(reachable) {
		return unknown("the source's purged GTID set does not fit its executed set"), prev
	}
	if reachable.Equal(wrote) {
		return unknown("the source is ahead only by transactions it purged, which no binlog carries for capture to read"), prev
	}
	if countGTIDs(wrote)-countGTIDs(reachable) <= 1 {
		return unknown("the source is one transaction ahead, which may be a statement capture records with the next one"), prev
	}
	sample := &captureSample{executed: r.executed, purged: r.purged, at: now}
	if prev == nil {
		a := unknown("the source looked ahead on a first read, and is asked again")
		a.RetryInSeconds = int(captureStatusRetry / time.Second)
		return a, sample
	}
	earlier, err := gomysql.ParseMysqlGTIDSet(prev.executed)
	if err != nil {
		return unknown("the earlier read of the source does not parse"), sample
	}
	// Against the purged set of the EARLIER read: a transaction purged
	// since then that capture never read is a loss, not a catch-up.
	reached, ok := withPurged(have, prev.purged)
	if !ok {
		return unknown("the earlier read of the source does not parse"), sample
	}
	if reached.Contain(earlier) {
		return unknown("the source keeps writing and capture is reading it"), sample
	}
	if r.checkpoint.Sub(prev.at) < captureConfirmGap {
		// The earlier sample stays: it is what a newer position is held to.
		a := unknown("capture has not saved its position since the source was first asked")
		a.RetryInSeconds = int(captureStatusRetry / time.Second)
		return a, prev
	}
	return console.CaptureStatus{State: console.CaptureStateBehind, Detail: r.detail}, prev
}

// withPurged is have plus the purged set, as a new set; false when purged
// does not parse. An empty purged set adds nothing.
func withPurged(have *gomysql.MysqlGTIDSet, purged string) (*gomysql.MysqlGTIDSet, bool) {
	u, ok := have.Clone().(*gomysql.MysqlGTIDSet)
	if !ok {
		return nil, false
	}
	if err := u.Update(strings.Join(strings.Fields(purged), "")); err != nil {
		return nil, false
	}
	return u, true
}

// parseGTIDPair parses the capture's set and the source's.
func parseGTIDPair(captured, executed string) (have, wrote *gomysql.MysqlGTIDSet, ok bool) {
	h, err := gomysql.ParseMysqlGTIDSet(captured)
	if err != nil {
		return nil, nil, false
	}
	w, err := gomysql.ParseMysqlGTIDSet(executed)
	if err != nil {
		return nil, nil, false
	}
	have, okh := h.(*gomysql.MysqlGTIDSet)
	wrote, okw := w.(*gomysql.MysqlGTIDSet)
	return have, wrote, okh && okw
}

// hasTaggedGTIDs reports whether s holds a transaction with a tag.
func hasTaggedGTIDs(s *gomysql.MysqlGTIDSet) bool {
	for _, tags := range *s {
		for tag := range tags {
			if tag.String() != "" {
				return true
			}
		}
	}
	return false
}

// countGTIDs is the number of transactions in s.
func countGTIDs(s *gomysql.MysqlGTIDSet) int64 {
	var n int64
	for _, tags := range *s {
		for _, intervals := range tags {
			for _, in := range intervals {
				n += in.Stop - in.Start // Stop is one past the last
			}
		}
	}
	return n
}

// captureHeadFromDBs is captureFromDBs for the capture status: the same
// connections, bounds and scrubbing, and compareWithSource for the read,
// but refusing only on what checkpointComparable refuses.
func captureHeadFromDBs(ctx context.Context, indexDSN, sourceDSN string) captureProbeResult {
	idx, err := config.Connect(probeDSN(indexDSN))
	if err != nil {
		return captureProbeResult{detail: "the index did not answer", cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	defer idx.Close()
	st, err := status.LoadStreamState(ctx, idx)
	if err != nil {
		return captureProbeResult{detail: "the capture's checkpoint could not be read", cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	r, err := headFromState(ctx, idx, st, func() (*sql.DB, error) {
		return config.Connect(sourceProbeDSN(sourceDSN))
	})
	if err != nil {
		r.cause = config.ScrubDSNError(err, indexDSN, sourceDSN)
	}
	return r
}

// headFromState is the capture status read over an index connection and a
// stream_state already read from it: no connection to production for a
// verdict the checkpoint settles on its own.
func headFromState(ctx context.Context, idx *sql.DB, st *status.StreamStateInfo, openSource func() (*sql.DB, error)) (captureProbeResult, error) {
	if ok, detail := checkpointComparable(st); !ok {
		return captureProbeResult{detail: detail}, nil
	}
	return compareWithSource(ctx, idx, st, openSource, readExecutedAndPurgedGTIDs)
}
