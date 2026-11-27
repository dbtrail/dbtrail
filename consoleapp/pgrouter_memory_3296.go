package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// Whether the PostgreSQL router's memory fits the machine (#3296).
//
// Three things on one host are each sized on their own: the router's
// statements on the copy (its workers, each up to the SQL memory), its
// tails (a fifth of what it plans with), and the index MySQL's buffer pool
// (a quarter of the machine in compose). Each one is reasonable alone, and
// nothing added them up: on an 8 GB host, 2 workers at 2 GB, the tails and a
// 3 GB pool passed what the host had, it swapped for hours, and the kernel's
// OOM killer took DuckDB workers and then the index MySQL, three times. The
// same setup on 16 GB ran with 4 GB free.
//
// What is added up is each part's CEILING, not what it uses now: memory_limit
// is a cap, and a statement under it can still take that much at the moment
// the others do. The router says what it adds up, and what fits, at start
// and in /status; it changes nothing, and it never refuses to start.
//
// What is compared with what:
//   - With no ceiling of its own (the default: no mem_limit, no
//     BINTRAIL_MEMORY_SHARE), the router shares the machine with capture and
//     the index MySQL. Its statements, its tails, the results it holds for
//     clients and the buffer pool of an index MySQL on the same machine are
//     compared with the machine's memory. Past it, the kernel chooses what
//     to stop, and the index MySQL is a candidate.
//   - With a ceiling of its own the kernel enforces (a container's limit,
//     PG_ROUTER_MEMORY), its statements, tails and results are compared with
//     that ceiling, and the buffer pool is not: it is outside it. Past it,
//     the router's own statements are stopped, not the index.
//     BINTRAIL_MEMORY_SHARE is not such a ceiling: nothing enforces it.
//
// The tails are counted once per PostgreSQL server: each server's tails
// have a budget of their own (tail_wire.go, tailBytesShare).
//
// The daemon's snapshot work is left out of the sum on purpose: its 40%
// share is a plan for work that runs a few minutes an hour, and counting it
// at its full plan would warn on the 16 GB host that ran with 4 GB free.

// pgMemoryPerSlot is the memory one statement on the copy may take: the SQL
// memory DuckDB is given, and the headroom for what memory_limit does not
// count (the Go runtime, the driver's copy of a value, DuckDB's allocations
// outside its buffer manager). The same rule as the worker's own memory
// cgroup (internal/sqlsandbox fenceBytes), applied whether or not that
// cgroup is on: DuckDB allocates outside memory_limit either way.
func pgMemoryPerSlot(memory string) (int64, error) {
	mem, err := cliutil.ParseByteSize(memory)
	if err != nil {
		return 0, fmt.Errorf("the SQL memory %q does not parse: %w", memory, err)
	}
	if mem <= 0 {
		return 0, fmt.Errorf("the SQL memory %q is not a size", memory)
	}
	return mem + max(mem/4, pgMemoryMinHeadroom), nil
}

// pgMemoryMinHeadroom is sqlsandbox's fenceMinHeadroom.
const pgMemoryMinHeadroom = 256 << 20

// pgMemoryLimitFor is the largest SQL memory whose statement, headroom
// included, takes at most per bytes, in 128 MiB steps; 0 when none of at
// least console.SQLMemoryFloor (the least the router starts with) does.
func pgMemoryLimitFor(per int64) int64 {
	const step = 128 << 20
	l := min(per*4/5, per-pgMemoryMinHeadroom)
	l = l / step * step
	if l < console.SQLMemoryFloor {
		// The router refuses a SQL memory under the floor at start.
		return 0
	}
	return l
}

// pgPoolPlace is where an index MySQL runs, as far as the router can tell.
type pgPoolPlace string

const (
	// pgPoolBundled: compose's index-mysql service, on this host.
	pgPoolBundled pgPoolPlace = "bundled"
	// pgPoolThisMachine: a MySQL on loopback whose @@hostname is this one's.
	pgPoolThisMachine pgPoolPlace = "this_machine"
	// pgPoolElsewhere: another machine, or one the router cannot confirm is
	// this one. Its memory is not this machine's.
	pgPoolElsewhere pgPoolPlace = "elsewhere"
	// pgPoolUnread: an index on loopback that did not answer, so neither
	// its buffer pool nor whether it is this machine's is known.
	pgPoolUnread pgPoolPlace = "unread"
)

// pgIndexPool is one index MySQL's buffer pool and where it runs. Unread:
// the index did not answer, so Bytes is unknown; Place is where it may be
// (bundled from the DSN alone, unread for a loopback one).
type pgIndexPool struct {
	Bytes  uint64
	Place  pgPoolPlace
	Unread bool
}

// pgBundledIndexHost is the compose service the bundled index MySQL runs as
// (internal/doctor bundledIndexHost).
const pgBundledIndexHost = "index-mysql"

// pgPoolPlaceOf decides where the index of dsn runs, the way the doctor's
// buffer pool check does: compose's service name is this host; a loopback
// address is this machine only when the server's @@hostname is this one's
// (a port forward on 127.0.0.1 reaches another); anything else is
// elsewhere. serverHost is the server's @@hostname, "" unread.
func pgPoolPlaceOf(dsn, serverHost, localHost string) pgPoolPlace {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return pgPoolElsewhere
	}
	host := cfg.Addr
	if cfg.Net == "unix" {
		host = "localhost"
	} else if h, _, err := net.SplitHostPort(cfg.Addr); err == nil {
		host = h
	}
	if strings.EqualFold(host, pgBundledIndexHost) {
		return pgPoolBundled
	}
	ip := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
		return pgPoolElsewhere
	}
	if serverHost == "" || localHost == "" || !pgSameHostname(serverHost, localHost) {
		return pgPoolElsewhere
	}
	return pgPoolThisMachine
}

// pgSameHostname compares two host names, case aside, and by their first
// label when one is a FQDN and the other is not (internal/doctor
// sameHostname).
func pgSameHostname(a, b string) bool {
	first := func(s string) string {
		if i := strings.IndexByte(s, '.'); i > 0 {
			return s[:i]
		}
		return s
	}
	return strings.EqualFold(a, b) || strings.EqualFold(first(a), first(b))
}

// pgDSNKind says, from the DSN alone, where an index may run: compose's
// index-mysql ("bundled"), loopback or a socket ("loopback", this machine
// only if its @@hostname says so), anything else ("remote"), or "" when the
// DSN does not parse.
func pgDSNKind(dsn string) string {
	switch pgPoolPlaceOf(dsn, "x", "x") {
	case pgPoolBundled:
		return "bundled"
	case pgPoolThisMachine:
		return "loopback"
	}
	if _, err := mysql.ParseDSN(dsn); err != nil {
		return ""
	}
	return "remote"
}

// pgReadIndexPool reads the buffer pool of the index of dsn, on a connection
// of its own that is closed after, within ctx. An index on another machine
// is not asked: its memory is not this one's. One that may be on this
// machine and does not answer is Unread, never a guess, and the log says
// why at debug level.
func pgReadIndexPool(ctx context.Context, dsn string) pgIndexPool {
	kind := pgDSNKind(dsn)
	if kind == "remote" || kind == "" {
		return pgIndexPool{Place: pgPoolElsewhere}
	}
	unread := pgIndexPool{Place: pgPoolUnread, Unread: true}
	if kind == "bundled" {
		unread.Place = pgPoolBundled
	}
	addr := ""
	if cfg, err := mysql.ParseDSN(dsn); err == nil {
		addr = cfg.Addr
	}
	db, err := config.Open(dsn)
	if err != nil {
		slog.Debug("PostgreSQL router: the index MySQL's buffer pool could not be read", "addr", addr, "error", err)
		return unread
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var pool uint64
	var host string
	if err := db.QueryRowContext(ctx, "SELECT @@innodb_buffer_pool_size, @@hostname").Scan(&pool, &host); err != nil {
		slog.Debug("PostgreSQL router: the index MySQL's buffer pool could not be read", "addr", addr, "error", err)
		return unread
	}
	if kind == "bundled" {
		return pgIndexPool{Bytes: pool, Place: pgPoolBundled}
	}
	local, err := os.Hostname()
	if err != nil || local == "" {
		// Cannot say whether the loopback index is this machine's.
		slog.Debug("PostgreSQL router: this machine's host name could not be read", "error", err)
		return unread
	}
	return pgIndexPool{Bytes: pool, Place: pgPoolPlaceOf(dsn, host, local)}
}

// pgMemoryPlan is everything the verdict adds up.
type pgMemoryPlan struct {
	// Workers is --workers; the copy runs one statement more (pgCopySlots).
	Workers int
	// Servers is how many PostgreSQL servers are listed: each one's tails
	// have their own budget. Any of them can be switched to fresh mode
	// while the router runs, so all are counted.
	Servers int
	// Memory is the SQL memory each statement is given, as set.
	Memory string
	// ResultsBytes is what the router holds for clients that have not read
	// their answers yet (defaultFlashbackResultBudget).
	ResultsBytes int64
	In           console.PGMemoryInputs
	Pools        []pgIndexPool
}

// pgMemoryDoc is the verdict, in /status as "memory" and in the start-up
// lines.
type pgMemoryDoc struct {
	// Verdict: "fits"; "over"; "idle" when no PostgreSQL server is listed
	// (nothing runs on the copy, so nothing is said); "unknown" when the
	// machine's memory or the SQL memory cannot be read, or the buffer pool
	// of an index that may run on this machine could not be read and the
	// rest fits (Why says which).
	Verdict string `json:"verdict"`
	Why     string `json:"why,omitempty"`
	// ComparedWith: "machine" (the machine it shares with capture and the
	// index MySQL) or "own_ceiling" (a container's limit, PG_ROUTER_MEMORY,
	// which the router's own statements, tails and results passed).
	ComparedWith   string `json:"compared_with,omitempty"`
	AvailableBytes uint64 `json:"available_bytes,omitempty"`
	// NeedBytes is the sum of the parts below that are compared.
	NeedBytes uint64 `json:"need_bytes,omitempty"`
	// OwnCeilingBytes: the container's limit, when there is one.
	OwnCeilingBytes uint64 `json:"own_ceiling_bytes,omitempty"`
	// Statements: Slots at once, StatementBytes each, headroom included.
	// SQLMemoryBytes is the SQL memory each is given, before the headroom.
	Slots          int    `json:"slots,omitempty"`
	StatementBytes uint64 `json:"statement_bytes,omitempty"`
	SQLMemoryBytes uint64 `json:"sql_memory_bytes,omitempty"`
	// TailsBytes: every server's tails together, Servers of them.
	Servers    int    `json:"servers"`
	TailsBytes uint64 `json:"tails_bytes,omitempty"`
	// TailsUnbounded: BINTRAIL_SQL_TAIL_MAX_BYTES=0; the tails are not in
	// NeedBytes, and may take more.
	TailsUnbounded bool   `json:"tails_unbounded,omitempty"`
	ResultsBytes   uint64 `json:"results_bytes,omitempty"`
	// IndexPoolBytes: the buffer pools read of the index MySQL servers on
	// this machine. IndexPool says where the indexes are: "bundled",
	// "this_machine", "elsewhere", "unread" (on loopback, not read), "none"
	// (no PostgreSQL server yet), joined by "," when they differ.
	// IndexPoolUnread: one that may be on this machine was not read.
	IndexPoolBytes  uint64 `json:"index_pool_bytes,omitempty"`
	IndexPool       string `json:"index_pool,omitempty"`
	IndexPoolUnread bool   `json:"index_pool_unread,omitempty"`
	// Advice says what fits, when the verdict is "over".
	Advice string `json:"advice,omitempty"`
}

// pgMemoryVerdict adds the plan up.
//
// With no ceiling of its own, the router's statements, tails and results
// and the index pools on this machine are compared with the machine. With
// a ceiling, the router's own part is compared with the ceiling first; if
// it fits there, the router can take at most that much, and that plus the
// index pools is compared with the machine: a ceiling as big as the
// machine protects nothing.
func pgMemoryVerdict(p pgMemoryPlan) pgMemoryDoc {
	d := pgMemoryDoc{Slots: pgCopySlots(p.Workers), Servers: p.Servers}
	per, err := pgMemoryPerSlot(p.Memory)
	if err != nil {
		d.Verdict, d.Why = "unknown", err.Error()
		return d
	}
	d.StatementBytes = uint64(per)
	if mem, err := cliutil.ParseByteSize(p.Memory); err == nil {
		d.SQLMemoryBytes = uint64(mem)
	}
	d.ResultsBytes = uint64(max(p.ResultsBytes, 0))
	d.TailsUnbounded = p.In.TailsUnbounded
	if p.In.TailsBytes > 0 {
		d.TailsBytes = uint64(p.In.TailsBytes) * uint64(max(p.Servers, 0))
	}
	places := map[pgPoolPlace]bool{}
	for _, pl := range p.Pools {
		places[pl.Place] = true
		switch {
		case pl.Unread && pl.Place != pgPoolElsewhere:
			d.IndexPoolUnread = true
		case pl.Place == pgPoolBundled || pl.Place == pgPoolThisMachine:
			d.IndexPoolBytes += pl.Bytes
		}
	}
	d.IndexPool = pgPoolPlacesText(places)
	if p.In.MachineBytes == 0 {
		d.Verdict, d.Why = "unknown", "the memory of this machine cannot be read here"
		return d
	}
	own := uint64(d.Slots)*d.StatementBytes + d.TailsBytes + d.ResultsBytes
	if p.In.OwnCeiling {
		d.OwnCeilingBytes = p.In.BudgetBytes
	}
	if p.In.OwnCeiling && p.In.BudgetBytes > 0 && own > p.In.BudgetBytes {
		d.ComparedWith, d.AvailableBytes, d.NeedBytes = "own_ceiling", p.In.BudgetBytes, own
	} else {
		d.ComparedWith, d.AvailableBytes, d.NeedBytes = "machine", p.In.MachineBytes, own+d.IndexPoolBytes
	}
	switch {
	case p.Servers <= 0:
		// No PostgreSQL server: no statement runs on the copy, and the
		// tails hold nothing. The sum is in /status; it is said once a
		// server is added (pgMemoryWatch reads it again every minute).
		d.Verdict = "idle"
	case d.NeedBytes > d.AvailableBytes:
		d.Verdict = "over"
		d.Advice = pgMemoryAdvice(d, p.Workers)
	case d.IndexPoolUnread:
		d.Verdict = "unknown"
		d.Why = "the buffer pool of an index MySQL that may run on this machine could not be read, and without it the rest fits"
	default:
		d.Verdict = "fits"
	}
	return d
}

// pgMemoryAdvice is what fits in what is left once the tails, the results
// (and, against the machine, the index's pool) are taken off: fewer workers
// at the same memory, or, when not even one worker fits, less memory each
// with one worker. Against the machine with a ceiling of its own, the
// ceiling that fits is said too.
func pgMemoryAdvice(d pgMemoryDoc, workers int) string {
	short := d.NeedBytes - d.AvailableBytes
	others := d.TailsBytes + d.ResultsBytes
	more := fmt.Sprintf("raise PG_ROUTER_MEMORY by %s", pgGiB(short))
	if d.ComparedWith == "machine" {
		others += d.IndexPoolBytes
		more = fmt.Sprintf("give the machine %s more", pgGiB(short))
		if d.OwnCeilingBytes > 0 && d.AvailableBytes > d.IndexPoolBytes {
			more = fmt.Sprintf("lower PG_ROUTER_MEMORY to at most %s, so that its statements are stopped before the machine runs out, or %s", pgGiB(d.AvailableBytes-d.IndexPoolBytes), more)
		}
	}
	if others >= d.AvailableBytes {
		if d.ComparedWith == "machine" {
			return fmt.Sprintf("the tails, the results held for clients and the index MySQL's buffer pool take %s of the machine's %s alone; give the machine more memory, or lower the index MySQL's buffer pool",
				pgGiB(others), pgGiB(d.AvailableBytes))
		}
		return fmt.Sprintf("the tails and the results held for clients take %s of its %s ceiling alone; raise PG_ROUTER_MEMORY, or lower BINTRAIL_SQL_TAIL_MAX_BYTES",
			pgGiB(others), pgGiB(d.AvailableBytes))
	}
	room := d.AvailableBytes - others
	if fit := int(room / d.StatementBytes); fit >= 2 && fit-1 < workers {
		return fmt.Sprintf("with the SQL memory as it is, %s (PG_ROUTER_WORKERS=%d in .env, or --workers %d), %s with the one kept for short statements; or %s",
			pgWorkersFit(fit-1), fit-1, fit-1, countOfStatementsAtOnce(fit), more)
	}
	if limit := pgMemoryLimitFor(int64(room / 2)); limit > 0 {
		return fmt.Sprintf("with 1 worker (PG_ROUTER_WORKERS=1 in .env, or --workers 1), each statement fits with at most %s of SQL memory (SQL_MEMORY=%s in .env, or --sql-memory %s); or %s",
			pgGiB(uint64(limit)), pgMiBSetting(limit), pgMiBSetting(limit), more)
	}
	return fmt.Sprintf("not even one statement with the least SQL memory the router takes (%s) fits beside the rest; %s", pgGiB(console.SQLMemoryFloor), more)
}

// pgMiBSetting writes a SQL memory the way --sql-memory and SQL_MEMORY take
// it: whole GiB as "2GiB", anything else in MiB.
func pgMiBSetting(b int64) string {
	if b%(1<<30) == 0 {
		return fmt.Sprintf("%dGiB", b>>30)
	}
	return fmt.Sprintf("%dMiB", b>>20)
}

func pgWorkersFit(n int) string {
	if n == 1 {
		return "1 worker fits"
	}
	return fmt.Sprintf("%d workers fit", n)
}

// countOfStatementsAtOnce is "1 statement at once" or "N statements at once".
func countOfStatementsAtOnce(n int) string {
	if n == 1 {
		return "1 statement at once"
	}
	return fmt.Sprintf("%d statements at once", n)
}

func pgPoolPlacesText(places map[pgPoolPlace]bool) string {
	if len(places) == 0 {
		return "none"
	}
	var out []string
	for _, p := range []pgPoolPlace{pgPoolBundled, pgPoolThisMachine, pgPoolElsewhere, pgPoolUnread} {
		if places[p] {
			out = append(out, string(p))
		}
	}
	return strings.Join(out, ",")
}

func pgTailsServers(n int) string {
	if n == 1 {
		return "1 PostgreSQL server"
	}
	return fmt.Sprintf("%d PostgreSQL servers, a budget each", n)
}

// pgGiB writes bytes as GiB with one decimal, or as whole MiB under 1 GiB.
func pgGiB(b uint64) string {
	if b < 1<<30 {
		return fmt.Sprintf("%d MiB", b>>20)
	}
	return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
}

// pgMemoryLines is the verdict as start-up lines: one that says what was
// added up, and, when it is over, the warning and what fits.
func pgMemoryLines(d pgMemoryDoc) string {
	switch {
	case d.Verdict == "idle":
		return "PostgreSQL router: no PostgreSQL server is listed yet, so whether its memory fits this machine is said once one is (in /status, and in the log if it does not fit).\n"
	case d.Verdict == "unknown" && d.AvailableBytes == 0:
		return "PostgreSQL router: whether its memory fits this machine is not said: " + d.Why + ".\n"
	}
	var parts []string
	parts = append(parts, fmt.Sprintf("%s at %s each (%s of SQL memory and its headroom)", countOfStatementsAtOnce(d.Slots), pgGiB(d.StatementBytes), pgGiB(d.SQLMemoryBytes)))
	switch {
	case d.TailsUnbounded:
		parts = append(parts, "the tails, which have no bound (BINTRAIL_SQL_TAIL_MAX_BYTES=0) and are not counted")
	case d.TailsBytes > 0:
		parts = append(parts, fmt.Sprintf("the tails' %s (%s)", pgGiB(d.TailsBytes), pgTailsServers(d.Servers)))
	}
	parts = append(parts, fmt.Sprintf("the %s of results held for clients", pgGiB(d.ResultsBytes)))
	var b strings.Builder
	if d.ComparedWith == "machine" {
		parts = append(parts, pgPoolPart(d))
		ceiling := ""
		if d.OwnCeilingBytes > 0 {
			ceiling = fmt.Sprintf(" (it fits its own %s ceiling, which does not keep the machine from running out)", pgGiB(d.OwnCeilingBytes))
		}
		fmt.Fprintf(&b, "PostgreSQL router: at most it and the index MySQL on this machine may need %s of this machine's %s%s: %s.\n", pgGiB(d.NeedBytes), pgGiB(d.AvailableBytes), ceiling, strings.Join(parts, ", "))
	} else {
		fmt.Fprintf(&b, "PostgreSQL router: at most it may need %s of its own %s ceiling: %s.\n", pgGiB(d.NeedBytes), pgGiB(d.AvailableBytes), strings.Join(parts, ", "))
	}
	switch d.Verdict {
	case "unknown":
		fmt.Fprintf(&b, "PostgreSQL router: whether that fits is not said: %s. It is read again every minute (/status).\n", d.Why)
	case "over":
		if d.ComparedWith == "machine" {
			fmt.Fprintf(&b, "WARNING: PostgreSQL router: that is %s more than this machine has. Under heavy reports the machine swaps and the kernel stops processes for memory, and it can stop the index MySQL, which capture writes to. Advice: %s.\n",
				pgGiB(d.NeedBytes-d.AvailableBytes), d.Advice)
		} else {
			fmt.Fprintf(&b, "WARNING: PostgreSQL router: that is %s more than its ceiling. Under heavy reports its own statements are stopped for memory. Advice: %s.\n",
				pgGiB(d.NeedBytes-d.AvailableBytes), d.Advice)
		}
	}
	return b.String()
}

func pgPoolPart(d pgMemoryDoc) string {
	switch {
	case d.IndexPool == "none":
		return "no index MySQL yet (no PostgreSQL server)"
	case d.IndexPoolBytes == 0 && !d.IndexPoolUnread:
		return "no index MySQL on this machine"
	case d.IndexPoolBytes == 0:
		return "the buffer pool of the index MySQL, which could not be read and is not counted"
	}
	s := fmt.Sprintf("the %s buffer pool of the index MySQL on this machine", pgGiB(d.IndexPoolBytes))
	if d.IndexPoolUnread {
		s += " (another one could not be read and is not counted)"
	}
	return s
}

// pgMemoryWatch keeps the verdict for /status. follow reads it again every
// minute, outside any request: the buffer pool can be resized online, and a
// PostgreSQL server can be added while the router runs. /status only takes
// the last one, so an index that does not answer never slows it. The SQL
// memory and the workers are the ones applied at start.
type pgMemoryWatch struct {
	plan  func(ctx context.Context) pgMemoryPlan
	every time.Duration

	mu   sync.Mutex
	last *pgMemoryDoc
}

func newPGMemoryWatch(plan func(ctx context.Context) pgMemoryPlan) *pgMemoryWatch {
	return &pgMemoryWatch{plan: plan, every: time.Minute}
}

// doc is the last verdict read, nil before the first.
func (w *pgMemoryWatch) doc() *pgMemoryDoc {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last
}

// read reads the verdict now, outside the lock, and keeps it. The log says
// it when it becomes over: at start, or when a server is added or a buffer
// pool grows while the router runs.
func (w *pgMemoryWatch) read() *pgMemoryDoc {
	d := pgMemoryVerdict(w.plan(context.Background()))
	w.mu.Lock()
	was := w.last
	w.last = &d
	w.mu.Unlock()
	if d.Verdict == "over" && (was == nil || was.Verdict != "over") {
		logPGMemory(&d)
	}
	return &d
}

// follow reads the verdict again every minute until ctx ends.
func (w *pgMemoryWatch) follow(ctx context.Context) {
	t := time.NewTicker(w.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.read()
		}
	}
}

// pgMemoryReadWait bounds the read of each index server's buffer pool; the
// servers are read at once, so one that does not answer does not use up
// another's time.
const pgMemoryReadWait = 2 * time.Second

// pgMemoryPlanFor is the plan of a router with these workers and memory over
// srv's PostgreSQL servers.
func pgMemoryPlanFor(workers int, memory string, srv interface {
	PostgresIndexDSNs() []string
	PostgresServerIDs() []string
}) func(ctx context.Context) pgMemoryPlan {
	return func(ctx context.Context) pgMemoryPlan {
		p := pgMemoryPlan{Workers: workers, Servers: len(srv.PostgresServerIDs()), Memory: memory, ResultsBytes: defaultFlashbackResultBudget, In: console.PGMemoryInputsNow()}
		dsns := srv.PostgresIndexDSNs()
		p.Pools = make([]pgIndexPool, len(dsns))
		var wg sync.WaitGroup
		for i, dsn := range dsns {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rctx, cancel := context.WithTimeout(ctx, pgMemoryReadWait)
				defer cancel()
				p.Pools[i] = pgReadIndexPool(rctx, dsn)
			}()
		}
		wg.Wait()
		return p
	}
}

// logPGMemory writes the verdict to the log as well: a WARN when it is over.
func logPGMemory(d *pgMemoryDoc) {
	if d == nil || d.Verdict != "over" {
		return
	}
	slog.Warn("PostgreSQL router: its statements, tails and the index MySQL may need more memory than there is",
		"need", pgGiB(d.NeedBytes), "available", pgGiB(d.AvailableBytes), "compared_with", d.ComparedWith, "advice", d.Advice)
}
