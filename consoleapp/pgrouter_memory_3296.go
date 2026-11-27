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
//   - With a ceiling of its own, its statements, tails and results are
//     compared with that ceiling, and the buffer pool is not: it is outside
//     it. Past it, the router's own statements are stopped, not the index.
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
// least 256 MiB does.
func pgMemoryLimitFor(per int64) int64 {
	const step = 128 << 20
	l := min(per*4/5, per-pgMemoryMinHeadroom)
	l = l / step * step
	if l < 2*step {
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
	// pgPoolUnread: the index did not answer, so its buffer pool is unknown.
	pgPoolUnread pgPoolPlace = "unread"
)

// pgIndexPool is one index MySQL's buffer pool and where it runs.
type pgIndexPool struct {
	Bytes uint64
	Place pgPoolPlace
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

// pgReadIndexPool reads the buffer pool of the index of dsn, on a connection
// of its own that is closed after, within ctx. A server that does not answer
// is pgPoolUnread, never a guess.
func pgReadIndexPool(ctx context.Context, dsn string) pgIndexPool {
	db, err := config.Open(dsn)
	if err != nil {
		return pgIndexPool{Place: pgPoolUnread}
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var pool uint64
	var host string
	if err := db.QueryRowContext(ctx, "SELECT @@innodb_buffer_pool_size, @@hostname").Scan(&pool, &host); err != nil {
		return pgIndexPool{Place: pgPoolUnread}
	}
	local, _ := os.Hostname()
	return pgIndexPool{Bytes: pool, Place: pgPoolPlaceOf(dsn, host, local)}
}

// pgMemoryPlan is everything the verdict adds up.
type pgMemoryPlan struct {
	// Workers is --workers; the copy runs one statement more (pgCopySlots).
	Workers int
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
	// Verdict: "fits", "over", or "unknown" when the machine's memory or the
	// SQL memory cannot be read (Why says which).
	Verdict string `json:"verdict"`
	Why     string `json:"why,omitempty"`
	// ComparedWith: "machine" (no ceiling of its own: the machine it shares
	// with capture and the index MySQL) or "own_ceiling".
	ComparedWith   string `json:"compared_with,omitempty"`
	AvailableBytes uint64 `json:"available_bytes,omitempty"`
	// NeedBytes is the sum of the parts below.
	NeedBytes uint64 `json:"need_bytes,omitempty"`
	// Statements: Slots at once, StatementBytes each, headroom included.
	Slots          int    `json:"slots,omitempty"`
	StatementBytes uint64 `json:"statement_bytes,omitempty"`
	TailsBytes     uint64 `json:"tails_bytes,omitempty"`
	// TailsUnbounded: BINTRAIL_SQL_TAIL_MAX_BYTES=0; the tails are not in
	// NeedBytes, and may take more.
	TailsUnbounded bool   `json:"tails_unbounded,omitempty"`
	ResultsBytes   uint64 `json:"results_bytes,omitempty"`
	// IndexPoolBytes: the buffer pools of the index MySQL servers on this
	// machine, counted only when compared with the machine. IndexPool says
	// where they are: "bundled", "this_machine", "elsewhere", "unread",
	// "none" (no PostgreSQL server yet), joined by "," when they differ.
	IndexPoolBytes uint64 `json:"index_pool_bytes,omitempty"`
	IndexPool      string `json:"index_pool,omitempty"`
	// Advice says what fits, when the verdict is "over".
	Advice string `json:"advice,omitempty"`
}

// pgMemoryVerdict adds the plan up.
func pgMemoryVerdict(p pgMemoryPlan) pgMemoryDoc {
	d := pgMemoryDoc{Slots: pgCopySlots(p.Workers)}
	per, err := pgMemoryPerSlot(p.Memory)
	if err != nil {
		d.Verdict, d.Why = "unknown", err.Error()
		return d
	}
	d.StatementBytes = uint64(per)
	d.ResultsBytes = uint64(max(p.ResultsBytes, 0))
	d.TailsUnbounded = p.In.TailsUnbounded
	if p.In.TailsBytes > 0 {
		d.TailsBytes = uint64(p.In.TailsBytes)
	}
	places := map[pgPoolPlace]bool{}
	var pool uint64
	for _, pl := range p.Pools {
		places[pl.Place] = true
		if pl.Place == pgPoolBundled || pl.Place == pgPoolThisMachine {
			pool += pl.Bytes
		}
	}
	d.IndexPool = pgPoolPlacesText(places)
	if p.In.OwnCeiling {
		d.ComparedWith, d.AvailableBytes = "own_ceiling", p.In.BudgetBytes
	} else {
		d.ComparedWith, d.AvailableBytes = "machine", p.In.MachineBytes
		d.IndexPoolBytes = pool
	}
	if d.AvailableBytes == 0 {
		d.Verdict, d.Why = "unknown", "the memory of this machine cannot be read here"
		d.ComparedWith = ""
		return d
	}
	others := d.TailsBytes + d.ResultsBytes + d.IndexPoolBytes
	d.NeedBytes = uint64(d.Slots)*d.StatementBytes + others
	if d.NeedBytes <= d.AvailableBytes {
		d.Verdict = "fits"
		return d
	}
	d.Verdict = "over"
	d.Advice = pgMemoryAdvice(d, p.Workers, others)
	return d
}

// pgMemoryAdvice is what fits in what is left once the tails, the results
// and the index's pool are taken off: fewer workers at the same memory, or,
// when not even one worker fits, less memory each with one worker.
func pgMemoryAdvice(d pgMemoryDoc, workers int, others uint64) string {
	if others >= d.AvailableBytes {
		return fmt.Sprintf("the tails, the results held for clients and the index MySQL's buffer pool take %s of the %s alone; give the machine more memory, or lower the index MySQL's buffer pool",
			pgGiB(others), pgGiB(d.AvailableBytes))
	}
	room := d.AvailableBytes - others
	if fit := int(room / d.StatementBytes); fit >= 2 && fit-1 < workers {
		return fmt.Sprintf("at %s of SQL memory each, %s fit (PG_ROUTER_WORKERS=%d in .env, or --workers %d), or give the machine %s more",
			pgGiB(d.StatementBytes), countOfStatementsAtOnce(fit-1), fit-1, fit-1, pgGiB(d.NeedBytes-d.AvailableBytes))
	}
	if limit := pgMemoryLimitFor(int64(room / 2)); limit > 0 {
		return fmt.Sprintf("with 1 worker (PG_ROUTER_WORKERS=1 in .env, or --workers 1), each statement fits with at most %s of SQL memory (SQL_MEMORY=%s in .env, or --sql-memory %s), or give the machine %s more",
			pgGiB(uint64(limit)), pgMiBSetting(limit), pgMiBSetting(limit), pgGiB(d.NeedBytes-d.AvailableBytes))
	}
	return fmt.Sprintf("not even one statement fits beside the rest; give the machine %s more", pgGiB(d.NeedBytes-d.AvailableBytes))
}

// pgMiBSetting writes a SQL memory the way --sql-memory and SQL_MEMORY take
// it: whole GiB as "2GiB", anything else in MiB.
func pgMiBSetting(b int64) string {
	if b%(1<<30) == 0 {
		return fmt.Sprintf("%dGiB", b>>30)
	}
	return fmt.Sprintf("%dMiB", b>>20)
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

// pgGiB writes bytes as GiB with one decimal.
func pgGiB(b uint64) string {
	return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
}

// pgMemoryLines is the verdict as start-up lines: one that says what was
// added up, and, when it is over, the warning and what fits.
func pgMemoryLines(d pgMemoryDoc) string {
	if d.Verdict == "unknown" {
		return "PostgreSQL router: whether its memory fits this machine is not said: " + d.Why + ".\n"
	}
	var parts []string
	parts = append(parts, fmt.Sprintf("%d statements at %s each, headroom included", d.Slots, pgGiB(d.StatementBytes)))
	switch {
	case d.TailsUnbounded:
		parts = append(parts, "the tails, which have no bound (BINTRAIL_SQL_TAIL_MAX_BYTES=0) and are not counted")
	case d.TailsBytes > 0:
		parts = append(parts, fmt.Sprintf("the tails' %s", pgGiB(d.TailsBytes)))
	}
	parts = append(parts, fmt.Sprintf("the %s of results held for clients", pgGiB(d.ResultsBytes)))
	var b strings.Builder
	if d.ComparedWith == "machine" {
		parts = append(parts, pgPoolPart(d))
		fmt.Fprintf(&b, "PostgreSQL router: at most it may need %s of this machine's %s: %s.\n", pgGiB(d.NeedBytes), pgGiB(d.AvailableBytes), strings.Join(parts, ", "))
	} else {
		fmt.Fprintf(&b, "PostgreSQL router: at most it may need %s of its own %s ceiling: %s.\n", pgGiB(d.NeedBytes), pgGiB(d.AvailableBytes), strings.Join(parts, ", "))
	}
	if d.Verdict != "over" {
		return b.String()
	}
	if d.ComparedWith == "machine" {
		fmt.Fprintf(&b, "WARNING: PostgreSQL router: that is %s more than this machine has. Under heavy reports the machine swaps and the kernel stops processes for memory, and it can stop the index MySQL, which capture writes to. Advice: %s. Or give this service a ceiling of its own (PG_ROUTER_MEMORY in .env), so that its statements are stopped first.\n",
			pgGiB(d.NeedBytes-d.AvailableBytes), d.Advice)
	} else {
		fmt.Fprintf(&b, "WARNING: PostgreSQL router: that is %s more than its ceiling. Under heavy reports its own statements are stopped for memory. Advice: %s.\n",
			pgGiB(d.NeedBytes-d.AvailableBytes), strings.Replace(d.Advice, "give the machine", "raise the ceiling by", 1))
	}
	return b.String()
}

func pgPoolPart(d pgMemoryDoc) string {
	switch d.IndexPool {
	case "none":
		return "no index MySQL yet (no PostgreSQL server)"
	case string(pgPoolElsewhere):
		return "no index MySQL on this machine"
	case string(pgPoolUnread):
		return "the index MySQL's buffer pool, which could not be read and is not counted"
	}
	s := fmt.Sprintf("the %s buffer pool of the index MySQL on this machine", pgGiB(d.IndexPoolBytes))
	if strings.Contains(d.IndexPool, string(pgPoolUnread)) {
		s += " (another index could not be read and is not counted)"
	}
	return s
}

// pgMemoryWatch keeps the verdict for /status, read again at most once a
// minute: the buffer pool can be resized online, and a PostgreSQL server can
// be added while the router runs. The SQL memory and the workers are the
// ones applied at start.
type pgMemoryWatch struct {
	plan  func(ctx context.Context) pgMemoryPlan
	every time.Duration
	now   func() time.Time

	mu   sync.Mutex
	at   time.Time
	last *pgMemoryDoc
}

func newPGMemoryWatch(plan func(ctx context.Context) pgMemoryPlan) *pgMemoryWatch {
	return &pgMemoryWatch{plan: plan, every: time.Minute, now: time.Now}
}

// doc is the verdict now, from the last minute's.
func (w *pgMemoryWatch) doc() *pgMemoryDoc {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.last != nil && w.now().Sub(w.at) < w.every {
		return w.last
	}
	ctx, cancel := context.WithTimeout(context.Background(), pgMemoryReadWait)
	defer cancel()
	d := pgMemoryVerdict(w.plan(ctx))
	w.at, w.last = w.now(), &d
	return w.last
}

// pgMemoryReadWait bounds the read of the index servers' buffer pools.
const pgMemoryReadWait = 3 * time.Second

// pgMemoryPlanFor is the plan of a router with these workers and memory over
// srv's PostgreSQL servers.
func pgMemoryPlanFor(workers int, memory string, srv interface{ PostgresIndexDSNs() []string }) func(ctx context.Context) pgMemoryPlan {
	return func(ctx context.Context) pgMemoryPlan {
		p := pgMemoryPlan{Workers: workers, Memory: memory, ResultsBytes: defaultFlashbackResultBudget, In: console.PGMemoryInputsNow()}
		for _, dsn := range srv.PostgresIndexDSNs() {
			p.Pools = append(p.Pools, pgReadIndexPool(ctx, dsn))
		}
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
