// Package sqlsandbox runs ONE read-only SQL statement against the Parquet copy
// in a separate child process, with hard caps, killable without touching the
// parent (#1952, slice 1).
//
// Why a child process: `bintrail-console watch` is the web console AND the
// capture plane in one process. A query that exhausts memory in that process
// is a capture outage, not a slow page. DuckDB's memory_limit bounds its
// buffer manager, not the process, and a runaway query cannot be interrupted
// from Go without cooperation. A child process can simply be killed: the
// parent sets a wall-clock deadline, and on timeout or cancel kills the
// child's whole process group. Nothing the child does can reach the parent's
// memory, file handles or DuckDB sessions.
//
// Why the session is locked (all verified on DuckDB v1.4.5, the pinned 1.4
// LTS line; see lockdownStatements for what each setting blocks): the copy is
// plain Parquet on the index host's disk, next to credentials, other
// customers' backups and the operator's home directory. The worker opens an
// in-memory DuckDB, admits only the copy directories, turns external access
// off, turns extension autoload off, turns spilling off, installs the views
// the console already generates, and locks the configuration LAST so the
// user's SQL cannot undo any of it.
//
// Why a single SELECT with an allowlist of table functions:
// allowed_directories admits WRITES into the copy (COPY ... TO a path under
// it succeeds on 1.4.5, verified), so the statement shape is checked with
// DuckDB's own parser (json_serialize_sql) before anything runs. Only a
// SELECT-shaped statement (SELECT, FROM-first, VALUES, WITH, set operations,
// DESCRIBE, SHOW, SUMMARIZE) is admitted, and only one. And a SELECT is not
// read-only by shape alone: table functions such as enable_logging(...),
// checkpoint() or query('...') change state or run arbitrary text, and they
// run AFTER the lock (verified on 1.4.5). So every table function in the
// statement, at any depth, must be on an allowlist of readers.
//
// v1 reads a LOCAL copy. A copy that lives only on S3 is refused with
// ErrCopyNotLocal; the worker never loads httpfs or opens the network.
//
// One limit to know: DuckDB checks allowed_directories against the path TEXT,
// not against where it resolves, so a symbolic link planted INSIDE the copy
// that points outside is readable through it (verified on 1.4.5). The copy
// is written by the daemon and owned by the operator, so that link has to be
// put there on purpose; the parent admits a copy directory that is itself a
// link by also allowing its resolved path (a linked backup directory is an
// ordinary setup), and a link below it stays out of scope here.
//
// Why the process is also bounded from outside DuckDB: memory_limit bounds
// the buffer manager, not the process (a single huge string is built outside
// it, verified), so on Linux the worker raises its own oom_score_adj to be
// the kernel's first choice ahead of the console, cells are cut at a fixed
// size, the result is counted in bytes on both sides of the pipe, and the
// runner caps how many workers run at once.
//
// The parent API is Runner.Run. Slice 2 (the panel and its HTTP route) calls
// it with a Job built from the selected server's copy location and the views
// text from the same generator that serves /api/views.sql; slice 3 adds the
// permission gate and the audit emission around the call.
package sqlsandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
)

// WorkerCommand is the hidden subcommand the console binary registers for the
// child. The double underscore keeps it out of usage telemetry the same way
// cobra's own __complete is kept out.
const WorkerCommand = "__sql-worker"

// workerEnv marks the child process. The parent always sets it; the console's
// hidden command and the package's own test binary both read it.
const workerEnv = "BINTRAIL_SQL_SANDBOX_WORKER"

// Limits are the caps one query runs under. A zero field means "the Runner's
// default" on a Job and "the package default" on a Config.
type Limits struct {
	// Threads is DuckDB's thread count inside the worker.
	Threads int
	// MemoryLimit is DuckDB's memory budget inside the worker, e.g. "2GB".
	// Past it the worker spills to a private directory of its own, up to
	// SpillFactor times this budget (Config.SpillDir, #2210), and only past
	// that fails with an out-of-memory error.
	MemoryLimit string
	// Timeout is the wall clock for the whole child: start, views install and
	// query. Past it the process group is killed.
	Timeout time.Duration
	// MaxRows is how many result rows come back. The worker fetches one more
	// to know whether the result was cut, and reports it as Truncated.
	MaxRows int
	// MaxResultBytes bounds the encoded result the parent will read from the
	// child. Past it the child is killed and ErrResultTooLarge is returned.
	MaxResultBytes int64
}

// DefaultLimits are the caps the issue asks for: 2 threads, 2 GB, 60 s,
// 1,000 rows. The byte cap is a defensive bound on the wire, not a product
// number: 1,000 rows of 16 KiB statement texts fit with room.
func DefaultLimits() Limits {
	// 2048MiB, not "2GB": DuckDB reads GB as decimal (1.86 GiB), and the
	// unmerged-changes line SQL on the copy scales from this is in binary
	// units, as --sql-memory is (#2210).
	return Limits{Threads: 2, MemoryLimit: "2048MiB", Timeout: 60 * time.Second, MaxRows: 1000, MaxResultBytes: 64 << 20}
}

// DefaultMaxInFlight is how many workers a Runner runs at once by default:
// two, so one heavy query and one quick one can overlap on a host that also
// captures, and no more. The per-user gate is one per user on top of this.
const DefaultMaxInFlight = 2

// MaxCellBytes is the size a single cell is cut at inside the worker. A
// longer value is truncated with a marker and counted in Result.TruncatedCells.
// The cut bounds what travels over the pipe and what the parent holds, NOT
// the worker's memory: the driver materializes the full value before the cut
// (an 800 MB repeat() still exists in the worker for a moment), and it is
// oom_score_adj, not this, that protects the host when that goes wrong. A
// cut JSON or nested value is no longer valid JSON, and the count says how
// many cells were cut, not which.
const MaxCellBytes = 1 << 20

// cellTruncatedMarker is appended to a cut cell.
const cellTruncatedMarker = " [cell cut at 1 MiB]"

func (l Limits) withDefaults(d Limits) Limits {
	if l.Threads == 0 {
		l.Threads = d.Threads
	}
	if l.MemoryLimit == "" {
		l.MemoryLimit = d.MemoryLimit
	}
	if l.Timeout == 0 {
		l.Timeout = d.Timeout
	}
	if l.MaxRows == 0 {
		l.MaxRows = d.MaxRows
	}
	if l.MaxResultBytes == 0 {
		l.MaxResultBytes = d.MaxResultBytes
	}
	return l
}

// Job is one query to run.
type Job struct {
	// User keys the one-query-at-a-time gate. Empty runs ungated.
	User string
	// CopyDirs are the LOCAL directories the query may read: the archive
	// directory and the backup directory of the selected server. Every entry
	// must be an absolute local path; an s3:// location or an empty list is
	// ErrCopyNotLocal.
	CopyDirs []string
	// ViewsSQL is the views script the console generates for the same server
	// (views.Generate), installed in the worker before the query runs. It
	// must reference nothing outside CopyDirs.
	ViewsSQL string
	// ViewsFor, when set, replaces ViewsSQL (#2029): the worker parses the
	// statement first and the parent calls ViewsFor with what it names, so
	// the script can define only the views the statement reads. It runs in
	// the parent, while the worker waits, and is not called for a statement
	// the worker refuses. Its error fails the run as a WorkerError.
	ViewsFor func(Refs) (string, error)
	// SQL is the user's statement.
	SQL string
	// Schema, when set, is the schema unqualified names in SQL resolve in
	// (DuckDB's search_path): the MySQL-protocol port's USE. The views put
	// each source schema's tables in a schema of that name, and events in
	// main, which DuckDB searches after the search_path, so events stays
	// reachable. A schema the views did not create (a default seeded from
	// the source DSN, a typo) leaves the default in place, so SHOW
	// DATABASES, events and anything qualified keep working; a name that
	// then fails to resolve gets a hint naming the missing schema. Empty
	// leaves DuckDB's default (main).
	Schema string
	// Session carries the connection's own settings (the MySQL-protocol
	// port's SET time_zone and SET sql_select_limit). The zero value changes
	// nothing.
	Session Session
	// Limits override the Runner's defaults field by field.
	Limits Limits
}

// Session is what a client connection set for itself and the statement must
// run under. The MySQL-protocol port fills it; the console's SQL card has no
// session and leaves it zero.
type Session struct {
	// TimeZone is the session time zone, as a name DuckDB's SET TimeZone
	// takes ('America/Argentina/Buenos_Aires', 'Etc/GMT-3'). Empty leaves the
	// zone the views script pins, UTC. The worker sets it AFTER the views
	// script (which sets UTC) and BEFORE the lock (after which no SET runs).
	// A name the engine refuses fails the statement as a RefusedError that
	// names it: the statement never runs under another zone.
	TimeZone string
	// SelectLimit is MySQL's sql_select_limit: the most rows a SELECT with no
	// LIMIT of its own returns. Zero is no limit. It never raises the cap:
	// at or under Limits.MaxRows the result is cut at it and NOT reported as
	// Truncated (the client asked for the cut); above MaxRows the cap rules
	// and a cut there is Truncated as always. A statement with its own
	// top-level LIMIT ignores it, as on MySQL, and so do SHOW, DESCRIBE and
	// SUMMARIZE, which are not SELECTs there.
	SelectLimit int
	// StrictStar asks for a refusal instead of an answer when the statement
	// holds a star over a table whose `SELECT *` on the copy is not MySQL's:
	// the order of its columns is not known, or MySQL returns a different set
	// of columns (#2111); and when the statement names a column the copy does
	// not hold, or reads a table whose missing columns are not known (#2123):
	// the name could bind to something else there. Set under read routing,
	// where the caller answers a refusal by sending the statement to MySQL. The browser and a port with
	// no routing leave it off and get the copy's answer. The worker never
	// sees it: the caller's ViewsFor decides, from what the statement names.
	StrictStar bool
	// Types answers, for the statement as the client sent it, whether the
	// copy must not answer because of the type of a column it names (#2133):
	// arithmetic on a date column, or a TIME or YEAR column named. Set with
	// StrictStar, by the routing layer, which holds the reading of the
	// statement the question needs (readrouter.ShapeOf: of the client's own
	// text, or of a prepared statement's template); the statement handed over
	// to run has already been written for the copy, with its arguments in
	// it. The caller's ViewsFor asks it once, with the column types of the
	// tables the statement reads. Nil under StrictStar means "not
	// known", and a statement over a table with such a column is then
	// refused.
	Types ColumnTypes
	// UnchangedWithin, when not zero, asks for a refusal instead of an answer
	// unless every table the statement reads has had no change on the source
	// since the snapshot the copy holds of it, as far as that can be known:
	// capture must be known to have read everything the source had written
	// at some moment no longer ago than this (#2085). Set under read routing
	// for a statement whose snapshot is older than the port's freshness
	// limit, with that limit: the copy's answer is then the source's as of
	// that moment, whatever the snapshot's age. Zero asks nothing, and costs
	// nothing. Like StrictStar, the worker never sees it: the caller decides,
	// from the tables the statement names.
	UnchangedWithin time.Duration
}

// ColumnTypes is the question Session.Types answers about the tables a
// statement reads, all together: dates are their DATE, DATETIME and
// TIMESTAMP columns (and those of a type that is not known), whole their
// TIME and YEAR columns, by name, and star says the statement holds a star
// (Refs). The answer is why the copy must not answer the statement, or "".
//
// NameVeto is the second question, about how the statement writes the names
// of those tables' columns (#2131): names are all of them, and the answer
// is why the copy would refuse the statement, or "". It spares the copy a
// statement it cannot run; it is not what keeps a wrong answer away.
type ColumnTypes interface {
	ColumnVeto(dates, whole []string, star bool) string
	NameVeto(names []string) string
}

// Column is one result column with DuckDB's type name (INTEGER, VARCHAR,
// DECIMAL(10,2), TIMESTAMP, ...).
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Result is what came back. Cells are JSON-shaped values a browser can show
// as they are: nil, bool, string, json.Number (every digit kept), []any for
// a LIST and map[string]any for a STRUCT or JSON. Dates are "2006-01-02",
// timestamps RFC 3339 in UTC, HUGEINT and DECIMAL are text (a DECIMAL with
// exactly its scale: "10.00" for a DECIMAL(10,2)), a BLOB is its
// text when valid UTF-8 and "0x.." otherwise, NaN and infinities are text.
// A text cell longer than MaxCellBytes is cut and ends with a marker;
// TruncatedCells counts them.
type Result struct {
	Columns        []Column      `json:"columns"`
	Rows           [][]any       `json:"rows"`
	Truncated      bool          `json:"truncated"`
	TruncatedCells int           `json:"truncated_cells"`
	Elapsed        time.Duration `json:"elapsed_ns"`
	// Phases is where the time went, for the measurement #2026 asks for.
	Phases Phases `json:"phases"`
	// Note is a sentence the caller adds for the reader, set by the parent
	// and never by the worker (#2210: the answer came from an earlier copy).
	Note string `json:"-"`
}

// Phases times one run, worker side (reported by the child) and parent
// side. Every field is a duration; a zero one did not happen or was not
// measured. They nest: Total (the parent's whole run) contains Spawn (what
// the worker cost this statement, from starting it to its exit), which
// contains every child phase; Spawn minus the child phases is process start,
// result encoding and exit.
type Phases struct {
	// Child: opening the in-memory DuckDB and taking the connection.
	Open time.Duration `json:"open_ns"`
	// Child: checking the statement's shape, the caps and the restrictions
	// (everything before the views script).
	Lockdown time.Duration `json:"lockdown_ns"`
	// Child: installing the views script (CREATE VIEW over the copy), the
	// USE and the final configuration lock.
	Views time.Duration `json:"views_ns"`
	// Child: running the statement and collecting rows (= Elapsed). The
	// child cannot time its own encoding from inside the encoded message;
	// Spawn minus the child phases is that cost plus process start and exit.
	Query time.Duration `json:"query_ns"`
	// Parent: the worker for this statement, from starting it to its exit,
	// pipe included: NOT process start alone, which is Spawn minus the child
	// phases.
	Spawn time.Duration `json:"spawn_ns"`
	// Parent: decoding the child's result.
	Decode time.Duration `json:"decode_ns"`
	// Parent: the whole run, from Slot.Run's entry.
	Total time.Duration `json:"total_ns"`
	// Standby: the statement ran on a worker started before it arrived
	// (#2236). Open is then zero, paid ahead of the statement, and Spawn
	// runs from the hand-over of the job to the worker's exit.
	Standby bool `json:"standby,omitempty"`
}

// ErrCopyNotLocal: the job names no local copy directory. v1 reads a local
// copy only; a copy that lives only on S3 is not queryable here.
var ErrCopyNotLocal = errors.New("running SQL on the copy needs a local copy directory; this copy lives only on S3")

// ErrBusy: the user already has a query running, or the runner is at its
// limit of concurrent workers.
var ErrBusy = errors.New("SQL on the copy is busy: every slot is taken and the line for one is full; try again in a moment")

// ErrResultTooLarge: the result would exceed Limits.MaxResultBytes. Either
// side may say so: the worker counts as it collects rows, and the parent
// counts what it reads from the pipe.
var ErrResultTooLarge = errors.New("the result is too large to return; narrow the query")

// UnavailableError: the copy cannot be queried here at all, whatever the
// statement (it lives only on S3, defines no view, holds the console's own
// configuration). Reason is written for the reader of whichever wire asked.
type UnavailableError struct{ Reason string }

func (e *UnavailableError) Error() string { return e.Reason }

// RefusedError: the statement was not run at all because of its shape (empty,
// more than one statement, not a SELECT, a syntax error).
type RefusedError struct{ Reason string }

func (e *RefusedError) Error() string { return "query refused: " + e.Reason }

// ColumnsDifferError: the statement was not run because the caller asked for
// MySQL's answer (Session.StrictStar) and the copy's would hold other columns
// or other rows: a star, or a NATURAL JOIN, over a table whose columns on the
// copy are not MySQL's (#2111), or the name of a column the copy does not
// hold (#2123). A decision about the statement, not a fault
// of the copy: read routing sends the statement to MySQL and counts it apart.
type ColumnsDifferError struct{ Reason string }

func (e *ColumnsDifferError) Error() string { return e.Reason }

// MayHaveChangedError: the statement was not run because the caller asked
// for an answer only over tables with no change since their snapshot
// (Session.UnchangedWithin), and that does not hold or could not be
// established (#2085). Reason says which: a table that changed, or what kept
// the question from being answered. Like ColumnsDifferError, a decision
// about the statement and not a fault of the copy: read routing sends the
// statement to MySQL under the freshness rule it had before asking.
type MayHaveChangedError struct{ Reason string }

func (e *MayHaveChangedError) Error() string { return e.Reason }

// QueryError: DuckDB ran (or tried to run) the statement inside the sandbox
// and failed. Message is DuckDB's own text: a Permission Error for a path
// outside the copy, an Out of Memory Error past the cap, a Binder Error for
// an unknown column.
type QueryError struct{ Message string }

func (e *QueryError) Error() string { return e.Message }

// TimeoutError: the worker ran past Limits.Timeout and was killed.
type TimeoutError struct {
	Limit time.Duration
	PID   int
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("the query ran longer than %s and was stopped", e.Limit)
}

// WorkerError: the child could not start, died without a result, or could
// not prepare its session. Stderr carries the child's last output.
type WorkerError struct {
	Err    error
	PID    int
	Stderr string
}

func (e *WorkerError) Error() string {
	msg := "sql worker: " + e.Err.Error()
	if s := strings.TrimSpace(e.Stderr); s != "" {
		msg += "\n" + s
	}
	return msg
}

func (e *WorkerError) Unwrap() error { return e.Err }

// Config builds a Runner.
type Config struct {
	// Exe is the worker executable; empty means the current binary, which
	// is what the console uses (on Linux /proc/self/exe, so a package
	// upgrade mid-run keeps re-executing the binary this process runs, not
	// a newer one with a different protocol; elsewhere os.Executable).
	Exe string
	// Args are the worker's arguments; nil means []string{WorkerCommand}, the
	// console's hidden subcommand. A non-nil empty slice means no arguments
	// (the package's tests re-execute the test binary).
	Args []string
	// Limits are the defaults every Job runs under unless it overrides them;
	// zero fields fall back to DefaultLimits.
	Limits Limits
	// MaxInFlight caps how many workers run at once across all users; 0
	// means DefaultMaxInFlight.
	MaxInFlight int
	// MaxWait is how long Reserve waits for a slot before refusing (#2033);
	// 0 means DefaultMaxWait, a negative value refuses at once.
	MaxWait time.Duration
	// MaxWaiters bounds how many callers wait at once; past it Reserve
	// refuses at once. 0 means DefaultMaxWaiters.
	MaxWaiters int
	// SpillDir is where workers spill past their memory limit (#2210): the
	// parent makes one private directory per statement under it and removes
	// it when the worker exits. Empty means os.TempDir(), where bintrail's
	// other DuckDB sessions spill too. It must not lie inside a copy
	// directory: those are readable by the statement.
	SpillDir string
	// Standby keeps workers started ahead of the statements that will use
	// them (#2236): once a statement has run, one per slot waits with its
	// DuckDB open, so the next statement does not pay the process start and
	// the open. Each still serves ONE statement and exits. Off unless set:
	// a Runner that keeps processes must be closed (Close).
	Standby bool
	// StandbyIdle is how long the standbys are kept with no statement
	// running or arriving; after it they are stopped, and the next statement
	// starts its own worker. 0 means DefaultStandbyIdle.
	StandbyIdle time.Duration
}

// DefaultStandbyIdle is how long standby workers outlive the last statement:
// long enough for a person or a dashboard working with the copy to find one
// ready every time, short enough that an install nobody runs SQL on keeps no
// idle process.
const DefaultStandbyIdle = 5 * time.Minute

// Standbys that die before they are used are a sign that keeping them costs
// more than it gives (a host short of memory picks them first: they ask for
// it). After standbyMaxDeaths in a row none is started for standbyOffFor.
const (
	standbyMaxDeaths = 3
	standbyOffFor    = 5 * time.Minute
)

// SpillFactor is how much disk a worker may spill to, as a multiple of its
// memory limit (#2210): 8 GiB at the default 2 GiB, so two workers at once
// hold at most 16 GiB, and only while they run. Measured on DuckDB 1.4.5 over
// sysbench-tpcc snapshots (20 M and 60 M row tables with 4 M and 8.5 M changes
// waiting): statements that ran out of a 512 MiB limit without a temp
// directory finished with one, writing at most 1.6 times the limit; a DISTINCT
// over 20 M integers at 128 MB needed more than twice it.
const SpillFactor = 4

// spillCapMessage is the message a user reads for a statement that ran out
// of room. DuckDB's own text for a full spill directory goes on to advise
// "PRAGMA max_temp_directory_size", which the locked session refuses, and the
// figures it quotes are the DISK cap; a MySQL client shows 512 bytes of it.
// So its first sentence is kept and the rest replaced. A statement that had
// no disk at all (noSpill) and ran out of memory says why it had none.
func spillCapMessage(msg string, spill spillSpec, noSpill error) string {
	first := func(s string) string {
		if i := strings.Index(s, ")."); i >= 0 {
			return s[:i+2]
		}
		if i := strings.IndexByte(s, '\n'); i >= 0 {
			return s[:i]
		}
		return s
	}
	switch {
	case spill.Dir != "" && strings.Contains(msg, "failed to offload data block"):
		return first(msg) + fmt.Sprintf(" The statement needed more than its memory and also filled the %d MB of disk it may use past it.", spill.MaxBytes>>20)
	case spill.Dir != "" && strings.Contains(msg, "No space left on device"):
		return "Out of Memory Error: the disk this statement spills to past its memory is full."
	case spill.Dir == "" && noSpill != nil && strings.Contains(msg, "Out of Memory Error"):
		return first(msg) + " It could not use the disk past its memory: " + noSpill.Error() + "."
	}
	return msg
}

// spillPrefix names a statement's spill directory under Config.SpillDir.
// minSpillBytes is the least a statement is given to spill to; with less
// room than that it runs without the disk, as before #2210.
const (
	spillPrefix   = "bintrail-sql-spill-"
	minSpillBytes = 256 << 20
)

// staleSpillAge is how old a spill directory must be before a sweep removes
// it as left behind: one a worker kept writing after its parent died (the
// worker has its own process group, so it outlives the parent until its own
// deadline, Timeout plus selfDeadlineGrace) and that nothing removed. Ten
// minutes, or four times the timeout when that is longer, so a live
// statement of another console on the same host is never taken. A sweep runs
// when the Runner is built and again at most every spillSweepEvery, so a
// directory a crash left goes within minutes of the restart, not at the next
// one.
const (
	minStaleSpillAge = 10 * time.Minute
	spillSweepEvery  = 10 * time.Minute
)

func (r *Runner) staleSpillAge() time.Duration {
	return max(minStaleSpillAge, 4*r.limits.Timeout)
}

// DefaultMaxWait and DefaultMaxWaiters bound the line for a slot (#2033):
// a burst of statements (a dashboard drawing its panels, two people at
// once) waits up to half a minute instead of failing, and only a bounded
// number of callers wait, so a flood is still refused rather than parked.
const (
	DefaultMaxWait    = 30 * time.Second
	DefaultMaxWaiters = 16
)

// Runner spawns workers and keeps the per-user and the global gate.
type Runner struct {
	exe         string
	args        []string
	limits      Limits
	maxInFlight int
	maxWait     time.Duration
	maxWaiters  int
	spillRoot   string

	// spillMu guards the spill bookkeeping: when the last sweep ran, and the
	// last reason a statement could not spill ("" when it could), so the log
	// says so once per change instead of once per statement.
	spillMu        sync.Mutex
	spillSweptAt   time.Time
	spillLastError string

	mu       sync.Mutex
	busy     map[string]bool
	inFlight int
	// waiting counts the callers parked in Reserve; freed is closed (and
	// replaced) on every release, which wakes all of them to try again.
	waiting int
	freed   chan struct{}

	// The standby workers (#2236), guarded by mu: at most one per slot.
	standby      bool
	standbyIdle  time.Duration
	standbys     []*worker
	standbyTimer *time.Timer
	// standbyDeaths counts standbys found dead in a row; standbyOffUntil is
	// until when none is started after too many; standbyNote is the last
	// thing logged about them, so the log says it once per change.
	standbyDeaths   int
	standbyOffUntil time.Time
	standbyNote     string
	closed          bool
	// standbyUsed counts statements that ran on a standby.
	standbyUsed atomic.Int64

	// onStart is a test hook that receives the child's pid.
	onStart func(pid int)
	// beforeHandOver is a test hook that receives a standby just taken for
	// a statement, before its job is written.
	beforeHandOver func(w *worker)
}

// New builds a Runner.
func New(cfg Config) *Runner {
	r := &Runner{
		exe:         cfg.Exe,
		args:        cfg.Args,
		limits:      cfg.Limits.withDefaults(DefaultLimits()),
		maxInFlight: cfg.MaxInFlight,
		maxWait:     cfg.MaxWait,
		maxWaiters:  cfg.MaxWaiters,
		spillRoot:   cfg.SpillDir,
		standby:     cfg.Standby,
		standbyIdle: cfg.StandbyIdle,
		busy:        map[string]bool{},
		freed:       make(chan struct{}),
	}
	if r.maxInFlight <= 0 {
		r.maxInFlight = DefaultMaxInFlight
	}
	if r.maxWait == 0 {
		r.maxWait = DefaultMaxWait
	}
	if r.maxWaiters <= 0 {
		r.maxWaiters = DefaultMaxWaiters
	}
	if r.spillRoot == "" {
		r.spillRoot = os.TempDir()
	}
	if r.standbyIdle <= 0 {
		r.standbyIdle = DefaultStandbyIdle
	}
	r.sweepSpillDue(time.Now(), true)
	if _, err := r.SpillState(r.limits.MemoryLimit); err != nil {
		r.noteSpill(err)
	}
	return r
}

// sweepSpillDue sweeps when the last sweep is older than spillSweepEvery, or
// when forced.
func (r *Runner) sweepSpillDue(now time.Time, force bool) {
	r.spillMu.Lock()
	due := force || now.Sub(r.spillSweptAt) >= spillSweepEvery
	if due {
		r.spillSweptAt = now
	}
	r.spillMu.Unlock()
	if due {
		sweepSpill(r.spillRoot, now.Add(-r.staleSpillAge()))
	}
}

// sweepSpill removes the spill directories under root last written before
// cutoff: what a parent that died mid-statement left behind. Best-effort.
func sweepSpill(root string, cutoff time.Time) {
	entries, err := os.ReadDir(root)
	if err != nil {
		// SpillState reports a root that cannot be used; this is only the
		// cleanup, and it has nothing to remove from a root it cannot read.
		return
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), spillPrefix) {
			continue
		}
		if info, err := e.Info(); err == nil && info.ModTime().Before(cutoff) {
			if err := os.RemoveAll(filepath.Join(root, e.Name())); err != nil {
				slog.Warn("sql on the copy: could not remove a spill directory left behind", "dir", filepath.Join(root, e.Name()), "error", err)
			}
		}
	}
}

// spillFSFor is spillFS, a variable so a test can give a full or an
// in-memory root.
var spillFSFor = spillFS

// SpillState is how much disk one statement with memoryLimit may spill to
// past it: SpillFactor times the memory, and no more than a share of the
// space free under the spill root (half of it, split between the statements
// that can run at once), because that disk may be the one capture writes to.
// The error says why a statement cannot spill at all: the memory does not
// parse, the root cannot be read, it is in memory (a tmpfs, where spilling
// would take memory memory_limit does not count), or it has too little room.
// The console's settings panel shows the same answer.
func (r *Runner) SpillState(memoryLimit string) (int64, error) {
	mem, err := cliutil.ParseByteSize(memoryLimit)
	if err != nil {
		return 0, fmt.Errorf("the memory limit %q does not parse: %w", memoryLimit, err)
	}
	if mem <= 0 {
		return 0, fmt.Errorf("the memory limit %q is not a size", memoryLimit)
	}
	free, inMemory, err := spillFSFor(r.spillRoot)
	if err != nil {
		return 0, fmt.Errorf("the temporary directory %s cannot be used: %w", r.spillRoot, err)
	}
	if inMemory {
		return 0, fmt.Errorf("the temporary directory %s is in memory (tmpfs), where spilling would use memory; point TMPDIR at a folder on disk", r.spillRoot)
	}
	limit := SpillFactor * mem
	if free >= 0 {
		limit = min(limit, free/int64(2*r.maxInFlight))
	}
	if limit < minSpillBytes {
		return 0, fmt.Errorf("the temporary directory %s has %d MB free, and a statement's share of it (half, split between the %d that can run at once) is too little to spill to", r.spillRoot, free>>20, r.maxInFlight)
	}
	return limit, nil
}

// noteSpill logs a change in whether statements can spill: the first
// statement that cannot, with why, and the first that can again. Not every
// statement: a dashboard sends several at once.
func (r *Runner) noteSpill(err error) {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	r.spillMu.Lock()
	changed := msg != r.spillLastError
	r.spillLastError = msg
	r.spillMu.Unlock()
	switch {
	case !changed:
	case err != nil:
		slog.Warn("sql on the copy: statements cannot use the disk past their memory, so a heavy one fails instead of slowing down", "reason", msg)
	default:
		slog.Info("sql on the copy: statements can use the disk past their memory again", "root", r.spillRoot)
	}
}

// makeSpill makes the private spill directory for one statement and returns
// its spec, or a zero spec with the reason when it cannot spill: the
// statement then runs as before #2210, failing past its memory instead of
// slowing down, and its error says why it had no disk (spillCapMessage).
func (r *Runner) makeSpill(memoryLimit string) (spillSpec, error) {
	r.sweepSpillDue(time.Now(), false)
	limit, err := r.SpillState(memoryLimit)
	if err == nil {
		var dir string
		if dir, err = os.MkdirTemp(r.spillRoot, spillPrefix); err == nil {
			r.noteSpill(nil)
			return spillSpec{Dir: dir, MaxBytes: limit}, nil
		}
		err = fmt.Errorf("could not make a folder under %s: %w", r.spillRoot, err)
	}
	r.noteSpill(err)
	return spillSpec{}, err
}

// IsWorkerInvocation reports whether args (os.Args) start a worker: the first
// argument is WorkerCommand. Embedders that do work in main() before calling
// consoleapp.Main should check this first (consoleapp.RunSQLWorkerIfInvoked
// does), so that work is not repeated for every query.
func IsWorkerInvocation(args []string) bool {
	return len(args) > 1 && args[1] == WorkerCommand
}

// Run runs one job to completion and returns its result, or one of the
// typed errors above (RefusedError, QueryError, TimeoutError, WorkerError,
// ErrCopyNotLocal, ErrBusy, ErrResultTooLarge) or ctx.Err() when the caller
// cancelled. Refusals that need no worker (empty SQL, no local copy, user
// busy) return before any process starts. Reserve + Slot.Run, for a caller
// that has nothing to prepare between the two.
func (r *Runner) Run(ctx context.Context, job Job) (Result, error) {
	if strings.TrimSpace(job.SQL) == "" {
		return Result{}, &RefusedError{Reason: "the query is empty"}
	}
	if !localCopy(job.CopyDirs) {
		return Result{}, ErrCopyNotLocal
	}
	slot, err := r.Reserve(ctx, job.User)
	if err != nil {
		return Result{}, err
	}
	return slot.Run(ctx, job)
}

// Slot is a reserved worker slot: the global one and, when a user was
// named, that user's. It serves ONE job (Run releases it) or is given back
// with Release. A Slot is what lets a caller take the slot BEFORE the work
// that prepares a job (#2026: the console builds the views, which reads the
// index and walks the copy, and a request that will be refused as busy must
// not pay for that), so the refusal costs nothing and the slot is not held
// idle by a caller that failed to prepare.
type Slot struct {
	r        *Runner
	user     string
	released bool
	running  bool
	mu       sync.Mutex
}

// Reserve takes a slot for user ("" = the global slot only). When none is
// free it waits, up to MaxWait, for one (#2033), then refuses with an error
// that is ErrBusy. A full line (MaxWaiters callers already waiting) refuses
// at once, and so does a negative MaxWait. A ctx that ends while waiting
// returns its error: the caller left, nobody is told "busy".
//
// Not first-come-first-served: every release wakes every waiter and the
// first to take a slot it can use wins. That is deliberate, because a waiter
// whose own per-person slot is busy can never use the freed slot, and a
// strict line would park everyone behind it.
func (r *Runner) Reserve(ctx context.Context, user string) (*Slot, error) {
	r.mu.Lock()
	if r.acquireLocked(user) {
		r.mu.Unlock()
		return &Slot{r: r, user: user}, nil
	}
	if r.maxWait < 0 || r.waiting >= r.maxWaiters {
		r.mu.Unlock()
		return nil, ErrBusy
	}
	r.waiting++
	defer func() {
		r.mu.Lock()
		r.waiting--
		r.mu.Unlock()
	}()
	timer := time.NewTimer(r.maxWait)
	defer timer.Stop()
	for {
		freed := r.freed
		r.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, &BusyError{Waited: r.maxWait, MaxInFlight: r.maxInFlight}
		}
		r.mu.Lock()
		if r.acquireLocked(user) {
			r.mu.Unlock()
			return &Slot{r: r, user: user}, nil
		}
	}
}

// BusyError is a statement that waited MaxWait for a slot and got none. It
// is ErrBusy (errors.Is), with the wait said in its message.
type BusyError struct {
	Waited      time.Duration
	MaxInFlight int
}

func (e *BusyError) Error() string {
	return fmt.Sprintf("SQL on the copy is busy: waited %s and no query finished (%d run at once, and one at a time per person on the console or per server on the MySQL port); try again in a moment",
		e.Waited.Round(time.Second), e.MaxInFlight)
}

func (e *BusyError) Is(target error) bool { return target == ErrBusy }

// waitingNow is how many callers wait in Reserve (tests).
func (r *Runner) waitingNow() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.waiting
}

// Release gives the slot back without running. Idempotent, and a no-op
// after Run, which releases on its own.
func (s *Slot) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return
	}
	s.released = true
	s.r.release(s.user)
}

// Run runs job in this slot and releases it. job.User must be the user the
// slot was reserved for; a mismatch is a programming error and is refused as
// a WorkerError so it cannot pass silently. The pre-worker refusals (empty
// SQL, no local copy) still return before any process starts.
func (s *Slot) Run(ctx context.Context, job Job) (Result, error) {
	// Claimed in one step under the lock: a released slot holds no gate, and
	// a second concurrent Run on the same slot would run ungated too.
	s.mu.Lock()
	spent := s.released || s.running
	s.running = true
	s.mu.Unlock()
	if spent {
		return Result{}, &WorkerError{Err: errors.New("the slot was already used or released; reserve another")}
	}
	// Registered first so it runs last, after the slot is given back: the
	// standbys are counted against the free slots.
	ran := false
	defer func() {
		if ran {
			s.r.topUpStandbys()
		}
	}()
	defer s.Release()
	if job.User != s.user {
		return Result{}, &WorkerError{Err: fmt.Errorf("job for user %q run in a slot reserved for %q", job.User, s.user)}
	}
	if strings.TrimSpace(job.SQL) == "" {
		return Result{}, &RefusedError{Reason: "the query is empty"}
	}
	if !localCopy(job.CopyDirs) {
		return Result{}, ErrCopyNotLocal
	}
	start := time.Now()
	ran = true
	res, err := s.r.spawn(ctx, job, job.Limits.withDefaults(s.r.limits))
	if err == nil {
		res.Phases.Total = time.Since(start)
	}
	return res, err
}

// acquire takes a worker slot: the global one always, the user's when the
// job names a user.
// acquireLocked takes the global slot and user's; r.mu is held.
func (r *Runner) acquireLocked(user string) bool {
	if r.inFlight >= r.maxInFlight {
		return false
	}
	if user != "" {
		if r.busy[user] {
			return false
		}
		r.busy[user] = true
	}
	r.inFlight++
	return true
}

func (r *Runner) release(user string) {
	r.mu.Lock()
	r.inFlight--
	if user != "" {
		delete(r.busy, user)
	}
	close(r.freed)
	r.freed = make(chan struct{})
	r.mu.Unlock()
}

// localCopy reports whether every copy directory is an absolute local path.
// An s3:// location is not local (filepath.IsAbs says so on every platform),
// and an empty list is a copy nowhere.
func localCopy(dirs []string) bool {
	if len(dirs) == 0 {
		return false
	}
	for _, d := range dirs {
		if strings.HasPrefix(d, "s3://") || !filepath.IsAbs(d) {
			return false
		}
	}
	return true
}

// wireJob is what the parent writes to the child's stdin.
type wireJob struct {
	CopyDirs []string `json:"copy_dirs"`
	ViewsSQL string   `json:"views_sql"`
	// AskViews: ViewsSQL is empty, and the worker asks for the script once
	// it has parsed the statement (Job.ViewsFor).
	AskViews    bool   `json:"ask_views,omitempty"`
	SQL         string `json:"sql"`
	Schema      string `json:"schema,omitempty"`
	TimeZone    string `json:"time_zone,omitempty"`
	SelectLimit int    `json:"select_limit,omitempty"`
	Threads     int    `json:"threads"`
	MemoryLimit string `json:"memory_limit"`
	MaxRows     int    `json:"max_rows"`
	// MaxResultBytes is counted by the worker as it collects rows, so it
	// stops before building a result the parent would refuse anyway.
	MaxResultBytes int64 `json:"max_result_bytes"`
	// SpillDir is the private directory the parent made for this statement,
	// and SpillMaxBytes what it may hold (#2210). Empty: no spill.
	SpillDir      string `json:"spill_dir,omitempty"`
	SpillMaxBytes int64  `json:"spill_max_bytes,omitempty"`
	// TimeoutNS is the parent's wall clock, so the child can stop on its own
	// shortly after it if the parent is no longer there to kill it.
	TimeoutNS int64 `json:"timeout_ns"`
}

// wireViews is the parent's answer to an Ask: the views script to install.
type wireViews struct {
	ViewsSQL string `json:"views_sql"`
}

// wireResult is what the child writes to stdout: a result, or a structured
// error. The child exits 0 in both cases; a non-zero exit is a protocol
// failure (it could not even read the job or encode its answer).
type wireResult struct {
	// Ask is the worker's question, not a result: the relations the
	// statement names, sent ahead of the result when the job has AskViews.
	Ask            *Refs    `json:"ask,omitempty"`
	Columns        []Column `json:"columns,omitempty"`
	Rows           [][]any  `json:"rows,omitempty"`
	Truncated      bool     `json:"truncated"`
	TruncatedCells int      `json:"truncated_cells"`
	ElapsedNS      int64    `json:"elapsed_ns"`
	// Child-side phase durations, ns (see Phases).
	OpenNS     int64      `json:"open_ns,omitempty"`
	LockdownNS int64      `json:"lockdown_ns,omitempty"`
	ViewsNS    int64      `json:"views_ns,omitempty"`
	Error      *wireError `json:"error,omitempty"`
}

type wireError struct {
	// Kind is one of errRefused, errQuery, errSession.
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

const (
	errRefused  = "refused"   // the statement's shape; nothing ran
	errQuery    = "query"     // DuckDB failed the statement
	errSession  = "session"   // the worker could not prepare its locked session
	errTooLarge = "too_large" // the result passed MaxResultBytes while collecting
)

// errNotHandedOver: the job could not be written to the worker, so the
// worker never read it and the statement has not run. It is the ONLY failure
// after which a statement may be given to another worker: once the job is
// written, a worker that dies may have run it, and a statement that got a
// host out of memory must not quietly do it a second time.
var errNotHandedOver = errors.New("the job did not reach the worker")

// worker is one started worker process: its DuckDB open (or opening), blocked
// reading its job from stdin. It serves ONE job and exits; it is never handed
// a second one.
type worker struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *cappedBuffer
	stderr *cappedBuffer
	// kill ends the process group: the timeout, a cancelled caller and a
	// result past its cap all go through it. Safe to call more than once,
	// and after the process is gone.
	kill context.CancelFunc
	// done is closed once the process has exited and its pipes are drained;
	// waitErr is its exit then. Wait is called in one place only, so a
	// worker that dies before it is used is reaped all the same.
	done    chan struct{}
	waitErr error
	// standby: started ahead of its statement, not for it (#2236).
	standby bool
	// used is set by the one run this worker serves. A second hand-over is
	// refused: its stdout still holds the first statement's answer.
	used atomic.Bool
}

// startWorker starts a worker with no job. Nothing of a statement is in its
// environment or its arguments: the job goes in by stdin (run).
func (r *Runner) startWorker() (*worker, error) {
	exe := r.exe
	if exe == "" {
		var err error
		if exe, err = workerExe(); err != nil {
			return nil, fmt.Errorf("locate the worker executable: %w", err)
		}
	}
	args := r.args
	if args == nil {
		args = []string{WorkerCommand}
	}
	// The process is tied to its own context, not to a statement's: the
	// statement's deadline does not exist yet when a worker starts.
	wctx, kill := context.WithCancel(context.Background())
	cmd := exec.CommandContext(wctx, exe, args...)
	cmd.Env = childEnv()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		kill()
		return nil, fmt.Errorf("stdin: %w", err)
	}
	setProcessGroup(cmd)
	// Kill the whole process group: DuckDB's worker threads belong to the
	// child, and a plain Kill of the leader is enough for them, but a group
	// kill also covers anything the child might spawn.
	cmd.Cancel = func() error { return killProcessGroup(cmd.Process) }
	// If the group kill somehow leaves the pipes open, stop waiting on them.
	cmd.WaitDelay = 5 * time.Second
	w := &worker{cmd: cmd, stdin: stdin, kill: kill, done: make(chan struct{}),
		// Until a job arms it (run), stdout takes nothing: a worker has
		// nothing to say before its job.
		stdout: &cappedBuffer{},
		// stderr is diagnostics: past its cap the rest is dropped, never a
		// reason to fail the child.
		stderr: &cappedBuffer{max: 64 << 10, drop: true},
	}
	cmd.Stdout, cmd.Stderr = w.stdout, w.stderr
	if err := cmd.Start(); err != nil {
		kill()
		return nil, fmt.Errorf("start: %w", err)
	}
	go func() {
		w.waitErr = cmd.Wait()
		// Release the context whatever ended the process.
		kill()
		close(w.done)
	}()
	return w, nil
}

// marshalJob is the job as the worker reads it. A job that asks for its views
// (ViewsFor) carries none: the worker asks once it has parsed the statement.
func marshalJob(job Job, limits Limits, spill spillSpec) ([]byte, error) {
	asking := job.ViewsFor != nil
	viewsSQL := job.ViewsSQL
	if asking {
		viewsSQL = ""
	}
	return json.Marshal(wireJob{
		CopyDirs: allowedDirs(job.CopyDirs), ViewsSQL: viewsSQL, AskViews: asking, SQL: job.SQL, Schema: job.Schema,
		TimeZone: job.Session.TimeZone, SelectLimit: job.Session.SelectLimit,
		Threads: limits.Threads, MemoryLimit: limits.MemoryLimit, MaxRows: limits.MaxRows,
		MaxResultBytes: limits.MaxResultBytes, TimeoutNS: int64(limits.Timeout),
		SpillDir: spill.Dir, SpillMaxBytes: spill.MaxBytes,
	})
}

func (r *Runner) spawn(ctx context.Context, job Job, limits Limits) (Result, error) {
	spill, noSpill := r.makeSpill(limits.MemoryLimit)
	if spill.Dir != "" {
		// After the worker has exited on every path (a group kill at the
		// timeout included: run returns only then), so nothing writes here
		// any more.
		defer func() {
			if err := os.RemoveAll(spill.Dir); err != nil {
				slog.Warn("sql on the copy: could not remove a statement's spill directory", "dir", spill.Dir, "error", err)
			}
		}()
	}
	in, err := marshalJob(job, limits, spill)
	if err != nil {
		return Result{}, &WorkerError{Err: err}
	}
	if err := ctx.Err(); err != nil {
		// Already cancelled: report the cancel, not a worker that failed.
		return Result{}, err
	}
	if w := r.takeStandby(); w != nil {
		// Its replacement starts now, while this statement runs.
		r.topUpStandbys()
		if r.beforeHandOver != nil {
			r.beforeHandOver(w)
		}
		res, err := r.run(ctx, w, time.Now(), in, job.ViewsFor, limits, spill, noSpill)
		if !errors.Is(err, errNotHandedOver) {
			if err == nil {
				// Opening DuckDB was paid before this statement arrived.
				res.Phases.Open = 0
				res.Phases.Standby = true
			}
			r.standbyUsed.Add(1)
			return res, err
		}
		// The standby was gone before it read the job, so the statement
		// has not run anywhere: start a worker for it, as if there had
		// been no standby.
		r.noteStandbyDeath()
	}
	// A worker started for this statement: starting it is part of the
	// statement's cost (Phases.Spawn).
	began := time.Now()
	w, err := r.startWorker()
	if err != nil {
		return Result{}, &WorkerError{Err: err}
	}
	return r.run(ctx, w, began, in, job.ViewsFor, limits, spill, noSpill)
}

// run hands the job to w and waits for its answer. The statement's timeout
// starts here, when the job is handed over, not when the process started.
// began is when this statement started paying for the worker: before the
// start for a worker started for it. It returns only after the process has
// exited.
func (r *Runner) run(ctx context.Context, w *worker, began time.Time, in []byte, viewsFor func(Refs) (string, error), limits Limits, spill spillSpec, noSpill error) (Result, error) {
	if w.used.Swap(true) {
		return Result{}, &WorkerError{Err: errors.New("the worker was already handed a job; start another"), PID: w.cmd.Process.Pid}
	}
	// Whatever happens below, the process does not outlive this call.
	defer func() {
		w.kill()
		<-w.done
	}()
	cctx, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()
	// On timeout or cancel, kill the worker's process group.
	stop := context.AfterFunc(cctx, w.kill)
	defer stop()

	asking := viewsFor != nil
	var firstLine chan []byte
	if asking {
		firstLine = make(chan []byte, 1)
	}
	w.stdout.arm(limits.MaxResultBytes, func() { _ = killProcessGroup(w.cmd.Process) }, firstLine)
	// wrote receives the result of writing the job, once.
	wrote := make(chan error, 1)

	pid := w.cmd.Process.Pid
	if r.onStart != nil {
		r.onStart(pid)
	}
	var waitErr error
	if asking {
		var askErr error
		waitErr, askErr = answerAsk(w, in, wrote, firstLine, viewsFor)
		if askErr != nil && ctx.Err() == nil && cctx.Err() == nil {
			return Result{}, &WorkerError{Err: askErr, PID: pid, Stderr: w.stderr.text()}
		}
	} else {
		// Written from its own goroutine: a statement can be larger than
		// the pipe's buffer, and a worker that dies before reading it must
		// not leave this write blocked. Closed after it: this worker gets
		// no second message.
		go func() {
			_, err := w.stdin.Write(in)
			wrote <- err
			_ = w.stdin.Close()
		}()
		<-w.done
		waitErr = w.waitErr
	}

	switch {
	case ctx.Err() != nil:
		return Result{}, ctx.Err()
	case errors.Is(cctx.Err(), context.DeadlineExceeded):
		return Result{}, &TimeoutError{Limit: limits.Timeout, PID: pid}
	case w.stdout.didOverflow():
		return Result{}, ErrResultTooLarge
	}
	// The process has exited, so the write of the job has returned. If it
	// failed, the worker never had the whole job and so never ran it: a
	// standby that was gone before its statement. (For a worker started for
	// the statement the same failure is reported below, with its stderr.)
	if err := <-wrote; err != nil && w.standby {
		return Result{}, fmt.Errorf("%w: %v", errNotHandedOver, err)
	}

	spawnTook := time.Since(began)
	decodeStart := time.Now()
	var out wireResult
	// The process has exited and its pipes are drained (done is closed), so
	// nothing writes to the buffers any more.
	dec := json.NewDecoder(bytes.NewReader(w.stdout.buf.Bytes()))
	dec.UseNumber()
	err := dec.Decode(&out)
	if err == nil && out.Ask != nil {
		// The question came first; the result follows it.
		out = wireResult{}
		err = dec.Decode(&out)
	}
	if err != nil {
		if waitErr != nil {
			err = fmt.Errorf("%w (no result: %v)", waitErr, err)
		} else {
			err = fmt.Errorf("unreadable result: %w", err)
		}
		return Result{}, &WorkerError{Err: err, PID: pid, Stderr: w.stderr.text()}
	}
	if out.Error != nil {
		switch out.Error.Kind {
		case errRefused:
			return Result{}, &RefusedError{Reason: out.Error.Message}
		case errQuery:
			return Result{}, &QueryError{Message: spillCapMessage(out.Error.Message, spill, noSpill)}
		case errTooLarge:
			return Result{}, ErrResultTooLarge
		default:
			return Result{}, &WorkerError{Err: errors.New(out.Error.Message), PID: pid, Stderr: w.stderr.text()}
		}
	}
	res := Result{Columns: out.Columns, Rows: out.Rows, Truncated: out.Truncated,
		TruncatedCells: out.TruncatedCells, Elapsed: time.Duration(out.ElapsedNS),
		Phases: Phases{
			Open: time.Duration(out.OpenNS), Lockdown: time.Duration(out.LockdownNS),
			Views: time.Duration(out.ViewsNS), Query: time.Duration(out.ElapsedNS),
			Spawn: spawnTook, Decode: time.Since(decodeStart),
		}}
	if res.Rows == nil {
		res.Rows = [][]any{}
	}
	return res, nil
}

// answerAsk runs a worker that asks for its views (Job.ViewsFor): it sends
// the job, waits for the worker's first line, and when that line is the
// question it answers with the script ViewsFor returns. A first line that is
// not a question is the result itself (a statement the worker refused, a
// session that failed before asking), and a worker that exits without a
// line answers nothing: either way stdin is closed and the run ends as
// usual. The second error is a failure of the exchange itself; a killed
// worker (timeout, cancel) is reported by the caller from the contexts.
func answerAsk(w *worker, job []byte, wrote chan<- error, firstLine <-chan []byte, viewsFor func(Refs) (string, error)) (waitErr, askErr error) {
	// The job is written from its own goroutine: a statement can be larger
	// than the pipe's buffer, and a worker that dies before reading it must
	// not leave this write blocked.
	go func() {
		_, err := w.stdin.Write(job)
		wrote <- err
		if err != nil {
			_ = w.stdin.Close()
		}
	}()
	exited := func() error {
		<-w.done
		return w.waitErr
	}
	select {
	case line := <-firstLine:
		var q wireResult
		if err := json.Unmarshal(line, &q); err != nil || q.Ask == nil {
			_ = w.stdin.Close()
			return exited(), nil
		}
		script, err := viewsFor(*q.Ask)
		if err != nil {
			w.kill()
			_ = w.stdin.Close()
			<-w.done
			return nil, fmt.Errorf("build the views for the statement: %w", err)
		}
		answer, err := json.Marshal(wireViews{ViewsSQL: script})
		if err != nil {
			w.kill()
			_ = w.stdin.Close()
			<-w.done
			return nil, err
		}
		// A write that fails means the worker is gone; its exit says why.
		_, _ = w.stdin.Write(answer)
		_ = w.stdin.Close()
		return exited(), nil
	case <-w.done:
		return w.waitErr, nil
	}
}

// takeStandby returns a standby worker for a statement, or nil when there is
// none: the statement then starts its own. A standby found dead is dropped
// here and never handed over.
func (r *Runner) takeStandby() *worker {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.standbys) > 0 {
		// The oldest first: it has had the longest to finish opening, and
		// the newest may have started a moment ago.
		w := r.standbys[0]
		r.standbys[0] = nil
		r.standbys = r.standbys[1:]
		select {
		case <-w.done:
			r.standbyDiedLocked()
			continue
		default:
		}
		r.standbyDeaths = 0
		r.touchStandbysLocked()
		return w
	}
	return nil
}

// topUpStandbys starts standbys up to one per slot. It runs when a statement
// takes one and after every statement, so an install nobody runs SQL on never
// starts one, and starting them never delays a statement: it happens off the
// caller.
//
// One per slot, not per FREE slot: the replacement starts while the statement
// that took a standby is still running, so statements that follow each other
// closely find one that has finished opening. Measured on a c7g.2xlarge with
// capture running, a standby that waits holds about 11 MB of private memory
// (26 MB counting its share of the binary's pages), against the memory limit
// of up to 2 GB a running statement has. The statements running at once are
// still at most maxInFlight; the standbys add that small, fixed amount.
func (r *Runner) topUpStandbys() {
	if !r.standby {
		return
	}
	go func() {
		for r.addStandby() {
		}
	}()
}

// addStandby starts one standby if there is a free slot without one, and
// reports whether it did.
func (r *Runner) addStandby() bool {
	r.mu.Lock()
	r.pruneStandbysLocked()
	want := !r.closed && time.Now().After(r.standbyOffUntil) && len(r.standbys) < r.maxInFlight
	r.mu.Unlock()
	if !want {
		return false
	}
	w, err := r.startWorker()
	if err != nil {
		// Not a failed statement: the next one starts its own worker, and
		// that start reports the error if it is still there.
		r.noteStandby("could not start a standby worker; statements start their own", err)
		return false
	}
	r.mu.Lock()
	// Counted again: another top-up may have added one while this started.
	if r.closed || len(r.standbys) >= r.maxInFlight {
		r.mu.Unlock()
		stopStandby(w)
		return false
	}
	w.standby = true
	r.standbys = append(r.standbys, w)
	r.touchStandbysLocked()
	r.mu.Unlock()
	return true
}

// pruneStandbysLocked drops standbys that died while they waited.
func (r *Runner) pruneStandbysLocked() {
	live := r.standbys[:0]
	for _, w := range r.standbys {
		select {
		case <-w.done:
			r.standbyDiedLocked()
		default:
			live = append(live, w)
		}
	}
	for i := len(live); i < len(r.standbys); i++ {
		r.standbys[i] = nil
	}
	r.standbys = live
}

// standbyDiedLocked counts a standby that died before it was used, and stops
// starting them for a while after too many in a row.
func (r *Runner) standbyDiedLocked() {
	r.standbyDeaths++
	if r.standbyDeaths >= standbyMaxDeaths {
		r.standbyDeaths = 0
		r.standbyOffUntil = time.Now().Add(standbyOffFor)
		go r.noteStandby(fmt.Sprintf("%d standby workers in a row ended before they were used; none is started for %s, and statements start their own",
			standbyMaxDeaths, standbyOffFor), nil)
	}
}

func (r *Runner) noteStandbyDeath() {
	r.mu.Lock()
	r.standbyDiedLocked()
	r.mu.Unlock()
}

// touchStandbysLocked restarts the idle clock: a statement used a standby, or
// one was added after a statement.
func (r *Runner) touchStandbysLocked() {
	if r.standbyTimer == nil {
		r.standbyTimer = time.AfterFunc(r.standbyIdle, r.stopStandbys)
		return
	}
	r.standbyTimer.Reset(r.standbyIdle)
}

// stopStandbys stops every waiting standby. A statement that arrives later
// starts its own worker, and standbys come back after it.
func (r *Runner) stopStandbys() {
	r.mu.Lock()
	ws := r.standbys
	r.standbys = nil
	r.mu.Unlock()
	for _, w := range ws {
		stopStandby(w)
	}
}

// stopStandby ends a worker that was never given a job, and waits for it:
// closing its stdin is how it is told, and the kill is for one that does not
// listen.
func stopStandby(w *worker) {
	_ = w.stdin.Close()
	select {
	case <-w.done:
	case <-time.After(5 * time.Second):
		w.kill()
		<-w.done
	}
}

// noteStandby logs a change in the standbys' state once, not once per
// statement.
func (r *Runner) noteStandby(msg string, err error) {
	key := msg
	if err != nil {
		key += ": " + err.Error()
	}
	r.mu.Lock()
	same := r.standbyNote == key
	r.standbyNote = key
	r.mu.Unlock()
	if same {
		return
	}
	if err != nil {
		slog.Warn("sql on the copy: "+msg, "error", err)
		return
	}
	slog.Warn("sql on the copy: " + msg)
}

// Close stops the standby workers and starts no more. Statements still run
// after it, each on a worker of its own. A Runner with Config.Standby must be
// closed when it is no longer used; without Close its standbys still end when
// this process does, since their stdin closes with it.
func (r *Runner) Close() {
	r.mu.Lock()
	r.closed = true
	if r.standbyTimer != nil {
		r.standbyTimer.Stop()
	}
	r.mu.Unlock()
	r.stopStandbys()
}

// childEnv is the scrubbed environment the worker gets: what a process and
// an in-memory DuckDB need to start, and the worker marker. No credentials,
// no DSNs, no AWS variables: the worker has no network and no business with
// them, and a scrubbed environment is one less thing a query could read
// back (DuckDB 1.4.5 has no getenv function, but the rule does not depend on
// that staying true).
func childEnv() []string {
	keep := map[string]bool{"PATH": true, "HOME": true, "TMPDIR": true, "TMP": true, "TEMP": true,
		"SYSTEMROOT": true, "USERPROFILE": true}
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if keep[k] {
			env = append(env, kv)
		}
	}
	return append(env, workerEnv+"=1")
}

// allowedDirs is what the worker admits: each copy directory as given (the
// views script names files under that spelling) plus, when it is a symbolic
// link, the directory it resolves to, since DuckDB compares path text.
func allowedDirs(dirs []string) []string {
	out := make([]string, 0, len(dirs))
	seen := map[string]bool{}
	add := func(d string) {
		if !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	for _, d := range dirs {
		add(d)
		if real, err := filepath.EvalSymlinks(d); err == nil {
			add(real)
		}
	}
	return out
}

// cappedBuffer collects a pipe up to max bytes. Past it, with drop set the
// rest is discarded (diagnostics); otherwise the writer reports an error
// (which stops exec's copy goroutine) and fires onOverflow, which kills the
// child so it does not block forever on a pipe nobody reads.
//
// exec's copy goroutine writes to it from the moment the process starts, and
// a worker's stdout learns its cap only when the job is handed over (arm), so
// the fields are guarded by mu. buf is read without it only once the process
// has exited and its pipes are drained.
type cappedBuffer struct {
	mu         sync.Mutex
	buf        bytes.Buffer
	max        int64
	drop       bool
	overflowed bool
	onOverflow func()
	// firstLine, when set, receives a copy of the first newline-terminated
	// line once it has been written (the worker's question, #2029). The line
	// stays in buf.
	firstLine chan []byte
	scanned   int
	lineSent  bool
}

// arm sets what a job allows this buffer: its cap, what to do past it, and
// where the first line goes.
func (c *cappedBuffer) arm(max int64, onOverflow func(), firstLine chan []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.max, c.onOverflow, c.firstLine = max, onOverflow, firstLine
}

func (c *cappedBuffer) didOverflow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.overflowed
}

// text is what was collected so far, for an error report.
func (c *cappedBuffer) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.overflowed {
		if c.drop {
			return len(p), nil
		}
		return 0, ErrResultTooLarge
	}
	if int64(c.buf.Len())+int64(len(p)) > c.max {
		c.overflowed = true
		if c.drop {
			room := c.max - int64(c.buf.Len())
			c.buf.Write(p[:room])
			return len(p), nil
		}
		if c.onOverflow != nil {
			c.onOverflow()
		}
		return 0, ErrResultTooLarge
	}
	n, err := c.buf.Write(p)
	if c.firstLine != nil && !c.lineSent {
		b := c.buf.Bytes()
		if i := bytes.IndexByte(b[c.scanned:], '\n'); i >= 0 {
			c.lineSent = true
			c.firstLine <- bytes.Clone(b[:c.scanned+i])
		} else {
			c.scanned = len(b)
		}
	}
	return n, err
}
