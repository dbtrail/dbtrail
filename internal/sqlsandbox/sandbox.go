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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
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
	// Spilling is off, so a query past it fails with an out-of-memory error
	// instead of slowing down and writing to disk.
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
	return Limits{Threads: 2, MemoryLimit: "2GB", Timeout: 60 * time.Second, MaxRows: 1000, MaxResultBytes: 64 << 20}
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
	// SQL is the user's statement.
	SQL string
	// Limits override the Runner's defaults field by field.
	Limits Limits
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
// timestamps RFC 3339 in UTC, HUGEINT and DECIMAL are text, a BLOB is its
// text when valid UTF-8 and "0x.." otherwise, NaN and infinities are text.
// A text cell longer than MaxCellBytes is cut and ends with a marker;
// TruncatedCells counts them.
type Result struct {
	Columns        []Column      `json:"columns"`
	Rows           [][]any       `json:"rows"`
	Truncated      bool          `json:"truncated"`
	TruncatedCells int           `json:"truncated_cells"`
	Elapsed        time.Duration `json:"elapsed_ns"`
}

// ErrCopyNotLocal: the job names no local copy directory. v1 reads a local
// copy only; a copy that lives only on S3 is not queryable here.
var ErrCopyNotLocal = errors.New("running SQL on the copy needs a local copy directory; this copy lives only on S3")

// ErrBusy: the user already has a query running, or the runner is at its
// limit of concurrent workers.
var ErrBusy = errors.New("a query is already running (yours, or the server is at its limit); wait for it to finish")

// ErrResultTooLarge: the result would exceed Limits.MaxResultBytes. Either
// side may say so: the worker counts as it collects rows, and the parent
// counts what it reads from the pipe.
var ErrResultTooLarge = errors.New("the result is too large to return; narrow the query")

// RefusedError: the statement was not run at all because of its shape (empty,
// more than one statement, not a SELECT, a syntax error).
type RefusedError struct{ Reason string }

func (e *RefusedError) Error() string { return "query refused: " + e.Reason }

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
}

// Runner spawns workers and keeps the per-user and the global gate.
type Runner struct {
	exe         string
	args        []string
	limits      Limits
	maxInFlight int

	mu       sync.Mutex
	busy     map[string]bool
	inFlight int

	// onStart is a test hook that receives the child's pid.
	onStart func(pid int)
}

// New builds a Runner.
func New(cfg Config) *Runner {
	r := &Runner{
		exe:         cfg.Exe,
		args:        cfg.Args,
		limits:      cfg.Limits.withDefaults(DefaultLimits()),
		maxInFlight: cfg.MaxInFlight,
		busy:        map[string]bool{},
	}
	if r.maxInFlight <= 0 {
		r.maxInFlight = DefaultMaxInFlight
	}
	return r
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
// busy) return before any process starts.
func (r *Runner) Run(ctx context.Context, job Job) (Result, error) {
	if strings.TrimSpace(job.SQL) == "" {
		return Result{}, &RefusedError{Reason: "the query is empty"}
	}
	if !localCopy(job.CopyDirs) {
		return Result{}, ErrCopyNotLocal
	}
	limits := job.Limits.withDefaults(r.limits)
	if !r.acquire(job.User) {
		return Result{}, ErrBusy
	}
	defer r.release(job.User)
	return r.spawn(ctx, job, limits)
}

// acquire takes a worker slot: the global one always, the user's when the
// job names a user.
func (r *Runner) acquire(user string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
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
	CopyDirs    []string `json:"copy_dirs"`
	ViewsSQL    string   `json:"views_sql"`
	SQL         string   `json:"sql"`
	Threads     int      `json:"threads"`
	MemoryLimit string   `json:"memory_limit"`
	MaxRows     int      `json:"max_rows"`
	// MaxResultBytes is counted by the worker as it collects rows, so it
	// stops before building a result the parent would refuse anyway.
	MaxResultBytes int64 `json:"max_result_bytes"`
	// TimeoutNS is the parent's wall clock, so the child can stop on its own
	// shortly after it if the parent is no longer there to kill it.
	TimeoutNS int64 `json:"timeout_ns"`
}

// wireResult is what the child writes to stdout: a result, or a structured
// error. The child exits 0 in both cases; a non-zero exit is a protocol
// failure (it could not even read the job or encode its answer).
type wireResult struct {
	Columns        []Column   `json:"columns,omitempty"`
	Rows           [][]any    `json:"rows,omitempty"`
	Truncated      bool       `json:"truncated"`
	TruncatedCells int        `json:"truncated_cells"`
	ElapsedNS      int64      `json:"elapsed_ns"`
	Error          *wireError `json:"error,omitempty"`
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

func (r *Runner) spawn(ctx context.Context, job Job, limits Limits) (Result, error) {
	exe := r.exe
	if exe == "" {
		var err error
		if exe, err = workerExe(); err != nil {
			return Result{}, &WorkerError{Err: fmt.Errorf("locate the worker executable: %w", err)}
		}
	}
	args := r.args
	if args == nil {
		args = []string{WorkerCommand}
	}
	in, err := json.Marshal(wireJob{
		CopyDirs: allowedDirs(job.CopyDirs), ViewsSQL: job.ViewsSQL, SQL: job.SQL,
		Threads: limits.Threads, MemoryLimit: limits.MemoryLimit, MaxRows: limits.MaxRows,
		MaxResultBytes: limits.MaxResultBytes, TimeoutNS: int64(limits.Timeout),
	})
	if err != nil {
		return Result{}, &WorkerError{Err: err}
	}
	if err := ctx.Err(); err != nil {
		// Already cancelled: report the cancel, not a worker that failed.
		return Result{}, err
	}

	cctx, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, exe, args...)
	cmd.Env = childEnv()
	cmd.Stdin = bytes.NewReader(in)
	setProcessGroup(cmd)
	// On timeout or cancel, kill the whole process group: DuckDB's worker
	// threads belong to the child, and a plain Kill of the leader is enough
	// for them, but a group kill also covers anything the child might spawn.
	cmd.Cancel = func() error { return killProcessGroup(cmd.Process) }
	// If the group kill somehow leaves the pipes open, stop waiting on them.
	cmd.WaitDelay = 5 * time.Second
	stdout := &cappedBuffer{max: limits.MaxResultBytes, onOverflow: func() { _ = killProcessGroup(cmd.Process) }}
	// stderr is diagnostics: past its cap the rest is dropped, never a reason
	// to fail the child.
	stderr := &cappedBuffer{max: 64 << 10, drop: true}
	cmd.Stdout, cmd.Stderr = stdout, stderr

	if err := cmd.Start(); err != nil {
		return Result{}, &WorkerError{Err: fmt.Errorf("start: %w", err)}
	}
	pid := cmd.Process.Pid
	if r.onStart != nil {
		r.onStart(pid)
	}
	waitErr := cmd.Wait()

	switch {
	case ctx.Err() != nil:
		return Result{}, ctx.Err()
	case errors.Is(cctx.Err(), context.DeadlineExceeded):
		return Result{}, &TimeoutError{Limit: limits.Timeout, PID: pid}
	case stdout.overflowed:
		return Result{}, ErrResultTooLarge
	}

	var out wireResult
	dec := json.NewDecoder(bytes.NewReader(stdout.buf.Bytes()))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		if waitErr != nil {
			err = fmt.Errorf("%w (no result: %v)", waitErr, err)
		} else {
			err = fmt.Errorf("unreadable result: %w", err)
		}
		return Result{}, &WorkerError{Err: err, PID: pid, Stderr: stderr.buf.String()}
	}
	if out.Error != nil {
		switch out.Error.Kind {
		case errRefused:
			return Result{}, &RefusedError{Reason: out.Error.Message}
		case errQuery:
			return Result{}, &QueryError{Message: out.Error.Message}
		case errTooLarge:
			return Result{}, ErrResultTooLarge
		default:
			return Result{}, &WorkerError{Err: errors.New(out.Error.Message), PID: pid, Stderr: stderr.buf.String()}
		}
	}
	res := Result{Columns: out.Columns, Rows: out.Rows, Truncated: out.Truncated,
		TruncatedCells: out.TruncatedCells, Elapsed: time.Duration(out.ElapsedNS)}
	if res.Rows == nil {
		res.Rows = [][]any{}
	}
	return res, nil
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
type cappedBuffer struct {
	buf        bytes.Buffer
	max        int64
	drop       bool
	overflowed bool
	onOverflow func()
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
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
	return c.buf.Write(p)
}
