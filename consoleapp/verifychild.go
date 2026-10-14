package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/debug"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// The heavy verify work runs in a process of its own (#2385, slice 4): the
// drill-down (Explain), and each table a baseline-anchored check compares.
// `watch` is the console AND the capture plane: a table that needs more
// memory than the host has used to take the whole process with it, capture
// included. In a child, the kernel stops the child.
//
// How far that goes depends on the host. Where the daemon was handed a
// cgroup (a systemd unit with Delegate=yes, or a container whose cgroup is
// writable), the child gets a memory cgroup of its own and reaches ITS
// ceiling first: nothing else is a candidate. Everywhere else the child is
// only the kernel's first choice once the HOST is out of memory, which is
// better than today and not the same promise.
//
// The parent keeps everything that is not the work: the heavy-work gate
// (#2389), the job bookkeeping, the log line, the verdicts it persists, and
// the audit record, which the HTTP handler writes after an explanation is
// delivered.

// verifyWorkerCommand is the hidden subcommand the child is started as. The
// double underscore keeps it out of usage telemetry, as __sql-worker is.
const verifyWorkerCommand = "__verify-worker"

// verifyChildMaxOutput bounds what the parent reads back. An explanation
// carries a capped list of differing rows and its rendered text, a verdict
// a few lines; this is a guard on the pipe, not a product number.
const verifyChildMaxOutput = 64 << 20

// verifyChildJob is what the child is handed on stdin. The DSN carries a
// password, so it never goes in the arguments or the environment, where any
// local user could read it.
type verifyChildJob struct {
	// Kind is the work: verifyChildExplain or verifyChildCheck.
	Kind      string              `json:"kind"`
	IndexDSN  string              `json:"index_dsn"`
	NoArchive bool                `json:"no_archive"`
	Schema    string              `json:"schema"`
	Table     string              `json:"table"`
	Pair      verify.BaselinePair `json:"pair"`
	// SourceFlavor is the run's, read once by the parent; "" reads it in the
	// child (Explain).
	SourceFlavor string `json:"source_flavor,omitempty"`
	// BucketStores is the console's bucket→store table (an S3 endpoint and
	// keys per bucket, from its registry), which lives only in the parent's
	// memory: without it a child reads a server's copy on its own S3 with the
	// environment's endpoint and keys, and fails. Secret included, so it
	// travels on stdin only (storage.EncodeBucketStoresForChild).
	BucketStores json.RawMessage `json:"bucket_stores,omitempty"`
	// Tuning is the DuckDB budget per session the parent's lane gave
	// (laneCtx): the child's sessions get the same.
	Tuning duckdbutil.Tuning `json:"tuning"`
}

// The kinds of work a child does.
const (
	verifyChildExplain = "explain"
	verifyChildCheck   = "check"
)

// verifyChildAnswer is what the child writes on stdout: the explanation or
// the table's verdict, or the error the work returned.
type verifyChildAnswer struct {
	Result *console.VerifyExplanation `json:"result,omitempty"`
	Table  *verify.TableResult        `json:"table,omitempty"`
	Error  string                     `json:"error,omitempty"`
}

// verifyChildGoAllowance is what the child may hold outside DuckDB's limits:
// each session's memory outside its memory_limit (a session capped at 64 MB
// was measured at about 250 MB resident, see heavybudget.go) and Go's own
// heap, where the work keeps the latest change of every key in its window.
const verifyChildGoAllowance = 512 << 20

// verifyChildMemory is the memory the child may use in all: an Explain runs
// checkSessions DuckDB sessions together, each with the lane's budget (a
// check table runs no more than that), plus
// verifyChildGoAllowance. The child's ceiling is this plus the worker headroom
// (sqlsandbox.CeilingBytes). perSession is one session's limit.
func verifyChildMemory(t duckdbutil.Tuning) (total string, perSession int64, err error) {
	per, err := cliutil.ParseByteSize(t.MemoryLimit)
	if err != nil {
		return "", 0, fmt.Errorf("the session memory %q does not parse: %w", t.MemoryLimit, err)
	}
	if per <= 0 {
		return "", 0, fmt.Errorf("the session memory %q is not a size", t.MemoryLimit)
	}
	return fmt.Sprintf("%dMiB", (per*checkSessions+verifyChildGoAllowance)>>20), per, nil
}

// duckdbOutsideLimit is what one DuckDB session holds outside its
// memory_limit: a session capped at 64 MB was measured at about 250 MB
// resident (heavybudget.go).
const duckdbOutsideLimit = 190 << 20

// verifyChildEnv is the console's environment plus, unless the operator set
// one, a GOMEMLIMIT for the child's Go heap: what its ceiling leaves after
// the DuckDB sessions that run at once (two for Explain, one for a check
// table, whose reads run one after the other), their limits and what they
// hold outside them; and the console's log level, so the child logs what
// the console would. Go does
// not know the cgroup's ceiling, and without it lets the heap grow to twice
// what it holds before collecting; a limit above what fits would only start
// collecting harder once the kernel had stopped the child.
func verifyChildEnv(mem string, perSession int64, sessions int) []string {
	env := append(os.Environ(), verifyChildLogLevelEnv+"="+consoleLogLevel())
	if _, set := os.LookupEnv("GOMEMLIMIT"); set {
		return env
	}
	ceiling, err := sqlsandbox.CeilingBytes(mem)
	goHeap := ceiling - int64(sessions)*(perSession+duckdbOutsideLimit)
	if err != nil || goHeap <= 0 {
		return env
	}
	return append(env, fmt.Sprintf("GOMEMLIMIT=%dMiB", goHeap>>20))
}

// verifyChildLogLevelEnv carries the console's log level to the child.
const verifyChildLogLevelEnv = "BINTRAIL_VERIFY_CHILD_LOG_LEVEL"

// consoleLogLevel is the lowest level this process's logger takes.
func consoleLogLevel() string {
	ctx := context.Background()
	switch {
	case slog.Default().Enabled(ctx, slog.LevelDebug):
		return "debug"
	case slog.Default().Enabled(ctx, slog.LevelInfo):
		return "info"
	case slog.Default().Enabled(ctx, slog.LevelWarn):
		return "warn"
	}
	return "error"
}

// verifyChildSeen is the once-per-process notices a verify child already
// relayed in this process. The verify engine gives some once per process
// (an index that `bintrail index` also wrote into, say, through
// reconstruct.NoticeOnce); they would otherwise come once per check table,
// since each table is a new process. Every other line is relayed each time.
var verifyChildSeen = &sqlsandbox.LineSet{}

// verifyChildStopGrace is how long a child whose job was dropped has to stop
// by itself and remove its working files before it is killed.
const verifyChildStopGrace = 10 * time.Second

// verifyChildSessions is how many DuckDB sessions a job runs at once: an
// Explain its reconstruction and its join together, a check table its reads
// one after the other.
func verifyChildSessions(kind string) int {
	if kind == verifyChildCheck {
		return 1
	}
	return checkSessions
}

// runVerifyChild runs job in a child process and returns its answer. what
// names the work in errors ("explain wp.posts"), noun the process ("drill-
// down"). Beyond the work's own error, two of its own: a child stopped at its
// memory ceiling, and one that ended without an answer. ctx ending stops the
// child and returns ctx.Err() unwrapped, so a cancel still reads as one.
func runVerifyChild(ctx context.Context, job verifyChildJob, what, noun string) (verifyChildAnswer, error) {
	t := duckdbutil.DefaultTuningFor(ctx)
	mem, perSession, err := verifyChildMemory(t)
	if err != nil {
		return verifyChildAnswer{}, fmt.Errorf("%s: %w", what, err)
	}
	job.Tuning = t
	if job.BucketStores, err = storage.EncodeBucketStoresForChild(storage.BucketStores()); err != nil {
		return verifyChildAnswer{}, fmt.Errorf("%s: encode the S3 stores: %w", what, err)
	}
	sessions := verifyChildSessions(job.Kind)
	in, err := json.Marshal(job)
	if err != nil {
		return verifyChildAnswer{}, fmt.Errorf("%s: encode the job: %w", what, err)
	}
	out, err := sqlsandbox.RunChild(ctx, sqlsandbox.ChildJob{
		Args: []string{verifyWorkerCommand},
		// The console's own environment: the child reads the index, S3 and
		// the archives exactly as the console would.
		Env:         verifyChildEnv(mem, perSession, sessions),
		MemoryLimit: mem,
		Input:       append(in, '\n'),
		MaxOutput:   verifyChildMaxOutput,
		StopGrace:   verifyChildStopGrace,
		LogMsg:      "verify: " + noun + " process",
		LogAttrs:    []any{"table", job.Schema + "." + job.Table},
		LogOnce:     verifyChildSeen,
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			return verifyChildAnswer{}, err
		}
		var oom *sqlsandbox.ChildOOMError
		if errors.As(err, &oom) {
			return verifyChildAnswer{}, fmt.Errorf("%s ran out of memory: %w (the ceiling grows with this host's memory)", what, err)
		}
		if errors.Is(err, sqlsandbox.ErrResultTooLarge) {
			return verifyChildAnswer{}, fmt.Errorf("%s: the %s's answer was larger than %d MiB", what, noun, verifyChildMaxOutput>>20)
		}
		return verifyChildAnswer{}, fmt.Errorf("%s: the %s process failed: %w", what, noun, err)
	}
	var ans verifyChildAnswer
	if err := json.Unmarshal(out, &ans); err != nil {
		return verifyChildAnswer{}, fmt.Errorf("%s: the %s process answered something unreadable (%q): %w", what, noun, truncate(out, 200), err)
	}
	if ans.Error != "" {
		return verifyChildAnswer{}, errors.New(ans.Error)
	}
	return ans, nil
}

// explainInChild is explainNow, run in a child process.
func explainInChild(ctx context.Context, indexDSN string, noArchive bool, schema, table string, pair verify.BaselinePair) (*console.VerifyExplanation, error) {
	ans, err := runVerifyChild(ctx, verifyChildJob{Kind: verifyChildExplain,
		IndexDSN: indexDSN, NoArchive: noArchive, Schema: schema, Table: table, Pair: pair,
	}, fmt.Sprintf("explain %s.%s", schema, table), "drill-down")
	if err != nil {
		return nil, err
	}
	if ans.Result == nil {
		return nil, fmt.Errorf("explain %s.%s: the drill-down process answered nothing", schema, table)
	}
	return ans.Result, nil
}

// checkPairInChild is checkPairNow, run in a child process: one table of a
// baseline-anchored check.
func checkPairInChild(ctx context.Context, indexDSN string, noArchive bool, flavor string, p verify.BaselinePair) (verify.TableResult, error) {
	ans, err := runVerifyChild(ctx, verifyChildJob{Kind: verifyChildCheck, SourceFlavor: flavor,
		IndexDSN: indexDSN, NoArchive: noArchive, Schema: p.Schema, Table: p.Table, Pair: p,
	}, fmt.Sprintf("check %s.%s", p.Schema, p.Table), "check")
	if err != nil {
		return verify.TableResult{}, err
	}
	if ans.Table == nil {
		return verify.TableResult{}, fmt.Errorf("check %s.%s: the check process answered nothing", p.Schema, p.Table)
	}
	return *ans.Table, nil
}

// checkPairNow compares one table's pair, in the process it is called in:
// the child's half of checkPairInChild. It opens its own index connection,
// as explainNow does.
func checkPairNow(ctx context.Context, indexDSN string, noArchive bool, flavor string, p verify.BaselinePair) (verify.TableResult, error) {
	cfg, closeDB, err := baselineConfigFor(indexDSN, noArchive, flavor)
	if err != nil {
		return verify.TableResult{}, err
	}
	defer closeDB()
	return verify.VerifyBaselinePair(ctx, cfg, p)
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

// explainChildRun and checkChildRun are what the child runs; variables so a
// test can give it work that needs no index.
var (
	explainChildRun = explainNow
	checkChildRun   = checkPairNow
)

// verifyWorkerMain is the child: it reads one job from stdin, runs it, and
// writes the answer to stdout. stdin stays open for as long as the parent
// wants the answer, so reading it to the end means the parent dropped the
// job or is gone: the work is canceled, so its cleanup runs (the join's
// working files), and if it has not stopped within verifyChildStopGrace,
// parentGone is called. The parent kills it at that same grace, but a parent
// that died cannot.
func verifyWorkerMain(stdin io.Reader, stdout, stderr io.Writer, parentGone func()) (code int) {
	sqlsandbox.LowerOOMPriority(stderr)
	dec := json.NewDecoder(stdin)
	var job verifyChildJob
	if err := dec.Decode(&job); err != nil {
		fmt.Fprintf(stderr, "verify worker: read the job: %v\n", err)
		return 2
	}
	stores, err := storage.DecodeBucketStoresForChild(job.BucketStores)
	if err != nil {
		fmt.Fprintf(stderr, "verify worker: read the S3 stores: %v\n", err)
		return 2
	}
	storage.SetBucketStores(stores)
	ctx, cancel := context.WithCancel(duckdbutil.WithSessionTuning(context.Background(), job.Tuning))
	defer cancel()
	go func() {
		_, _ = io.Copy(io.Discard, io.MultiReader(dec.Buffered(), stdin))
		cancel()
		time.Sleep(verifyChildStopGrace)
		parentGone()
	}()
	ans := runVerifyJob(ctx, job, stderr)
	if ctx.Err() != nil {
		// Nobody is waiting for the answer.
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(ans); err != nil {
		fmt.Fprintf(stderr, "verify worker: write the answer: %v\n", err)
		return 2
	}
	return 0
}

// runVerifyJob runs the job and turns a panic into the answer the parent
// used to give one in-process ("internal error: ..."): the page shows that
// line, and the stack goes to stderr, which the parent logs.
func runVerifyJob(ctx context.Context, job verifyChildJob, stderr io.Writer) (ans verifyChildAnswer) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(stderr, "verify worker: panic: %v\n%s", r, debug.Stack())
			ans = verifyChildAnswer{Error: fmt.Sprintf("internal error: %v", r)}
		}
	}()
	switch job.Kind {
	case verifyChildCheck:
		res, err := checkChildRun(ctx, job.IndexDSN, job.NoArchive, job.SourceFlavor, job.Pair)
		if err != nil {
			return verifyChildAnswer{Error: err.Error()}
		}
		return verifyChildAnswer{Table: &res}
	case verifyChildExplain:
		res, err := explainChildRun(ctx, job.IndexDSN, job.NoArchive, job.Schema, job.Table, job.Pair)
		if err != nil {
			return verifyChildAnswer{Error: err.Error()}
		}
		return verifyChildAnswer{Result: res}
	}
	// A parent newer than this binary cannot happen (the child is the same
	// executable), so this is a bug, said plainly rather than guessed at.
	return verifyChildAnswer{Error: fmt.Sprintf("unknown verify job %q", job.Kind)}
}

// isVerifyWorkerInvocation reports whether this process was started as the
// verify child (Explain or a check table).
func isVerifyWorkerInvocation(args []string) bool {
	return len(args) > 1 && args[1] == verifyWorkerCommand
}

// runVerifyWorker is the child's whole life, for a main() or the hidden
// command.
func runVerifyWorker() int {
	level := os.Getenv(verifyChildLogLevelEnv)
	if level == "" {
		level = "warn"
	}
	observe.Setup(os.Stderr, "text", level)
	return verifyWorkerMain(os.Stdin, os.Stdout, os.Stderr, func() { os.Exit(1) })
}

// verifyWorkerCmd serves the child one cobra dispatch in, for a main() that
// does not call RunSQLWorkerIfInvoked first.
var verifyWorkerCmd = &cobra.Command{
	Use:    verifyWorkerCommand,
	Hidden: true,
	Short:  "Internal: run one verify job for the console (started by the console itself)",
	Args:   cobra.NoArgs,
	RunE: func(*cobra.Command, []string) error {
		if code := runVerifyWorker(); code != 0 {
			return fmt.Errorf("verify worker exited with status %d", code)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(verifyWorkerCmd)
}
