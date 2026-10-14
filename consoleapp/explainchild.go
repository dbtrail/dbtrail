package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// The verify drill-down (Explain) runs in a process of its own (#2385, slice
// 4). `watch` is the console AND the capture plane: a table whose drill-down
// needs more memory than the host has used to take the whole process with
// it, capture included. In a child, the kernel stops the child.
//
// How far that goes depends on the host. Where the daemon was handed a
// cgroup (a systemd unit with Delegate=yes, or a container whose cgroup is
// writable), the child gets a memory cgroup of its own and reaches ITS
// ceiling first: nothing else is a candidate. Everywhere else the child is
// only the kernel's first choice once the HOST is out of memory, which is
// better than today and not the same promise.
//
// The parent keeps everything that is not the work: the heavy-work gate
// (#2389), the job bookkeeping, the log line, and the audit record, which
// the HTTP handler writes after the result is delivered.

// verifyWorkerCommand is the hidden subcommand the child is started as. The
// double underscore keeps it out of usage telemetry, as __sql-worker is.
const verifyWorkerCommand = "__verify-worker"

// explainMaxOutput bounds what the parent reads back. An explanation carries
// a capped list of differing rows and its rendered text; this is a guard on
// the pipe, not a product number.
const explainMaxOutput = 64 << 20

// explainChildJob is what the child is handed on stdin. The DSN carries a
// password, so it never goes in the arguments or the environment, where any
// local user could read it.
type explainChildJob struct {
	IndexDSN  string              `json:"index_dsn"`
	NoArchive bool                `json:"no_archive"`
	Schema    string              `json:"schema"`
	Table     string              `json:"table"`
	Pair      verify.BaselinePair `json:"pair"`
	// Tuning is the DuckDB budget per session the parent's lane gave
	// (laneCtx): the child's sessions get the same.
	Tuning duckdbutil.Tuning `json:"tuning"`
}

// explainChildAnswer is what the child writes on stdout: the explanation, or
// the error explainNow returned.
type explainChildAnswer struct {
	Result *console.VerifyExplanation `json:"result,omitempty"`
	Error  string                     `json:"error,omitempty"`
}

// explainChildMemory is the memory the child may use in all: an Explain runs
// checkSessions DuckDB sessions together, each with the lane's budget. The
// child's ceiling is this plus the worker headroom (sqlsandbox.CeilingBytes).
func explainChildMemory(t duckdbutil.Tuning) (string, error) {
	per, err := cliutil.ParseByteSize(t.MemoryLimit)
	if err != nil {
		return "", fmt.Errorf("the session memory %q does not parse: %w", t.MemoryLimit, err)
	}
	if per <= 0 {
		return "", fmt.Errorf("the session memory %q is not a size", t.MemoryLimit)
	}
	return fmt.Sprintf("%dMiB", per*checkSessions>>20), nil
}

// explainInChild is explainNow, run in a child process. Same arguments, same
// answers, plus two of its own: a child stopped at its memory ceiling, and
// one that ended without an answer. ctx ending kills the child.
func explainInChild(ctx context.Context, indexDSN string, noArchive bool, schema, table string, pair verify.BaselinePair) (*console.VerifyExplanation, error) {
	t := duckdbutil.DefaultTuningFor(ctx)
	mem, err := explainChildMemory(t)
	if err != nil {
		return nil, fmt.Errorf("explain %s.%s: %w", schema, table, err)
	}
	in, err := json.Marshal(explainChildJob{
		IndexDSN: indexDSN, NoArchive: noArchive, Schema: schema, Table: table, Pair: pair, Tuning: t,
	})
	if err != nil {
		return nil, fmt.Errorf("explain %s.%s: encode the job: %w", schema, table, err)
	}
	out, err := sqlsandbox.RunChild(ctx, sqlsandbox.ChildJob{
		Args: []string{verifyWorkerCommand},
		// The console's own environment: the child reads the index, S3 and
		// the archives exactly as the console would.
		Env:         os.Environ(),
		MemoryLimit: mem,
		Input:       append(in, '\n'),
		MaxOutput:   explainMaxOutput,
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			// Unwrapped, so explainLogVerdict still reads a cancel as one.
			return nil, err
		}
		var oom *sqlsandbox.ChildOOMError
		if errors.As(err, &oom) {
			return nil, fmt.Errorf("explain %s.%s ran out of memory: %w", schema, table, err)
		}
		return nil, fmt.Errorf("explain %s.%s: the drill-down process failed: %w", schema, table, err)
	}
	var ans explainChildAnswer
	if err := json.Unmarshal(out, &ans); err != nil {
		return nil, fmt.Errorf("explain %s.%s: the drill-down process answered something unreadable: %w", schema, table, err)
	}
	if ans.Error != "" {
		return nil, errors.New(ans.Error)
	}
	if ans.Result == nil {
		return nil, fmt.Errorf("explain %s.%s: the drill-down process answered nothing", schema, table)
	}
	return ans.Result, nil
}

// explainChildRun is what the child runs; a variable so a test can give it
// work that needs no index.
var explainChildRun = explainNow

// verifyWorkerMain is the child: it reads one job from stdin, runs it, and
// writes the answer to stdout. stdin stays open for as long as the parent
// lives, so reading it to the end means the parent is gone, and then
// parentGone is called: nobody is left to read the answer, and the memory
// is better given back at once.
func verifyWorkerMain(stdin io.Reader, stdout, stderr io.Writer, parentGone func()) int {
	sqlsandbox.LowerOOMPriority(stderr)
	dec := json.NewDecoder(stdin)
	var job explainChildJob
	if err := dec.Decode(&job); err != nil {
		fmt.Fprintf(stderr, "verify worker: read the job: %v\n", err)
		return 2
	}
	go func() {
		_, _ = io.Copy(io.Discard, io.MultiReader(dec.Buffered(), stdin))
		parentGone()
	}()
	ctx := duckdbutil.WithSessionTuning(context.Background(), job.Tuning)
	res, err := explainChildRun(ctx, job.IndexDSN, job.NoArchive, job.Schema, job.Table, job.Pair)
	ans := explainChildAnswer{Result: res}
	if err != nil {
		ans = explainChildAnswer{Error: err.Error()}
	}
	if err := json.NewEncoder(stdout).Encode(ans); err != nil {
		fmt.Fprintf(stderr, "verify worker: write the answer: %v\n", err)
		return 2
	}
	return 0
}

// isVerifyWorkerInvocation reports whether this process was started as the
// drill-down child.
func isVerifyWorkerInvocation(args []string) bool {
	return len(args) > 1 && args[1] == verifyWorkerCommand
}

// runVerifyWorker is the child's whole life, for a main() or the hidden
// command.
func runVerifyWorker() int {
	observe.Setup(os.Stderr, "text", "warn")
	return verifyWorkerMain(os.Stdin, os.Stdout, os.Stderr, func() { os.Exit(1) })
}

// verifyWorkerCmd serves the child one cobra dispatch in, for a main() that
// does not call RunSQLWorkerIfInvoked first.
var verifyWorkerCmd = &cobra.Command{
	Use:    verifyWorkerCommand,
	Hidden: true,
	Short:  "Internal: run one verify drill-down for the console (started by the console itself)",
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
