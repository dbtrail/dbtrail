package consoleapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The worker command exists under the name the runner spawns, is hidden, and
// is named so the root's telemetry hook records nothing for it (the hook
// skips __-prefixed commands; TestRootHookIsNotShadowed forbids a per-command
// override, so the name is the mechanism).
func TestSQLWorkerCommand_registeredHiddenAndUninstrumented(t *testing.T) {
	var found bool
	for _, c := range rootCmd.Commands() {
		if c.Name() != sqlsandbox.WorkerCommand {
			continue
		}
		found = true
		if !c.Hidden {
			t.Error("the sql worker command must be hidden from help")
		}
	}
	if !found {
		t.Fatalf("no %q command registered on the console root", sqlsandbox.WorkerCommand)
	}
	if !strings.HasPrefix(sqlsandbox.WorkerCommand, "__") {
		t.Errorf("WorkerCommand %q must start with __ so telemetry's uninstrumented rule skips it", sqlsandbox.WorkerCommand)
	}
}

// The command runs a real job through the cobra tree: a job on stdin comes
// back as a result on stdout. This is the wiring the runner depends on when
// it re-executes the console binary.
func TestSQLWorkerCommand_runsAJobThroughCobra(t *testing.T) {
	job := map[string]any{
		"copy_dirs": []string{t.TempDir()}, "views_sql": "", "sql": "SELECT 41 + 1 AS answer",
		"threads": 1, "memory_limit": "64MB", "max_rows": 10,
	}
	in, _ := json.Marshal(job)
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origIn, origOut := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = stdinR, stdoutW
	t.Cleanup(func() { os.Stdin, os.Stdout = origIn, origOut })
	go func() { stdinW.Write(in); stdinW.Close() }()
	var out bytes.Buffer
	readDone := make(chan struct{})
	go func() { out.ReadFrom(stdoutR); close(readDone) }()

	rootCmd.SetArgs([]string{sqlsandbox.WorkerCommand})
	t.Cleanup(func() { rootCmd.SetArgs(nil) })
	execErr := rootCmd.Execute()
	stdoutW.Close()
	<-readDone
	os.Stdin, os.Stdout = origIn, origOut
	if execErr != nil {
		t.Fatalf("Execute: %v", execErr)
	}
	var res struct {
		Columns []struct{ Name string }
		Rows    [][]any
		Error   *struct{ Kind, Message string }
	}
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		t.Fatalf("worker stdout is not a result: %v\n%s", err, out.String())
	}
	if res.Error != nil {
		t.Fatalf("worker error: %+v", res.Error)
	}
	if len(res.Columns) != 1 || res.Columns[0].Name != "answer" || len(res.Rows) != 1 || res.Rows[0][0] != float64(42) {
		t.Errorf("result = %+v", res)
	}
}

// Production wiring across a real process boundary: a Runner with a zero
// Config (this binary, the hidden command, default caps) runs a query
// through the whole cobra tree of the console, root pre-run hook included
// (TestMain dispatches the worker invocation to Main).
func TestSQLWorker_realDefaultsThroughTheCobraTree(t *testing.T) {
	r := sqlsandbox.New(sqlsandbox.Config{})
	res, err := r.Run(context.Background(), sqlsandbox.Job{CopyDirs: []string{t.TempDir()}, SQL: "SELECT 41 + 1 AS answer"})
	if err != nil {
		t.Fatalf("through the real tree: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != json.Number("42") || res.Columns[0].Name != "answer" {
		t.Errorf("result = %+v", res)
	}
}

// The thin wrapper answers the worker invocation BEFORE Main, so an embedder
// copying main.go gets the early exit too. A text guard on the one file the
// thin-wrapper test allows.
func TestConsoleMainAnswersTheWorkerFirst(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "cmd", "bintrail-console", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	early := strings.Index(string(src), "consoleapp.RunSQLWorkerIfInvoked()")
	main := strings.Index(string(src), "consoleapp.Main(")
	if early < 0 || main < 0 || early > main {
		t.Errorf("cmd/bintrail-console/main.go must call consoleapp.RunSQLWorkerIfInvoked() before consoleapp.Main (positions %d, %d)", early, main)
	}
}
