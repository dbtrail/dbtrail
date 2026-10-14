package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// explainChildFakeEnv names, for a re-executed test binary, which of
// explainChildFakes it runs in place of a real drill-down. The child gets
// the parent's environment, so t.Setenv in the parent reaches it.
const explainChildFakeEnv = "BINTRAIL_TEST_EXPLAIN_CHILD"

// explainChildPIDFileEnv is where the "hang" fake writes its pid.
const explainChildPIDFileEnv = "BINTRAIL_TEST_EXPLAIN_CHILD_PIDFILE"

var explainChildFakes = map[string]func(context.Context, string, bool, string, string, verify.BaselinePair) (*console.VerifyExplanation, error){
	// ok answers with what it was handed, so the test sees the job cross.
	"ok": func(ctx context.Context, dsn string, noArchive bool, schema, table string, pair verify.BaselinePair) (*console.VerifyExplanation, error) {
		return &console.VerifyExplanation{
			Schema: schema, Table: table, Anchor: pair.NewAnchor.File,
			Total:    int(pair.NewAnchor.Pos),
			Rendered: dsn + " " + strconv.FormatBool(noArchive) + " " + duckdbutil.DefaultTuningFor(ctx).MemoryLimit,
			Diffs:    []console.VerifyRowDiff{{PK: "7", Kind: "changed", Cells: []console.VerifyCellDiff{{Column: "c", Recovery: "a", Baseline: "b"}}}},
		}, nil
	},
	"error": func(context.Context, string, bool, string, string, verify.BaselinePair) (*console.VerifyExplanation, error) {
		return nil, errors.New("explain wp.posts: connect index: refused")
	},
	"panic": func(context.Context, string, bool, string, string, verify.BaselinePair) (*console.VerifyExplanation, error) {
		panic("the drill-down blew up")
	},
	"hang": func(context.Context, string, bool, string, string, verify.BaselinePair) (*console.VerifyExplanation, error) {
		_ = os.WriteFile(os.Getenv(explainChildPIDFileEnv), []byte(strconv.Itoa(os.Getpid())), 0o600)
		select {}
	},
	"silent": func(context.Context, string, bool, string, string, verify.BaselinePair) (*console.VerifyExplanation, error) {
		return nil, nil
	},
}

// fillAll sets every field of v, at any depth, to a value that is not its
// zero, and fails on a field it cannot reach: an unexported one would not
// cross the pipe at all.
func fillAll(t *testing.T, v reflect.Value, path string) {
	t.Helper()
	switch v.Kind() {
	case reflect.String:
		v.SetString("x-" + path)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(7)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(7.5)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fillAll(t, v.Elem(), path)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillAll(t, v.Index(0), path+"[0]")
	case reflect.Map:
		m := reflect.MakeMap(v.Type())
		k, e := reflect.New(v.Type().Key()).Elem(), reflect.New(v.Type().Elem()).Elem()
		fillAll(t, k, path+".key")
		fillAll(t, e, path+".value")
		m.SetMapIndex(k, e)
		v.Set(m)
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			v.Set(reflect.ValueOf(time.Date(2026, 10, 14, 9, 30, 15, 123456789, time.UTC)))
			return
		}
		for i := range v.NumField() {
			f := v.Type().Field(i)
			if !f.IsExported() {
				t.Fatalf("%s.%s is unexported: it would not reach the drill-down's process", path, f.Name)
			}
			fillAll(t, v.Field(i), path+"."+f.Name)
		}
	default:
		t.Fatalf("%s: a %s field; teach fillAll to fill it, and check it crosses JSON", path, v.Kind())
	}
	if v.IsZero() {
		t.Fatalf("%s is still zero after filling", path)
	}
}

// TestExplainChildJob_EveryFieldCrosses: the job goes to the child as JSON,
// so a field that does not survive the round trip arrives as its zero and
// the drill-down runs against a different pair than the verdict on screen.
// Every field is filled by reflection, so one added later is covered without
// anyone remembering this test.
func TestExplainChildJob_EveryFieldCrosses(t *testing.T) {
	var want explainChildJob
	fillAll(t, reflect.ValueOf(&want).Elem(), "job")
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got explainChildJob
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the job changed crossing to the child:\n got %+v\nwant %+v", got, want)
	}
}

func TestExplainChildMemory(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		// Two sessions plus explainGoAllowance.
		{"546MiB", "1604MiB"},
		{"512MiB", "1536MiB"},
		// The byte parser reads GB as binary, DuckDB as decimal: the ceiling
		// comes out above what DuckDB will use, never below. The SQL
		// worker's fence reads it the same way.
		{"4GB", "8704MiB"},
	} {
		if got, _, err := explainChildMemory(duckdbutil.Tuning{MemoryLimit: tc.in}); err != nil || got != tc.want {
			t.Errorf("explainChildMemory(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"", "0", "lots"} {
		if got, _, err := explainChildMemory(duckdbutil.Tuning{MemoryLimit: in}); err == nil {
			t.Errorf("explainChildMemory(%q) = %q, want an error", in, got)
		}
	}
}

func testPair() verify.BaselinePair {
	return verify.BaselinePair{Schema: "wp", Table: "posts", NewAnchor: query.BinlogPos{File: "binlog.000042", Pos: 991}}
}

// TestExplainInChild_Answers: the answer comes back from another process,
// with the job's DSN, archive switch, pair and the lane's DuckDB budget all
// as the parent had them.
func TestExplainInChild_Answers(t *testing.T) {
	t.Setenv(explainChildFakeEnv, "ok")
	ctx := duckdbutil.WithSessionTuning(context.Background(), duckdbutil.Tuning{Threads: 2, MemoryLimit: "600MiB"})
	ex, err := explainInChild(ctx, "u:p@tcp(h:3306)/idx", true, "wp", "posts", testPair())
	if err != nil {
		t.Fatal(err)
	}
	if ex.Schema != "wp" || ex.Table != "posts" || ex.Anchor != "binlog.000042" || ex.Total != 991 {
		t.Errorf("explanation = %+v, want the job's schema, table and anchor back", ex)
	}
	if want := "u:p@tcp(h:3306)/idx true 600MiB"; ex.Rendered != want {
		t.Errorf("the child saw %q, want %q (DSN, archive switch, session memory)", ex.Rendered, want)
	}
	if len(ex.Diffs) != 1 || len(ex.Diffs[0].Cells) != 1 || ex.Diffs[0].Cells[0].Baseline != "b" {
		t.Errorf("diffs = %+v, want the one row the child found", ex.Diffs)
	}
}

func TestExplainInChild_Failures(t *testing.T) {
	for _, tc := range []struct{ fake, want string }{
		// The drill-down's own error, word for word: the page shows it.
		{"error", "explain wp.posts: connect index: refused"},
		// A crash in the child is an error for one Explain, never the
		// console's, and it reads as it did in-process: one line, no stack.
		{"panic", "internal error: the drill-down blew up"},
		{"silent", "answered nothing"},
	} {
		t.Run(tc.fake, func(t *testing.T) {
			t.Setenv(explainChildFakeEnv, tc.fake)
			ex, err := explainInChild(context.Background(), "dsn", false, "wp", "posts", testPair())
			if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "goroutine") {
				t.Fatalf("explainInChild = %+v, %v; want an error containing %q", ex, err, tc.want)
			}
		})
	}
	t.Run("error is the child's own text", func(t *testing.T) {
		t.Setenv(explainChildFakeEnv, "error")
		_, err := explainInChild(context.Background(), "dsn", false, "wp", "posts", testPair())
		if want := "explain wp.posts: connect index: refused"; err == nil || err.Error() != want {
			t.Errorf("err = %v, want exactly %q", err, want)
		}
	})
}

// TestExplainInChild_CancelKillsTheProcess: a new run or a shutdown cancels
// the drill-down. In-process that stopped DuckDB through the context; in a
// child the context cannot reach, so the PROCESS must be gone, not just the
// wait for it. A child left running holds its memory for a result nobody
// will read, which is the outage this exists to prevent.
func TestExplainInChild_CancelKillsTheProcess(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pid")
	t.Setenv(explainChildFakeEnv, "hang")
	t.Setenv(explainChildPIDFileEnv, pidFile)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := explainInChild(ctx, "dsn", false, "wp", "posts", testPair())
		errc <- err
	}()
	pid := waitPIDFile(t, pidFile)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled (explainLogVerdict reads it as a cancel, not a failure)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("explainInChild did not return after its context was canceled")
	}
	if processRunning(pid) {
		t.Errorf("the child (pid %d) is still running after the cancel", pid)
	}
}

func waitPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
			pid, err := strconv.Atoi(string(b))
			if err != nil {
				t.Fatal(err)
			}
			return pid
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the child never started its work")
	return 0
}

// TestVerifyWorkerMain_ParentGone: the parent holds the child's stdin open
// until it has the answer. When it closes (the job was dropped, or the parent
// died) the work is canceled, so its cleanup runs, and the child ends rather
// than finish a drill-down nobody will read.
func TestVerifyWorkerMain_ParentGone(t *testing.T) {
	old := explainChildRun
	t.Cleanup(func() { explainChildRun = old })
	started, canceled := make(chan struct{}), make(chan struct{})
	explainChildRun = func(ctx context.Context, _ string, _ bool, _, _ string, _ verify.BaselinePair) (*console.VerifyExplanation, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}
	r, w := io.Pipe()
	code := make(chan int, 1)
	go func() { code <- verifyWorkerMain(r, io.Discard, io.Discard, func() {}) }()
	job, _ := json.Marshal(explainChildJob{Schema: "wp", Table: "posts"})
	if _, err := fmt.Fprintf(w, "%s\n", job); err != nil {
		t.Fatal(err)
	}
	<-started
	select {
	case <-canceled:
		t.Fatal("the child took a live parent for a dead one")
	case <-time.After(100 * time.Millisecond):
	}
	_ = w.Close()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("the work was not canceled when the parent went away")
	}
	select {
	case c := <-code:
		if c == 0 {
			t.Error("a child whose parent went away reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the child did not end after its work was canceled")
	}
}

// TestExplainChildEnv_GoMemLimit: Go does not see the cgroup's ceiling, so
// the child is told what its heap may use: the ceiling less the DuckDB
// sessions. An operator's own GOMEMLIMIT is left as it is.
func TestExplainChildEnv_GoMemLimit(t *testing.T) {
	mem, duck, err := explainChildMemory(duckdbutil.Tuning{MemoryLimit: "546MiB"})
	if err != nil {
		t.Fatal(err)
	}
	// 1604 MiB + a quarter of it (401) = 2005 MiB ceiling, less the two
	// sessions' 1092 and the 2 x 190 they hold outside their limits.
	if got := lastEnv(explainChildEnv(mem, duck), "GOMEMLIMIT"); got != "533MiB" {
		t.Errorf("GOMEMLIMIT = %q, want 533MiB", got)
	}
	t.Setenv("GOMEMLIMIT", "300MiB")
	if got := lastEnv(explainChildEnv(mem, duck), "GOMEMLIMIT"); got != "300MiB" {
		t.Errorf("GOMEMLIMIT = %q, want the operator's 300MiB", got)
	}
}

func lastEnv(env []string, key string) string {
	v := ""
	for _, kv := range env {
		if k, val, ok := strings.Cut(kv, "="); ok && k == key {
			v = val
		}
	}
	return v
}

// TestVerifySupervisor_ExplainRunsInAChild pins the wiring: with no test
// stand-in, Explain's work is explainInChild. Reverting it to explainNow
// would try to reach the job's index (which does not exist) in this process
// and fail with a connection error instead of the child's answer.
func TestVerifySupervisor_ExplainRunsInAChild(t *testing.T) {
	t.Setenv(explainChildFakeEnv, "ok")
	s := explainableSupervisor(t)
	ex, err := waitExplainFor(t, s, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ex.Rendered, "u:p@tcp(127.0.0.1:1)/idx ") {
		t.Errorf("rendered = %q, want the child's echo of the job's index", ex.Rendered)
	}
}

func waitExplainFor(t *testing.T, s *verifySupervisor, d time.Duration) (*console.VerifyExplanation, error) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		ex, err := s.Explain("s1", "wp", "posts")
		if !errors.Is(err, console.ErrExplainRunning) {
			return ex, err
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("explain never left the running state")
	return nil, nil
}

// processRunning reports whether pid is a live process. The child is reaped
// by the parent's Wait, so a killed one is gone, not a zombie.
func processRunning(pid int) bool {
	return !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
}
