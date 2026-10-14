package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/storage"
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

var checkChildFakes = map[string]func(context.Context, string, bool, string, verify.BaselinePair) (verify.TableResult, error){
	// check-ok answers a mismatch carrying what it was handed, the console's
	// S3 stores included.
	"check-ok": func(ctx context.Context, dsn string, noArchive bool, flavor string, p verify.BaselinePair) (verify.TableResult, error) {
		st, _ := storage.BucketStoreFor("copies")
		return verify.TableResult{Schema: p.Schema, Table: p.Table, Status: verify.StatusMismatch,
			Anchor: p.NewAnchor.File, SourceRows: 3, ReconstructRows: 2,
			Detail: strings.Join([]string{dsn, strconv.FormatBool(noArchive), duckdbutil.DefaultTuningFor(ctx).MemoryLimit, flavor,
				st.Endpoint.URL, strconv.FormatBool(st.Endpoint.PathStyleExplicit()), st.AccessKeyID, st.SecretKey}, " ")}, nil
	},
	"check-error": func(context.Context, string, bool, string, verify.BaselinePair) (verify.TableResult, error) {
		return verify.TableResult{}, errors.New("resolve shop.orders: no such table")
	},
	"check-panic": func(context.Context, string, bool, string, verify.BaselinePair) (verify.TableResult, error) {
		panic("the comparison blew up")
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
		if v.Type() == reflect.TypeFor[json.RawMessage]() {
			v.SetBytes([]byte(`{"x":1}`))
			return
		}
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
	var want verifyChildJob
	fillAll(t, reflect.ValueOf(&want).Elem(), "job")
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got verifyChildJob
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the job changed crossing to the child:\n got %+v\nwant %+v", got, want)
	}
}

// TestVerifyChildAnswer_EveryFieldCrosses: a table's verdict comes back as
// JSON and is persisted (#1191). A field lost on the way would be a verdict
// recorded without it.
func TestVerifyChildAnswer_EveryFieldCrosses(t *testing.T) {
	var want verifyChildAnswer
	fillAll(t, reflect.ValueOf(&want).Elem(), "answer")
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got verifyChildAnswer
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the answer changed crossing back:\n got %+v\nwant %+v", got, want)
	}
}

func TestExplainChildMemory(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		// Two sessions plus verifyChildGoAllowance.
		{"546MiB", "1604MiB"},
		{"512MiB", "1536MiB"},
		// The byte parser reads GB as binary, DuckDB as decimal: the ceiling
		// comes out above what DuckDB will use, never below. The SQL
		// worker's fence reads it the same way.
		{"4GB", "8704MiB"},
	} {
		if got, _, err := verifyChildMemory(duckdbutil.Tuning{MemoryLimit: tc.in}); err != nil || got != tc.want {
			t.Errorf("verifyChildMemory(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"", "0", "lots"} {
		if got, _, err := verifyChildMemory(duckdbutil.Tuning{MemoryLimit: in}); err == nil {
			t.Errorf("verifyChildMemory(%q) = %q, want an error", in, got)
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
	job, _ := json.Marshal(verifyChildJob{Kind: verifyChildExplain, Schema: "wp", Table: "posts"})
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
	mem, per, err := verifyChildMemory(duckdbutil.Tuning{MemoryLimit: "546MiB"})
	if err != nil {
		t.Fatal(err)
	}
	// 1604 MiB + a quarter of it (401) = 2005 MiB ceiling, less each session
	// that runs at once: its 546 and the 190 it holds outside its limit.
	if got := lastEnv(verifyChildEnv(mem, per, 2), "GOMEMLIMIT"); got != "533MiB" {
		t.Errorf("Explain (two sessions at once): GOMEMLIMIT = %q, want 533MiB", got)
	}
	if got := lastEnv(verifyChildEnv(mem, per, 1), "GOMEMLIMIT"); got != "1269MiB" {
		t.Errorf("a check table (one at a time): GOMEMLIMIT = %q, want 1269MiB", got)
	}
	t.Setenv("GOMEMLIMIT", "300MiB")
	if got := lastEnv(verifyChildEnv(mem, per, 2), "GOMEMLIMIT"); got != "300MiB" {
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

// checkFixture writes two reads of shop.orders a day apart, so the newer has
// an earlier one to be compared with and the pair is not settled while
// pairing: the check has to compare it.
func checkFixture(t *testing.T) (*verifySupervisor, string, *metadata.Resolver) {
	t.Helper()
	root := t.TempDir()
	read := time.Date(2026, 9, 1, 3, 0, 0, 0, time.UTC)
	writeSnapshotAs(t, root, read.Add(-24*time.Hour), read.Add(-24*time.Hour), baseline.ProducerDump)
	writeSnapshotAs(t, root, read, read, baseline.ProducerDump)
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.orders": {Schema: "shop", Table: "orders"}})
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.jobs["s1"] = &verifyJob{status: console.VerifyStatus{State: "running"}, mode: console.VerifyModeBaselineAnchored}
	return s, root, resolver
}

// TestRunBaselineAnchored_comparesInAChild pins the wiring: a table the check
// must compare is compared in a child, with the run's index DSN and archive
// switch and the check lane's DuckDB budget, and its mismatch is kept for
// Explain as before.
func TestRunBaselineAnchored_comparesInAChild(t *testing.T) {
	t.Setenv(explainChildFakeEnv, "check-ok")
	// A server's copy on its own S3: the store the console built from its
	// registry lives in this process's memory, and the child must get it,
	// secret and explicit path style included, or it reads with the
	// environment's endpoint and keys and fails.
	st, err := storage.NewBucketStore("http://minio:9000", storage.PathStylePath, "")
	if err != nil {
		t.Fatal(err)
	}
	if st, err = st.WithKeys("AKID", "s3cr3t"); err != nil {
		t.Fatal(err)
	}
	storage.SetBucketStores(map[string]storage.BucketStore{"copies": st})
	t.Cleanup(func() { storage.SetBucketStores(nil) })
	s, root, resolver := checkFixture(t)
	req := console.VerifyRequest{ServerID: "s1", IndexDSN: "u:p@tcp(h:3306)/idx", NoArchive: true}
	if err := s.runBaselineAnchored(req, root, nil, resolver, "idx", "postgres"); err != nil {
		t.Fatal(err)
	}
	rs := s.jobs["s1"].status.Results
	if len(rs) != 1 || rs[0].Status != "mismatch" {
		t.Fatalf("results = %+v, want the child's mismatch", rs)
	}
	if !strings.HasPrefix(rs[0].Detail, "u:p@tcp(h:3306)/idx true ") || !strings.HasSuffix(rs[0].Detail, " postgres http://minio:9000 true AKID s3cr3t") {
		t.Errorf("the child saw %q, want the run's DSN, archive switch, flavor and S3 store", rs[0].Detail)
	}
	if _, ok := s.jobs["s1"].pairs["shop.orders"]; !ok {
		t.Error("the mismatch's pair was not kept, so Explain would answer unavailable")
	}
}

// TestRunBaselineAnchored_childErrorIsTheTablesError: the child's own error
// becomes that table's error verdict, word for word, and the run goes on.
func TestRunBaselineAnchored_childErrorIsTheTablesError(t *testing.T) {
	t.Setenv(explainChildFakeEnv, "check-error")
	s, root, resolver := checkFixture(t)
	if err := s.runBaselineAnchored(console.VerifyRequest{ServerID: "s1", IndexDSN: "dsn"}, root, nil, resolver, "idx", ""); err != nil {
		t.Fatal(err)
	}
	rs := s.jobs["s1"].status.Results
	if len(rs) != 1 || rs[0].Status != "error" || rs[0].Detail != "resolve shop.orders: no such table" {
		t.Fatalf("results = %+v, want the child's error as the table's", rs)
	}
}

// TestRunBaselineAnchored_settledStartsNoChild: a table the pairing already
// answered has nothing to compare; it is answered here, with no process
// started for it.
func TestRunBaselineAnchored_settledStartsNoChild(t *testing.T) {
	root := t.TempDir()
	read := time.Date(2026, 9, 1, 3, 0, 0, 0, time.UTC)
	writeFoldSnapshot(t, root, read.Add(24*time.Hour), read)
	writeFoldSnapshot(t, root, read.Add(48*time.Hour), read)
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.orders": {Schema: "shop", Table: "orders"}})
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.checkPairFn = func(context.Context, string, bool, verify.BaselinePair) (verify.TableResult, error) {
		t.Error("a settled table was sent to be compared")
		return verify.TableResult{}, nil
	}
	s.jobs["s1"] = &verifyJob{status: console.VerifyStatus{State: "running"}, mode: console.VerifyModeBaselineAnchored}
	if err := s.runBaselineAnchored(console.VerifyRequest{ServerID: "s1"}, root, nil, resolver, "idx", ""); err != nil {
		t.Fatal(err)
	}
	if rs := s.jobs["s1"].status.Results; len(rs) != 1 || rs[0].Status != "inconclusive" {
		t.Fatalf("results = %+v, want the settled answer", rs)
	}
}

// TestRunVerifyJob_UnknownKind: a job the child does not know is an error it
// says, never an explanation or a verdict guessed from an empty answer.
func TestRunVerifyJob_UnknownKind(t *testing.T) {
	ans := runVerifyJob(context.Background(), verifyChildJob{Kind: "mystery"}, io.Discard)
	if ans.Error != `unknown verify job "mystery"` || ans.Result != nil || ans.Table != nil {
		t.Errorf("answer = %+v, want the unknown kind named", ans)
	}
}

// TestRunBaselineAnchored_childPanicIsTheTablesError: a crash comparing one
// table is that table's error, in one line, and not the console's.
func TestRunBaselineAnchored_childPanicIsTheTablesError(t *testing.T) {
	t.Setenv(explainChildFakeEnv, "check-panic")
	s, root, resolver := checkFixture(t)
	if err := s.runBaselineAnchored(console.VerifyRequest{ServerID: "s1", IndexDSN: "dsn"}, root, nil, resolver, "idx", ""); err != nil {
		t.Fatal(err)
	}
	rs := s.jobs["s1"].status.Results
	if len(rs) != 1 || rs[0].Status != "error" || rs[0].Detail != "internal error: the comparison blew up" {
		t.Fatalf("results = %+v, want the panic as the table's error", rs)
	}
}

// TestRunBaselineAnchored_stopDuringLastTable: a run stopped while its last
// table is compared ends interrupted. Recording that table's "context
// canceled" as a verdict and ending the run as succeeded would persist a
// result nobody produced (#1191).
func TestRunBaselineAnchored_stopDuringLastTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, root, resolver := checkFixture(t)
	s.ctx = ctx
	s.checkPairFn = func(ctx context.Context, _ string, _ bool, _ verify.BaselinePair) (verify.TableResult, error) {
		cancel()
		return verify.TableResult{}, ctx.Err()
	}
	err := s.runBaselineAnchored(console.VerifyRequest{ServerID: "s1"}, root, nil, resolver, "idx", "")
	if err == nil || !strings.Contains(err.Error(), "verify interrupted") {
		t.Fatalf("err = %v, want the run interrupted", err)
	}
	if rs := s.jobs["s1"].status.Results; len(rs) != 0 {
		t.Errorf("results = %+v, want none: the stopped table has no verdict", rs)
	}
}

// TestVerifyChildEnv_LogLevel: the child logs at the console's level, so a
// console asked for info sees the check's info lines.
func TestVerifyChildEnv_LogLevel(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if got := lastEnv(verifyChildEnv("1024MiB", 256<<20, 1), verifyChildLogLevelEnv); got != "info" {
		t.Errorf("child log level = %q, want info", got)
	}
}

func TestVerifyChildSessions(t *testing.T) {
	if got := verifyChildSessions(verifyChildExplain); got != 2 {
		t.Errorf("Explain runs %d sessions at once, want 2", got)
	}
	if got := verifyChildSessions(verifyChildCheck); got != 1 {
		t.Errorf("a check table runs %d sessions at once, want 1", got)
	}
}

// TestNoticeOnceCarriesTheOnceMarker: the console keeps a once-per-process
// notice once across its verify children by a mark in the line. The mark
// the notice writes and the one the relay looks for are spelled in two
// packages; this is where they must agree.
func TestNoticeOnceCarriesTheOnceMarker(t *testing.T) {
	var buf strings.Builder
	var n reconstruct.NoticeOnce
	n.To(slog.New(slog.NewTextHandler(&buf, nil)))(slog.LevelWarn, "index also written", "mark", 7)
	if !strings.Contains(buf.String(), " "+sqlsandbox.OnceMarker) {
		t.Errorf("a NoticeOnce line %q lacks %q, so every check table would log it again", buf.String(), sqlsandbox.OnceMarker)
	}
}

// TestRunBaselineAnchored_finishedTableKeepsItsVerdict: a stop that arrives
// after a table finished does not take that table's verdict with it.
func TestRunBaselineAnchored_finishedTableKeepsItsVerdict(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, root, resolver := checkFixture(t)
	s.ctx = ctx
	s.checkPairFn = func(context.Context, string, bool, verify.BaselinePair) (verify.TableResult, error) {
		cancel()
		return verify.TableResult{Schema: "shop", Table: "orders", Status: verify.StatusMatch}, nil
	}
	if err := s.runBaselineAnchored(console.VerifyRequest{ServerID: "s1"}, root, nil, resolver, "idx", ""); err != nil {
		t.Fatalf("err = %v, want the run to end with its one table done", err)
	}
	if rs := s.jobs["s1"].status.Results; len(rs) != 1 || rs[0].Status != "match" {
		t.Errorf("results = %+v, want the finished table's match", rs)
	}
}
