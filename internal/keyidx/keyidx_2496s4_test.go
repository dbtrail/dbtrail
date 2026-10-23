package keyidx

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// #2496 slice 4, round 2: the publish policy's lines name the retry
// condition, a cancelled build is Debug whatever shape its error has, and
// the file identity the memo keys by is the one the index binds by.

// TestWriteBeside_namesTheRetryCondition_2496s4 (F5): a skip for disk and a
// failed build are WARN lines that say the build is not tried again until
// the table's file is rewritten or DBTrail restarts.
func TestWriteBeside_namesTheRetryCondition_2496s4(t *testing.T) {
	ctx := context.Background()
	const want = "not tried again until the table's file is rewritten or DBTrail restarts"
	logs := captureDebug(t)
	null := writeBase(t, "SELECT NULL::INTEGER AS id", 1000)
	WriteBeside(ctx, "s", "t", null, []string{"id"}, duckdbutil.Tuning{})
	if line := lastLine(logs.String()); !strings.Contains(line, "level=WARN") || !strings.Contains(line, want) {
		t.Errorf("failed build: %s", line)
	}
	logs.Reset()
	good := writeBase(t, "SELECT i::INTEGER AS id FROM range(10) r(i)", 1000)
	prevDisk := diskSpace
	diskSpace = func(string) (uint64, uint64, error) { return 1, 1 << 40, nil }
	t.Cleanup(func() { diskSpace = prevDisk })
	WriteBeside(ctx, "s", "t", good, []string{"id"}, duckdbutil.Tuning{})
	if line := lastLine(logs.String()); !strings.Contains(line, "level=WARN") || !strings.Contains(line, want) {
		t.Errorf("short disk: %s", line)
	}
}

// TestWriteBeside_cancelledIsDebugWhateverTheError_2496s4 (F6): DuckDB's
// interrupt error does not wrap context.Canceled; a build cut by shutdown
// is still a Debug line, decided by the context, not by the error's shape.
func TestWriteBeside_cancelledIsDebugWhateverTheError_2496s4(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(10) r(i)", 1000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prev := buildIndex
	buildIndex = func(context.Context, string, []string, duckdbutil.Tuning) (Result, error) {
		return Result{Path: PathFor(base)}, errors.New("INTERRUPT Error: Interrupted!")
	}
	t.Cleanup(func() { buildIndex = prev })
	logs := captureDebug(t)
	WriteBeside(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	if strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), `level=DEBUG msg="key index not written: the build was cancelled"`) {
		t.Fatalf("a cancelled build with DuckDB's error shape is not a Debug line:\n%s", logs.String())
	}
	// And through the real build, cancelled from the hook while it holds
	// its slot: the same Debug line.
	buildIndex = prev
	ctx2, cancel2 := context.WithCancel(context.Background())
	BuildHook = cancel2
	t.Cleanup(func() { BuildHook = nil })
	logs.Reset()
	WriteBeside(ctx2, "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	if strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "the build was cancelled") {
		t.Fatalf("a build cancelled from the hook is not a Debug line:\n%s", logs.String())
	}
	if _, err := os.Stat(PathFor(base)); err == nil {
		t.Fatal("a cancelled build left an index")
	}
}

// TestIdentity_statHalfIsTheBindingsStatHalf_2496s4 (F9): the identity the
// memo keys by is the stat part of the identity the index binds by, built
// from one function, so the two cannot drift.
func TestIdentity_statHalfIsTheBindingsStatHalf_2496s4(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(10) r(i)", 1000)
	stat, err := StatIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	full, err := readBaseIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	if full.FileIdentity != stat || stat.Size == 0 || stat.Mtime == 0 {
		t.Fatalf("stat identity %+v, the binding's %+v", stat, full)
	}
}
