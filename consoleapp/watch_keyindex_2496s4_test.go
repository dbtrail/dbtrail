package consoleapp

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestNewBaselineSupervisorFromConfig_readsTheKeyIndexSwitch (#2496, slice
// 4): building the supervisor, which every daemon that refreshes does, reads
// the key index switch once, so its line is at boot in the compose stack too
// (whose refresh is scheduled from the console and never passes the
// --baseline-refresh-interval loop that also reports it). The switch logs
// once per process, so the wiring is pinned through the seam, and the seam
// is pinned to the real switch.
func TestNewBaselineSupervisorFromConfig_readsTheKeyIndexSwitch(t *testing.T) {
	// Pointer equality holds today because both are the same func value:
	// every closure newKeyIndexSwitch returns shares one code pointer, and
	// the seam is assigned the switch itself. Wrapping the seam in another
	// func (`func() bool { return reconstruct.KeyIndexEnabled() }`) would
	// break this check on purpose: the wiring would then need a new pin.
	if reflect.ValueOf(keyIndexSwitch).Pointer() != reflect.ValueOf(reconstruct.KeyIndexEnabled).Pointer() {
		t.Fatal("keyIndexSwitch is not reconstruct.KeyIndexEnabled")
	}
	prev := keyIndexSwitch
	calls := 0
	keyIndexSwitch = func() bool {
		calls++
		slog.Info("key index lookup enabled by default (seam)")
		return true
	}
	t.Cleanup(func() { keyIndexSwitch = prev })
	var buf bytes.Buffer
	prevLog := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prevLog) })
	ctx, cancel := context.WithCancel(context.Background())
	newBaselineSupervisorFromConfig(ctx, t.TempDir(), nil)
	cancel() // stops the reaper goroutine the constructor starts
	if calls != 1 {
		t.Fatalf("the switch was read %d times by the supervisor's construction, want 1", calls)
	}
	if !strings.Contains(buf.String(), "key index lookup enabled") {
		t.Fatalf("the switch's line is not in the boot log:\n%s", buf.String())
	}
}
