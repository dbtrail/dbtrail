package consoleapp

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The flavor watch's own capture runs as: declared with --source-flavor, then
// whatever the source reports on every resolution.
func TestSourceFlavorCell(t *testing.T) {
	cases := []struct {
		declared string
		sets     []string
		want     string
	}{
		{declared: "", want: ""},
		{declared: "mysql", want: "mysql"},
		{declared: " MariaDB ", want: "mariadb"},
		{declared: "", sets: []string{"mariadb"}, want: "mariadb"},
		{declared: "", sets: []string{"mariadb", "mysql"}, want: "mysql"},
		{declared: "", sets: []string{" MARIADB "}, want: "mariadb"},
		{declared: "mariadb", sets: []string{"postgres"}, want: "mariadb"},
		{declared: "mariadb", sets: []string{""}, want: "mariadb"},
		{declared: "oracle", want: ""},
	}
	for _, tc := range cases {
		c := newSourceFlavorCell(tc.declared)
		for _, s := range tc.sets {
			c.set(s)
		}
		if got := c.get(); got != tc.want {
			t.Errorf("declared %q then %q: got %q, want %q", tc.declared, tc.sets, got, tc.want)
		}
	}
	var nilCell *sourceFlavorCell
	if got := nilCell.get(); got != "" {
		t.Errorf("nil cell: %q", got)
	}
}

// The main stream's hook records every resolution (a write-deadline restart
// asks again) and starts the source jobs once.
func TestMainStreamFlavorHook(t *testing.T) {
	cell := newSourceFlavorCell("")
	var started []string
	hook := mainStreamFlavorHook(cell, func(f string) { started = append(started, f) })
	hook(console.FlavorMariaDB)
	if cell.get() != console.FlavorMariaDB {
		t.Fatalf("after the first resolution: %q", cell.get())
	}
	hook(console.FlavorMySQL)
	if cell.get() != console.FlavorMySQL {
		t.Errorf("after the second resolution: %q, want it to follow", cell.get())
	}
	if len(started) != 1 || started[0] != console.FlavorMariaDB {
		t.Errorf("source jobs started with %q, want once with the first flavor", started)
	}
}

// The capture status read of the daemon's own source follows the flavor its
// capture detected, not only a declared --source-flavor: with detection (the
// default), a MariaDB main source used to be asked with the MySQL read.
func TestCaptureStatus_bootSourceFollowsTheDetectedFlavor(t *testing.T) {
	var mysqlReads, mariadbReads int
	c, clock := captureReporter("u:p@tcp(boot-src:3306)/", func(context.Context, string, string) captureProbeResult {
		mysqlReads++
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	})
	cell := newSourceFlavorCell("")
	c.withBootFlavor(cell.get)
	c.readMariaDB = func(context.Context, string, string) captureProbeResult {
		mariadbReads++
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	}
	boot := console.ServerEntry{ID: bootCaptureServerID, DSN: "u:p@tcp(idx:3306)/boot"}
	if got := c.CaptureStatus(context.Background(), boot); got.State != console.CaptureStateUpToDate || mysqlReads != 1 || mariadbReads != 0 {
		t.Fatalf("before detection: %+v, MySQL reads %d, MariaDB reads %d", got, mysqlReads, mariadbReads)
	}
	cell.set(console.FlavorMariaDB)
	// Within the TTL: the flavor is part of the slot key, so the change is
	// read at once instead of serving the MySQL read's answer.
	*clock = clock.Add(captureStatusTTL / 2)
	if got := c.CaptureStatus(context.Background(), boot); got.State != console.CaptureStateUpToDate || mariadbReads != 1 {
		t.Fatalf("after detection: %+v, MySQL reads %d, MariaDB reads %d", got, mysqlReads, mariadbReads)
	}
}

// Both watch entry points build the console from upConsoleConfig. The
// source-ful one hands it the cell its stream's hook writes; the list label and
// the capture status read both read that cell.
func TestUpConsoleConfig_bootFlavorWiring(t *testing.T) {
	old := upSourceDSN
	t.Cleanup(func() { upSourceDSN = old })

	upSourceDSN = "u:p@tcp(boot-src:3306)/"
	cell := newSourceFlavorCell("")
	cfg, err := upConsoleConfigFor(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}, nil, cell)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BootSourceFlavor == nil {
		t.Fatal("BootSourceFlavor not wired for a daemon with a source")
	}
	cell.set(console.FlavorMariaDB)
	if got := cfg.BootSourceFlavor(); got != console.FlavorMariaDB {
		t.Errorf("BootSourceFlavor() = %q, want mariadb", got)
	}
	rep := cfg.CaptureStatus.(*captureStatusReporter)
	if rep.bootFlavor == nil || rep.bootFlavor() != console.FlavorMariaDB {
		t.Error("the capture status reporter does not read the same cell")
	}

	// No source: nothing to label, and the reporter never gets that far.
	upSourceDSN = ""
	cfg, err = upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BootSourceFlavor != nil {
		t.Error("BootSourceFlavor wired for a daemon without a source")
	}
}
