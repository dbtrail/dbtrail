package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// TestDoctorFullReadDiskWiring pins the doctor wiring of the full-read disk
// check (#2259). Without --staging-dir the report carries one skipped line
// that says where the check runs, and no warning. With it, the folder is
// measured even when the source cannot be reached (an unroutable DSN here),
// and that still is not a failure of its own.
func TestDoctorFullReadDiskWiring(t *testing.T) {
	badDSN := "nouser:nopass@tcp(127.0.0.1:1)/"
	if doctorCmd.Flags().Lookup("staging-dir") == nil {
		t.Fatal("doctor is missing the --staging-dir flag")
	}
	line := func(out string) string {
		for l := range strings.SplitSeq(out, "\n") {
			if strings.Contains(l, doctor.FullReadDiskCheckName) {
				return l
			}
		}
		return ""
	}

	var without bytes.Buffer
	_ = runDoctorTo(context.Background(), &without, "text", badDSN, "", "", 0, "", "", "", "", fullReadDiskCheck("", badDSN, ""))
	if l := line(without.String()); !strings.HasPrefix(l, "- ") || !strings.Contains(l, "--staging-dir") || !strings.Contains(l, "Snapshots page") {
		t.Fatalf("without the flag: %q\n%s", l, without.String())
	}

	dir := t.TempDir()
	var with bytes.Buffer
	_ = runDoctorTo(context.Background(), &with, "text", badDSN, "", "shop", 0, "", "", "", "", fullReadDiskCheck(dir, badDSN, "shop"))
	l := line(with.String())
	if !strings.HasPrefix(l, "- ") || !strings.Contains(l, dir) || !strings.Contains(l, "free at") || !strings.Contains(l, "could not be read from the source") {
		t.Fatalf("with the flag and no source: %q\n%s", l, with.String())
	}
}
