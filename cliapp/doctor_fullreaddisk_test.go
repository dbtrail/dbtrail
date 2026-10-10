package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// TestDoctorFullReadDiskWiring pins the doctor wiring of the full-read disk
// check (#2259), through the list runDoctor hands to the report. Without
// --staging-dir the report carries one skipped line that says where the
// check runs. With it, the folder is measured even when the source cannot be
// reached (an unroutable DSN here): a skipped line with the free space. The
// check is in the list once, after the snapshot expiry check when that one
// is asked for.
func TestDoctorFullReadDiskWiring(t *testing.T) {
	badDSN := "nouser:nopass@tcp(127.0.0.1:1)/"
	if doctorCmd.Flags().Lookup("staging-dir") == nil {
		t.Fatal("doctor is missing the --staging-dir flag")
	}
	lines := func(out string) []string {
		var ls []string
		for l := range strings.SplitSeq(out, "\n") {
			if strings.Contains(l, doctor.FullReadDiskCheckName) {
				ls = append(ls, l)
			}
		}
		return ls
	}
	report := func(baselineS3, stagingDir, schemas string) string {
		var buf bytes.Buffer
		_ = runDoctorTo(context.Background(), &buf, "text", badDSN, "", schemas, 0, "", "", "", "",
			doctorOptInChecks(baselineS3, "", 0, stagingDir, badDSN, schemas)...)
		return buf.String()
	}

	out := report("", "", "")
	if ls := lines(out); len(ls) != 1 || !strings.HasPrefix(ls[0], "- ") || !strings.Contains(ls[0], "--staging-dir") || !strings.Contains(ls[0], "Snapshots page") {
		t.Fatalf("without the flag: %q\n%s", ls, out)
	}

	dir := t.TempDir()
	out = report("/var/backups", dir, "shop")
	ls := lines(out)
	if len(ls) != 1 || !strings.HasPrefix(ls[0], "- ") || !strings.Contains(ls[0], dir) || !strings.Contains(ls[0], "free at") || !strings.Contains(ls[0], "could not be read from the source") {
		t.Fatalf("with the flag and no source: %q\n%s", ls, out)
	}
	if expiry := strings.Index(out, doctor.SnapshotExpiryCheckName); expiry < 0 || expiry > strings.Index(out, doctor.FullReadDiskCheckName) {
		t.Fatalf("the snapshot expiry check is missing or printed after the disk check:\n%s", out)
	}
	if n := len(doctorOptInChecks("", "", 0, "", badDSN, "")); n != 1 {
		t.Fatalf("%d opt-in checks with no flags, want the disk check alone", n)
	}
}

// The command itself hands that list to the report: with --staging-dir set,
// `bintrail doctor` prints the folder's line.
func TestDoctorCommandRunsTheFullReadDiskCheck(t *testing.T) {
	prevOut, prevSrc, prevDir, prevFmt, prevIdx := doctorOut, docSourceDSN, docStagingDir, docFormat, docIndexDSN
	t.Cleanup(func() {
		doctorOut, docSourceDSN, docStagingDir, docFormat, docIndexDSN = prevOut, prevSrc, prevDir, prevFmt, prevIdx
	})
	var buf bytes.Buffer
	dir := t.TempDir()
	doctorOut, docSourceDSN, docStagingDir, docFormat, docIndexDSN = &buf, "nouser:nopass@tcp(127.0.0.1:1)/", dir, "text", ""
	doctorCmd.SetContext(context.Background())
	_ = runDoctor(doctorCmd, nil)
	if n := strings.Count(buf.String(), doctor.FullReadDiskCheckName); n != 1 || !strings.Contains(buf.String(), dir) {
		t.Fatalf("the report names the check %d time(s), want once with %s:\n%s", n, dir, buf.String())
	}
}
