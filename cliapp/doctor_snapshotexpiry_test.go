package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// TestDoctorSnapshotExpiryWiring pins the doctor wiring of the snapshot
// expiry check (#1680): --baseline-s3 adds it to the report, and without the
// flag (a local destination, or none) the check is not there at all. A
// destination that is not an s3:// URL answers before any AWS call, so no
// network or credentials are involved.
func TestDoctorSnapshotExpiryWiring(t *testing.T) {
	badDSN := "nouser:nopass@tcp(127.0.0.1:1)/"

	var with bytes.Buffer
	_ = runDoctorTo(context.Background(), &with, "text", badDSN, "", "", 0, "", "", "", "",
		snapshotExpiryChecks("/var/backups", "", 0)...)
	if !strings.Contains(with.String(), doctor.SnapshotExpiryCheckName) {
		t.Fatalf("report does not contain %q:\n%s", doctor.SnapshotExpiryCheckName, with.String())
	}
	if !strings.Contains(with.String(), "is not an s3://bucket/prefix destination") {
		t.Fatalf("report does not say the destination is not S3:\n%s", with.String())
	}

	var without bytes.Buffer
	_ = runDoctorTo(context.Background(), &without, "text", badDSN, "", "", 0, "", "", "", "",
		snapshotExpiryChecks("", "", time.Hour)...)
	if strings.Contains(without.String(), doctor.SnapshotExpiryCheckName) {
		t.Fatalf("check present without --baseline-s3:\n%s", without.String())
	}
}

func TestDoctorSnapshotExpiryFlags(t *testing.T) {
	for _, name := range []string{"baseline-s3", "baseline-s3-region", "snapshot-every"} {
		if doctorCmd.Flags().Lookup(name) == nil {
			t.Fatalf("doctor is missing the --%s flag", name)
		}
	}
	for in, want := range map[string]time.Duration{"": 0, "6h": 6 * time.Hour, "1d": 24 * time.Hour, "30m": 30 * time.Minute} {
		got, err := parseDocSnapshotEvery(in)
		if err != nil || got != want {
			t.Errorf("parseDocSnapshotEvery(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"soon", "-1h", "0h", "6"} {
		if got, err := parseDocSnapshotEvery(in); err == nil {
			t.Errorf("parseDocSnapshotEvery(%q) = %v with no error; a schedule that cannot be read must not be compared as if it were none", in, got)
		}
	}
}
