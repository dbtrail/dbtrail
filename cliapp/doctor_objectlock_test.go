package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// TestRunDoctorToWiresObjectLockCheck pins the runDoctorTo wiring: a non-empty
// --archive-s3 adds the Object Lock check to the report, and an empty one does
// not. The invalid-URL path returns before any AWS call, so no network or
// credentials are involved.
func TestRunDoctorToWiresObjectLockCheck(t *testing.T) {
	badDSN := "nouser:nopass@tcp(127.0.0.1:1)/"

	var with bytes.Buffer
	_ = runDoctorTo(context.Background(), &with, "text", badDSN, "", "", 0, "", "", "not-an-s3-url", "")
	if !strings.Contains(with.String(), doctor.ObjectLockCheckName) {
		t.Fatalf("report does not contain %q:\n%s", doctor.ObjectLockCheckName, with.String())
	}
	if !strings.Contains(with.String(), "invalid --archive-s3") {
		t.Fatalf("report does not carry the invalid-URL detail:\n%s", with.String())
	}

	var without bytes.Buffer
	_ = runDoctorTo(context.Background(), &without, "text", badDSN, "", "", 0, "", "", "", "")
	if strings.Contains(without.String(), doctor.ObjectLockCheckName) {
		t.Fatalf("check present without --archive-s3:\n%s", without.String())
	}
}

// TestDoctorObjectLockFlagNames pins the flag names to the ones bound in
// internal/cli/env.go — renaming either silently disconnects
// BINTRAIL_ARCHIVE_S3/_REGION from doctor while the generic env-binding
// resolve test stays green (agent/rotate still register the old names).
func TestDoctorObjectLockFlagNames(t *testing.T) {
	for _, name := range []string{"archive-s3", "archive-s3-region"} {
		if doctorCmd.Flags().Lookup(name) == nil {
			t.Fatalf("doctor is missing the --%s flag (env binding depends on the name)", name)
		}
	}
}
