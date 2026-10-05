package cliapp

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/serverid"
)

// TestAutoServerID_AsksThisCommandsIndex: `up` with no --server-id derives
// from ITS source and ITS index. A call that dropped the index would give
// every installation the same id again, and no other test would notice.
func TestAutoServerID_AsksThisCommandsIndex(t *testing.T) {
	src, idx := upSourceDSN, upIndexDSN
	t.Cleanup(func() { upSourceDSN, upIndexDSN = src, idx })
	upSourceDSN, upIndexDSN = "u:p@tcp(h:3306)/", "root:pw@tcp(index-mysql:3306)/bintrail_index"
	var asked string
	t.Cleanup(serverid.SetIndexServerUUIDForTest(func(_ context.Context, dsn string) (string, error) {
		asked = dsn
		return "8b0c2f3e-7a11-4d5e-9c1a-000000000001", nil
	}))
	var out strings.Builder
	id, err := autoServerID(context.Background(), &out)
	want, sourceOnly, _ := serverid.DeriveForInstall(context.Background(), upSourceDSN, upIndexDSN)
	if err != nil || id != want || sourceOnly != nil || asked != upIndexDSN {
		t.Fatalf("autoServerID = %d, %v (index asked: %q); want %d from %q", id, err, asked, want, upIndexDSN)
	}
}
