//go:build integration

package cli

import (
	"errors"
	"os"
	"testing"

	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// On an index written by PostgreSQL capture, recover-cascade refuses before
// fetching anything, and above all before opening --output: an existing file
// at that path keeps its bytes.
func TestRecoverCascade_postgresIndexRefusedUpFront(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id)
		VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1)`)
	out := t.TempDir() + "/cascade.sql"
	if err := os.WriteFile(out, []byte("KEEP"), 0o600); err != nil {
		t.Fatal(err)
	}

	rcIndexDSN, rcSchema, rcTable = dsn, dbName, "parent"
	rcPK, rcPKs, rcSince, rcUntil = "", nil, "", ""
	rcOutput, rcDryRun, rcFormat = out, false, "text"
	rcLookback, rcMaxDepth, rcLimit, rcAllowIncomplete = "30d", 5, 1000, false

	err := runCascadeCmd(t)
	t.Logf("screen text: %v", err)
	var pe *cascaderecover.PostgresCascadeError
	if !errors.As(err, &pe) || pe.Surface != cascaderecover.SurfaceCLI || pe.Schema != dbName {
		t.Fatalf("want the CLI PostgreSQL refusal for %s, got %v", dbName, err)
	}
	if b, _ := os.ReadFile(out); string(b) != "KEEP" {
		t.Fatalf("the refusal touched --output: %q", b)
	}
}
