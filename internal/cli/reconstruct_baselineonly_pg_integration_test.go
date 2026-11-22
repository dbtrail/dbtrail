//go:build integration

package cli

import (
	"context"
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The index `reconstruct --baseline-only` checks a PostgreSQL snapshot
// against must be this PostgreSQL capture's: another source's index, or a
// database with no capture state, answers nothing, and is said so.
func TestConnectAndCheckPGPastRead(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	md := baseline.DumpMetadata{LSN: 1000}
	for _, tc := range []struct {
		name, flavor string // "" = no stream_state row
		notChecked   bool
	}{
		{name: "a database with no capture state", notChecked: true},
		{name: "a MySQL source's index", flavor: "mysql", notChecked: true},
		{name: "this PostgreSQL capture's index", flavor: "postgres"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, name := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatal(err)
			}
			if tc.flavor != "" {
				testutil.MustExec(t, db, "INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id) VALUES (1, 'gtid', ?, NOW(), 1)", tc.flavor)
			}
			err := connectAndCheckPGPastRead(context.Background(), testutil.IntegrationDSN(name), "public", "t", md)
			if got := errors.Is(err, errPGIndexNotChecked); got != tc.notChecked {
				t.Errorf("err = %v, want not checked = %v", err, tc.notChecked)
			}
			if !tc.notChecked && err != nil {
				t.Errorf("covered: %v", err)
			}
		})
	}
}
