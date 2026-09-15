//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationSourceConnectedWaitsForValidation (#1606): a source whose
// binlog settings fail validation has not been connected to for capture, so
// the first-run list shows the failure on "Connect to the source".
func TestIntegrationSourceConnectedWaitsForValidation(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	deps := testStreamDeps()
	deps.ValidateBinlogFormat = func(*sql.DB) error { return errors.New("binlog_format is STATEMENT") }
	var connected atomic.Bool
	err := One(context.Background(), Config{
		IndexDSN:   testutil.IntegrationDSN(indexName),
		SourceDSN:  testutil.BaseDSN() + "/",
		Flavor:     gomysql.MySQLFlavor,
		ServerID:   4242,
		BatchSize:  1,
		Checkpoint: 1,
		GapTimeout: 30,
		Format:     "text",
		SSLMode:    "preferred",
		Deps:       deps,
		Hooks:      &Hooks{OnSourceConnected: func() { connected.Store(true) }},
	})
	// The error must be the validation's own, or the run stopped before it
	// and this test would pass without reaching the check.
	if err == nil || !strings.Contains(err.Error(), "binlog_format is STATEMENT") {
		t.Fatalf("One = %v, want the validation error", err)
	}
	if connected.Load() {
		t.Error("OnSourceConnected fired for a source that failed validation")
	}
}
