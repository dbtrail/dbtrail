//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationSourceConnectedWaitsForValidation (#1606): a source whose
// binlog settings fail validation has not been connected to for capture, so
// the first-run list shows the failure on "Connect to the source".
func TestIntegrationSourceConnectedWaitsForValidation(t *testing.T) {
	_, indexName := testutil.CreateTestDB(t)
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
	if err == nil {
		t.Fatal("One accepted a source that failed binlog validation")
	}
	if connected.Load() {
		t.Error("OnSourceConnected fired for a source that failed validation")
	}
}
