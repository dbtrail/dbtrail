//go:build integration

package streamrun

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/telemetry"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestOne_startSyncRefusalIsClassed drives streamrun's own StartSync exit —
// the `stream` / `up` / `watch` first-run refusal — against a real server: a
// user with REPLICATION CLIENT and SELECT (enough for the format, row-image,
// identity and snapshot steps) but no REPLICATION SLAVE is refused inside
// StartSync, and the error One returns must carry *parser.ReplicationError so
// usage telemetry reports db_permission, not unknown (#1503). The "StartSync("
// prefix pins WHICH exit fired.
func TestOne_startSyncRefusalIsClassed(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	testutil.MustExec(t, sourceDB, "CREATE TABLE orders (id INT PRIMARY KEY, total DECIMAL(10,2))")

	rootDB, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("connect as root: %v", err)
	}
	t.Cleanup(func() { rootDB.Close() }) // before the DROP USER cleanup: LIFO
	baseCfg, err := drivermysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatalf("parse base DSN: %v", err)
	}
	const user, pass = "bt_stream_noslave", "noslave-pw"
	testutil.MustExec(t, rootDB, "DROP USER IF EXISTS '"+user+"'@'%'")
	testutil.MustExec(t, rootDB, "CREATE USER '"+user+"'@'%' IDENTIFIED BY '"+pass+"'")
	testutil.MustExec(t, rootDB, "GRANT REPLICATION CLIENT, SELECT ON *.* TO '"+user+"'@'%'")
	t.Cleanup(func() { rootDB.Exec("DROP USER IF EXISTS '" + user + "'@'%'") })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = One(ctx, Config{
		IndexDSN:   testutil.IntegrationDSN(indexName),
		SourceDSN:  user + ":" + pass + "@tcp(" + baseCfg.Addr + ")/" + sourceName + "?parseTime=true",
		Flavor:     gomysql.MySQLFlavor,
		ServerID:   99893,
		BatchSize:  1,
		Schemas:    sourceName,
		Checkpoint: 1,
		GapTimeout: 30,
		Format:     "text",
		SSLMode:    "preferred",
		Deps:       testStreamDeps(),
	})
	if err == nil {
		t.Fatal("One without REPLICATION SLAVE must fail, got nil")
	}
	if !strings.Contains(err.Error(), "StartSync(") {
		t.Fatalf("expected the StartSync exit to fire, got: %v", err)
	}
	var re *parser.ReplicationError
	if !errors.As(err, &re) {
		t.Fatalf("One returned %T (%v), want a chain carrying *parser.ReplicationError", err, err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassDBPermission {
		t.Errorf("ClassifyError = %q (server error %d), want %q", got, re.MySQLErrorNumber(), telemetry.ClassDBPermission)
	}
	// A start the source refused leaves no checkpoint: the next run must
	// honor a corrected --start-* flag instead of resuming a start that never
	// connected, and the console must not show capture as started.
	if st, err := loadStreamState(indexDB); err != nil || st != nil {
		t.Errorf("a refused first start left a checkpoint: %+v (%v)", st, err)
	}
}
