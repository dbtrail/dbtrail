//go:build integration

package reconstruct

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationCreateOrReplace_refusesAndDefinesTheTable is #1664 against a
// real schema_changes table: MariaDB's CREATE OR REPLACE TABLE drops an
// existing table's rows with no row events, so it refuses a reconstruct over
// its window like a DROP, and it is the newest definition the binlog-only
// fallback can write.
func TestIntegrationCreateOrReplace_refusesAndDefinesTheTable(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()
	at := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)

	replace := "CREATE OR REPLACE TABLE t (id INT PRIMARY KEY, c INT)"
	for i, row := range []struct {
		kind  event.DDLKind
		query string
		ts    time.Time
	}{
		{event.DDLCreateTable, "CREATE TABLE t (id INT PRIMARY KEY)", at.Add(-2 * time.Hour)},
		{event.DDLReplaceTable, replace, at.Add(-time.Hour)},
	} {
		if _, err := db.Exec(`INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
			VALUES (?, 'binlog.000001', ?, 'shop', 't', ?, ?)`, row.ts, 100+i, string(row.kind), row.query); err != nil {
			t.Fatalf("seed schema_changes: %v", err)
		}
	}

	if err := CheckDestructiveDDL(ctx, db, "shop", "t", at.Add(-90*time.Minute), at); !errors.Is(err, ErrDestructiveDDL) {
		t.Errorf("CheckDestructiveDDL over the replace = %v, want ErrDestructiveDDL", err)
	}
	if err := CheckDestructiveDDL(ctx, db, "shop", "t", at.Add(-30*time.Minute), at); err != nil {
		t.Errorf("CheckDestructiveDDL after the replace = %v, want nil", err)
	}
	ddl, found, err := findCapturedCreateTableDDL(ctx, db, "shop", "t", at)
	if err != nil || !found || ddl != replace {
		t.Errorf("findCapturedCreateTableDDL = %q found=%v err=%v, want the replace statement", ddl, found, err)
	}
}
