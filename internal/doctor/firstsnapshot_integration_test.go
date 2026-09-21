//go:build integration

package doctor

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestPrimaryKeyFindingFailsOnlyBeforeTheFirstSnapshot drives Build against a
// real MySQL (#1766): the same key-less table is a FAIL while the index has
// no schema snapshot, whether its database is absent or its table is empty,
// and a WARN once one exists. ForUnsavedServer (#1767) grades it FAIL with no
// index at all, and leaves the index checks out, since the write-access probe
// creates a database and Test connection must leave nothing behind.
func TestPrimaryKeyFindingFailsOnlyBeforeTheFirstSnapshot(t *testing.T) {
	src, srcName := testutil.CreateTestDB(t)
	testutil.MustExec(t, src, "CREATE TABLE nokey (a INT)")
	testutil.MustExec(t, src, "CREATE TABLE withkey (id INT PRIMARY KEY)")
	testutil.MustExec(t, src, "CREATE TABLE legacy (id INT PRIMARY KEY) ENGINE=MyISAM")
	source := testutil.BaseDSN() + "/?parseTime=true"
	pk := func(t *testing.T, r *Report) CheckResult {
		t.Helper()
		for _, c := range r.Checks {
			if c.Name == PrimaryKeyCheckName {
				return c
			}
		}
		t.Fatalf("no %q check in the report", PrimaryKeyCheckName)
		return CheckResult{}
	}

	absent := fmt.Sprintf("bintrail_doctor_pk_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if root, err := sql.Open("mysql", source); err == nil {
			root.Exec("DROP DATABASE IF EXISTS `" + absent + "`")
			root.Close()
		}
	})
	first := Build(t.Context(), source, testutil.IntegrationDSN(absent), srcName, 0)
	if got := pk(t, first); got.Status != StatusFail {
		t.Errorf("index database absent: %v (%s), want FAIL: the first snapshot will refuse", got.Status, got.Detail)
	}
	var engine *CheckResult
	for i := range first.Checks {
		if first.Checks[i].Name == InnoDBCheckName {
			engine = &first.Checks[i]
		}
	}
	if engine == nil || engine.Status != StatusFail || !strings.Contains(engine.Detail, srcName+".legacy") {
		t.Errorf("the MyISAM table before the first snapshot: %+v, want a FAIL naming it", engine)
	}

	idx, idxName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, idx)
	if got := pk(t, Build(t.Context(), source, testutil.IntegrationDSN(idxName), srcName, 0)); got.Status != StatusFail {
		t.Errorf("empty schema_snapshots: %v, want FAIL", got.Status)
	}
	testutil.MustExec(t, idx, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable)
		VALUES (1, NOW(), ?, 'withkey', 'id', 1, 'PRI', 'int', 'NO')`, srcName)
	if got := pk(t, Build(t.Context(), source, testutil.IntegrationDSN(idxName), srcName, 0)); got.Status != StatusWarn {
		t.Errorf("snapshot taken: %v, want WARN: later snapshots leave the table out and capture the rest", got.Status)
	}

	if got := pk(t, Build(t.Context(), source, "", srcName, 0)); got.Status != StatusWarn {
		t.Errorf("no index to ask: %v, want WARN", got.Status)
	}
	unsaved := Build(t.Context(), source, "", srcName, 0, ForUnsavedServer())
	if got := pk(t, unsaved); got.Status != StatusFail {
		t.Errorf("ForUnsavedServer: %v, want FAIL: a new server's first snapshot is certainly pending", got.Status)
	}
	for _, c := range unsaved.Checks {
		if c.Name == "Index database" || c.Name == IndexConnectionCheckName || c.Name == IndexWriteAccessCheckName {
			t.Errorf("ForUnsavedServer ran the index check %q; the write-access probe creates a database", c.Name)
		}
	}
}
