//go:build integration

package consoleapp

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The optional flag and count have to survive the supervisor's mapping into
// the web interface's shape. Dropped there, the two optional items come back
// as plain warnings and the screen says "with 2 warnings" again.
func TestIntegrationDoctorUnsaved_carriesOptional(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	if _, err := db.Exec("CREATE TABLE keyed (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	var stmtLog, rowMeta string
	if err := db.QueryRow("SELECT @@global.binlog_rows_query_log_events, @@global.binlog_row_metadata").Scan(&stmtLog, &rowMeta); err != nil {
		t.Fatal(err)
	}
	if (stmtLog != "0" && !strings.EqualFold(stmtLog, "OFF")) || !strings.EqualFold(rowMeta, "MINIMAL") {
		t.Skipf("the test MySQL is not stock (binlog_rows_query_log_events=%s, binlog_row_metadata=%s)", stmtLog, rowMeta)
	}
	sup := newMonitorSupervisor(context.Background(), testutil.IntegrationDSN(name), nil, 0)
	r, err := sup.DoctorUnsaved(context.Background(), console.ServerEntry{SourceDSN: testutil.BaseDSN() + "/", Schemas: name})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"Statement capture (query_text)", "Schema-drift detection (binlog_row_metadata)"} {
		if c := checkNamed(t, r, n); c.Status != "warn" || !c.Optional {
			t.Errorf("%s: status %s optional %v; want an optional warn", n, c.Status, c.Optional)
		}
	}
	warns := 0
	for _, c := range r.Checks {
		if c.Status == "warn" && !c.Optional {
			warns++
		}
	}
	if r.Optional != 2 || r.Warnings != warns {
		t.Errorf("optional %d warnings %d; want 2 and %d", r.Optional, r.Warnings, warns)
	}
}
