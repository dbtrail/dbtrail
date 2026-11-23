//go:build integration

package console

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The list of columns on the PostgreSQL port does not wait for an update of
// the tails, and is the list a statement that takes tails reads.
func TestIntegrationPGCatalogReaderDoesNotWaitForTheTails(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := newTailRig(t)
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) {
		return func(string, string) bool { return true }, true
	}
	r.table(nil)
	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	sess := sqlsandbox.Session{PostgresRules: true, PostgresNames: true}
	const list = "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns WHERE table_catalog = current_database() AND table_schema <> 'main' ORDER BY table_schema, table_name, ordinal_position"
	want, err := q.Run(context.Background(), list, "", sess)
	if err != nil || len(want.Rows) == 0 {
		t.Fatalf("the list with tails: %d rows, %v", len(want.Rows), err)
	}
	q.last.mu.Lock()
	before := q.last.pending
	q.last.mu.Unlock()
	k := r.s.tailKeeperFor(bootServerID, r.b)
	if k == nil {
		t.Fatal("no keeper")
	}
	k.mu.Lock()
	release := time.AfterFunc(10*time.Second, k.mu.Unlock)
	start := time.Now()
	got, err := q.RunPGCatalog(context.Background(), list, "", sess)
	took := time.Since(start)
	if release.Stop() {
		k.mu.Unlock()
	}
	if err != nil {
		t.Fatal(err)
	}
	if took > 3*time.Second {
		t.Errorf("waited %v for the update of the tails", took)
	}
	if fmt.Sprint(got.Rows) != fmt.Sprint(want.Rows) {
		t.Errorf("the list without tails: %v, with: %v", got.Rows, want.Rows)
	}
	// What the list ran is not the connection's last answer.
	q.last.mu.Lock()
	after := q.last.pending
	q.last.mu.Unlock()
	if after != before {
		t.Error("reading the list replaced the connection's last answer")
	}
}
