//go:build integration

package readrouter

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2607 against a real MySQL: with autocommit off the session is in a
// transaction from its first statement, a COMMIT or a ROLLBACK only starts
// the next one, and turning autocommit back on ends it. A BEGIN under
// autocommit is a transaction until its COMMIT, as before.
func TestIntegrationForwarder_InTransaction_autocommit_2607(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	fw, err := NewForwarder(testutil.BaseDSN()+"/", config.SSL{Mode: "preferred"}, DefaultPolicy(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer fw.Close()
	ctx := context.Background()
	for _, step := range []struct {
		stmt string
		in   bool
	}{
		{"SELECT 1", false},
		{"SET autocommit=0", true},
		{"SELECT 1", true},
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"SET @@session.autocommit = 1", false},
		{"SELECT 1", false},
		{"BEGIN", true},
		{"SELECT 1", true},
		{"COMMIT", false},
		{"SET SESSION autocommit = OFF", true},
		{"SET autocommit=ON", false},
	} {
		if _, err := fw.Forward(ctx, step.stmt, &BufferSink{}); err != nil {
			t.Fatalf("%s: %v", step.stmt, err)
		}
		if got := fw.InTransaction(); got != step.in {
			t.Errorf("after %s: InTransaction() = %v, want %v", step.stmt, got, step.in)
		}
	}
}
