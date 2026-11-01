//go:build integration

package mydumperlock

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2530 on a real server: the wait holds an ftwrl read back behind a running
// statement, sent as plain text (Command "Query") or prepared (the Go driver
// sends a statement with arguments as COM_STMT_EXECUTE, Command "Execute",
// which mydumper's own check does not count). Only a lower bound is asserted:
// other tests share the server and may hold the wait longer, never shorter.
func TestIntegrationWaitForQuietSource_waitsForQueryAndExecute(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	blocker, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()

	for _, tc := range []struct {
		name    string
		command string // what the process list must show for the blocker
		run     func(context.Context) error
	}{
		{"query", "Query", func(ctx context.Context) error { _, err := blocker.ExecContext(ctx, "SELECT SLEEP(5)"); return err }},
		{"prepared", "Execute", func(ctx context.Context) error { _, err := blocker.ExecContext(ctx, "SELECT SLEEP(?)", 5); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			done := make(chan error, 1)
			go func() { done <- tc.run(ctx) }()
			t.Cleanup(func() { <-done })
			time.Sleep(2 * time.Second) // past the 1 s threshold
			// The case is what it says: a prepared statement really is
			// Command "Execute" on this server, a plain one "Query".
			var n int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND = ? AND INFO LIKE 'SELECT SLEEP%'",
				tc.command).Scan(&n); err != nil || n == 0 {
				t.Fatalf("no %s blocker in the process list (n=%d, err=%v)", tc.command, n, err)
			}

			// Generous: other tests share the server and can hold the wait up.
			w := &quietWait{interval: 500 * time.Millisecond, total: 3 * time.Minute, sleep: sleepCtx}
			start := time.Now()
			if err := w.run(ctx, db, baseline.LockModeFTWRL, RemedyCLI); err != nil {
				t.Fatalf("wait: %v", err)
			}
			if took := time.Since(start); took < 2*time.Second {
				t.Fatalf("the wait returned after %v, while the %s blocker still had about 3 s to run", took, tc.name)
			}
		})
	}
}
