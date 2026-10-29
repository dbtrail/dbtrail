//go:build integration

package readrouter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// slowStatement takes five seconds on MySQL and, stopped by KILL QUERY,
// answers 1317: SLEEP alone would not (an interrupted SLEEP returns 1 and
// the statement succeeds), but the next row's check of the kill flag does.
const slowStatement = "SELECT COUNT(*) FROM (SELECT 1 a UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) x WHERE SLEEP(%s) = 0"

// On a real MySQL, with an account that may only read: a statement past its
// deadline is stopped on the source, the client reads 1317, and the
// connection keeps its session (a user variable) and its open transaction.
func TestIntegrationForwarder_deadlineKeepsTheSession(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	root, err := sql.Open("mysql", testutil.BaseDSN()+"/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	user := fmt.Sprintf("bt_deadline_%d", time.Now().UnixNano()%1_000_000)
	const pass = "Deadline-1317"
	for _, q := range []string{
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, pass),
		fmt.Sprintf("GRANT SELECT ON *.* TO '%s'@'%%'", user),
	} {
		if _, err := root.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = root.Exec(fmt.Sprintf("DROP USER '%s'@'%%'", user)) })

	base, err := drivermysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	const limit = time.Second
	fw, err := NewForwarder(fmt.Sprintf("%s:%s@tcp(%s)/", user, pass, base.Addr), config.SSL{Mode: "preferred"}, DefaultPolicy(), limit)
	if err != nil {
		t.Fatal(err)
	}
	fw.StopOnSource = true
	defer fw.Close()
	run := func(ctx context.Context, q string) (*BufferSink, error) {
		var buf BufferSink
		_, err := fw.Forward(ctx, q, &buf)
		return &buf, err
	}
	for _, q := range []string{"SET @kept = 7", "BEGIN"} {
		if _, err := run(context.Background(), q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	id := fw.ThreadID()

	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	start := time.Now()
	_, err = run(ctx, fmt.Sprintf(slowStatement, "1"))
	if errCode(err) != mysql.ER_QUERY_INTERRUPTED || IsLost(err) {
		t.Fatalf("past its deadline: %v, want 1317 with the connection kept", err)
	}
	if !strings.Contains(err.Error(), "still open") {
		t.Errorf("the client is told %q, want it to say the connection is still open", err)
	}
	if took := time.Since(start); took > 4*time.Second {
		t.Errorf("answered after %v: the statement ran to its end instead of being stopped", took)
	}
	if fw.ThreadID() != id || !fw.InTransaction() {
		t.Errorf("thread %d then %d, in a transaction %v: the session was not kept", id, fw.ThreadID(), fw.InTransaction())
	}
	buf, err := run(context.Background(), "SELECT @kept")
	if err != nil || len(buf.Rows) != 1 || fmt.Sprint(buf.Rows[0][0]) != "7" {
		t.Errorf("SELECT @kept after the stop: rows %v, err %v, want 7", buf.Rows, err)
	}

	// Prepared: the source shows it with its argument in place.
	st, err := fw.Prepare(context.Background(), fmt.Sprintf(slowStatement, "?"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), limit)
	defer cancel2()
	if _, err := st.Execute(ctx2, []any{1}, &BufferSink{}); errCode(err) != mysql.ER_QUERY_INTERRUPTED || IsLost(err) {
		t.Fatalf("prepared, past its deadline: %v, want 1317 with the connection kept", err)
	}
	if _, err := run(context.Background(), "COMMIT"); err != nil || fw.ThreadID() != id {
		t.Errorf("COMMIT on the same connection: %v (thread %d, then %d)", err, id, fw.ThreadID())
	}
}
