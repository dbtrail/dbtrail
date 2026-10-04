//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationFlashbackPreparedStatements drives the port with a real
// driver on the binary protocol (#2036): database/sql sends a statement with
// arguments as COM_STMT_PREPARE + COM_STMT_EXECUTE, which the port used to
// refuse. Free SQL on the copy and the time-travel shapes both answer, with
// typed columns, and refusals keep their codes.
func TestIntegrationFlashbackPreparedStatements(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	dsn := seedFlashbackIndex(t, "alice", now)
	baseDir := t.TempDir()
	writeFreeSQLBaseline(t, baseDir)

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: dsn, SourceDSN: "r:p@tcp(x:3306)/shop", BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = serveFlashback(ctx, srv, ln, flashbackConfig{}); close(served) }()
	defer func() { cancel(); <-served }()

	db := openFlashback(t, ln.Addr().String(), ent.ID, "tok", "")
	defer db.Close()
	// One connection for the whole test: the USE below must apply to the
	// statements after it.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// A statement with arguments: prepared, executed on the copy, typed.
	rows, err := conn.QueryContext(ctx, "SELECT id, status FROM orders WHERE status = ? AND id > ?", "paid", 0)
	if err != nil {
		t.Fatalf("prepared free SQL: %v", err)
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if got := cts[0].DatabaseTypeName() + "/" + cts[1].DatabaseTypeName(); got != "BIGINT/VARCHAR" {
		t.Errorf("column types = %s, want BIGINT/VARCHAR", got)
	}
	var n int
	for rows.Next() {
		var id int64
		var status string
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatal(err)
		}
		if id != 2 || status != "paid" {
			t.Errorf("row = %d/%s, want 2/paid", id, status)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if n != 1 {
		t.Errorf("%d rows, want 1", n)
	}

	// An explicit Prepare, executed twice with different arguments.
	st, err := conn.PrepareContext(ctx, "SELECT count(*) FROM orders WHERE id >= ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	for arg, want := range map[int]int{1: 2, 2: 1, 3: 0} {
		var got int
		if err := st.QueryRowContext(ctx, arg).Scan(&got); err != nil || got != want {
			t.Errorf("count(id >= %d) = %d, %v; want %d", arg, got, err, want)
		}
	}
	if err := st.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// An argument is a value, whatever it holds: a quote, a backslash, a
	// statement.
	for _, arg := range []string{"it's", `a\b`, "x'; DROP TABLE orders; --"} {
		var c int
		if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM orders WHERE status = ?", arg).Scan(&c); err != nil || c != 0 {
			t.Errorf("status = %q: count %d, err %v; want 0 rows and no error", arg, c, err)
		}
	}
	// A NULL argument next to a real one, and a NULL result, both travel.
	var maybe sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT max(status) FROM orders WHERE status = ? AND id > ?", nil, -1).Scan(&maybe); err != nil || maybe.Valid {
		t.Errorf("max over no rows = %+v, %v; want NULL", maybe, err)
	}
	// So does a statement whose every argument is NULL.
	if err := conn.QueryRowContext(ctx, "SELECT max(status) FROM orders WHERE status = ?", nil).Scan(&maybe); err != nil || maybe.Valid {
		t.Errorf("an all-NULL execution = %+v, %v; want NULL", maybe, err)
	}
	// A negative argument after a minus is arithmetic, not the copy's comment.
	var sum int
	if err := conn.QueryRowContext(ctx, "SELECT 5-? AS n", -3).Scan(&sum); err != nil || sum != 8 {
		t.Errorf("5-(-3) = %d, %v; want 8", sum, err)
	}

	// Refusals reach the client with their words and their code, the same
	// ones the statement gets as text.
	if _, err := conn.ExecContext(ctx, "DELETE FROM orders WHERE id = ?", 1); mysqlCode(err) != 1064 || !strings.Contains(err.Error(), "only a single SELECT") {
		t.Errorf("prepared DELETE: err = %v, want the 1064 refusal", err)
	}
	_, textErr := conn.QueryContext(ctx, "SELECT * FROM nope WHERE id = 1")
	if _, err := conn.QueryContext(ctx, "SELECT * FROM nope WHERE id = ?", 1); err == nil || !strings.Contains(err.Error(), "does not exist") || mysqlCode(err) != mysqlCode(textErr) {
		t.Errorf("prepared SELECT on a missing table: err = %v; want the error the text statement gets (%v)", err, textErr)
	}

	// The time-travel shapes take arguments the same way, on the same
	// connection.
	if _, err := conn.ExecContext(ctx, "USE myapp"); err != nil {
		t.Fatal(err)
	}
	asOf := now.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	var id int64
	var name string
	if err := conn.QueryRowContext(ctx, "SELECT * FROM _flashback.users AS OF ? WHERE id = ?", asOf, 1).Scan(&id, &name); err != nil {
		t.Fatalf("prepared time travel: %v", err)
	}
	if id != 1 || name != "alice" {
		t.Errorf("time travel = %d/%q, want 1/alice", id, name)
	}
}
