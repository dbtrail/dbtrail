package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// writeSQLStarTable adds one table with its CREATE TABLE in the footer to the
// fixture snapshot; row lists the stored columns' values in declared order.
func writeSQLStarTable(t *testing.T, root, table, createSQL string, row []string) {
	t.Helper()
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(root, sqlSnapshotDirName, "shop", table+".parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100, Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow(row, make([]bool, len(row))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// #2111 through runSQL and the real worker: a star returns the table's
// columns in the table's order, and where the copy's star is not MySQL's (the
// order is not known, or MySQL returns other columns) a caller that asked for
// MySQL's answer (read routing: Session.StrictStar) is refused, by table and
// reason, while every other caller gets the copy's answer as before.
func TestSQL_realWorkerSelectStar_2111(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "lines", "CREATE TABLE `lines` (\n  `id` int NOT NULL,\n  `zeta` int DEFAULT NULL,\n"+
		"  `alpha` decimal(6,2) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "26", "1.50"})
	writeSQLStarTable(t, f.root, "gen", "CREATE TABLE `gen` (\n  `id` int NOT NULL,\n  `twice` int GENERATED ALWAYS AS (`id` * 2) STORED,\n"+
		"  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "3"})
	ctx := context.Background()
	strict := sqlsandbox.Session{StrictStar: true}
	names := func(res sqlsandbox.Result) []string {
		out := make([]string, len(res.Columns))
		for i, c := range res.Columns {
			out[i] = c.Name
		}
		return out
	}

	// The table's order, with or without the strict session.
	for _, sess := range []sqlsandbox.Session{{}, strict} {
		out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT * FROM shop.lines", "", 0, sess)
		if err != nil {
			t.Fatalf("shop.lines under %+v: %v", sess, err)
		}
		if got, want := names(out.Result), []string{"id", "zeta", "alpha"}; !reflect.DeepEqual(got, want) {
			t.Errorf("SELECT * FROM shop.lines under %+v: columns %q, want the table's order %q", sess, got, want)
		}
		if row := out.Result.Rows[0]; fmt.Sprintf("%v", row) != "[1 26 1.50]" {
			t.Errorf("SELECT * FROM shop.lines under %+v: row %v, want 1, 26, 1.50", sess, row)
		}
	}

	refused := func(stmt string, wantInMessage ...string) {
		t.Helper()
		_, err := f.s.runSQL(ctx, f.s.cm.boot, "u", stmt, "", 0, strict)
		var refusal *sqlStarRefusal
		if !errors.As(err, &refusal) {
			t.Errorf("%s under StrictStar: err = %v, want a refusal", stmt, err)
			return
		}
		for _, w := range wantInMessage {
			if !strings.Contains(refusal.Message, w) {
				t.Errorf("%s: refusal %q does not say %q", stmt, refusal.Message, w)
			}
		}
	}
	answered := func(stmt string, sess sqlsandbox.Session) {
		t.Helper()
		if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", stmt, "", 0, sess); err != nil {
			t.Errorf("%s under %+v: %v, want an answer", stmt, sess, err)
		}
	}
	// shop.orders carries no CREATE TABLE: its order is not known.
	refused("SELECT * FROM shop.orders", "shop.orders", "SELECT *", "order")
	refused("SELECT o.* FROM shop.orders o", "shop.orders")
	refused("SELECT l.id, o.* FROM shop.lines l JOIN shop.orders o ON o.id = l.id", "shop.orders")
	refused("SELECT id FROM (SELECT * FROM shop.orders) q", "shop.orders")
	// MySQL returns a generated column the snapshot does not hold.
	refused("SELECT * FROM shop.gen", "shop.gen", "generated column twice")
	// A listing may read any table.
	refused("SHOW ALL TABLES", "SELECT *")
	// So may a statement the worker cannot be sure about (a table function
	// reads what it does not name): refused too, star or not.
	refused("SELECT range FROM range(3)", "shop.")
	// A star over a USING or NATURAL join is ordered by the join, and MySQL
	// puts the join's columns first: refused whatever the tables.
	refused("SELECT * FROM shop.lines a JOIN shop.lines b USING (id)", "USING or NATURAL")
	refused("SELECT * FROM shop.lines a NATURAL JOIN shop.lines b", "USING or NATURAL")
	answered("SELECT a.id, b.zeta FROM shop.lines a JOIN shop.lines b USING (id)", strict)
	answered("SELECT * FROM shop.lines a JOIN shop.lines b ON a.id = b.id", strict)
	answered("SELECT * FROM shop.lines a JOIN shop.lines b USING (id)", sqlsandbox.Session{})
	// Without a star the same tables answer.
	answered("SELECT id, status FROM shop.orders", strict)
	answered("SELECT count(*) FROM shop.orders", strict)
	answered("SELECT id, a FROM shop.gen", strict)
	// A star over a table that is not the problem is not held back by one the statement does not read.
	answered("SELECT * FROM shop.lines WHERE id IN (SELECT id FROM shop.lines)", strict)
	// And nothing is refused for a caller that did not ask: the SQL card and a port with no routing.
	answered("SELECT * FROM shop.orders", sqlsandbox.Session{})
	answered("SELECT * FROM shop.gen", sqlsandbox.Session{})
	answered("SHOW ALL TABLES", sqlsandbox.Session{})

	// On the port's wire it is a refusal the client is shown, which read
	// routing answers by sending the statement to MySQL.
	_, err = (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, "SELECT * FROM shop.orders", "", strict)
	var shape *sqlsandbox.RefusedError
	if !errors.As(err, &shape) || !strings.Contains(shape.Reason, "shop.orders") {
		t.Errorf("port: err = %v (%T), want a RefusedError naming the table", err, err)
	}
}
