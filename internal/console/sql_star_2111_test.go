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
	// Every table before the first statement: the console remembers a
	// snapshot's table definitions once it has read them.
	writeSQLStarTable(t, f.root, "g2", "CREATE TABLE `g2` (\n  `id` int NOT NULL,\n  `twice` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "99", "7"})
	writeSQLStarTable(t, f.root, "inv", "CREATE TABLE `inv` (\n  `id` int NOT NULL,\n  `secret` int DEFAULT NULL /*!80023 INVISIBLE */,\n  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "5", "3"})
	writeSQLStarTable(t, f.root, "inv2", "CREATE TABLE `inv2` (\n  `id` int NOT NULL,\n  `secret` int DEFAULT NULL,\n  `b` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n", []string{"1", "6", "7"})
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

	// Only the tables a star really expands count, and the refusal names them.
	// Answered: MySQL returns the same columns as the copy for each of these.
	answered("SELECT l.id FROM shop.lines l WHERE EXISTS (SELECT * FROM shop.gen g WHERE g.id = l.id)", strict)
	answered("SELECT l.id FROM shop.lines l WHERE NOT EXISTS (SELECT * FROM shop.inv o WHERE o.id = l.id)", strict)
	answered("SELECT * FROM (SELECT id, a FROM shop.gen) x", strict)
	answered("SELECT x.* FROM (SELECT id, a FROM shop.gen) x", strict)
	answered("SELECT l.*, g.id FROM shop.lines l JOIN shop.gen g ON g.id = l.id", strict)
	answered("SELECT l.* FROM shop.lines l JOIN shop.inv o ON o.id = l.id", strict)
	answered("SELECT l.* FROM shop.lines l JOIN shop.lines m USING (id)", strict)
	answered("WITH q AS (SELECT id, a FROM shop.gen) SELECT * FROM q", strict)
	answered("SELECT id FROM shop.gen UNION ALL SELECT * FROM (SELECT id FROM shop.lines) x", strict)
	// Refused: the star expands the table that differs, and the message says which.
	refused("SELECT g.*, l.id FROM shop.lines l JOIN shop.gen g ON g.id = l.id", "SELECT * on shop.gen", "generated column twice")
	refused("SELECT l.*, g.* FROM shop.lines l JOIN shop.gen g ON g.id = l.id", "SELECT * on shop.gen")
	refused("SELECT * FROM shop.lines l JOIN shop.gen g ON g.id = l.id", "SELECT * on shop.gen")
	refused("SELECT * FROM (SELECT * FROM shop.gen) x", "SELECT * on shop.gen")
	refused("SELECT count(*) FROM (SELECT * FROM shop.gen) x", "SELECT * on shop.gen") // conservative: only the count is read
	refused("SELECT id FROM shop.lines WHERE id IN (SELECT * FROM shop.gen)", "SELECT * on shop.gen")
	refused("SELECT l.id FROM shop.lines l WHERE EXISTS (SELECT * FROM (SELECT * FROM shop.gen) g)", "SELECT * on shop.gen")
	refused("SELECT l.id FROM shop.lines l WHERE EXISTS (SELECT DISTINCT * FROM shop.gen LIMIT 1 OFFSET 1)", "SELECT * on shop.gen")
	refused("SELECT id FROM shop.lines UNION ALL SELECT * FROM shop.orders", "SELECT * on shop.orders")
	// A star the walk cannot attribute counts over every table the statement reads.
	refused("SELECT (SELECT g.* FROM shop.lines LIMIT 1) FROM shop.gen g", "shop.gen")
	// A name with no schema is every table of that name: refused when one of them differs.
	refused("SELECT * FROM gen", "shop.gen")

	// A NATURAL JOIN pairs on every column the two tables share by name, so
	// it depends on each table's column SET with or without a star. Measured
	// on MySQL 8.4 and MariaDB 11.4: gen NATURAL JOIN g2 pairs on (id, twice)
	// there and returns no row, where the copy, which holds no generated
	// column, pairs on id alone and returns one; inv NATURAL JOIN inv2 pairs
	// on id alone there (the invisible column is left out) and returns a row,
	// where the copy pairs on (id, secret) and returns none.
	refused("SELECT gen.a, g2.b FROM shop.gen NATURAL JOIN shop.g2", "shop.gen", "NATURAL")
	refused("SELECT count(*) FROM shop.gen NATURAL JOIN shop.g2", "shop.gen")
	refused("SELECT inv.a, inv2.b FROM shop.inv NATURAL JOIN shop.inv2", "shop.inv", "invisible column secret")
	refused("SELECT count(*) FROM shop.g2 NATURAL LEFT JOIN shop.orders", "shop.orders")
	refused("SELECT id FROM shop.lines WHERE id IN (SELECT g2.id FROM shop.g2 NATURAL JOIN shop.gen)", "shop.gen")
	// Every table's column set is MySQL's: the copy answers.
	answered("SELECT g2.b, inv2.b FROM shop.g2 NATURAL JOIN shop.inv2", strict)
	// USING names its columns, so with a select list it does not depend on the set.
	answered("SELECT gen.a, g2.b FROM shop.gen JOIN shop.g2 USING (id)", strict)
	// And outside routing nothing is refused.
	answered("SELECT gen.a, g2.b FROM shop.gen NATURAL JOIN shop.g2", sqlsandbox.Session{})
	// Without a star the tables whose columns are known answer. One with no
	// table definition does not, star or not (#2123): what it lacks on the
	// copy is not known, and a name could then mean something else there.
	answered("SELECT id, a FROM shop.gen", strict)
	answered("SELECT id, a FROM shop.inv", strict)
	refused("SELECT id, status FROM shop.orders", "shop.orders", "no table definition")
	refused("SELECT count(*) FROM shop.orders", "shop.orders")
	refused("SELECT l.id FROM shop.lines l WHERE NOT EXISTS (SELECT * FROM shop.orders o WHERE o.id = l.id)", "shop.orders")
	refused("SELECT l.* FROM shop.lines l JOIN shop.orders o ON o.id = l.id", "shop.orders")
	// A star over a table that is not the problem is not held back by one the statement does not read.
	answered("SELECT * FROM shop.lines WHERE id IN (SELECT id FROM shop.lines)", strict)
	// And nothing is refused for a caller that did not ask: the SQL card and a port with no routing.
	answered("SELECT * FROM shop.orders", sqlsandbox.Session{})
	answered("SELECT * FROM shop.gen", sqlsandbox.Session{})
	answered("SHOW ALL TABLES", sqlsandbox.Session{})

	// On the port's wire it is a refusal the client is shown, which read
	// routing answers by sending the statement to MySQL.
	_, err = (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, "SELECT * FROM shop.orders", "", strict)
	var differ *sqlsandbox.ColumnsDifferError
	if !errors.As(err, &differ) || !strings.Contains(differ.Reason, "shop.orders") {
		t.Errorf("port: err = %v (%T), want a ColumnsDifferError naming the table", err, err)
	}
}
