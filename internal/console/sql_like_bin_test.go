package console

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A routed statement with ILIKE (the LIKE switch) over a table with a _bin
// text column is refused, through the real worker: MySQL compares LIKE on
// such a column with case, and the copy's ILIKE ignores the byte collation
// the column's view gives it, so 'AB1' would match 'ab%' on the copy only.
// Not routed (the copy answering alone), ILIKE is the client's own and is
// answered.
func TestSQL_realWorkerILikeOverBinaryText(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLStarTable(t, f.root, "codes", "CREATE TABLE `codes` (\n  `id` int NOT NULL,\n  `code` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"+
		"  PRIMARY KEY (`id`)\n);\n", []string{"1", "AB1"})
	writeSQLStarTable(t, f.root, "names", "CREATE TABLE `names` (\n  `id` int NOT NULL,\n  `name` varchar(20) DEFAULT NULL,\n"+
		"  PRIMARY KEY (`id`)\n);\n", []string{"1", "PRODUCT"})
	ctx := context.Background()
	routed := func(stmt string) sqlsandbox.Session {
		return sqlsandbox.Session{StrictStar: true, Types: readrouter.ShapeOf(stmt)}
	}

	const binStmt = "SELECT count(*) FROM shop.codes WHERE code ILIKE 'ab%'"
	_, err = f.s.runSQL(ctx, f.s.cm.boot, "u", binStmt, "", 0, routed(binStmt))
	var refusal *sqlStarRefusal
	if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, "shop.codes") {
		t.Fatalf("routed ILIKE over a _bin column: err = %v, want a refusal naming shop.codes", err)
	}
	// The name glued to the keyword, as the copy is sent `code`LIKE.
	const glued = `SELECT count(*) FROM shop.codes WHERE "code"ILIKE 'ab%'`
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", glued, "", 0, routed(glued)); !errors.As(err, &refusal) {
		t.Fatalf("routed ILIKE glued to a quoted name over a _bin column: err = %v, want a refusal", err)
	}
	if out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", binStmt, "", 0, sqlsandbox.Session{}); err != nil || len(out.Result.Rows) != 1 {
		t.Fatalf("the copy answering alone: %v %v, want its own answer", out, err)
	}
	// No _bin column: the copy answers the routed ILIKE.
	const plain = "SELECT count(*) FROM shop.names WHERE name ILIKE 'pr%'"
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", plain, "", 0, routed(plain)); err != nil {
		t.Errorf("routed ILIKE over a table with no _bin column: %v", err)
	}
	// A _bin table read without ILIKE: nothing to refuse for this rule.
	const eq = "SELECT count(*) FROM shop.codes WHERE code = 'AB1'"
	if _, err := f.s.runSQL(ctx, f.s.cm.boot, "u", eq, "", 0, routed(eq)); err != nil {
		t.Errorf("routed = over a _bin column: %v", err)
	}
}
