package shim

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// On the port without routing, a statement written the MySQL way reaches
// the copy with its backtick-quoted names in double quotes and STRAIGHT_JOIN
// written as the join it means: the same rewrite the routing ladder applies
// (readrouter.ForCopy). Where that rewrite would be a guess it is not made
// and the statement runs as written, as it did before.
func TestPlainPort_mysqlNamesReachTheCopy(t *testing.T) {
	cases := []struct {
		name, stmt, want string
		keepJoins        bool
	}{
		{"a table", "SELECT * FROM `orders`", `SELECT * FROM "orders"`, false},
		{"columns, alias and schema", "SELECT `o`.`id`, count(*) FROM `shop`.`orders` `o` GROUP BY `o`.`id`",
			`SELECT "o"."id", count(*) FROM "shop"."orders" "o" GROUP BY "o"."id"`, false},
		{"case kept", "SELECT `Total` FROM `Orders`", `SELECT "Total" FROM "Orders"`, false},
		{"a name with an accent", "SELECT `año` FROM `pedidos`", `SELECT "año" FROM "pedidos"`, false},
		{"a backtick inside a string stays", "SELECT 'a`b' AS x FROM `orders`", `SELECT 'a` + "`" + `b' AS x FROM "orders"`, false},
		{"a backtick inside a comment stays", "SELECT id /* `x` */ FROM `orders`", "SELECT id /* `x` */ FROM \"orders\"", false},
		{"nothing to rewrite", "SELECT id FROM orders", "SELECT id FROM orders", false},
		{"STRAIGHT_JOIN", "SELECT a.x FROM a STRAIGHT_JOIN b ON a.id = b.id", "SELECT a.x FROM a JOIN b ON a.id = b.id", true},
		// Not rewritable with certainty: run as written, as before.
		{"a name holding a double quote", "SELECT * FROM `a\"b`", "SELECT * FROM `a\"b`", false},
		{"a # comment", "SELECT * FROM `orders` # note", "SELECT * FROM `orders` # note", false},
		{"a double-quoted string", "SELECT \"x\" FROM `orders`", "SELECT \"x\" FROM `orders`", false},
		// A dollar-quoted string is the copy's: a backtick pair inside it
		// must not become double quotes in the string.
		{"a dollar-quoted string", "SELECT $$a`b`c$$ FROM `orders`", "SELECT $$a`b`c$$ FROM `orders`", false},
		{"a tagged dollar-quoted string", "SELECT $q$x`y`z$q$", "SELECT $q$x`y`z$q$", false},
		{"a dollar quote tagged outside ASCII", "SELECT $é$`x`$é$ AS s", "SELECT $é$`x`$é$ AS s", false},
		{"another one", "SELECT $ñ$it`s`$ñ$ FROM `t`", "SELECT $ñ$it`s`$ñ$ FROM `t`", false},
		// -- with no space after it is a comment on the copy and two minus
		// signs on MySQL: a quote inside it would end a string on one side.
		{"a -- comment with no space", "SELECT count(*) FROM t --don't count drafts\nWHERE tag <> '`draft`.' --'",
			"SELECT count(*) FROM t --don't count drafts\nWHERE tag <> '`draft`.' --'", false},
		{"a -- comment with a space is rewritten around", "SELECT `id` FROM `t` -- note\n", "SELECT \"id\" FROM \"t\" -- note\n", false},
		{"STRAIGHT_JOIN mixed with a comma", "SELECT a.x FROM a STRAIGHT_JOIN b ON a.id = b.id, c", "SELECT a.x FROM a JOIN b ON a.id = b.id, c", false},
		{"a plain $ in a string is no dollar quote", "SELECT `id` FROM `orders` WHERE note = '$5 and $6'", `SELECT "id" FROM "orders" WHERE note = '$5 and $6'`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeFreeSQL{res: oneCell("id", "INTEGER", json.Number("7"))}
			h := NewHandler(nil, nil)
			h.BindFreeSQL(f)
			if _, err := h.HandleQuery(c.stmt); err != nil {
				t.Fatalf("HandleQuery(%q): %v", c.stmt, err)
			}
			if f.gotStmt != c.want {
				t.Errorf("copy got %q, want %q", f.gotStmt, c.want)
			}
			if f.gotSess.KeepJoinOrder != c.keepJoins {
				t.Errorf("KeepJoinOrder = %v, want %v", f.gotSess.KeepJoinOrder, c.keepJoins)
			}
		})
	}
}

// The rewrite is not the SHOW rewrite: a statement whose names were put in
// double quotes keeps the connection's sql_select_limit, and SHOW COLUMNS
// with a backtick-quoted table still becomes DESCRIBE as before.
func TestPlainPort_rewriteKeepsTheSelectLimitAndShowColumns(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("id", "INTEGER", json.Number("7"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	if _, err := h.HandleQuery("SET sql_select_limit = 50"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleQuery("SELECT * FROM `orders`"); err != nil {
		t.Fatal(err)
	}
	if f.gotStmt != `SELECT * FROM "orders"` || f.gotSess.SelectLimit != 50 {
		t.Errorf("got (%q, limit %d), want the rewritten statement under limit 50", f.gotStmt, f.gotSess.SelectLimit)
	}
	if _, err := h.HandleQuery("SHOW COLUMNS FROM `orders`"); err != nil {
		t.Fatal(err)
	}
	if f.gotStmt != `DESCRIBE "orders"` {
		t.Errorf("SHOW COLUMNS reached the copy as %q, want DESCRIBE \"orders\"", f.gotStmt)
	}
}

// What the port refused before it still refuses, read on the client's text:
// the rewrite comes after the refusals, never instead of them.
func TestPlainPort_refusalsStillComeFirst(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("id", "INTEGER", json.Number("7"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	_, err := h.HandleQuery("SELECT `a` DIV 2 FROM `t`")
	var me *mysql.MyError
	if err == nil || !errors.As(err, &me) || me.Code != mysql.ER_NOT_SUPPORTED_YET || !strings.Contains(me.Message, "DIV") {
		t.Fatalf("err = %v, want the DIV refusal", err)
	}
	if f.calls != 0 {
		t.Errorf("the copy ran a refused statement (%d calls)", f.calls)
	}
}

// A prepared statement on the port without routing is its template with the
// arguments written in, run as text: the names are rewritten the same way.
// Unlike the routed path (copyParts), the finished text, arguments included,
// is read MySQL's way here; that is safe because a backslash anywhere in a
// string stops the rewrite and the statement runs as written.
func TestPlainPort_preparedStatementNames(t *testing.T) {
	f := &fakeFreeSQL{res: oneCell("id", "INTEGER", json.Number("7"))}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	_, _, st, err := h.HandleStmtPrepare("SELECT `id` FROM `orders` WHERE `id` = ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(st, "", []any{int64(7)}); err != nil {
		t.Fatal(err)
	}
	if want := `SELECT "id" FROM "orders" WHERE "id" =  7 `; f.gotStmt != want {
		t.Errorf("copy got %q, want %q", f.gotStmt, want)
	}
}
