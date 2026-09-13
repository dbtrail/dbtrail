package recovery

import (
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// unscopedEdgeQuery matches CascadeConstraintsInIndex's SQL only when no
// schema filter was appended (sqlmock collapses whitespace before matching).
const unscopedEdgeQuery = `update_rule IN \('CASCADE', 'SET NULL'\)\) ORDER BY schema_name`

var fkEdgeCols = []string{"schema_name", "table_name", "column_name", "referenced_schema_name", "referenced_table_name", "delete_rule", "update_rule"}

// The list under "this table has children (...)" must be true of THIS table:
// an unrelated cascade in the same schema is not named, and a child in
// another schema (#833) is. The parent flags come from the same edges.
func TestDetectCascade_namesTheTargetsOwnChildrenAcrossSchemas(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(true))
	// The pattern pins the INDEX-WIDE read (nothing between the rule filter
	// and ORDER BY): scoped by the child schema the query carries "AND
	// schema_name IN (?)" there, and a real index would never return
	// billing.invoices. sqlmock's WithArgs() with no arguments does not pin
	// anything (a nil argument list means "do not check").
	mock.ExpectQuery(unscopedEdgeQuery).WillReturnRows(sqlmock.NewRows(fkEdgeCols).
		AddRow("shop", "order_items", "order_id", "shop", "orders", "CASCADE", "NO ACTION").
		AddRow("billing", "invoices", "customer_id", "shop", "customers", "CASCADE", "NO ACTION").
		AddRow("billing", "invoices", "account_id", "shop", "customers", "NO ACTION", "SET NULL").
		AddRow("crm", "notes", "customer_id", "crm", "customers", "CASCADE", "NO ACTION"))

	adv, err := DetectCascade(db, "shop", "customers")
	if err != nil {
		t.Fatal(err)
	}
	if len(adv.ChildTables) != 1 || adv.ChildTables[0] != "billing.invoices" {
		t.Errorf("children of shop.customers = %v, want [billing.invoices] (not shop.order_items, not crm.customers' child)", adv.ChildTables)
	}
	if !adv.ParentOnDelete || !adv.ParentOnUpdate {
		t.Errorf("flags per action: delete=%v update=%v, want both (one edge each)", adv.ParentOnDelete, adv.ParentOnUpdate)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A table without a schema (MCP does not require one) still matches by table
// name; with only a schema, the edges whose PARENT lives in it are listed,
// which names a child in another schema (#833) and leaves out a cascade
// whose parent this window could never hold.
func TestDetectCascade_scopes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(true))
	mock.ExpectQuery(unscopedEdgeQuery).WillReturnRows(sqlmock.NewRows(fkEdgeCols).
		AddRow("shop", "order_items", "order_id", "shop", "orders", "CASCADE", "NO ACTION").
		AddRow("crm", "notes", "customer_id", "crm", "customers", "SET NULL", "NO ACTION"))
	adv, err := DetectCascade(db, "", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(adv.ChildTables) != 1 || adv.ChildTables[0] != "shop.order_items" || !adv.ParentOnDelete || adv.ParentOnUpdate {
		t.Errorf("table without schema: %+v", adv)
	}

	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(true))
	mock.ExpectQuery(unscopedEdgeQuery).WillReturnRows(sqlmock.NewRows(fkEdgeCols).
		AddRow("shop", "order_items", "order_id", "shop", "orders", "CASCADE", "NO ACTION").
		AddRow("shop", "order_items", "coupon_id", "shop", "coupons", "SET NULL", "NO ACTION").
		AddRow("shop", "shipments", "order_id", "shop", "orders", "CASCADE", "CASCADE").
		AddRow("shop", "audit_rows", "actor_id", "crm", "users", "CASCADE", "NO ACTION").
		AddRow("billing", "invoices", "customer_id", "shop", "customers", "CASCADE", "NO ACTION"))
	adv, err = DetectCascade(db, "shop", "")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(adv.ChildTables, ",") != "shop.order_items,shop.shipments,billing.invoices" || adv.ParentOnDelete || adv.ParentOnUpdate {
		t.Errorf("schema scope must list children of PARENTS in shop (billing.invoices in, shop.audit_rows out): %+v", adv)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Names compare the way the index compares them, case-insensitively: the
// rows were fetched for "Orders" and the edge is stored as "orders".
func TestDetectCascade_matchesNamesCaseInsensitively(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(true))
	mock.ExpectQuery(unscopedEdgeQuery).WillReturnRows(sqlmock.NewRows(fkEdgeCols).
		AddRow("shop", "order_items", "order_id", "shop", "orders", "CASCADE", "NO ACTION"))
	adv, err := DetectCascade(db, "Shop", "Orders")
	if err != nil {
		t.Fatal(err)
	}
	if !adv.ParentOnDelete || len(adv.ChildTables) != 1 {
		t.Fatalf("a differently-cased spelling lost the advisory: %+v", adv)
	}
	rows := []query.ResultRow{{TableName: "orders", EventType: event.EventDelete}}
	if !adv.AppliesTo(rows, "Orders") {
		t.Error("AppliesTo compared the row's table name by bytes")
	}
}

// An index without the fk_constraints table answers "no cascade" in one
// query and no error.
func TestDetectCascade_noFKTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(false))
	adv, err := DetectCascade(db, "app", "orders")
	if err != nil || !adv.Empty() {
		t.Errorf("adv=%+v err=%v", adv, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// AppliesTo is the anti-cry-wolf gate; its arms are pinned one by one, since
// the surface tests reach only two of them.
func TestCascadeAdvisory_AppliesTo(t *testing.T) {
	del := func(tbl string) query.ResultRow { return query.ResultRow{TableName: tbl, EventType: event.EventDelete} }
	upd := func(tbl string) query.ResultRow { return query.ResultRow{TableName: tbl, EventType: event.EventUpdate} }
	ins := func(tbl string) query.ResultRow { return query.ResultRow{TableName: tbl, EventType: event.EventInsert} }
	children := CascadeAdvisory{ChildTables: []string{"app.order_items"}}
	onDelete := CascadeAdvisory{ParentOnDelete: true}
	onUpdate := CascadeAdvisory{ParentOnUpdate: true}
	cases := []struct {
		name  string
		adv   CascadeAdvisory
		rows  []query.ResultRow
		table string
		want  bool
	}{
		{"empty advisory never applies", CascadeAdvisory{}, []query.ResultRow{del("orders")}, "orders", false},
		{"named parent, DELETE, on-delete rule", onDelete, []query.ResultRow{del("orders")}, "orders", true},
		{"named parent, DELETE, on-update rule only (#1002: rules never merge)", onUpdate, []query.ResultRow{del("orders")}, "orders", false},
		{"named parent, UPDATE, on-update rule", onUpdate, []query.ResultRow{upd("orders")}, "orders", true},
		{"named parent, UPDATE, on-delete rule only", onDelete, []query.ResultRow{upd("orders")}, "orders", false},
		{"named parent, INSERT never cascades", onDelete, []query.ResultRow{ins("orders")}, "orders", false},
		{"named table that is a CHILD, not a parent: undoing it cascades nothing", children, []query.ResultRow{del("order_items")}, "order_items", false},
		{"named parent, rows of another table only", onDelete, []query.ResultRow{del("shops")}, "orders", false},
		{"table-less, DELETE anywhere with children in scope (coarse on purpose)", children, []query.ResultRow{del("shops")}, "", true},
		{"table-less, UPDATE anywhere with children in scope", children, []query.ResultRow{upd("shops")}, "", true},
		{"table-less, INSERT-only window", children, []query.ResultRow{ins("orders"), ins("shops")}, "", false},
		{"table-less, parent flags without children (no table named, so none set)", onDelete, []query.ResultRow{del("orders")}, "", false},
	}
	for _, c := range cases {
		if got := c.adv.AppliesTo(c.rows, c.table); got != c.want {
			t.Errorf("%s: AppliesTo = %v, want %v", c.name, got, c.want)
		}
	}
}

// A parent with no readable child list (a newer index could hide one) still
// renders a remedy, and never an empty parenthesis.
func TestCascadeAdvisory_WarningWithoutChildList(t *testing.T) {
	w := CascadeAdvisory{ParentOnDelete: true}.Warning("orders", "X")
	if !strings.HasPrefix(w, "this table has") || !strings.Contains(w, "Use X to reconstruct") || strings.Contains(w, "()") {
		t.Errorf("warning without a child list: %s", w)
	}
	w = CascadeAdvisory{ChildTables: []string{"a.b", "a.c"}}.Warning("", "Y")
	if !strings.HasPrefix(w, "tables in this window have") || !strings.Contains(w, "(a.b, a.c)") || !strings.Contains(w, "Use Y to reconstruct") {
		t.Errorf("warning for a table-less reversal names \"this table\" or drops the list: %s", w)
	}
	if !RowsCanCascade([]query.ResultRow{{EventType: event.EventUpdate}}) || RowsCanCascade([]query.ResultRow{{EventType: event.EventInsert}}) {
		t.Error("RowsCanCascade: an UPDATE can cascade, an INSERT cannot")
	}
}
