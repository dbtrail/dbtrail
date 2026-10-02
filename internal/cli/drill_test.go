package cli

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func TestParseDrillTables(t *testing.T) {
	tables, schemas, err := parseDrillTables(" shop.orders, shop.users ,hr.people,shop.orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 3 || tables[0] != "shop.orders" || tables[2] != "hr.people" {
		t.Fatalf("tables = %v", tables)
	}
	if len(schemas) != 2 || schemas[0] != "shop" || schemas[1] != "hr" {
		t.Fatalf("schemas = %v", schemas)
	}
	// A table name may hold a dot (#2006): split at the first one, as the
	// snapshot and the reconstruct do.
	if tables, schemas, err := parseDrillTables("demo.order.items"); err != nil || tables[0] != "demo.order.items" || schemas[0] != "demo" {
		t.Fatalf("dotted table: %v %v %v", tables, schemas, err)
	}
	for _, bad := range []string{"", "noschema", ".t", "s.", "s.`t`"} {
		if _, _, err := parseDrillTables(bad); err == nil {
			t.Fatalf("entry %q must be rejected", bad)
		}
	}
}

func TestDrillTargetEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// First schema empty, second holds a table → refuse naming it.
	mock.ExpectQuery("information_schema.TABLES").WithArgs("shop").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}))
	mock.ExpectQuery("information_schema.TABLES").WithArgs("hr").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME"}).AddRow("salaries"))
	err = drillTargetEmpty(context.Background(), db, []string{"shop", "hr"})
	if err == nil || !strings.Contains(err.Error(), "hr.salaries") {
		t.Fatalf("non-empty target must be refused with the table named: %v", err)
	}
}

func TestDrillLoadTable_ordersSchemaBeforeChunks(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"shop.orders-schema.sql": "CREATE TABLE `orders` (id INT PRIMARY KEY)",
		"shop.orders.00000.sql":  "INSERT INTO `shop`.`orders` (`id`) VALUES\n(1),\n(2);\n",
		"shop.orders.00001.sql":  "INSERT INTO `shop`.`orders` (`id`) VALUES\n(3);\n",
		"metadata":               "started: now",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Ordered (sqlmock's default ordered matching IS the assertion here):
	// session setup, database, USE, schema file, then chunks in Files order
	// — and the metadata file is never applied.
	expectLoadSession(mock)
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `shop`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("USE `shop`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`VALUES \(1\), \(2\)`).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`VALUES \(3\)`).WillReturnResult(sqlmock.NewResult(0, 1))
	rep := &reconstruct.TableReport{Schema: "shop", Table: "orders",
		Files: []string{"metadata", "shop.orders-schema.sql", "shop.orders.00000.sql", "shop.orders.00001.sql"}}
	if err := drillLoadTable(context.Background(), db, dir, rep); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectLoadSession(mock sqlmock.Sqlmock) {
	mock.ExpectExec("SET NAMES binary").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("FOREIGN_KEY_CHECKS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("sql_mode").WillReturnResult(sqlmock.NewResult(0, 0))
}

// TestDrillTable_verdicts pins the verdict wiring itself: fail-by-default,
// the count comparison, and the binlog-only refusal — the halves a
// regression could quietly turn into a rubber stamp.
func TestDrillTable_verdicts(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"shop.orders-schema.sql": "CREATE TABLE `orders` (id INT PRIMARY KEY)",
		"shop.orders.00000.sql":  "INSERT INTO `shop`.`orders` (`id`) VALUES (1), (2), (3);\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rep := func() *reconstruct.TableReport {
		return &reconstruct.TableReport{Schema: "shop", Table: "orders", RowsWritten: 3,
			Files: []string{"shop.orders-schema.sql", "shop.orders.00000.sql"}}
	}
	newDB := func(t *testing.T, count int64) *sql.DB {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		expectLoadSession(mock)
		mock.ExpectExec("CREATE DATABASE").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("USE").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("VALUES").WillReturnResult(sqlmock.NewResult(0, 3))
		mock.ExpectQuery(`COUNT\(\*\)`).WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(count))
		return db
	}

	got := drillTable(context.Background(), newDB(t, 3), dir, rep())
	if got.Status != "pass" || got.RowsLoaded != 3 || got.Error != "" {
		t.Fatalf("matching count must pass: %+v", got)
	}

	got = drillTable(context.Background(), newDB(t, 2), dir, rep())
	if got.Status != "fail" || !strings.Contains(got.Error, "loaded 2 rows, dump wrote 3") {
		t.Fatalf("count mismatch must fail with both numbers: %+v", got)
	}

	// Binlog-only: refused up front — no statement ever reaches the target.
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := rep()
	r.BinlogOnly = true
	got = drillTable(context.Background(), db, dir, r)
	if got.Status != "fail" || !got.BinlogOnly || !strings.Contains(got.Error, "baseline") {
		t.Fatalf("binlog-only must fail without loading: %+v", got)
	}
}

func TestDrillReportExitError(t *testing.T) {
	r := &drillReport{Tables: []drillTableResult{
		{Schema: "s", Table: "a", Status: "pass"},
		{Schema: "s", Table: "b", Status: "pass"},
	}}
	if err := r.ExitError(); err != nil {
		t.Fatalf("all-pass must exit clean: %v", err)
	}
	r.Tables[1].Status = "fail"
	r.DumpDir = "/tmp/d"
	err := r.ExitError()
	if err == nil || !strings.Contains(err.Error(), "1 of 2") || !strings.Contains(err.Error(), "/tmp/d") {
		t.Fatalf("failure must name counts and the kept dump: %v", err)
	}
}
