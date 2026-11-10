package reconstruct

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestAlterChangesRowsOf: which table an ALTER TABLE that changes rows with
// no row events (#2632) counts for. rowSchema/rowTable are what
// schema_changes recorded the statement under.
func TestAlterChangesRowsOf(t *testing.T) {
	for _, tc := range []struct {
		name, text, rowSchema, rowTable, schema, table string
		wantOp                                         string
	}{
		{"its own table", "ALTER TABLE orders DROP PARTITION p0", "shop", "orders", "shop", "orders", "DROP PARTITION"},
		{"its own table, other case", "ALTER TABLE ORDERS DROP PARTITION p0", "SHOP", "ORDERS", "shop", "orders", "DROP PARTITION"},
		{"a row with no schema matches any", "ALTER TABLE orders TRUNCATE PARTITION ALL", "", "orders", "shop", "orders", "TRUNCATE PARTITION"},
		{"another table's drop", "ALTER TABLE events DROP PARTITION p0", "shop", "events", "shop", "orders", ""},
		{"the same name in another schema", "ALTER TABLE orders DROP PARTITION p0", "shop2", "orders", "shop", "orders", ""},
		{"a clause that keeps the rows", "ALTER TABLE orders ADD PARTITION (PARTITION p9 VALUES LESS THAN (10))", "shop", "orders", "shop", "orders", ""},
		{"exchange: the table it names", "ALTER TABLE orders EXCHANGE PARTITION p0 WITH TABLE orders_old", "shop", "orders", "shop", "orders_old", "EXCHANGE PARTITION"},
		{"exchange: the partitioned table", "ALTER TABLE orders EXCHANGE PARTITION p0 WITH TABLE orders_old", "shop", "orders", "shop", "orders", "EXCHANGE PARTITION"},
		{"exchange: a name that only starts the same", "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE orders_archive", "shop", "t", "shop", "orders", ""},
		{"exchange: qualified other table", "ALTER TABLE orders EXCHANGE PARTITION p0 WITH TABLE archive.old", "shop", "orders", "archive", "old", "EXCHANGE PARTITION"},
		{"exchange: qualified other table, other schema", "ALTER TABLE orders EXCHANGE PARTITION p0 WITH TABLE archive.old", "shop", "orders", "shop", "old", ""},
		// The ALTER named its schema, so the session's default database,
		// which an unqualified name belongs to, is not on record.
		{"exchange: unqualified name after a qualified ALTER matches any schema", "ALTER TABLE shop.orders EXCHANGE PARTITION p0 WITH TABLE old", "shop", "orders", "archive", "old", "EXCHANGE PARTITION"},
		{"exchange: unqualified name after an unqualified ALTER is in its schema", "ALTER TABLE orders EXCHANGE PARTITION p0 WITH TABLE old", "shop", "orders", "archive", "old", ""},
		// The capture parser records `a``b` as a: the name read from the
		// text counts too.
		{"a doubled backtick the row lost", "ALTER TABLE `a``b` DROP PARTITION p0", "shop", "a", "shop", "a`b", "DROP PARTITION"},
		{"the name read from the text, another schema", "ALTER TABLE other.`a``b` DROP PARTITION p0", "shop", "a", "shop", "a`b", ""},
		{"MariaDB convert table into a partition", "ALTER TABLE orders CONVERT TABLE staging TO PARTITION p9 VALUES LESS THAN (10)", "shop", "orders", "shop", "staging", "CONVERT TABLE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op, ok := alterChangesRowsOf(tc.text, tc.rowSchema, tc.rowTable, tc.schema, tc.table)
			if op != tc.wantOp || ok != (tc.wantOp != "") {
				t.Errorf("alterChangesRowsOf = %q, %v, want %q", op, ok, tc.wantOp)
			}
		})
	}
}

// TestCheckDestructiveDDL_dropPartitionRefuses: a DROP PARTITION between the
// snapshot and the requested point removes rows with no row events, and the
// columns stay the same, so the shape check lets it through. It refuses,
// naming the clause, and an ADD PARTITION in the same window does not.
func TestCheckDestructiveDDL_dropPartitionRefuses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	at := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT id, ddl_type, detected_at, binlog_file, binlog_pos").
		WithArgs(since, until, "shop", "orders").
		WillReturnRows(sqlmock.NewRows(ddlCols).
			AddRow(1, "ALTER TABLE", at.Add(-time.Hour), "binlog.000001", 300, true, true, "", "shop", "orders",
				"ALTER TABLE orders ADD PARTITION (PARTITION p9 VALUES LESS THAN (2030))").
			AddRow(2, "ALTER TABLE", at.Add(-time.Minute), "binlog.000001", 350, true, true, "", "shop", "events",
				"ALTER TABLE events DROP PARTITION p2020").
			AddRow(3, "ALTER TABLE", at, "binlog.000001", 400, true, true, "", "shop", "orders",
				"ALTER TABLE orders DROP PARTITION p2020"))
	err = CheckDestructiveDDL(context.Background(), db, "shop", "orders", DDLWindow{Since: since, Until: until})
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL", err)
	}
	for _, want := range []string{"ALTER TABLE ... DROP PARTITION on shop.orders", "2026-01-02T12:00:00Z", "binlog.000001:400",
		"row 3 of schema_changes", "changed the table's rows without writing row changes", "after the alter table ... drop partition"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q should contain %q", err, want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestFindDestructiveDDL_exchangeFindsTheOtherTable: an EXCHANGE PARTITION is
// recorded under the partitioned table alone, and the table after WITH TABLE
// changed as much; verify's lookup finds it from that table.
func TestFindDestructiveDDL_exchangeFindsTheOtherTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	at := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT id, ddl_type, detected_at, binlog_file, binlog_pos").
		WithArgs(since, until, "shop", "orders_old").
		WillReturnRows(sqlmock.NewRows(ddlCols).
			AddRow(7, "ALTER TABLE", at, "binlog.000001", 400, true, true, "", "shop", "orders",
				"ALTER TABLE orders EXCHANGE PARTITION p2020 WITH TABLE orders_old"))
	typ, detectedAt, found, err := FindDestructiveDDL(context.Background(), db, "shop", "orders_old", since, until)
	if err != nil || !found || typ != "ALTER TABLE ... EXCHANGE PARTITION" || !detectedAt.Equal(at) {
		t.Errorf("FindDestructiveDDL = %q, %v, %v, %v; want the exchange at %v", typ, detectedAt, found, err, at)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestBinlogOnlyDestructiveDDLErr_partition: the binlog-only refusal says
// what an ALTER clause did, not that removed rows would come back, which an
// EXCHANGE or IMPORT need not have done.
func TestBinlogOnlyDestructiveDDLErr_partition(t *testing.T) {
	at := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	err := binlogOnlyDestructiveDDLErr("shop", "orders", "ALTER TABLE ... EXCHANGE PARTITION", at, at.Add(-time.Hour))
	if !errors.Is(err, ErrDestructiveDDL) || !strings.Contains(err.Error(), "the output would not match the table after it") {
		t.Errorf("err = %v", err)
	}
	err = binlogOnlyDestructiveDDLErr("shop", "orders", "TRUNCATE TABLE", at, at.Add(-time.Hour))
	if !strings.Contains(err.Error(), "the rows it removed would come back") {
		t.Errorf("a TRUNCATE keeps its wording: %v", err)
	}
}
