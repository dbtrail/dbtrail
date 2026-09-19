package reconstruct

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestCheckDestructiveDDL_truncateInWindowRefuses is the regression for #764:
// a TRUNCATE between the baseline snapshot and --at emits no row events, so
// without this check a reconstruct/_snapshot merge would silently resurrect
// every pre-truncate baseline row as if it still existed.
func TestCheckDestructiveDDL_truncateInWindowRefuses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	detectedAt := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)

	mock.ExpectQuery("SELECT ddl_type, detected_at FROM schema_changes").
		WithArgs("mydb", "orders", since, until).
		WillReturnRows(sqlmock.NewRows([]string{"ddl_type", "detected_at"}).
			AddRow("TRUNCATE TABLE", detectedAt))

	err = CheckDestructiveDDL(context.Background(), db, "mydb", "orders", since, until)
	if err == nil {
		t.Fatal("expected a refusal error for TRUNCATE TABLE in the window, got nil")
	}
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Errorf("error should wrap ErrDestructiveDDL, got: %v", err)
	}
	for _, want := range []string{"TRUNCATE TABLE", "mydb.orders", "2026-01-02T12:00:00Z"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error message %q should contain %q", err.Error(), want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestCheckDestructiveDDL_dropAndRenameAlsoRefuse covers DROP TABLE and
// RENAME TABLE, the other two DDL kinds that emit no row events.
func TestCheckDestructiveDDL_dropAndRenameAlsoRefuse(t *testing.T) {
	for _, ddlType := range []string{"DROP TABLE", "RENAME TABLE"} {
		t.Run(ddlType, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()

			since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			until := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
			detectedAt := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)

			mock.ExpectQuery("SELECT ddl_type, detected_at FROM schema_changes").
				WithArgs("mydb", "orders", since, until).
				WillReturnRows(sqlmock.NewRows([]string{"ddl_type", "detected_at"}).
					AddRow(ddlType, detectedAt))

			err = CheckDestructiveDDL(context.Background(), db, "mydb", "orders", since, until)
			if err == nil {
				t.Fatalf("expected a refusal error for %s in the window, got nil", ddlType)
			}
			if !errors.Is(err, ErrDestructiveDDL) {
				t.Errorf("error should wrap ErrDestructiveDDL, got: %v", err)
			}
		})
	}
}

// TestCheckDestructiveDDL_noneInWindowPasses verifies the common case: no
// destructive DDL on the table in (since, until] returns nil.
func TestCheckDestructiveDDL_noneInWindowPasses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery("SELECT ddl_type, detected_at FROM schema_changes").
		WithArgs("mydb", "orders", since, until).
		WillReturnRows(sqlmock.NewRows([]string{"ddl_type", "detected_at"}))

	if err := CheckDestructiveDDL(context.Background(), db, "mydb", "orders", since, until); err != nil {
		t.Errorf("expected nil error when no destructive DDL is in the window, got: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestCheckDestructiveDDL_missingTableIsNotFatal verifies that a pre-DDL-
// tracking index (no schema_changes table) degrades to "nothing to check"
// rather than failing the whole reconstruct run.
func TestCheckDestructiveDDL_missingTableIsNotFatal(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery("SELECT ddl_type, detected_at FROM schema_changes").
		WithArgs("mydb", "orders", since, until).
		WillReturnError(&mysqldriver.MySQLError{Number: 1146, Message: "Table 'bintrail_index.schema_changes' doesn't exist"})

	if err := CheckDestructiveDDL(context.Background(), db, "mydb", "orders", since, until); err != nil {
		t.Errorf("expected nil error for a missing schema_changes table, got: %v", err)
	}
}

// A tablespace that is gone (1932, "Table ... doesn't exist IN ENGINE") is a
// BROKEN check, not an absent one, and its message carries the same words as
// 1146. Reading it as "no destructive DDL" would answer a damaged index with
// a clean verdict — and on the binlog-only path that verdict is all that
// stands between a TRUNCATE and rows coming back.
func TestCheckDestructiveDDL_brokenTablespaceIsNotAMissingTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT ddl_type, detected_at FROM schema_changes").
		WithArgs("mydb", "orders", since, until).
		WillReturnError(&mysqldriver.MySQLError{Number: 1932, Message: "Table 'bintrail_index.schema_changes' doesn't exist in engine"})

	err = CheckDestructiveDDL(context.Background(), db, "mydb", "orders", since, until)
	if err == nil {
		t.Fatal("a missing tablespace was answered as \"nothing to check\"")
	}
	if errors.Is(err, errSchemaChangesMissing) {
		t.Errorf("1932 was graded as a missing table: %v", err)
	}
}
