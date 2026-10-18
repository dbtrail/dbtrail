package reconstruct

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/event"
)

// An inferred PostgreSQL DROP (#2441) committed somewhere in (low, high]. A
// snapshot taken inside that bracket may be from before the drop, so a
// reconstruct after it must refuse, although the row's detected_at (low) is
// before the snapshot. A bracket that ends at or before the snapshot does not
// refuse.
func TestCheckDestructiveDDL_inferredBracketOverlappingTheSnapshot(t *testing.T) {
	low := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	since := time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC) // snapshot inside the bracket
	until := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	marker := func(high time.Time) string {
		return event.InferredDDLPrefix + " oid=10; instant=at-or-before; low=0/1000@" + low.Format(time.RFC3339) +
			"; high=0/19DF9E8@" + high.Format(time.RFC3339) + " */ DROP TABLE public.t (its name now belongs to a new table)"
	}
	run := func(high time.Time) error {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT id, ddl_type, detected_at, binlog_file, binlog_pos").
			WithArgs(since, until, "public", "t").
			// The database's own comparison says "not after since": low is
			// before the snapshot.
			WillReturnRows(sqlmock.NewRows(ddlCols).AddRow(9, "DROP TABLE", low, "0/19DF9E8", 27130344, false, true, marker(high)))
		return CheckDestructiveDDL(context.Background(), db, "public", "t", DDLWindow{Since: since, Until: until})
	}

	err := run(time.Date(2026, 1, 1, 10, 20, 0, 0, time.UTC))
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("snapshot inside the bracket: err = %v, want a refusal", err)
	}
	t.Logf("refusal: %v", err)
	for _, want := range []string{"run after 2026-01-01T10:00:00Z and at or before 2026-01-01T10:20:00Z",
		"stream position 0/19DF9E8", "inferred from the change stream", "), may lie between the snapshot",
		"snapshot of the table after 2026-01-01T10:20:00Z"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q lacks %q", err, want)
		}
	}
	if strings.Contains(err.Error(), ":27130344") {
		t.Errorf("refusal prints the LSN as a MySQL file:pos: %v", err)
	}

	if err := run(time.Date(2026, 1, 1, 10, 4, 0, 0, time.UTC)); err != nil {
		t.Fatalf("bracket ending before the snapshot: err = %v, want none", err)
	}
}

// With no lower bound the change can have happened at any time up to its
// high end, so it overlaps every window that starts before that end, even
// one that ends long before it.
func TestCheckDestructiveDDL_inferredWithoutALowerBound(t *testing.T) {
	since := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	high := time.Date(2026, 1, 1, 12, 0, 0, 500000000, time.UTC)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := event.InferredDDLPrefix + " oid=10; instant=at-or-before; low=none; high=0/19DF9E8@" + high.Format(time.RFC3339Nano) + " */ DROP TABLE public.t"
	mock.ExpectQuery("SELECT id, ddl_type, detected_at, binlog_file, binlog_pos").
		WithArgs(since, until, "public", "t").
		// detected_at is the high end: the database says "not before until".
		WillReturnRows(sqlmock.NewRows(ddlCols).AddRow(9, "DROP TABLE", high, "0/19DF9E8", 27130344, true, false, q))
	err = CheckDestructiveDDL(context.Background(), db, "public", "t", DDLWindow{Since: since, Until: until})
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("err = %v, want a refusal", err)
	}
	// The requested point (10:30) is before the range ends (12:00): no
	// snapshot after 12:00 can rebuild it, and the advice must not say so.
	for _, want := range []string{"run at or before 2026-01-01T12:00:00Z", "may or may not have happened yet", "Pick a point after 2026-01-01T12:00:00Z"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q lacks %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "Take a new snapshot of the table after") {
		t.Errorf("refusal gives advice that cannot work: %v", err)
	}
	t.Logf("refusal: %v", err)
}

// The high end keeps its fraction of a second: a snapshot taken in the same
// second, before the change's high end, still refuses.
func TestCheckDestructiveDDL_inferredHighKeepsTheFraction(t *testing.T) {
	low := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	since := time.Date(2026, 1, 1, 10, 0, 0, 200000000, time.UTC)
	high := time.Date(2026, 1, 1, 10, 0, 0, 700000000, time.UTC)
	until := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	q := event.InferredDDLPrefix + " oid=10; instant=at-or-before; low=0/1@" + low.Format(time.RFC3339Nano) + "; high=0/2@" + high.Format(time.RFC3339Nano) + " */ DROP TABLE public.t"
	mock.ExpectQuery("SELECT id, ddl_type, detected_at, binlog_file, binlog_pos").
		WithArgs(since, until, "public", "t").
		WillReturnRows(sqlmock.NewRows(ddlCols).AddRow(9, "DROP TABLE", low, "0/2", 2, false, true, q))
	if err := CheckDestructiveDDL(context.Background(), db, "public", "t", DDLWindow{Since: since, Until: until}); !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("err = %v, want a refusal", err)
	}
}

// A table added back to the publication (#2441) left changes uncaptured
// inside its bracket: a window across it refuses, saying so. A first add
// marks no gap and does not refuse: the changes before it are before capture
// covered the table, as the changes before capture started.
func TestCheckDestructiveDDL_addedBackToThePublication(t *testing.T) {
	since := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	low := time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC)
	run := func(gap string) error {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		q := event.InferredDDLPrefix + " oid=10; " + gap + "instant=exact; at=2026-01-01T10:15:00Z; low=0/1000@" + low.Format(time.RFC3339) +
			"; high=0/19DF9E8@2026-01-01T10:20:00Z */ ALTER PUBLICATION ... ADD TABLE public.t"
		mock.ExpectQuery("ddl_type IN \\('TRUNCATE TABLE', 'DROP TABLE', 'RENAME TABLE', 'CREATE OR REPLACE TABLE', 'ALTER PUBLICATION'\\)").
			WithArgs(since, until, "public", "t").
			WillReturnRows(sqlmock.NewRows(ddlCols).AddRow(9, "ALTER PUBLICATION", low, "0/19DF9E8", 27130344, true, true, q))
		return CheckDestructiveDDL(context.Background(), db, "public", "t", DDLWindow{Since: since, Until: until})
	}
	err := run("gap=uncaptured; ")
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("added back: err = %v, want a refusal", err)
	}
	t.Logf("refusal: %v", err)
	for _, want := range []string{"ALTER PUBLICATION on public.t", "out of the publication", "never have been captured"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q lacks %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "keep the rows it removed") {
		t.Errorf("the refusal speaks of removed rows: %v", err)
	}
	if err := run(""); err != nil {
		t.Fatalf("a first add: err = %v, want none", err)
	}
	// A row whose marker cannot be read is not taken for a first add.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT id, ddl_type, detected_at").WithArgs(since, until, "public", "t").
		WillReturnRows(sqlmock.NewRows(ddlCols).AddRow(9, "ALTER PUBLICATION", low, "0/19DF9E8", 27130344, true, true,
			event.InferredDDLPrefix+" oid=10; gap=uncaptured; low=garbled */ ALTER PUBLICATION"))
	if err := CheckDestructiveDDL(context.Background(), db, "public", "t", DDLWindow{Since: since, Until: until}); !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("an unreadable marker: err = %v, want a refusal", err)
	}
}
