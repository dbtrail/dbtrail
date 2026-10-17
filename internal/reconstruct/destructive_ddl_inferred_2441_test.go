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
		"stream position 0/19DF9E8", "inferred from the change stream", "snapshot of the table after 2026-01-01T10:20:00Z"} {
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
