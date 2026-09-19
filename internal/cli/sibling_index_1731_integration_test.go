//go:build integration

package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The shape #1731 is about: the daemon's own env file names the boot index,
// the events live in the per-source database the control plane provisioned,
// and every CLI command run with that env answers zero. Without the hint an
// operator concludes, mid-incident, that there is no history for the table.
func TestHintSiblingIndexes_namesTheDatabaseThatHoldsTheEvents(t *testing.T) {
	// The sibling: created first so it exists while the empty one is asked.
	sibDB, sibName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, sibDB)
	testutil.InsertEvent(t, sibDB, "binlog.000001", 100, 200,
		time.Now().UTC().Format("2006-01-02 15:04:05"), nil,
		"shop", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	// ANALYZE so information_schema carries a row estimate for the sibling;
	// the hint prints the count only when the catalogue has one, and a test
	// that never populated it would pass without ever exercising that arm.
	testutil.MustExec(t, sibDB, "ANALYZE TABLE `"+sibName+"`.binlog_events")

	emptyDB, emptyName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, emptyDB)

	// Asserted on the LISTING, not on the rendered top five: this runs against
	// a shared server that accumulates index databases from other tests, and
	// the line shows the fullest few. The rendering has its own test.
	got, err := siblingIndexes(context.Background(), emptyDB, emptyName)
	if err != nil {
		t.Fatalf("siblingIndexes: %v", err)
	}
	found := false
	for _, s := range got {
		if s.Name == sibName {
			found = true
			if s.Rows == 0 {
				t.Errorf("%s came back with no row estimate: the hint could not tell it from an empty sibling", sibName)
			}
		}
	}
	if !found {
		t.Fatalf("the database holding the events is not in the listing: %+v", got)
	}

	var out bytes.Buffer
	HintSiblingIndexes(context.Background(), emptyDB, emptyName, &out)
	line := out.String()
	for _, want := range []string{"holds no events", "--index-dsn"} {
		if !strings.Contains(line, want) {
			t.Errorf("the hint does not say %q:\n%s", want, line)
		}
	}
}

// The fullest sibling comes first: on a server that has accumulated index
// databases, naming them alphabetically buries the one holding the data.
func TestSiblingIndexes_fullestFirst(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	got, err := siblingIndexes(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("siblingIndexes: %v", err)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].Rows < got[i].Rows {
			t.Fatalf("listing is not fullest-first: %s (%d) before %s (%d)",
				got[i-1].Name, got[i-1].Rows, got[i].Name, got[i].Rows)
		}
	}
}

// An index that HAS events answered about real data: anything printed here
// would question a correct answer during an incident.
func TestHintSiblingIndexes_saysNothingWhenTheIndexHasEvents(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		time.Now().UTC().Format("2006-01-02 15:04:05"), nil,
		"shop", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))

	var out bytes.Buffer
	HintSiblingIndexes(context.Background(), db, dbName, &out)
	if out.Len() != 0 {
		t.Fatalf("an index holding events produced a hint:\n%s", out.String())
	}
}

// A database that is not an index at all (no binlog_events) is not something
// this hint understands, so it stays quiet rather than guessing.
func TestHintSiblingIndexes_saysNothingWithoutAnEventsTable(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	var out bytes.Buffer
	HintSiblingIndexes(context.Background(), db, dbName, &out)
	if out.Len() != 0 {
		t.Fatalf("a database with no events table produced a hint:\n%s", out.String())
	}
}
