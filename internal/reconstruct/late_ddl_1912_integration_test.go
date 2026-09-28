//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A TRUNCATE that reaches the index late (#1912).
//
// detected_at is when the statement RAN on the source. With capture behind,
// the row is written after a refresh already published, and its time is before
// that refresh's. A check that places the statement by time alone never sees
// it: the first refresh ran before the row existed, and every later one looks
// from its predecessor's time onward.
//
// The timeline, all on the source's binlog:
//
//	  4      the first snapshot's anchor (ids 1, 2, 3)
//	100-200  UPDATE id=1, ran at +10s       indexed before the first refresh
//	200-300  TRUNCATE orders, ran at +20s   indexed AFTER the first refresh
//	300-400  INSERT id=9, ran at +25s       indexed after the first refresh
//
// The TRUNCATE's row is written by the writer capture uses, from an event
// shaped like the one the parser emits: the statement's execution time, the
// file, and its END position.
func TestReconstructParquet_aTruncateIndexedLateIsStillReported(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	dsn := testutil.BaseDSN() + "/" + dbName
	const schema = "shop"

	// One hour for the whole timeline, as in the #1169 tests: the planner
	// reads an hour with no live rows as a coverage gap.
	base := time.Now().UTC().Truncate(time.Hour)
	first := base.Add(30 * time.Second)
	second := base.Add(60 * time.Second)
	ts := func(d time.Duration) string { return base.Add(d).Format("2006-01-02 15:04:05") }

	seedOrdersSnapshot(t, db, schema, base)
	root := t.TempDir()
	seedSourceBaseline(t, root, base, schema)

	refresh := func(at time.Time) error {
		_, err := reconstruct.ReconstructTables(ctx, reconstruct.FullTableConfig{
			IndexDSN:     dsn,
			BaselineSrc:  root,
			Tables:       []string{schema + ".orders"},
			At:           at,
			OutputDir:    root,
			OutputFormat: reconstruct.OutputFormatParquet,
		})
		return err
	}

	// Capture is behind: only the UPDATE has reached the index.
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts(10*time.Second), nil,
		schema, "orders", 2, "1", nil, nil, []byte(`{"id":1,"status":"A"}`))

	if err := refresh(first); err != nil {
		t.Fatalf("the first refresh: %v", err)
	}
	mid, midTime, _, err := reconstruct.FindBaseline(ctx, root, schema, "orders", second)
	if err != nil {
		t.Fatalf("FindBaseline after the first refresh: %v", err)
	}
	if !midTime.Equal(first) {
		t.Fatalf("the second refresh would start from the %s snapshot, not the one just published (%s)", midTime, first)
	}
	if got, want := readOrders(t, mid), []string{"1=A", "2=paid", "3=shipped"}; !equalStrings(got, want) {
		t.Fatalf("first snapshot = %v, want %v", got, want)
	}

	// Capture catches up. The TRUNCATE ran 10 seconds BEFORE the first
	// refresh's time and is recorded only now.
	if err := indexer.InsertSchemaChange(db, event.Event{
		BinlogFile: "binlog.000001",
		EndPos:     300,
		Timestamp:  base.Add(20 * time.Second),
		Schema:     schema,
		Table:      "orders",
		EventType:  event.EventDDL,
		DDLType:    event.DDLTruncateTable,
		DDLQuery:   "TRUNCATE TABLE orders",
	}, nil); err != nil {
		t.Fatalf("InsertSchemaChange: %v", err)
	}
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts(25*time.Second), nil,
		schema, "orders", 1, "9", nil, nil, []byte(`{"id":9,"status":"after"}`))

	err = refresh(second)
	if err == nil {
		out, _, _, ferr := reconstruct.FindBaseline(ctx, root, schema, "orders", second.Add(time.Second))
		if ferr != nil {
			t.Fatalf("FindBaseline after the second refresh: %v", ferr)
		}
		t.Fatalf("the refresh published over a TRUNCATE and said nothing. The snapshot holds %v: "+
			"ids 1, 2 and 3 were removed by the TRUNCATE and only id 9 exists on the source",
			readOrders(t, out))
	}
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("refused for another reason: %v", err)
	}
	// What ran, on which table, when, where in the binlog.
	for _, want := range []string{"TRUNCATE TABLE", "shop.orders", base.Add(20 * time.Second).Format(time.RFC3339), "binlog.000001:300"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}
