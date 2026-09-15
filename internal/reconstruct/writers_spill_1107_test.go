package reconstruct

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Both writers run the #602 added-column guard on a fold whose changes went to
// disk (#1107). The fold's own map is empty then, so a guard left on that map
// alone would pass every spilled table.
func TestSpilledMerge_writersRefuseAColumnAddedAfterTheBaseline(t *testing.T) {
	added := func() map[string]*query.ResultRow {
		pk := pkStrForInt(1)
		return map[string]*query.ResultRow{pk: {EventType: event.EventUpdate, PKValues: pk,
			RowAfter: map[string]any{"id": json.Number("1"), "status": "x", "added_later": "y"}}}
	}
	check := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, ErrSchemaChanged) || !strings.Contains(err.Error(), "added_later") {
			t.Fatalf("err = %v, want the added-column refusal naming the column", err)
		}
	}

	t.Run("mydumper", func(t *testing.T) {
		err := mergeBaselineIntoWriter(t.Context(), mergeInput{
			LocalBaselinePath: writeTestBaseline(t, [][]string{{"1", "a"}}),
			CreateTableSQL:    "-- schema",
			Schema:            "mydb",
			Table:             "orders",
			PKCols:            pkColsIntID(),
			Changes:           map[string]*query.ResultRow{},
			Spill:             spillOf(t, 1000, added()),
			OutputDir:         t.TempDir(),
		}, &TableReport{Schema: "mydb", Table: "orders"})
		check(t, err)
	})
	t.Run("parquet", func(t *testing.T) {
		rows, nulls := zooRows()
		src := writeZooBaseline(t, rows, nulls)
		srcMeta, err := baseline.ReadParquetMetadata(src)
		if err != nil {
			t.Fatal(err)
		}
		at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
		err = mergeBaselineIntoParquet(t.Context(), mergeInput{
			LocalBaselinePath: src,
			CreateTableSQL:    zooCreateTableSQL,
			Schema:            "mydb",
			Table:             "orders",
			PKCols:            pkColsIntID(),
			Changes:           map[string]*query.ResultRow{},
			Spill:             spillOf(t, 1000, added()),
			SnapshotDir:       t.TempDir(),
			SnapshotAt:        at,
			SourceBaseline:    baselineMeta{Path: src, Time: at.Add(-time.Hour), Metadata: srcMeta},
		}, &TableReport{Schema: "mydb", Table: "orders"})
		check(t, err)
	})
}
