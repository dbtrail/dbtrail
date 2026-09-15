package reconstruct

import (
	"context"
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Both writers of the mydumper output get the .sql backup's disk check
// (#1614): the merge over a baseline and the binlog-only fallback.
func TestChunkSpace_reachesBothMydumperWriters(t *testing.T) {
	full := errors.New("disk full")
	refuse := func(string, int64) error { return full }

	t.Run("merge over a baseline", func(t *testing.T) {
		rep := &TableReport{Schema: "mydb", Table: "orders"}
		err := mergeBaselineIntoWriter(context.Background(), mergeInput{
			LocalBaselinePath: writeTestBaseline(t, [][]string{{"1", "new"}, {"2", "paid"}}),
			CreateTableSQL:    "-- schema",
			Schema:            "mydb",
			Table:             "orders",
			PKCols:            pkColsIntID(),
			Changes:           map[string]*query.ResultRow{},
			OutputDir:         t.TempDir(),
			ChunkSpace:        refuse,
		}, rep)
		if !errors.Is(err, full) {
			t.Fatalf("err = %v, want the disk refusal", err)
		}
	})
	t.Run("binlog-only fallback", func(t *testing.T) {
		rep := &TableReport{Schema: "mydb", Table: "orders"}
		changes := map[string]*query.ResultRow{
			pkStrForInt(1): {EventType: parser.EventInsert, PKValues: pkStrForInt(1), RowAfter: map[string]any{"id": float64(1), "status": "new"}},
		}
		err := writeBinlogOnlyChanges(t.TempDir(), "mydb", "orders", pkColsIntID(), []string{"id", "status"}, 0, refuse,
			binlogOnlySchemaPlaceholder("mydb", "orders"), changes, rep)
		if !errors.Is(err, full) {
			t.Fatalf("err = %v, want the disk refusal", err)
		}
	})
}
