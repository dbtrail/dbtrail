package reconstruct

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestReconstruct602_columnAddedAfterBaselineFailsLoud is the regression for
// #602. A column ADDed to the source table after the baseline snapshot lives
// only in the delta events' row_after (the baseline Parquet predates it). The
// mydumper writer projects every row onto the baseline column set, so that
// column's value used to be dropped silently from the dump. The fix refuses
// the run loudly instead — and must do so BEFORE writing any chunk file, so
// no partial output is left on disk.
func TestReconstruct602_columnAddedAfterBaselineFailsLoud(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
	})
	outDir := t.TempDir()

	// id=2 is UPDATEd after a `note` column was ADDed. The event row_after
	// carries id, status AND note. note ∉ baseline columns (id, status).
	changes := map[string]*query.ResultRow{
		pkStrForInt(2): {
			EventType: parser.EventUpdate,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "paid"},
			RowAfter:  map[string]any{"id": float64(2), "status": "shipped", "note": "gift-wrap"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)

	if err == nil {
		t.Fatalf("expected a fail-loud error for the post-baseline 'note' column, got nil")
	}
	if !strings.Contains(err.Error(), "note") {
		t.Errorf("error should name the dropped column %q, got: %v", "note", err)
	}

	// No partial output may be left on disk: the guard fires before the writer
	// opens, so the output dir must contain no .sql chunk files.
	entries, derr := os.ReadDir(outDir)
	if derr != nil {
		t.Fatalf("read output dir: %v", derr)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			t.Errorf("partial output left on disk after failed run: %s", e.Name())
		}
	}
}

// TestPostBaselineColumns covers the pure detection helper directly.
func TestPostBaselineColumns(t *testing.T) {
	baseCols := []string{"id", "status"}

	cases := []struct {
		name    string
		changes map[string]*query.ResultRow
		want    []string
	}{
		{
			name:    "no events",
			changes: map[string]*query.ResultRow{},
			want:    nil,
		},
		{
			name: "no drift",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventUpdate, RowAfter: map[string]any{"id": 2, "status": "x"}},
			},
			want: nil,
		},
		{
			name: "one added column",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventUpdate, RowAfter: map[string]any{"id": 2, "status": "x", "note": "n"}},
			},
			want: []string{"note"},
		},
		{
			name: "multiple added columns sorted and deduped across events",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventUpdate, RowAfter: map[string]any{"id": 2, "zeta": "z", "note": "n"}},
				"3": {EventType: event.EventInsert, RowAfter: map[string]any{"id": 3, "note": "n2", "alpha": "a"}},
			},
			want: []string{"alpha", "note", "zeta"},
		},
		{
			name: "DELETE events ignored (no row_after)",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventDelete, RowBefore: map[string]any{"id": 2, "ghost": "g"}},
			},
			want: nil,
		},
		{
			name: "nil row_after ignored",
			changes: map[string]*query.ResultRow{
				"2": {EventType: event.EventInsert, RowAfter: nil},
			},
			want: nil,
		},
		{
			name: "nil event entry ignored",
			changes: map[string]*query.ResultRow{
				"2": nil,
				"3": {EventType: event.EventUpdate, RowAfter: map[string]any{"id": 3, "note": "n"}},
			},
			want: []string{"note"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := postBaselineColumns(c.changes, baseCols)
			if len(got) != len(c.want) {
				t.Fatalf("postBaselineColumns = %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("postBaselineColumns = %v, want %v", got, c.want)
				}
			}
		})
	}
}

// TestReconstruct1624_generatedColumnIsNotAPostBaselineColumn is the regression
// for #1624. A STORED generated column is absent from the baseline on purpose
// (mydumper leaves it out) while every ROW image carries it, so the #602 guard
// used to refuse the table as if the column had been ADDed after the snapshot.
// With the CREATE TABLE declaring it generated, the merge must succeed and the
// emitted rows must not carry the column (the server recomputes it on load).
func TestReconstruct1624_generatedColumnIsNotAPostBaselineColumn(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
	})
	outDir := t.TempDir()
	// Built per call: the merge consumes the change map as it folds.
	changes := func() map[string]*query.ResultRow {
		return map[string]*query.ResultRow{
			pkStrForInt(2): {
				EventType: parser.EventUpdate,
				PKValues:  pkStrForInt(2),
				RowBefore: map[string]any{"id": float64(2), "status": "paid"},
				RowAfter:  map[string]any{"id": float64(2), "status": "shipped", "line_total": "90.00"},
			},
		}
	}
	create := "CREATE TABLE `orders` (\n" +
		"  `id` int unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `status` varchar(20) NOT NULL DEFAULT 'new',\n" +
		"  `line_total` decimal(10,2) GENERATED ALWAYS AS ((`quantity` * `unit_price`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"
	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    create,
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes(),
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("a STORED generated column must not be treated as a post-baseline column: %v", err)
	}
	entries, derr := os.ReadDir(outDir)
	if derr != nil {
		t.Fatalf("read output dir: %v", derr)
	}
	var sawSQL bool
	for _, e := range entries {
		// Data chunks only: the -schema.sql file carries the CREATE TABLE, which
		// names the generated column on purpose.
		if !strings.HasSuffix(e.Name(), ".sql") || strings.HasSuffix(e.Name(), "-schema.sql") {
			continue
		}
		sawSQL = true
		b, rerr := os.ReadFile(outDir + "/" + e.Name())
		if rerr != nil {
			t.Fatalf("read %s: %v", e.Name(), rerr)
		}
		if strings.Contains(string(b), "line_total") {
			t.Errorf("%s: the generated column must be left out of the emitted rows", e.Name())
		}
		if !strings.Contains(string(b), "shipped") {
			t.Errorf("%s: the folded UPDATE must reach the output", e.Name())
		}
	}
	if !sawSQL {
		t.Fatalf("no .sql chunk written")
	}

	// The footer says generated but the schema snapshot of today says plain:
	// the column was converted after the baseline (MODIFY on a STORED
	// generated column), so the baseline holds no value for it. Dropping it
	// would lose every post-ALTER value, so this refuses and names the column.
	err = mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath, CreateTableSQL: create, Schema: "mydb", Table: "orders",
		PKCols: pkColsIntID(), Changes: changes(), OutputDir: t.TempDir(),
		CurrentGenerated: map[string]bool{"id": false, "status": false, "line_total": false},
	}, &TableReport{})
	if err == nil || !strings.Contains(err.Error(), "line_total") || !strings.Contains(err.Error(), "plain column now") {
		t.Fatalf("a generated column turned plain after the baseline must refuse and say so, got: %v", err)
	}
	// The snapshot agreeing (still generated) or not knowing the column at all
	// keeps the lossless drop.
	for _, cur := range []map[string]bool{{"line_total": true}, {"id": false}, nil} {
		if err := mergeBaselineIntoWriter(context.Background(), mergeInput{
			LocalBaselinePath: baselinePath, CreateTableSQL: create, Schema: "mydb", Table: "orders",
			PKCols: pkColsIntID(), Changes: changes(), OutputDir: t.TempDir(), CurrentGenerated: cur,
		}, &TableReport{}); err != nil {
			t.Fatalf("CurrentGenerated=%v must not refuse: %v", cur, err)
		}
	}

	// A column that is NOT generated still fails loud, so the #602 guard has
	// lost nothing: same shape, the CREATE TABLE declares line_total as a
	// plain column.
	plain := strings.Replace(create, "GENERATED ALWAYS AS ((`quantity` * `unit_price`)) STORED", "NOT NULL DEFAULT '0.00'", 1)
	err = mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath, CreateTableSQL: plain, Schema: "mydb", Table: "orders",
		PKCols: pkColsIntID(), Changes: changes(), OutputDir: t.TempDir(),
	}, &TableReport{})
	if err == nil || !strings.Contains(err.Error(), "line_total") {
		t.Fatalf("a plain column absent from the baseline must still refuse and name it, got: %v", err)
	}
}

// TestGeneratedColumnsIn pins the set the guard excludes: STORED, VIRTUAL and
// MariaDB's short PERSISTENT form are found, an expression DEFAULT is not, a
// COMMENT that mentions the words does not promote its column, a period
// column is found, and a column-shaped line past the first index line is
// not read.
func TestGeneratedColumnsIn(t *testing.T) {
	sql := "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `total` decimal(10,2) GENERATED ALWAYS AS ((`q` * `p`)) STORED,\n" +
		"  `upper_name` varchar(50) GENERATED ALWAYS AS (upper(`name`)) VIRTUAL,\n" +
		"  `made_at` datetime DEFAULT (now()),\n" +
		"  `note` varchar(20) DEFAULT NULL COMMENT 'not GENERATED ALWAYS AS anything',\n" +
		"  `short_form` int AS (`id` * 2) PERSISTENT,\n" +
		"  `row_end` timestamp(6) GENERATED ALWAYS AS ROW END,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  `ghost` int AS (`id`) STORED,\n" +
		"  KEY `k` (`id`)\n) ENGINE=InnoDB"
	got := generatedColumnsIn(sql)
	want := []string{"total", "upper_name", "short_form", "row_end"}
	if len(got) != len(want) {
		t.Fatalf("generatedColumnsIn = %v, want exactly %v", got, want)
	}
	for _, w := range want {
		if _, ok := got[w]; !ok {
			t.Errorf("missing %q in %v", w, got)
		}
	}
	for _, s := range []string{"", "-- test", "CREATE TABLE t (id int)"} {
		if n := len(generatedColumnsIn(s)); n != 0 {
			t.Errorf("%q: want no generated columns, got %d", s, n)
		}
	}
}
