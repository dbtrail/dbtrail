package console

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

func writeFooterFixture(t *testing.T, path string) {
	t.Helper()
	createSQL := "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `code` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n" +
		"  `total` decimal(10,2) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "AB", "1.00"}, make([]bool, 3)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// One table's footer failing to read must not be remembered for the life of
// the daemon. A snapshot is immutable, so a successful read never expires;
// before, a read where SOME files failed (one S3 hiccup on one object) counted
// as successful, and that table went without its decimal casts and its _bin
// collation until a restart. A partial read is served as far as it got and
// retried after the same short delay as a total failure.
func TestResolveBaselineDecimals_retriesAPartialRead(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "snap", "shop", "good.parquet")
	writeFooterFixture(t, good)
	flaky := filepath.Join(dir, "snap", "shop", "flaky.parquet")
	// Zero bytes: what a truncated or momentarily unreadable object looks like.
	if err := os.WriteFile(flaky, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	newInput := func() views.Input {
		return views.Input{
			BaselineSource:   dir,
			BaselineSnapshot: time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
			Baselines: []views.BaselineTable{
				{Schema: "shop", Table: "good", Path: good},
				{Schema: "shop", Table: "flaky", Path: flaky},
			},
		}
	}
	s := &Server{}

	in := newInput()
	s.resolveBaselineDecimals(context.Background(), &in)
	if !in.Baselines[0].SchemaKnown || len(in.Baselines[0].BinaryText) != 1 || len(in.Baselines[0].Decimals) != 1 {
		t.Fatalf("the readable table was not served while its sibling failed: %+v", in.Baselines[0])
	}
	if in.Baselines[1].SchemaKnown {
		t.Fatalf("the unreadable table claims a known schema: %+v", in.Baselines[1])
	}

	// The fault clears.
	writeFooterFixture(t, flaky)

	// Within the retry delay the remembered answer is served: this path runs
	// on every statement and must not read footers each time.
	in = newInput()
	s.resolveBaselineDecimals(context.Background(), &in)
	if in.Baselines[1].SchemaKnown {
		t.Error("the footers were read again inside the retry delay")
	}
	if !in.Baselines[0].SchemaKnown {
		t.Error("the readable table lost its columns on the cached path")
	}

	// After it, the read is tried again and the table gets its columns.
	s.baselineDecimalMu.Lock()
	for k, e := range s.baselineDecimals {
		e.at = time.Now().Add(-negativeDecimalTTL - time.Second)
		s.baselineDecimals[k] = e
	}
	s.baselineDecimalMu.Unlock()
	in = newInput()
	s.resolveBaselineDecimals(context.Background(), &in)
	if !in.Baselines[1].SchemaKnown || len(in.Baselines[1].BinaryText) != 1 {
		t.Errorf("a partial read was remembered as final: the table still has no columns after the retry delay: %+v", in.Baselines[1])
	}

	// And a complete read is final: aged the same way, it is not read again.
	if err := os.WriteFile(flaky, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	s.baselineDecimalMu.Lock()
	for k, e := range s.baselineDecimals {
		e.at = time.Now().Add(-negativeDecimalTTL - time.Second)
		s.baselineDecimals[k] = e
	}
	s.baselineDecimalMu.Unlock()
	in = newInput()
	s.resolveBaselineDecimals(context.Background(), &in)
	if !in.Baselines[1].SchemaKnown {
		t.Error("a complete read was thrown away and read again")
	}
}
