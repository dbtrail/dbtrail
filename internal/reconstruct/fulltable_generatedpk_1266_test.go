package reconstruct

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// svResolver1266 stubs the MariaDB system-versioning PK shape #1266 reports:
// the PRIMARY KEY silently extended with the STORED GENERATED ROW END period
// column (`PRIMARY KEY (id, row_end)`, observed on MariaDB 11.4).
func svResolver1266() *metadata.Resolver {
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"svdb.orders": {Schema: "svdb", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "val", OrdinalPosition: 2, DataType: "varchar"},
			{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "timestamp", ColumnType: "timestamp(6)", IsGenerated: true},
		}},
	})
}

func TestGeneratedPKColumn(t *testing.T) {
	tm, err := svResolver1266().Resolve("svdb", "orders")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	c, ok := GeneratedPKColumn(tm.PKColumnMetas())
	if !ok || c.Name != "row_end" {
		t.Fatalf("GeneratedPKColumn = (%q, %v), want (row_end, true)", c.Name, ok)
	}
	if _, ok := GeneratedPKColumn([]metadata.ColumnMeta{{Name: "id", IsPK: true}}); ok {
		t.Fatal("GeneratedPKColumn on a plain PK must report false")
	}
}

// TestReconstructTable_generatedPKRefusal_baselinePath wires the gate through
// the REAL baseline-merge path: a baseline snapshot exists and carries
// CreateTableSQL, so execution reaches the PK gates, where the generated
// row_end member must refuse up front — with the versioning-aware message,
// never the deep per-row MissingPKColumnError the issue reproduced. The gate
// fires before any DB access, so db/engine stay nil (mutation guard: deleting
// the gate makes this test die on the nil DB in CheckDestructiveDDL).
func TestReconstructTable_generatedPKRefusal_baselinePath(t *testing.T) {
	dir := writeGateBaseline(t, "svdb", "orders", map[string]string{
		baseline.MetaKeyCreateTableSQL: "CREATE TABLE `orders` (`id` int NOT NULL, PRIMARY KEY (`id`,`row_end`))",
	})
	cfg := FullTableConfig{BaselineSrc: dir, At: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)}

	_, err := ReconstructTable(context.Background(), cfg, "svdb", "orders", nil, nil, nil, svResolver1266(), "")
	if err == nil {
		t.Fatal("expected the generated-PK refusal, got nil")
	}
	if !strings.Contains(err.Error(), `"row_end"`) || !strings.Contains(err.Error(), "generated column") {
		t.Fatalf("want the generated-PK refusal naming row_end, got: %v", err)
	}
	if strings.Contains(err.Error(), "not in baseline row") {
		t.Fatalf("must refuse up front, not with the deep MissingPKColumnError: %v", err)
	}
}

// TestReconstructTable_generatedPKRefusal_binlogOnlyPath wires the sibling gate
// through the binlog-only fallback (no baseline found, mydumper output): the
// refusal must fire before the event fold — this path has no baseline probe to
// fail loudly, so without the gate a versioned table's history-row inserts
// would be emitted as duplicate live rows.
func TestReconstructTable_generatedPKRefusal_binlogOnlyPath(t *testing.T) {
	cfg := FullTableConfig{BaselineSrc: t.TempDir(), At: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), AllowChangesOnly: true}

	_, err := ReconstructTable(context.Background(), cfg, "svdb", "orders", nil, nil, nil, svResolver1266(), "")
	if err == nil {
		t.Fatal("expected the generated-PK refusal, got nil")
	}
	if !strings.Contains(err.Error(), `"row_end"`) || !strings.Contains(err.Error(), "generated column") {
		t.Fatalf("want the generated-PK refusal naming row_end, got: %v", err)
	}
}

// TestReconstructTable_emptyDataTypeKeepsWrongPathVerdict pins the gate
// ORDERING the in-code comment claims: a PK member with an empty DataType —
// the PG snapshot shape (#1009) — must keep winning the wrong-path verdict
// even when it is ALSO marked generated. Swapping the generated gate above the
// SupportedPKType loop would mask it with a MariaDB-shaped message; before
// this test, that ordering was prose, not a guard.
func TestReconstructTable_emptyDataTypeKeepsWrongPathVerdict(t *testing.T) {
	dir := writeGateBaseline(t, "svdb", "orders", map[string]string{
		baseline.MetaKeyCreateTableSQL: "CREATE TABLE `orders` (`id` int NOT NULL)",
	})
	cfg := FullTableConfig{BaselineSrc: dir, At: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)}
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"svdb.orders": {Schema: "svdb", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "", IsGenerated: true},
		}},
	})
	_, err := ReconstructTable(context.Background(), cfg, "svdb", "orders", nil, nil, nil, resolver, "")
	if err == nil {
		t.Fatal("expected the PG wrong-path refusal, got nil")
	}
	if strings.Contains(err.Error(), "generated column") {
		t.Fatalf("empty DataType must win the #1009 wrong-path verdict, got the generated-PK message: %v", err)
	}
	if !strings.Contains(err.Error(), "PostgreSQL") {
		t.Fatalf("want the PG wrong-path verdict, got: %v", err)
	}
}
