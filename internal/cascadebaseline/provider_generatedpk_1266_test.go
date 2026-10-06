package cascadebaseline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestProvider_generatedPKRefusesWithRealCause wires the #1266 gate through
// the real BaselineChildren path: a system-versioned child table (PK silently
// extended with the STORED GENERATED row_end period column) must fail loud
// with the versioning-aware cause, never reach per-row canonicalization and
// its misleading "run `bintrail snapshot` to refresh" remediation. The find
// func returns a nonexistent path on purpose: metadata read is best-effort
// (warn only), and the gate fires before ReadBaselineRows would die on the
// path — so deleting the gate flips this test's error to the file-open one.
func TestProvider_generatedPKRefusesWithRealCause(t *testing.T) {
	find := func(ctx context.Context, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
		return "/nonexistent/baseline.parquet", time.Now().Add(-time.Hour), reconstruct.StaleWarning{}, nil
	}
	resolver := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"shop.child": {Schema: "shop", Table: "child", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
			{Name: "pid", OrdinalPosition: 2, DataType: "int"},
			{Name: "row_end", OrdinalPosition: 4, IsPK: true, DataType: "timestamp", ColumnType: "timestamp(6)", IsGenerated: true},
		}},
	})

	_, _, err := New(find, resolver, nil).BaselineChildren(context.Background(), "shop", "child", "pid", "1", time.Now(), 100)
	if err == nil {
		t.Fatal("expected the generated-PK refusal, got nil")
	}
	// #1273: the REAL provider must wrap the sentinel — the cascade engine's
	// permanent-caveat classifier keys on errors.Is, and the engine-side
	// backstop test uses a fake provider that hand-writes the wrap, so this
	// is the one assertion tying the two together. Reverting the %w to a
	// plain %s would silently reclassify the refusal as transient
	// baselinefail AND let Phase-1 scan the history-polluted candidates.
	if !errors.Is(err, reconstruct.ErrGeneratedPK) {
		t.Fatalf("refusal must wrap reconstruct.ErrGeneratedPK, got: %v", err)
	}
	if !strings.Contains(err.Error(), "generated column") || !strings.Contains(err.Error(), `"row_end"`) {
		t.Fatalf("want the generated-PK cause naming row_end, got: %v", err)
	}
	if strings.Contains(err.Error(), "not in baseline row") {
		t.Fatalf("must refuse before per-row canonicalization, not with MissingPKColumnError: %v", err)
	}
}
