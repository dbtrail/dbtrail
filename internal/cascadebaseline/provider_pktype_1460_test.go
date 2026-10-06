package cascadebaseline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// resolverWithChildPK builds a one-table resolver for shop.child whose PK
// column carries pkType, plus an INT FK column so fkFilterSafe (which
// inspects the FK column, never the PK) passes and the PK gate is the only
// thing that can refuse. That combination is exactly the reachable shape
// #1460 describes.
func resolverWithChildPK(pkType string) *metadata.Resolver {
	return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"shop.child": {Schema: "shop", Table: "child", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: pkType},
			{Name: "pid", OrdinalPosition: 2, DataType: "int"},
		}},
	})
}

// nonexistentBaseline is the find func both tests wire: the path never
// exists, so if the gate under test is missing the run dies inside
// ReadBaselineRows with a file-open error instead of the refusal. That is
// what makes these tests go red against ungated code rather than passing on
// a coincidence.
func nonexistentBaseline(ctx context.Context, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
	return "/nonexistent/baseline.parquet", time.Now().Add(-time.Hour), reconstruct.StaleWarning{}, nil
}

// TestProvider_unsupportedPKTypeRefusesBeforeRead pins the #1460 gate: a child
// table whose PK type the baseline canonicalizer cannot handle must be refused
// UP FRONT, in the shared PKTypeGateReason words, carrying the
// ErrUnsupportedPKType sentinel — not read row by row and then failed by the
// per-row canonicalizer, whose plain error the cascade engine files as a
// transient baselinefail.
func TestProvider_unsupportedPKTypeRefusesBeforeRead(t *testing.T) {
	for _, pkType := range []string{"float", "double", "bit", "json", "time", "geometry"} {
		t.Run(pkType, func(t *testing.T) {
			_, ok, err := New(nonexistentBaseline, resolverWithChildPK(pkType), nil).
				BaselineChildren(context.Background(), "shop", "child", "pid", "1", time.Now(), 100)
			if err == nil {
				t.Fatalf("expected the PK-type refusal for a %s PK, got nil (ok=%v)", pkType, ok)
			}
			// The sentinel is the whole point: the engine's permanent-caveat
			// classifier keys on errors.Is, and the engine-side test uses a
			// fake provider, so this is the one assertion tying the two
			// together. Dropping the sentinel silently reclassifies the
			// refusal as transient AND restores the per-parent-key rescan.
			if !errors.Is(err, reconstruct.ErrUnsupportedPKType) {
				t.Fatalf("refusal must carry reconstruct.ErrUnsupportedPKType, got: %v", err)
			}
			// Same sentence every other surface renders (#1461), so an
			// operator who hits two of them can tell it is one limit.
			want := reconstruct.PKTypeGateReason(
				metadata.ColumnMeta{Name: "id", IsPK: true, DataType: pkType},
				"the cascade baseline fallback", "read")
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal must render the shared gate reason %q, got: %v", want, err)
			}
			// The gate must sit BEFORE ReadBaselineRows: a nonexistent path
			// proves it, because reaching the read would surface the open
			// error instead.
			if strings.Contains(err.Error(), "nonexistent") {
				t.Errorf("refusal must fire before the baseline read, got a read error: %v", err)
			}
			// Never the per-row canonicalizer backstop, whose plain error
			// carries no sentinel.
			if strings.Contains(err.Error(), "canonicalizePKValue") {
				t.Errorf("refusal must not come from per-row canonicalization: %v", err)
			}
		})
	}
}

// TestProvider_emptyPKDataTypeIsNotAPKTypeVerdict pins the deliberate
// non-change. An empty DataType is the PostgreSQL snapshot signature (#533),
// and NOTHING on the cascade path checks the recorded source flavor, so the
// wrong-path verdict PKTypeGateReason renders for an empty type would assert
// a cause this code cannot know. The gate therefore skips it and the run
// proceeds to the baseline read, exactly as it does today.
func TestProvider_emptyPKDataTypeIsNotAPKTypeVerdict(t *testing.T) {
	_, _, err := New(nonexistentBaseline, resolverWithChildPK(""), nil).
		BaselineChildren(context.Background(), "shop", "child", "pid", "1", time.Now(), 100)
	if err == nil {
		t.Fatal("expected the baseline read to fail on the nonexistent path, got nil")
	}
	if errors.Is(err, reconstruct.ErrUnsupportedPKType) {
		t.Fatalf("an empty DataType is the PostgreSQL snapshot shape, not a PK-type verdict: %v", err)
	}
	if strings.Contains(err.Error(), "PostgreSQL") {
		t.Fatalf("this path has no source-flavor check, so it must not claim a PostgreSQL cause: %v", err)
	}
}

// TestProvider_unsupportedPKTypeRefusesBeforeAnyBaselineIO pins that the gate
// sits above the position-anchor metadata read as well as the row read. That
// read is best-effort and only warns on failure, so a refused table used to
// emit "cascade: could not read baseline metadata for position-anchored
// victim fetch" naming a problem the operator would then chase, seconds
// before being told the table is refused for an unrelated and permanent
// reason. Nothing is lost by ordering it this way: the anchor is only ever
// used by a lookup that is about to be refused.
func TestProvider_unsupportedPKTypeRefusesBeforeAnyBaselineIO(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	_, _, err := New(nonexistentBaseline, resolverWithChildPK("float"), nil).
		BaselineChildren(context.Background(), "shop", "child", "pid", "1", time.Now(), 100)
	if !errors.Is(err, reconstruct.ErrUnsupportedPKType) {
		t.Fatalf("want the PK-type refusal, got: %v", err)
	}
	if strings.Contains(buf.String(), "could not read baseline metadata") {
		t.Errorf("a refused table must not first warn about the baseline metadata read: %s", buf.String())
	}
}

// TestProvider_supportedPKTypesStillReachTheRead guards the other direction:
// the gate must not refuse a type the canonicalizer handles. The binary
// family is the one that matters, supported since #1155 — a gate written
// against a stale type list would silently turn working tables into
// permanent refusals, which is worse than the bug being fixed.
func TestProvider_supportedPKTypesStillReachTheRead(t *testing.T) {
	for _, pkType := range []string{"int", "bigint", "varchar", "decimal", "datetime", "binary", "varbinary", "blob"} {
		t.Run(pkType, func(t *testing.T) {
			_, _, err := New(nonexistentBaseline, resolverWithChildPK(pkType), nil).
				BaselineChildren(context.Background(), "shop", "child", "pid", "1", time.Now(), 100)
			if err == nil {
				t.Fatal("expected the baseline read to fail on the nonexistent path, got nil")
			}
			if errors.Is(err, reconstruct.ErrUnsupportedPKType) {
				t.Fatalf("%s is a supported PK type and must reach the baseline read: %v", pkType, err)
			}
		})
	}
}
