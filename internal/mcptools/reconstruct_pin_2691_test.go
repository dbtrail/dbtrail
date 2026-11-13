package mcptools

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The tool pins the snapshot its lookup found with the surface's PinFunc
// (#2691), and looks it up again when it was removed before the pin. Here
// the first answer is a snapshot that is gone, the second says there is
// none: two lookups, one pin, released.
func TestReconstructPinsWithTheTargetsPinFunc(t *testing.T) {
	gone := filepath.Join(t.TempDir(), "2026-06-01T00-00-00Z", "app", "users.parquet")
	lookups := 0
	var pinned, released, readers []string
	cfg := Config{
		Reconstruct: true,
		Resolve: func(ctx context.Context, _ string) (*Target, error) {
			return &Target{
				BaselineConfigured: true,
				FindBaseline: func(context.Context, string, string, time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
					lookups++
					if lookups == 1 {
						return gone, time.Now(), reconstruct.StaleWarning{}, nil
					}
					return "", time.Time{}, reconstruct.StaleWarning{}, reconstruct.ErrNoBaseline
				},
				PinBaseline: func(_ context.Context, path, what string) func() {
					pinned, readers = append(pinned, path), append(readers, what)
					return func() { released = append(released, path) }
				},
				Resolver: metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
					"app.users": {Schema: "app", Table: "users", PKColumns: []string{"id"},
						Columns: []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"}}},
				}),
				ResolverLoaded: true,
			}, nil
		},
	}
	assertToolError(t, callReconstruct(t, cfg, ReconstructArgs{Schema: "app", Table: "users", PK: "1"}), "no baseline snapshot covers")
	if lookups != 2 {
		t.Fatalf("%d lookups, want 2: the tool does not look again for a snapshot removed before its pin", lookups)
	}
	if !slices.Equal(pinned, []string{gone}) || !slices.Equal(released, []string{gone}) || !slices.Equal(readers, []string{"the MCP reconstruct tool"}) {
		t.Fatalf("pinned %v, released %v, readers %v; want the one gone snapshot pinned and released by the MCP reconstruct tool", pinned, released, readers)
	}
}
