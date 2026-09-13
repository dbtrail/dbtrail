package recovery

import (
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// The two shared refusals cross the CLI, MCP and console surfaces unchanged,
// so their remedy must be spelled the way every surface can act on it:
// since/until and pk/pks, never a CLI flag (#1618; the #1114 rule, precedent
// in internal/mcptools/recover_cascade_test.go). The guard bans the specific
// CLI spellings rather than every "--", per #1271.
func TestRefusals_nameNoCLIFlag(t *testing.T) {
	msgs := map[string]string{
		"partialGenerationError": partialGenerationError([]genFailure{{eventID: 7, err: errors.New("nil row image")}}).Error(),
		"schemaDriftError":       schemaDriftError(map[string]map[string]bool{"shop.orders": {"legacy": true}}, []string{"shop.orders"}).Error(),
	}
	for name, msg := range msgs {
		for _, flag := range []string{"--since", "--until", "--pk", "--pks"} {
			if strings.Contains(msg, flag) {
				t.Errorf("%s leaks CLI flag %q: %s", name, flag, msg)
			}
		}
		if !strings.Contains(msg, "since/until") {
			t.Errorf("%s does not spell the window remedy as since/until: %s", name, msg)
		}
	}
	if !strings.Contains(msgs["partialGenerationError"], "pk/pks") {
		t.Errorf("partialGenerationError does not spell the key remedy as pk/pks: %s", msgs["partialGenerationError"])
	}
}

// driftResolvers builds the #601 shape without a database: snapshot 10 (the
// event's own) has a column that snapshot 20 (the latest) lacks. The
// generator's per-snapshot cache is pre-seeded, the pattern of
// TestGenerateSQLFromRows_differentSchemaVersions_differentPKs, so
// resolverForRow never touches the placeholder *sql.DB.
func driftResolvers() (evt, cur *metadata.Resolver) {
	evt = metadata.NewResolverFromTables(10, map[string]*metadata.TableMeta{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true},
			{Name: "status", OrdinalPosition: 2},
			{Name: "legacy", OrdinalPosition: 3},
		}},
	})
	cur = metadata.NewResolverFromTables(20, map[string]*metadata.TableMeta{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true},
			{Name: "status", OrdinalPosition: 2},
		}},
	})
	return evt, cur
}
