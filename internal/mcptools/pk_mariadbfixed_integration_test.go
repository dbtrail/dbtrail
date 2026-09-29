//go:build integration

package mcptools

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The query and recover tools looked a MariaDB UUID key up exactly as typed,
// while the index stores its bytes (pk_values in hex), so the text form an
// agent would pass returned nothing and an empty script. Both spellings must
// find the row, and a key the snapshot cannot type is refused, never answered
// empty.
func TestTools_MariaDBUUIDKeyTextSpelling(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	const (
		text   = "cc5c4e6e-bc5a-11f1-9a0c-0affd251cac9"
		stored = "0xCC5C4E6EBC5A11F19A0C0AFFD251CAC9"
	)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 11:00:00", "app", "sessions", "sid", 1, "PRI", "uuid", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 11:00:00", "app", "sessions", "payload", 2, "", "varchar", "YES")
	raw, err := hex.DecodeString(strings.TrimPrefix(stored, "0x"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(map[string]any{"sid": base64.StdEncoding.EncodeToString(raw), "payload": "s2"})
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"sessions", "ghost"} {
		testutil.InsertEvent(t, db, "bin.000001", 100, 200, "2026-06-01 12:00:00", nil,
			"app", table, 3 /*DELETE*/, stored, nil, before, nil)
	}
	cfg := Config{Resolve: func(context.Context, string) (*Target, error) {
		return &Target{DB: db, NoArchive: true}, nil
	}}

	for _, pk := range []string{text, stored} {
		res, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{},
			QueryArgs{Schema: "app", Table: "sessions", PK: pk})
		if err != nil || res.IsError {
			t.Fatalf("query pk=%s: %v %s", pk, err, resultText(res))
		}
		if out := resultText(res); !strings.Contains(out, stored) {
			t.Errorf("query pk=%s did not return the row:\n%s", pk, out)
		}
	}

	res, _, err := MakeRecoverTool(cfg)(context.Background(), &mcp.CallToolRequest{},
		RecoverArgs{Schema: "app", Table: "sessions", PK: text})
	if err != nil || res.IsError {
		t.Fatalf("recover: %v %s", err, resultText(res))
	}
	if script := resultText(res); !strings.Contains(script, "1 reversal statement(s) generated") {
		t.Errorf("recover with the text key did not reverse the event:\n%s", script)
	}

	res, _, err = MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{},
		QueryArgs{Schema: "app", Table: "ghost", PK: text})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Errorf("a key no snapshot can type was answered, not refused:\n%s", resultText(res))
	} else if msg := resultText(res); strings.Contains(msg, "--") {
		t.Errorf("the refusal names a CLI flag an agent cannot pass: %s", msg)
	}
}
