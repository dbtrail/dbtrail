//go:build integration

package mcptools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/audittest"
)

// callCascade runs the recover_cascade tool with the given extra arguments and
// decodes the payload.
func callCascade(t *testing.T, cs *mcp.ClientSession, schema string, extra map[string]any) recoverCascadeResult {
	t.Helper()
	args := map[string]any{"schema": schema, "table": "parent"}
	for k, v := range extra {
		args[k] = v
	}
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "recover_cascade", Arguments: args})
	if err != nil {
		t.Fatalf("CallTool recover_cascade %v: %v", extra, err)
	}
	text := resultText(res)
	if res.IsError {
		t.Fatalf("recover_cascade %v returned a tool error: %s", extra, text)
	}
	var out recoverCascadeResult
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decode payload: %v (payload=%s)", err, text)
	}
	return out
}

// TestIntegrationRecoverCascadeChunking drives the REAL generator (#1438): the
// statement offsets come from the emitter, so this is where a chunk built on
// them is proven against the script the same code path produces whole.
//
// The offsets are what make the fetch safe, and only a real emission exercises
// them: the unit tests build their own fixture, so a change to the emitter that
// stops recording an offset would leave them green.
func TestIntegrationRecoverCascadeChunking(t *testing.T) {
	db, dbName := seedCascadeIndex(t)
	cs := cascadeSession(t, db, dbName)

	whole := callCascade(t, cs, dbName, nil)
	if whole.SQL == "" || whole.StatementCount != 3 {
		t.Fatalf("the whole script did not come back: %d statement(s), %d bytes", whole.StatementCount, len(whole.SQL))
	}
	if whole.ScriptID == "" || whole.ScriptBytes != len(whole.SQL) {
		t.Fatalf("script id/size missing from a whole return: %+v", whole)
	}

	// Statement by statement, reassembled. The generated-at line makes two
	// builds differ by bytes, so the chunks are compared against a build
	// identified by the SAME script id — which is exactly the check the
	// response asks a client to make.
	var got strings.Builder
	offset, chunks := 0, 0
	for {
		c := callCascade(t, cs, dbName, map[string]any{"sql_offset": offset, "sql_limit": 1})
		if c.ScriptID != whole.ScriptID {
			t.Fatalf("chunk at offset %d came from a different build (%s vs %s); the index did not change between calls",
				offset, c.ScriptID, whole.ScriptID)
		}
		if c.SQLFrom != offset+1 || c.SQLTo != offset+1 {
			t.Fatalf("chunk at offset %d reports statements %d-%d", offset, c.SQLFrom, c.SQLTo)
		}
		got.WriteString(c.SQL)
		chunks++
		if !c.SQLMore {
			break
		}
		offset = c.NextSQLOffset
		if chunks > 5 {
			t.Fatal("chunking did not terminate")
		}
	}
	if chunks != 3 {
		t.Fatalf("%d chunk(s) for a 3-statement script", chunks)
	}
	if got.String() != whole.SQL {
		t.Errorf("the reassembled chunks are not the script.\n got %q\nwant %q", got.String(), whole.SQL)
	}

	// summary_only builds the same script and returns none of it.
	sum := callCascade(t, cs, dbName, map[string]any{"summary_only": true})
	if sum.SQL != "" {
		t.Errorf("summary_only returned %d bytes of script", len(sum.SQL))
	}
	if sum.StatementCount != whole.StatementCount || sum.ScriptBytes != whole.ScriptBytes || sum.ScriptID != whole.ScriptID {
		t.Errorf("the summary describes a different script than the whole return: %+v vs %+v", sum, whole)
	}
	if !sum.Complete || sum.Children != whole.Children {
		t.Errorf("the summary lost the coverage report: %+v", sum)
	}

	// Paging past the end names the count instead of returning an empty chunk
	// a client would read as the end of the script.
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "recover_cascade",
		Arguments: map[string]any{"schema": dbName, "table": "parent", "sql_offset": 99},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(res), "3 statement(s)") {
		t.Errorf("an out-of-range offset was not refused with the count: %s", resultText(res))
	}
}

// The audit record follows the BYTES: a chunk fetch serves row data and is
// recorded (naming its range, so N chunks do not read as N recoveries), while
// a summary serves counts only and is deliberately not — ext/audit.go leaves
// metadata reads out.
func TestIntegrationRecoverCascadeChunkAuditing(t *testing.T) {
	db, dbName := seedCascadeIndex(t)
	cs := cascadeSession(t, db, dbName)

	rec := audittest.Install(t)

	callCascade(t, cs, dbName, map[string]any{"summary_only": true})
	if n := len(rec.Events()); n != 0 {
		t.Fatalf("summary_only recorded %d audit event(s); it serves no row data", n)
	}

	callCascade(t, cs, dbName, map[string]any{"sql_offset": 1, "sql_limit": 1})
	events := rec.Events()
	if len(events) != 1 {
		t.Fatalf("a chunk fetch recorded %d event(s), want 1", len(events))
	}
	e := events[0]
	if e.Action != "recover.cascade" || e.Table != "parent" {
		t.Fatalf("unexpected audit event: %+v", e)
	}
	if got := e.Detail["chunk"]; got != "statements 2-2 of 3" {
		t.Errorf("chunk range in the audit detail = %q, want the range so a partial fetch is not read as a whole script", got)
	}
	if e.Detail["script_id"] == "" {
		t.Error("a chunk record carries no script id, so two chunks of one recovery cannot be tied together")
	}

	callCascade(t, cs, dbName, nil)
	events = rec.Events()
	if len(events) != 2 {
		t.Fatalf("a whole-script return recorded %d event(s) in total, want 2", len(events))
	}
	if _, ok := events[1].Detail["chunk"]; ok {
		t.Error("a whole-script return carries a chunk range; a reader could not tell it from a partial fetch")
	}
}
