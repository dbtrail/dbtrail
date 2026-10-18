package console

import (
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"
)

// TestMCPTokenLastUsed pins what the MCP Server page's "connected" line is
// built from: the last /mcp request a credential made AND that reached a
// server. It is only ever the current value's own use, so a rotation (here
// or by another process) or a revoke reads as "not used yet" instead of
// crediting the new value with the old one's connection.
func TestMCPTokenLastUsed(t *testing.T) {
	t.Run("nothing used yet", func(t *testing.T) {
		s := newManagedServer(t, "static-tok")
		s.startedAt = time.Date(2026, 10, 18, 14, 0, 0, 0, time.UTC)
		st := s.mcpTokenStatus()
		if st.LastUsedAt != "" || st.StaticLastUsedAt != "" {
			t.Fatalf("fresh server reports a use: %+v", st)
		}
		if st.CountingSince != "2026-10-18T14:00:00Z" {
			t.Fatalf("counting_since = %q, want the process start", st.CountingSince)
		}
	})

	t.Run("a refused or misaddressed request is not a use", func(t *testing.T) {
		s := newManagedServer(t, "")
		token, f, err := GenerateMCPToken(s.mcpTokenPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.managedTok.reload(f)
		if rec := doMCP(t, s, "/mcp", "wrong"); rec.Code != 401 {
			t.Fatalf("wrong token = %d, want 401", rec.Code)
		}
		// The right token on an address that names no server: the request
		// authenticated and still reached nothing, which is what a mis-pasted
		// address looks like from Claude's side.
		if rec := doMCP(t, s, "/mcp/nope", token); rec.Code != 404 {
			t.Fatalf("unknown server = %d, want 404", rec.Code)
		}
		if st := s.mcpTokenStatus(); st.LastUsedAt != "" {
			t.Fatalf("401/404 counted as a use: %+v", st)
		}
		if rec := doMCP(t, s, "/mcp", token); rec.Code != 200 {
			t.Fatalf("managed /mcp = %d", rec.Code)
		}
		if st := s.mcpTokenStatus(); st.LastUsedAt == "" || st.StaticLastUsedAt != "" {
			t.Fatalf("managed use not reported (or reported as static): %+v", st)
		}
	})

	t.Run("a new token starts unused", func(t *testing.T) {
		s := newManagedServer(t, "")
		token, f, err := GenerateMCPToken(s.mcpTokenPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.managedTok.reload(f)
		if rec := doMCP(t, s, "/mcp", token); rec.Code != 200 {
			t.Fatalf("managed /mcp = %d", rec.Code)
		}
		token2, f2, err := GenerateMCPToken(s.mcpTokenPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.managedTok.reload(f2)
		if st := s.mcpTokenStatus(); st.LastUsedAt != "" {
			t.Fatalf("rotated token inherits the old one's use: %+v", st)
		}
		// The old value is refused and stamps nothing.
		if rec := doMCP(t, s, "/mcp", token); rec.Code != 401 {
			t.Fatalf("old token = %d, want 401", rec.Code)
		}
		if st := s.mcpTokenStatus(); st.LastUsedAt != "" {
			t.Fatalf("refused old token counted: %+v", st)
		}
		if rec := doMCP(t, s, "/mcp", token2); rec.Code != 200 {
			t.Fatalf("new token = %d", rec.Code)
		}
		if st := s.mcpTokenStatus(); st.LastUsedAt == "" {
			t.Fatalf("new token's use not reported: %+v", st)
		}
	})

	t.Run("rotated or revoked by another process", func(t *testing.T) {
		s := newManagedServer(t, "")
		token, f, err := GenerateMCPToken(s.mcpTokenPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.managedTok.reload(f)
		if rec := doMCP(t, s, "/mcp", token); rec.Code != 200 {
			t.Fatalf("managed /mcp = %d", rec.Code)
		}
		// Another process writes a new value: no reload here.
		if _, _, err := GenerateMCPToken(s.mcpTokenPath, nil); err != nil {
			t.Fatal(err)
		}
		if st := s.mcpTokenStatus(); st.LastUsedAt != "" {
			t.Fatalf("value rotated on disk still reports the old use: %+v", st)
		}
		if err := os.Remove(s.mcpTokenPath); err != nil {
			t.Fatal(err)
		}
		if st := s.mcpTokenStatus(); st.LastUsedAt != "" || st.Managed {
			t.Fatalf("revoked on disk still reports a use: %+v", st)
		}
	})

	t.Run("the static token has its own stamp", func(t *testing.T) {
		s := newManagedServer(t, "static-tok")
		if rec := doMCP(t, s, "/mcp", "static-tok"); rec.Code != 200 {
			t.Fatalf("static /mcp = %d", rec.Code)
		}
		st := s.mcpTokenStatus()
		if st.StaticLastUsedAt == "" || st.LastUsedAt != "" {
			t.Fatalf("static use misreported: %+v", st)
		}
		// On the wire, under the names the page reads.
		rec := doJSON(t, s, "GET", "/api/mcp-token", "static-tok")
		var wire map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &wire); err != nil {
			t.Fatal(err)
		}
		if _, ok := wire["static_last_used_at"].(string); !ok {
			t.Fatalf("wire lacks static_last_used_at: %s", rec.Body.String())
		}
		if _, ok := wire["last_used_at"]; ok {
			t.Fatalf("wire carries last_used_at with no managed use: %s", rec.Body.String())
		}
	})

	t.Run("concurrent uses", func(t *testing.T) {
		s := newManagedServer(t, "static-tok")
		token, f, err := GenerateMCPToken(s.mcpTokenPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		s.managedTok.reload(f)
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tok := token
				if i%2 == 0 {
					tok = "static-tok"
				}
				doMCP(t, s, "/mcp", tok)
				_ = s.mcpTokenStatus()
			}()
		}
		wg.Wait()
		if st := s.mcpTokenStatus(); st.LastUsedAt == "" || st.StaticLastUsedAt == "" {
			t.Fatalf("concurrent uses lost: %+v", st)
		}
	})
}
