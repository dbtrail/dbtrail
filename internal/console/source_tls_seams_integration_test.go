//go:build integration

package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/ext/mcpext"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// With the index reachable, both console seams that hand a registry source to
// an extension (the view context and the MCP tool context) carry that entry's
// source TLS, per entry, with nothing leaking between entries; the boot
// entry has no source and hands over the zero value.
func TestIntegrationSourceTLSReachesExtensionSeams(t *testing.T) {
	srv, _, dbName := seedMCPConsole(t)
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	cases := sourceTLSEntries(testutil.SnapshotDSN(dbName))
	ids := make([]string, len(cases))
	for i := range cases {
		cases[i].entry.NoArchive = true
		e, err := reg.Add(cases[i].entry)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = e.ID
	}
	srv.cm.reg = reg

	mcpext.ResetForTest()
	t.Cleanup(mcpext.ResetForTest)
	var resolve mcpext.ToolContextFunc
	mcpext.Register(func(_ *mcp.Server, r mcpext.ToolContextFunc) { resolve = r })

	toolCtx := func(id string) mcpext.ToolContext {
		t.Helper()
		resolve = nil
		srv.newMCPServer(id, nil)
		if resolve == nil {
			t.Fatal("extension provider did not run")
		}
		tc, err := resolve(context.Background(), "")
		if err != nil {
			t.Fatalf("resolve %q: %v", id, err)
		}
		tc.Close()
		return tc
	}

	for range 2 {
		for i, c := range cases {
			r := httptest.NewRequest(http.MethodGet, "/api/ext/example/x", nil)
			r.Header.Set(serverHeader, ids[i])
			qc, err := srv.consoleQueryContext(r)
			if err != nil {
				t.Fatal(err)
			}
			if qc.DB == nil {
				t.Fatalf("%s: index should be reachable in this test", c.entry.Name)
			}
			if qc.SourceDSN != c.entry.SourceDSN || qc.SourceTLS != c.want {
				t.Errorf("view %s: SourceDSN %q SourceTLS %+v, want %q %+v", c.entry.Name, qc.SourceDSN, qc.SourceTLS, c.entry.SourceDSN, c.want)
			}

			tc := toolCtx(ids[i])
			if tc.SourceDSN != c.entry.SourceDSN || tc.SourceTLS != c.want {
				t.Errorf("mcp %s: SourceDSN %q SourceTLS %+v, want %q %+v", c.entry.Name, tc.SourceDSN, tc.SourceTLS, c.entry.SourceDSN, c.want)
			}
		}
	}

	if tc := toolCtx(""); tc.SourceDSN != "" || tc.SourceTLS != (ext.SourceTLS{}) {
		t.Errorf("boot entry: SourceDSN %q SourceTLS %+v, want both empty", tc.SourceDSN, tc.SourceTLS)
	}
}
