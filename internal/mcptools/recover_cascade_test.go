package mcptools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func callRecoverCascade(t *testing.T, cfg Config, args RecoverCascadeArgs) *mcp.CallToolResult {
	t.Helper()
	res, _, err := MakeRecoverCascadeTool(cfg)(context.Background(), nil, args)
	if err != nil {
		t.Fatalf("handler returned a protocol error: %v", err)
	}
	return res
}

// TestRecoverCascadeToolRegistration pins that recover_cascade is opt-in per
// surface via Config.RecoverCascade — and that, unlike reconstruct, the flag is
// the ONLY gate: no baseline lookup is needed to advertise it (Phase-1
// window-only synthesis works without one).
func TestRecoverCascadeToolRegistration(t *testing.T) {
	listTools := func(t *testing.T, cfg Config) map[string]*mcp.Tool {
		t.Helper()
		ctx := context.Background()
		clientT, serverT := mcp.NewInMemoryTransports()
		ss, err := NewServer(cfg).Connect(ctx, serverT, nil)
		if err != nil {
			t.Fatalf("server connect: %v", err)
		}
		defer ss.Close()

		client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "2025-06-18"}, nil)
		cs, err := client.Connect(ctx, clientT, nil)
		if err != nil {
			t.Fatalf("client connect: %v", err)
		}
		defer cs.Close()

		res, err := cs.ListTools(ctx, nil)
		if err != nil {
			t.Fatalf("ListTools: %v", err)
		}
		out := map[string]*mcp.Tool{}
		for _, tool := range res.Tools {
			out[tool.Name] = tool
		}
		return out
	}

	t.Run("opt-out surface omits it", func(t *testing.T) {
		tools := listTools(t, Config{Resolve: unreachableResolve(t)})
		if _, ok := tools["recover_cascade"]; ok {
			t.Error("recover_cascade must not be advertised when Config.RecoverCascade is false")
		}
	})

	t.Run("opt-in surface advertises it without any baseline", func(t *testing.T) {
		tools := listTools(t, Config{Resolve: unreachableResolve(t), RecoverCascade: true})
		tool, ok := tools["recover_cascade"]
		if !ok {
			t.Fatalf("recover_cascade not advertised; got %v", tools)
		}
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint || !tool.Annotations.IdempotentHint {
			t.Errorf("recover_cascade must be annotated read-only + idempotent, got %+v", tool.Annotations)
		}
		if tool.InputSchema == nil {
			t.Fatal("recover_cascade has no input schema")
		}
		raw, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatalf("marshal input schema: %v", err)
		}
		for _, want := range []string{"schema", "table", "pk", "pks", "since", "until",
			"lookback", "max_depth", "limit", "allow_incomplete", "baseline_dir", "baseline_s3"} {
			if !strings.Contains(string(raw), `"`+want+`"`) {
				t.Errorf("input schema is missing property %q: %s", want, raw)
			}
		}
	})
}

// TestRecoverCascadeRejectsRoutedSurfaceParams pins that a surface owning its
// own routing (the console) refuses the client-supplied DSN and baseline
// location BEFORE resolving a connection — an authenticated MCP client must
// not be able to point the console at arbitrary storage.
func TestRecoverCascadeRejectsRoutedSurfaceParams(t *testing.T) {
	cfg := Config{
		Resolve:             unreachableResolve(t),
		AllowDSNParam:       false,
		AllowBaselineParams: false,
		RecoverCascade:      true,
	}
	base := RecoverCascadeArgs{Schema: "app", Table: "orders"}

	dsn := base
	dsn.IndexDSN = "u:p@tcp(1.2.3.4:3306)/x"
	assertToolError(t, callRecoverCascade(t, cfg, dsn), "index_dsn is not accepted")

	dir := base
	dir.BaselineDir = "/tmp/baselines"
	assertToolError(t, callRecoverCascade(t, cfg, dir), "baseline_dir/baseline_s3 are not accepted")

	s3 := base
	s3.BaselineS3 = "s3://bucket/prefix"
	assertToolError(t, callRecoverCascade(t, cfg, s3), "baseline_dir/baseline_s3 are not accepted")
}

// TestRecoverCascadeValidation pins the pre-connection argument checks: they
// must all refuse before Resolve is ever called.
func TestRecoverCascadeValidation(t *testing.T) {
	cfg := Config{Resolve: unreachableResolve(t), AllowDSNParam: true, AllowBaselineParams: true, RecoverCascade: true}

	assertToolError(t, callRecoverCascade(t, cfg, RecoverCascadeArgs{Table: "orders"}), "schema and table are required")
	assertToolError(t, callRecoverCascade(t, cfg, RecoverCascadeArgs{Schema: "app"}), "schema and table are required")
	assertToolError(t, callRecoverCascade(t, cfg,
		RecoverCascadeArgs{Schema: "app", Table: "orders", PK: "1", PKs: []string{"2"}}), "mutually exclusive")
	assertToolError(t, callRecoverCascade(t, cfg,
		RecoverCascadeArgs{Schema: "app", Table: "orders", MaxDepth: -1}), "max_depth must be >= 1")
	assertToolError(t, callRecoverCascade(t, cfg,
		RecoverCascadeArgs{Schema: "app", Table: "orders", Lookback: "not-a-duration"}), "invalid lookback")
	assertToolError(t, callRecoverCascade(t, cfg,
		RecoverCascadeArgs{Schema: "app", Table: "orders", Since: "not-a-time"}), "invalid since")
}

// TestRecoverCascadeRefusesActiveProfile pins the RBAC guard: cascade victim
// synthesis fetches child rows without carrying deny/redact rules, so any
// active profile posture on the Target must refuse — the same enforcement the
// console's /api/recover-cascade endpoint applies.
func TestRecoverCascadeRefusesActiveProfile(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := Config{
		Resolve: func(ctx context.Context, _ string) (*Target, error) {
			return &Target{DB: db, ProfileActive: true, ResolverLoaded: true}, nil
		},
		RecoverCascade: true,
	}
	res := callRecoverCascade(t, cfg, RecoverCascadeArgs{Schema: "app", Table: "orders"})
	assertToolError(t, res, "access-control profile is active")
}

// cascadeMockConfig wires a sqlmock DB with the handler's real queries: the
// merged fetcher's one archive discovery (#1615), the parent DELETE fetch, the
// parent UPDATE fetch (both empty), and the archive-coverage probe. Discovery
// and probe read archive_state the same way, and the caller picks their
// shared outcome — probeErr non-nil makes coverage unknown, the one caveat
// class reachable without a live FK topology. Order is not asserted: the
// two archive_state reads are indistinguishable to the mock.
func cascadeMockConfig(t *testing.T, probeErr error) (Config, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(recoverToolMockCols))
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(recoverToolMockCols))
	mock.MatchExpectationsInOrder(false)
	for range 2 { // merged-fetcher discovery + coverage probe
		probe := mock.ExpectQuery("FROM archive_state")
		if probeErr != nil {
			probe.WillReturnError(probeErr)
		} else {
			probe.WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))
		}
	}
	cfg := Config{
		Resolve: func(ctx context.Context, _ string) (*Target, error) {
			return &Target{DB: db, ResolverLoaded: true}, nil
		},
		RecoverCascade: true,
	}
	return cfg, mock
}

// TestRecoverCascadeIncompleteWithoutAllow pins the fail-closed default: a
// provably partial synthesis is an ERROR that carries the caveats and the
// tool-parameter remediation — never a script whose gaps hide in a comment
// banner, and never CLI flag spellings an MCP client cannot use.
func TestRecoverCascadeIncompleteWithoutAllow(t *testing.T) {
	cfg, _ := cascadeMockConfig(t, errors.New("archive_state probe down"))
	res := callRecoverCascade(t, cfg, RecoverCascadeArgs{Schema: "app", Table: "orders"})
	assertToolError(t, res, "INCOMPLETE")
	text := resultText(res)
	if !strings.Contains(text, "coverage is unknown") {
		t.Errorf("error must carry the caveat itself, got: %s", text)
	}
	if !strings.Contains(text, "allow_incomplete: true") {
		t.Errorf("error must name the tool parameter that overrides it, got: %s", text)
	}
	// The #1114 precedent: never leak a CLI flag to an MCP client.
	for _, flag := range []string{"--allow-incomplete", "--lookback", "--limit"} {
		if strings.Contains(text, flag) {
			t.Errorf("error leaks CLI flag %q: %s", flag, text)
		}
	}
}

// TestRecoverCascadeIncompleteWithAllow pins the opt-in: the same partial
// synthesis returns the script, with the caveats surfaced in the payload's
// incomplete list and complete=false — an incomplete synthesis must never be
// presented as complete.
func TestRecoverCascadeIncompleteWithAllow(t *testing.T) {
	cfg, _ := cascadeMockConfig(t, errors.New("archive_state probe down"))
	res := callRecoverCascade(t, cfg, RecoverCascadeArgs{Schema: "app", Table: "orders", AllowIncomplete: true})
	if res.IsError {
		t.Fatalf("allow_incomplete must return the partial script, got error: %s", resultText(res))
	}
	var out recoverCascadeResult
	if err := json.Unmarshal([]byte(resultText(res)), &out); err != nil {
		t.Fatalf("decode payload: %v (payload=%s)", err, resultText(res))
	}
	if out.Complete {
		t.Error("complete must be false for a partial synthesis")
	}
	if len(out.Incomplete) == 0 || !strings.Contains(out.Incomplete[0], "coverage is unknown") {
		t.Errorf("incomplete must carry the caveats, got %v", out.Incomplete)
	}
	if !strings.Contains(out.SQL, "INCOMPLETE RECOVERY") {
		t.Errorf("the script's own banner must flag the partial recovery:\n%s", out.SQL)
	}
	if !strings.Contains(out.SQL, "SET FOREIGN_KEY_CHECKS=0;") || !strings.Contains(out.SQL, "SET FOREIGN_KEY_CHECKS=1;") {
		t.Errorf("script must be FK-checks wrapped:\n%s", out.SQL)
	}
	// Empty window: an advisory (never a caveat) tells the agent the filter
	// may be wrong rather than letting silence read as "nothing was changed".
	found := false
	for _, w := range out.Warnings {
		if strings.Contains(w, "no parent DELETE or UPDATE events matched") {
			found = true
		}
	}
	if !found {
		t.Errorf("warnings must carry the empty-match advisory, got %v", out.Warnings)
	}
}

// TestRecoverCascadeCompleteEmptyWindow pins the clean-empty case: no parents,
// no archives, no caveats — complete=true with an empty (but well-formed)
// script and the audit-free advisory in warnings.
func TestRecoverCascadeCompleteEmptyWindow(t *testing.T) {
	cfg, _ := cascadeMockConfig(t, nil)
	res := callRecoverCascade(t, cfg, RecoverCascadeArgs{Schema: "app", Table: "orders"})
	if res.IsError {
		t.Fatalf("clean empty window must succeed, got: %s", resultText(res))
	}
	var out recoverCascadeResult
	if err := json.Unmarshal([]byte(resultText(res)), &out); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if !out.Complete || len(out.Incomplete) != 0 {
		t.Errorf("complete=%v incomplete=%v, want complete with no caveats", out.Complete, out.Incomplete)
	}
	if out.StatementCount != 0 || out.Parents != 0 || out.Children != 0 {
		t.Errorf("counts = %d/%d/%d, want all zero", out.StatementCount, out.Parents, out.Children)
	}
}

// TestRecoverCascadeRoutedSurfaceNoBaseline pins that on a routed surface
// (console posture) WITHOUT a configured baseline the tool still serves —
// Phase-1 — instead of copying reconstruct's baselineConfigured refusal, and
// reports the phase honestly (baseline_active=false).
func TestRecoverCascadeRoutedSurfaceNoBaseline(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(recoverToolMockCols))
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(recoverToolMockCols))
	mock.MatchExpectationsInOrder(false)
	for range 2 { // merged-fetcher discovery + coverage probe
		mock.ExpectQuery("FROM archive_state").WillReturnRows(
			sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))
	}
	cfg := Config{
		Resolve: func(ctx context.Context, _ string) (*Target, error) {
			return &Target{DB: db, ResolverLoaded: true, BaselineConfigured: false}, nil
		},
		AllowDSNParam:       false,
		AllowBaselineParams: false,
		RecoverCascade:      true,
	}
	res := callRecoverCascade(t, cfg, RecoverCascadeArgs{Schema: "app", Table: "orders"})
	if res.IsError {
		t.Fatalf("Phase-1 without a baseline must serve, got: %s", resultText(res))
	}
	var out recoverCascadeResult
	if err := json.Unmarshal([]byte(resultText(res)), &out); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if out.BaselineActive {
		t.Error("baseline_active must be false when the surface has no baseline configured")
	}
}
