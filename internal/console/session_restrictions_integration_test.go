//go:build integration

package console

import (
	"net/http"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// restrictedBearer mints a session holding all permissions plus the given
// direct restrictions (#1449) — and deliberately NO profile name, because the
// point of the seam is that nothing needs to exist in the index for the
// restriction to enforce.
func restrictedBearer(t *testing.T, srv *Server, rest *ext.SessionRestrictions) string {
	t.Helper()
	tok, _, err := srv.sessions.IssueWithPolicy("test-user",
		&ext.AccessPolicy{Permissions: ext.AllPermissions(), Restrictions: rest})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// TestIntegrationSessionRestrictionsRedaction pins the label-free half of the
// redaction contract: a session whose POLICY carries deny/redact entries is
// enforced against an index holding no profiles, no table_flags and no
// access_rules rows — while the static token on the same server still reads
// raw (per-session, not process-global).
func TestIntegrationSessionRestrictionsRedaction(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		DenyTables:    []ext.TableRef{{Schema: "app", Table: "secrets"}},
		RedactColumns: []ext.TableColumnRef{{Schema: "app", Table: "users", Column: "ssn"}},
	})

	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=secrets", scoped); strings.Contains(rec.Body.String(), "topsecret") {
		t.Errorf("policy-denied table app.secrets leaked to a restricted session: %s", rec.Body.String())
	}
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=secrets", "static-tok"); !strings.Contains(rec.Body.String(), "topsecret") {
		t.Errorf("policy-less credential should see the raw table; per-session restriction leaked to the process: %s", rec.Body.String())
	}
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=users", scoped)
	if strings.Contains(rec.Body.String(), "ssn-12345") {
		t.Errorf("policy-redacted column ssn leaked to a restricted session: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "alice@example") {
		t.Errorf("non-redacted column email must remain: %s", rec.Body.String())
	}
}

// TestIntegrationSessionRestrictionsAllowList pins allow-list mode end to end:
// AllowTables withholds every table it does not name (no deny entry needed for
// app.secrets), and AllowColumns nulls the columns it does not name for the
// tables it covers.
func TestIntegrationSessionRestrictionsAllowList(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	// A DISTINCT table whose name differs only by case: the allow match must
	// be exact (the columns' collation is case-insensitive, and a
	// case-insensitive allow would serve this table too — the fail-open
	// direction).
	testutil.InsertEvent(t, srv.cm.boot.db, "bin.000001", 120, 160, "2026-06-01 12:03:00", nil,
		"app", "Users", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"value":"topsecret-case"}`))
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		AllowTables:  []ext.TableRef{{Schema: "app", Table: "users"}},
		AllowColumns: []ext.TableColumnRef{{Schema: "app", Table: "users", Column: "id"}, {Schema: "app", Table: "users", Column: "email"}},
	})

	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=secrets", scoped); strings.Contains(rec.Body.String(), "topsecret") {
		t.Errorf("table outside the allow list leaked to a restricted session: %s", rec.Body.String())
	}
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=Users", scoped); strings.Contains(rec.Body.String(), "topsecret-case") {
		t.Errorf("a case-variant table leaked through the allow list: %s", rec.Body.String())
	}
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=users", scoped)
	if strings.Contains(rec.Body.String(), "ssn-12345") {
		t.Errorf("column outside the allow list leaked: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "alice@example") {
		t.Errorf("allowed column email must remain: %s", rec.Body.String())
	}
	// The changed-column existence oracle is refused at the HTTP layer under
	// column-level rules (the engine's sentinel mapped to 403).
	rec = getPath(t, srv, "127.0.0.1:8090", "/api/events?schema=app&table=users&changed_column=ssn", scoped)
	if rec.Code != http.StatusForbidden {
		t.Errorf("changed_column under a column allow list = %d, want 403: %s", rec.Code, rec.Body.String())
	}
}

// TestIntegrationSessionRestrictionsSchemaListing pins the picker inventory:
// table (and schema) NAMES are filtered by the resolved scope — under
// allow-list mode the unfiltered listing would leak everything EXCEPT the
// allowed handful, the inverse of the restriction's meaning.
func TestIntegrationSessionRestrictionsSchemaListing(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	testutil.InsertEvent(t, srv.cm.boot.db, "bin.000001", 80, 120, "2026-06-01 12:02:00", nil,
		"hr", "salaries", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"amount":100}`))
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		AllowTables: []ext.TableRef{{Schema: "app", Table: "users"}},
	})

	rec := getPath(t, srv, "127.0.0.1:8090", "/api/schemas", scoped)
	if strings.Contains(rec.Body.String(), "hr") {
		t.Errorf("a schema outside the allow list surfaced in the listing: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "app") {
		t.Errorf("the allowed table's schema must be listed: %s", rec.Body.String())
	}
	rec = getPath(t, srv, "127.0.0.1:8090", "/api/schemas?schema=app", scoped)
	if strings.Contains(rec.Body.String(), "secrets") {
		t.Errorf("a table outside the allow list surfaced in the table picker: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "users") {
		t.Errorf("the allowed table must be listed: %s", rec.Body.String())
	}

	// Deny mode: the denied name disappears, siblings stay.
	denied := restrictedBearer(t, srv, &ext.SessionRestrictions{
		DenyTables: []ext.TableRef{{Schema: "app", Table: "secrets"}},
	})
	rec = getPath(t, srv, "127.0.0.1:8090", "/api/schemas?schema=app", denied)
	if strings.Contains(rec.Body.String(), "secrets") || !strings.Contains(rec.Body.String(), "users") {
		t.Errorf("deny filtering wrong in the table picker: %s", rec.Body.String())
	}
	// The unrestricted static token keeps the full inventory.
	rec = getPath(t, srv, "127.0.0.1:8090", "/api/schemas?schema=app", "static-tok")
	if !strings.Contains(rec.Body.String(), "secrets") {
		t.Errorf("the full-access picker must keep every table: %s", rec.Body.String())
	}
}

// TestIntegrationSessionRestrictionsMCPTokenMint pins the laundering door
// shut: a data-restricted session cannot mint the managed MCP token, because
// the token records permission grants only and would read the index through
// /mcp with the session's redaction gone.
func TestIntegrationSessionRestrictionsMCPTokenMint(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		RedactColumns: []ext.TableColumnRef{{Schema: "app", Table: "users", Column: "ssn"}},
	})
	rec := postJSON(t, srv, "/api/mcp-token", scoped, "")
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "data access policy") {
		t.Errorf("restricted mint = %d %s, want 403 naming the data policy", rec.Code, rec.Body.String())
	}
	// A profiled session is refused through the same door (the pre-existing
	// half of the hole).
	profiled := scopedBearer(t, srv, "sensitive")
	seedSensitiveProfile(t, srv)
	rec = postJSON(t, srv, "/api/mcp-token", profiled, "")
	if rec.Code != http.StatusForbidden {
		t.Errorf("profiled mint = %d, want 403", rec.Code)
	}
}

// TestIntegrationSessionRestrictionsVerifyStatus pins the fourth verify verb:
// status carries the per-table verdict inventory, so it refuses a restricted
// session like trigger/explain/history do — with the policy answer, not the
// not-enabled one.
func TestIntegrationSessionRestrictionsVerifyStatus(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		DenyTables: []ext.TableRef{{Schema: "app", Table: "secrets"}},
	})
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/servers/default/verify", scoped)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "access-control profile") {
		t.Errorf("restricted verify status = %d %s, want 403 with the policy refusal", rec.Code, rec.Body.String())
	}
}

// TestIntegrationSessionRestrictionsRecoverWarns pins the write-side honesty:
// a reversal generated from column-redacted rows says so, because redacted
// values land in the script as NULL.
func TestIntegrationSessionRestrictionsRecoverWarns(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		AllowTables:  []ext.TableRef{{Schema: "app", Table: "users"}},
		AllowColumns: []ext.TableColumnRef{{Schema: "app", Table: "users", Column: "id"}},
	})
	rec := postJSON(t, srv, "/api/recover", scoped, `{"schema":"app","table":"users"}`)
	if rec.Code != 200 {
		t.Fatalf("recover = %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "hides some column values") {
		t.Errorf("a script built from redacted rows must warn about the NULLs it writes: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "ssn-12345") {
		t.Errorf("redacted value leaked into the reversal script: %s", rec.Body.String())
	}
}

// TestIntegrationSessionRestrictionsGatesRawSurfaces pins that a restrictions-
// carrying session is refused the baseline-reading surfaces exactly as a
// profiled one is: those reads bypass redaction, so serving them would make
// the restriction a fiction.
func TestIntegrationSessionRestrictionsGatesRawSurfaces(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		RedactColumns: []ext.TableColumnRef{{Schema: "app", Table: "users", Column: "ssn"}},
	})

	for _, path := range []string{
		"/api/reconstruct?schema=app&table=users&pk=1",
		"/api/baselines",
	} {
		if rec := getPath(t, srv, "127.0.0.1:8090", path, scoped); rec.Code != http.StatusForbidden {
			t.Errorf("restricted session GET %s = %d, want 403: %s", path, rec.Code, rec.Body.String())
		}
	}
}
