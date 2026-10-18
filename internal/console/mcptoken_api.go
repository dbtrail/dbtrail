package console

import (
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/ext"
)

// mcpTokenStatusDTO is the wire view of the MCP-token configuration. Values
// are never serialized — only presence, provenance, and (for the managed
// token) the creation time the UI renders.
type mcpTokenStatusDTO struct {
	// Static: a token was supplied via --token / BINTRAIL_CONSOLE_TOKEN. It
	// is environment-owned — the console cannot rotate or revoke it.
	Static bool `json:"static"`
	// Managed: a UI-generated token exists (only its SHA-256 is stored).
	Managed   bool   `json:"managed"`
	CreatedAt string `json:"created_at,omitempty"`
	// ReadOnly: the on-disk token file was written by a newer bintrail —
	// the token authenticates, but generate/rotate/revoke refuse.
	ReadOnly bool `json:"read_only,omitempty"`
	// LastUsedAt / StaticLastUsedAt: when the managed (the current value
	// only) and the static token last made an /mcp request that reached a
	// server. Kept in memory, so they are empty again after a restart;
	// CountingSince is when this process started counting.
	LastUsedAt       string `json:"last_used_at,omitempty"`
	StaticLastUsedAt string `json:"static_last_used_at,omitempty"`
	CountingSince    string `json:"counting_since,omitempty"`
}

func (s *Server) mcpTokenStatus() mcpTokenStatusDTO {
	createdAt, readOnly, configured, used := s.managedTok.info()
	st := mcpTokenStatusDTO{
		Static:        s.token != "",
		Managed:       configured,
		CreatedAt:     createdAt,
		ReadOnly:      readOnly,
		CountingSince: startedAtWire(s.startedAt),
	}
	if !used.IsZero() {
		st.LastUsedAt = used.UTC().Format(time.RFC3339)
	}
	if n := s.mcpStaticUsedAt.Load(); n != 0 && s.token != "" {
		st.StaticLastUsedAt = time.Unix(0, n).UTC().Format(time.RFC3339)
	}
	return st
}

// handleMCPTokenGet reports whether tokens are configured — never their
// values. Behind tokenMiddleware like every /api route.
func (s *Server) handleMCPTokenGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.mcpTokenStatus())
}

// mcpTokenMutateMu serializes the persist+swap pair in the generate and
// revoke handlers, so a caller's response always describes the final
// persisted state (persist A / persist B / swap B / swap A would otherwise
// briefly leave memory=A while disk=B; the per-check re-read heals that on
// the next request, but the interleaved responses would still lie).
var mcpTokenMutateMu sync.Mutex

// handleMCPTokenGenerate mints (or rotates) the managed MCP token: persists
// its SHA-256 to the token file and swaps the in-memory credential so the new
// value authenticates immediately, no restart. The response is the ONE place
// the plaintext ever appears — it is not stored and cannot be re-displayed.
//
// Any authenticated caller may generate/rotate: reaching this handler already
// required the static token or a login session (the managed token itself is
// /mcp-only and cannot reach this route), and the minted credential grants
// strictly less — the read-only MCP tools, capped at the minter's own
// permission grants (#1124): the minting session's policy is recorded into
// the token file and re-checked per tool on /mcp dispatch, so a session that
// could not call /api/events, /api/recover or /api/reconstruct cannot mint a
// token that reaches the same data through the MCP door. A full-access
// minter (nil policy — the static token, a password login, every OSS
// session) records no cap.
func (s *Server) handleMCPTokenGenerate(w http.ResponseWriter, r *http.Request) {
	// A DATA-restricted session (profile or policy restrictions, #1449) is
	// refused outright: the token file records permission grants only, so a
	// minted token would read the same index through /mcp with the session's
	// redaction gone — the permission cap below cannot express a data scope.
	// Same posture as every surface that cannot honor redaction.
	if pol := policyFrom(r.Context()); pol.DataRestricted() {
		recordProfileGateDeny(r, "mcp-token")
		writeJSONError(w, http.StatusForbidden,
			"an MCP token cannot be generated from a session with a data access policy: the token would read the index without your session's redaction. Ask an operator without data restrictions to manage the MCP token.")
		return
	}
	mcpTokenMutateMu.Lock()
	defer mcpTokenMutateMu.Unlock()
	var grants []ext.Permission
	if pol := policyFrom(r.Context()); pol != nil {
		// Non-nil even when the policy holds zero permissions: an empty
		// recorded list grants nothing, which is exactly what an empty policy
		// could do itself.
		grants = make([]ext.Permission, 0, len(pol.Permissions))
		grants = append(grants, pol.Permissions...)
	}
	token, f, err := GenerateMCPToken(s.mcpTokenPath, grants)
	if err != nil {
		if errors.Is(err, ErrMCPTokenFileReadOnly) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.managedTok.reload(f)
	slog.Info("console: managed MCP token generated", "path", s.mcpTokenPath)
	writeJSON(w, http.StatusOK, struct {
		Token     string `json:"token"`
		CreatedAt string `json:"created_at"`
	}{Token: token, CreatedAt: f.CreatedAt})
}

// handleMCPTokenRevoke deletes the managed token (file and in-memory). The
// static environment token, if any, is untouched — it is not ours to revoke.
func (s *Server) handleMCPTokenRevoke(w http.ResponseWriter, r *http.Request) {
	mcpTokenMutateMu.Lock()
	defer mcpTokenMutateMu.Unlock()
	if err := RevokeMCPToken(s.mcpTokenPath); err != nil {
		if errors.Is(err, ErrMCPTokenFileReadOnly) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.managedTok.reload(nil)
	slog.Info("console: managed MCP token revoked", "path", s.mcpTokenPath)
	writeJSON(w, http.StatusOK, s.mcpTokenStatus())
}
