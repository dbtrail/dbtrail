package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/ext"
)

// maxLoginBody bounds the login/change-password request bodies; both are a
// few short strings.
const maxLoginBody = 4 << 10

// clientIP returns the host part of RemoteAddr. X-Forwarded-For is never
// consulted — it is attacker-controlled.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// isJSONContentType reports whether the Content-Type header names the
// application/json media type (parameters ignored). Shared by the pre-auth
// requireJSONBody gate and the cookie-auth CSRF belt (csrfMarkerOK) — one
// parse, so the two defenses cannot drift on what counts as JSON.
func isJSONContentType(ct string) bool {
	mt, _, _ := strings.Cut(ct, ";")
	return strings.TrimSpace(mt) == "application/json"
}

// requireJSONBody enforces Content-Type: application/json and a size cap on
// the pre-auth endpoints. The Content-Type check is a CSRF defense in its own
// right: an HTML form can only submit urlencoded/multipart/text-plain, so a
// cross-site form POST can never reach the login verifier.
func requireJSONBody(w http.ResponseWriter, r *http.Request) bool {
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		writeJSONError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxLoginBody)
	return true
}

// handleAuthInfo serves GET /api/auth — unauthenticated (root mux, like
// healthz; hostGuard still applies). It reveals whether password login exists
// (which the login form's existence would reveal anyway) and whether the
// console is in first-run setup, so the SPA shows the create-password screen.
// The file is statted per request so `user set-password` against a live server
// lights the login form up without a restart.
func (s *Server) handleAuthInfo(w http.ResponseWriter, r *http.Request) {
	info := map[string]any{
		"password_login": s.passwordLoginEnabled(),
		"setup":          s.setupAllowed(),
	}
	// An installed external auth provider adds the login screen's
	// "Continue with <name>" entry: its label and the initiation URL
	// (extAuthPrefix + "start" by the ext.ConsoleAuthProvider contract).
	// Both fields are absent when no provider is installed.
	if p := ext.ConsoleAuth(); p != nil {
		info["sso_name"] = p.DisplayName()
		info["sso_start"] = extAuthPrefix + "start"
	}
	writeJSON(w, http.StatusOK, info)
}

// extSessionIssuer adapts the session store to the ext.ConsoleSessionIssuer
// contract: the installed provider calls it after verifying an identity, and
// the returned token is a normal console session (same TTLs, same revocation).
// The session cookie is set on the provider's in-flight response here — the
// issue call, not the delivery mechanism, is what makes a login — so external
// logins extend to new tabs exactly like password logins do. The identity is
// logged for the operator's audit trail and never stored.
func (s *Server) extSessionIssuer() ext.ConsoleSessionIssuer {
	return func(w http.ResponseWriter, identity string, policy *ext.AccessPolicy) (string, time.Time, error) {
		token, expires, err := s.sessions.IssueWithPolicy(identity, policy)
		if err != nil {
			return "", time.Time{}, err
		}
		setSessionCookie(w, token, expires)
		slog.Info("console external-auth login", "identity", identity)
		return token, expires, nil
	}
}

// passwordLoginEnabled re-checks the auth file at call time. A corrupt file
// reads as enabled: login attempts against it fail loud (LoadAuthFile error)
// rather than silently downgrading the console to token-only auth.
func (s *Server) passwordLoginEnabled() bool {
	// An installed credential backend serves the login form on its own — it
	// supersedes the auth file, so its presence alone enables password login
	// (and, via setupAllowed, closes first-run setup: a backend is a credential).
	if ext.ConsoleCredential() != nil {
		return true
	}
	if s.authPath == "" {
		return false
	}
	a, err := LoadAuthFile(s.authPath)
	return err != nil || a != nil
}

// setupAllowed reports whether first-run setup is open: no static token, no
// password yet, and either a loopback bind or an explicit AllowSetup assertion
// (the compose case — 0.0.0.0 inside the container, host-loopback published).
// Re-evaluated live (passwordLoginEnabled stats the file), so the moment a
// password exists — set in the browser or via the CLI — the unauthenticated
// /api/auth/setup endpoint self-disables. The loopback-or-AllowSetup gate is
// the load-bearing guard: an unauthenticated set-password endpoint is only
// safe where reaching it already implies trusted (local) access.
func (s *Server) setupAllowed() bool {
	return s.token == "" && !s.passwordLoginEnabled() && (isLoopbackAddr(s.listen) || s.allowSetup)
}

// handleSetup serves POST /api/auth/setup — the first-run, unauthenticated
// password creation. It is gated by setupAllowed() (a loopback bind, OR a
// non-loopback bind the operator asserted is access-controlled via
// AllowSetup — the compose case) and self-disables the instant a password
// exists.
func (s *Server) handleSetup(w http.ResponseWriter, r *http.Request) {
	if !s.setupAllowed() {
		// Either a credential already exists, or this bind is neither loopback
		// nor marked --allow-setup. Creating the password is then a CLI
		// (`user set-password`) or authenticated (change-password) action.
		writeJSONError(w, http.StatusForbidden, "first-run setup is not available (a credential is already configured, or this bind is neither loopback nor marked --allow-setup)")
		return
	}
	if !requireJSONBody(w, r) {
		return
	}
	ip := clientIP(r)
	if ok, retry := s.loginLimiter.Allow(ip); !ok {
		secs := int(retry.Seconds()) + 1
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeJSONError(w, http.StatusTooManyRequests, fmt.Sprintf("too many attempts; retry in %ds", secs))
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.loginLimiter.Fail(ip)
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeJSONError(w, status, "invalid JSON body")
		return
	}
	if err := ValidateNewPassword(req.Password); err != nil {
		s.loginLimiter.Fail(ip)
		writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if err := SetAuthPassword(s.authPath, req.Username, req.Password); err != nil {
		slog.Error("console setup write failed", "path", s.authPath, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "couldn't save the login file; check the server log")
		return
	}
	// A successful setup closes the endpoint anyway, but clear the IP's
	// counters for parity with login/change-password (Allow() above only
	// throttles when Fail() has populated the per-IP windows).
	s.loginLimiter.Success(ip)
	token, expires, err := s.sessions.IssueWithPolicy(req.Username, nil)
	if err != nil {
		slog.Error("console session issue failed after setup", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "password set, but couldn't sign you in automatically; please sign in")
		return
	}
	slog.Info("console password created via first-run setup", "remote", ip)
	setSessionCookie(w, token, expires)
	writeJSON(w, http.StatusOK, map[string]string{
		"token":      token,
		"expires_at": expires.UTC().Format(time.RFC3339),
	})
}

// handleLogin serves POST /api/auth/login — unauthenticated, rate-limited,
// bcrypt-verified. Success mints an in-memory session token the SPA uses as
// its Bearer credential.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !requireJSONBody(w, r) {
		return
	}
	ip := clientIP(r)
	// Rate-limit check FIRST: a throttled attacker gets a cheap 429 before
	// any file read or bcrypt work.
	if ok, retry := s.loginLimiter.Allow(ip); !ok {
		secs := int(retry.Seconds()) + 1
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeJSONError(w, http.StatusTooManyRequests, fmt.Sprintf("too many attempts; retry in %ds", secs))
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeJSONError(w, status, "invalid JSON body")
		return
	}

	// An installed credential backend (ext seam) SUPERSEDES the built-in auth
	// file for login: the file is not consulted. The rate limiter, body cap, and
	// content-type gate above already ran, and the backend owns the constant-time
	// verify (its own dummy-hash-equivalent cost) — so the console only trades a
	// non-nil result for a session, and renders one uniform 401 for a nil one.
	if backend := ext.ConsoleCredential(); backend != nil {
		cred := backend.Verify(req.Username, req.Password)
		if cred == nil {
			s.loginLimiter.Fail(ip)
			// Uniform body, and the attempted username is NEVER logged (as with
			// the built-in path — a password often lands in the username field).
			slog.Warn("console login failed", "remote", ip)
			writeJSONError(w, http.StatusUnauthorized, "invalid username or password")
			return
		}
		s.loginLimiter.Success(ip)
		token, expires, err := s.sessions.IssueWithPolicy(cred.Identity, cred.Policy)
		if err != nil {
			slog.Error("console session issue failed", "error", err)
			writeJSONError(w, http.StatusInternalServerError, "something went wrong signing you in; try again")
			return
		}
		// The verified identity is the operator's audit trail (like the external
		// auth issuer); logged only on SUCCESS, where it is a real identity.
		slog.Info("console login", "remote", ip, "identity", cred.Identity)
		setSessionCookie(w, token, expires)
		writeJSON(w, http.StatusOK, map[string]string{
			"token":      token,
			"expires_at": expires.UTC().Format(time.RFC3339),
		})
		return
	}

	a, err := LoadAuthFile(s.authPath)
	if err != nil {
		slog.Error("console auth file unreadable", "path", s.authPath, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "couldn't read the web interface's saved login file; check the server log")
		return
	}
	if a == nil {
		writeJSONError(w, http.StatusForbidden, "password login is not enabled")
		return
	}

	if !verifyAndMaybeRehash(s.authPath, a, req.Username, req.Password) {
		s.loginLimiter.Fail(ip)
		// One uniform body for bad user and bad password, and the attempted
		// username is NEVER logged (passwords get typed into username fields).
		slog.Warn("console login failed", "remote", ip)
		writeJSONError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}

	s.loginLimiter.Success(ip)
	token, expires, err := s.sessions.IssueWithPolicy(a.Username, nil)
	if err != nil {
		slog.Error("console session issue failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "something went wrong signing you in; try again")
		return
	}
	slog.Info("console login", "remote", ip)
	setSessionCookie(w, token, expires)
	writeJSON(w, http.StatusOK, map[string]string{
		"token":      token,
		"expires_at": expires.UTC().Format(time.RFC3339),
	})
}

// handleLogout serves POST /api/auth/logout (authenticated). It revokes the
// presented Bearer AND the session cookie's session when they are sessions
// (usually the same one; a tab that logged in twice can hold two), and clears
// the cookie so every tab's ambient credential dies with this click — not
// just the tab that clicked. A static token is not revocable over HTTP by
// design, so logout with one only clears the cookie. Idempotent.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.sessions.Revoke(bearerToken(r))
	s.sessions.Revoke(sessionCookieToken(r))
	clearSessionCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

// handlePasswordChange serves POST /api/auth/password (authenticated,
// rate-limited — it bcrypt-verifies current_password, making it the
// privilege-escalation path for a stolen session token).
//
// Rules: with a password configured, current_password is required and
// verified no matter how the caller authenticated. With none configured
// (first set), the caller must hold the STATIC token: the token is the
// bootstrap trust root, and a session that outlived `user remove` must not
// be able to claim the new password.
func (s *Server) handlePasswordChange(w http.ResponseWriter, r *http.Request) {
	if !requireJSONBody(w, r) {
		return
	}
	ip := clientIP(r)
	if ok, retry := s.loginLimiter.Allow(ip); !ok {
		secs := int(retry.Seconds()) + 1
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeJSONError(w, http.StatusTooManyRequests, fmt.Sprintf("too many attempts; retry in %ds", secs))
		return
	}

	var req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeJSONError(w, status, "invalid JSON body")
		return
	}

	a, err := LoadAuthFile(s.authPath)
	if err != nil {
		slog.Error("console auth file unreadable", "path", s.authPath, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "couldn't read the web interface's saved login file; check the server log")
		return
	}

	switch {
	case a != nil:
		if !a.VerifyPassword(a.Username, req.CurrentPassword) {
			s.loginLimiter.Fail(ip)
			writeJSONError(w, http.StatusUnauthorized, "invalid current password")
			return
		}
	default: // first set
		if authKindFrom(r.Context()) != authKindToken {
			writeJSONError(w, http.StatusForbidden, "setting the first password requires authenticating with the access token")
			return
		}
		if req.CurrentPassword != "" {
			writeJSONError(w, http.StatusUnprocessableEntity, "no password is set; current_password must be empty")
			return
		}
	}

	if err := ValidateNewPassword(req.NewPassword); err != nil {
		writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if err := SetAuthPassword(s.authPath, "", req.NewPassword); err != nil {
		if errors.Is(err, ErrAuthFileReadOnly) {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		slog.Error("console password write failed", "path", s.authPath, "error", err)
		writeJSONError(w, http.StatusInternalServerError, "couldn't save the login file; check the server log")
		return
	}

	// Every existing session dies with the old password; the caller gets a
	// fresh one so they stay signed in.
	s.loginLimiter.Success(ip)
	s.sessions.RevokeAll()
	token, expires, err := s.sessions.Issue()
	if err != nil {
		slog.Error("console session issue failed after password change", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "password changed, but couldn't sign you in automatically; please sign in again")
		return
	}
	slog.Info("console password changed", "remote", ip)
	// The RevokeAll above also killed the session the cookie names; hand this
	// tab's cookie the fresh session so its new tabs stay signed in.
	setSessionCookie(w, token, expires)
	writeJSON(w, http.StatusOK, map[string]string{
		"token":      token,
		"expires_at": expires.UTC().Format(time.RFC3339),
	})
}
