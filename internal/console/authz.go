package console

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/dbtrail/dbtrail/ext"
)

// This file is the per-session authorization layer (issue #1074). It is inert in
// the OSS build: the built-in credentials mint policy-less (nil) sessions, and a
// nil policy is full access — so authzMiddleware waves every request through and
// the reported permission set is complete. Only an EE build that attaches an
// ext.AccessPolicy to a session (via the session-issuer seam) makes any of this
// bite. Enforcement is by PERMISSION, never by role: the core has no role
// vocabulary; an embedding build maps its roles onto these permission strings.

// permAny marks a route that any authenticated session may call regardless of
// its policy — the capabilities oracle the SPA needs to gate its own UI, and the
// session's own auth self-management (logout, change-password). It is the empty
// permission on purpose: a route MUST be listed either with a real permission or
// with permAny, never omitted (an omitted route is refused for policy sessions —
// see authzMiddleware).
const permAny ext.Permission = ""

// routePerm binds one authenticated /api route to the permission it requires.
// method is the HTTP method; pattern is the route's path with "{}" standing in
// for a single path segment (an id). Matching is segment-based with an exact
// segment-count requirement, so a pattern with a placeholder can never match a
// path of a different depth — a sibling route never silently inherits a parent's
// permission via prefix matching.
type routePerm struct {
	method  string
	pattern string
	perm    ext.Permission
}

// apiRoutePerms is the authoritative route→permission table, consulted on every
// policy-carrying /api request. ORDER MATTERS: matching is first-match-wins, so
// a route with a literal segment where another has a placeholder must be listed
// FIRST (e.g. POST /api/servers/test before a hypothetical POST /api/servers/{}).
// TestRouteTableCompleteness pins that every registered /api route appears here;
// TestRoutePermFirstMatchWins pins the ordering invariant. The /api/ext/ and
// /api/ext-settings/ subtrees are NOT here — they are matched by prefix in
// permForRoute (their depth is unbounded).
//
// Permission tiers (an EE build's roles map onto these): status:read and
// servers:read are the read-only floor; query/reconstruct read data; recover and
// baseline are operator actions; servers:write/delete and settings:read are the
// administrative surface.
var apiRoutePerms = []routePerm{
	// Status + data reads. coverage is metadata about the index — timestamps
	// and verdicts, plus a table-name half it self-gates on sessionRestricted —
	// so it sits with status on the read-only floor. (It was registered in
	// server.go but never classified here, so every policy-carrying session got
	// a 403 on the Overview's own coverage card; the drift went unseen because
	// registeredAPIPatterns, the completeness test's mirror, was missing it too.)
	//
	// activity does NOT sit there: it aggregates the indexed row data and
	// reports which tables changed and how often. The giveaway is that its
	// handler has to apply the profile's DenyTables to be safe — an endpoint
	// needing data-profile deny rules is not a health read. It is tiered with
	// query execution, and costs a caller nothing extra: the page that renders
	// it also reads /api/events.
	{"GET", "/api/status", ext.PermStatusRead},
	{"GET", "/api/coverage", ext.PermStatusRead},
	{"GET", "/api/capacity", ext.PermStatusRead},
	{"GET", "/api/activity", ext.PermQueryExecute},
	{"GET", "/api/events", ext.PermQueryExecute},
	// DDL history (#1443) is tiered with the events browser: it names tables
	// and serves the statements that shaped them, and its handler applies
	// the same deny/allow table scope — a read that needs data-profile rules
	// to be safe is not a status read.
	{"GET", "/api/schema-changes", ext.PermQueryExecute},
	{"GET", "/api/schemas", ext.PermQueryExecute},
	{"GET", "/api/reconstruct", ext.PermReconstructExecute},
	// The SQL panel reads the same indexed row data the events surface does —
	// free-form, so it is tiered with query execution, and its own handler
	// additionally refuses profiled sessions (redaction cannot reach it).

	// Recovery (reversal-SQL generation; never executes).
	{"POST", "/api/recover", ext.PermRecoverExecute},
	{"POST", "/api/recover-cascade", ext.PermRecoverExecute},

	// Acknowledging a capture-skip tally (#1314) is tiered with the other
	// actions on that box (servers:write). It writes no data and undoes no
	// loss — it retires an alarm for every viewer of this server, which is a
	// control-plane-shaped consequence even though the write is one column.
	{"POST", "/api/capture-skips/ack", ext.PermServersWrite},

	// Operator maintenance actions on a specific server. verify has no dedicated
	// permission; it is an operator integrity action, tiered with baseline:create.
	{"POST", "/api/servers/{}/baseline", ext.PermBaselineCreate},
	{"POST", "/api/servers/{}/baseline/restore", ext.PermBaselineCreate},
	{"POST", "/api/servers/{}/sql-export", ext.PermBaselineCreate},
	{"POST", "/api/servers/{}/verify", ext.PermBaselineCreate},
	// Refreshing the schema snapshot STOPS AND RESTARTS that server's capture
	// stream, so it is tiered with the monitor verbs below (servers:write), not
	// with baseline/verify. Those are maintenance actions that leave capture
	// alone; this one can leave capture down if the restart fails, which is
	// strictly the control-plane capability servers:write exists to gate.

	// Server registry + control-plane. List/get/status/verify-read and the
	// write-free test probe are reads; create/update/delete/monitor are writes.
	// Literal-segment routes precede the {} ones at the same depth.
	{"POST", "/api/servers/test", ext.PermServersRead},
	{"GET", "/api/servers/{}/monitor", ext.PermServersRead},
	{"POST", "/api/servers/{}/monitor/start", ext.PermServersWrite},
	{"POST", "/api/servers/{}/monitor/stop", ext.PermServersWrite},
	// See the note above the maintenance block: this one restarts capture.
	{"POST", "/api/servers/{}/schema-snapshot", ext.PermServersWrite},
	{"GET", "/api/servers/{}/baseline", ext.PermServersRead},
	{"GET", "/api/servers/{}/baseline/restore", ext.PermServersRead},
	{"GET", "/api/servers/{}/sql-export", ext.PermServersRead},
	// The dump download is a full unredacted copy of every row — the
	// row-data tier, exactly like /api/baselines/download.
	{"GET", "/api/servers/{}/sql-export/download", ext.PermQueryExecute},
	{"GET", "/api/servers/{}/schema-snapshot", ext.PermServersRead},
	{"GET", "/api/servers/{}/verify", ext.PermServersRead},
	{"GET", "/api/servers/{}/verify/explain", ext.PermServersRead},
	{"GET", "/api/servers/{}/verify/history", ext.PermServersRead},
	{"POST", "/api/servers/{}/test", ext.PermServersRead},
	{"GET", "/api/servers/{}", ext.PermServersRead},
	{"PUT", "/api/servers/{}", ext.PermServersWrite},
	{"DELETE", "/api/servers/{}", ext.PermServersDelete},
	{"GET", "/api/servers", ext.PermServersRead},
	{"POST", "/api/servers", ext.PermServersWrite},

	// Settings / administration. rotation PUT is a control-plane config write;
	// the storage/baseline listings, telemetry opt-out, and managed MCP token are
	// the settings surface.
	{"GET", "/api/rotation", ext.PermSettingsRead},
	{"PUT", "/api/rotation", ext.PermServersWrite},
	// The Backup settings page (#1582): the GET is the same
	// settings tier as /api/rotation; the PUT edits a registry entry, so it
	// carries the servers-write tier the entry's own editor does.
	{"GET", "/api/backup-settings", ext.PermSettingsRead},
	{"PUT", "/api/backup-settings/servers/{}", ext.PermServersWrite},
	// Baseline refresh, graded exactly like rotation: reading the effective
	// policy is a settings read, changing what the daemon's loop does is a
	// control-plane write.
	{"GET", "/api/baseline-refresh", ext.PermSettingsRead},
	{"PUT", "/api/baseline-refresh", ext.PermServersWrite},
	{"GET", "/api/baselines", ext.PermSettingsRead},
	// The per-server backup schedule (#1442) is a control-plane setting like
	// the rotation and refresh overrides: what it changes is what the daemon's
	// loop does, so writing it is servers:write, not baseline:create. Its
	// state rides on GET /api/baselines above.
	{"PUT", "/api/servers/{}/backup-schedule", ext.PermServersWrite},
	{"DELETE", "/api/servers/{}/backup-schedule", ext.PermServersWrite},
	// The per-snapshot files listing is metadata (names, sizes, timestamps),
	// same tier as the listing above. The DOWNLOAD is not: it is a full
	// unredacted copy of every baseline row, so it takes the row-data
	// permission — tiering it with the settings surface would make
	// settings:read a data-exfiltration path.
	{"GET", "/api/baselines/files", ext.PermSettingsRead},
	{"GET", "/api/baselines/download", ext.PermQueryExecute},
	{"GET", "/api/storage", ext.PermSettingsRead},
	// Data-profile NAMES on the selected server — access-control vocabulary
	// for administration panels (settings-surface pickers), not row data.
	{"GET", "/api/profiles", ext.PermSettingsRead},
	// Authoring the access profiles themselves (#1445): reading the
	// configuration is the settings surface; changing it is settings:write,
	// the permission split off settings:read precisely so an auditor who may
	// inspect administration cannot rewrite it. Literal-segment routes only,
	// so order among them does not matter.
	{"GET", "/api/access-profiles", ext.PermSettingsRead},
	{"POST", "/api/access-profiles/flags", ext.PermSettingsWrite},
	{"POST", "/api/access-profiles/flags/remove", ext.PermSettingsWrite},
	{"POST", "/api/access-profiles/profiles", ext.PermSettingsWrite},
	{"POST", "/api/access-profiles/profiles/remove", ext.PermSettingsWrite},
	{"POST", "/api/access-profiles/rules", ext.PermSettingsWrite},
	{"POST", "/api/access-profiles/rules/remove", ext.PermSettingsWrite},
	// views.sql is a Settings/Storage artifact: it names paths and column
	// names, never row data, so it reads like the storage panel it is offered
	// from rather than like an events read.
	//
	// With include_live=1 (#1480) it also names the INDEX ENDPOINT: host,
	// port, database and user, never the password. That is configuration, not
	// row data, so the tier and the unaudited posture do not move. Worth
	// stating plainly, though: GET /api/servers needs servers:read to show
	// the same facts, and this route needs settings:read, so a role holding
	// settings:read alone can learn the index endpoint here. Both are
	// admin-shaped tiers, and neither yields a credential.
	{"GET", "/api/views.sql", ext.PermSettingsRead},
	{"GET", "/api/telemetry", ext.PermSettingsRead},
	{"POST", "/api/telemetry", ext.PermSettingsRead},
	{"GET", "/api/mcp-token", ext.PermSettingsRead},
	{"POST", "/api/mcp-token", ext.PermSettingsRead},
	{"DELETE", "/api/mcp-token", ext.PermSettingsRead},
	// The time-travel port's address is daemon configuration shown on the
	// Connect page, beside the MCP token status: settings vocabulary, no row
	// data, and never the token that authenticates the port.
	{"GET", "/api/flashback", ext.PermSettingsRead},

	// The capabilities oracle and the session's own auth self-management: any
	// authenticated session, regardless of policy.
	{"GET", "/api/capabilities", permAny},
	{"POST", "/api/auth/logout", permAny},
	{"POST", "/api/auth/password", permAny},
}

// extAPIPrefix is the mount prefix for an installed extension view's data routes
// (see ext.ConsoleViewProvider). The whole subtree requires PermExtViewRead; its
// depth is unbounded, so it is matched by prefix rather than an apiRoutePerms
// row. rbacViewGuard additionally refuses it under an active data profile.
const extAPIPrefix = "/api/ext/"

// extSettingsAPIPrefix is the mount prefix for an installed settings panel's
// data routes (see ext.ConsoleSettingsProvider). Like the view subtree its depth
// is unbounded, so it is prefix-matched rather than given apiRoutePerms rows —
// but unlike the view subtree it is NOT wrapped in rbacViewGuard: a panel is
// handed no database, so the "a third-party handler may not honor redaction"
// reasoning that withholds the view surface under a data profile does not reach
// it, and blanket-refusing it would lock out the profile-carrying administrator
// the panel exists for. Permission is the gate here, per method (see
// extSettingsPerm).
const extSettingsAPIPrefix = "/api/ext-settings/"

// extSettingsPerm classifies a settings-panel data route by HTTP METHOD: GET and
// HEAD need settings:read, every other method needs settings:write. The core
// classifies rather than asking the provider because a provider cannot be
// trusted to declare its own routes read-only, and the method is the one thing
// the core can see without running the handler. Anything not explicitly a read
// falls to the write permission — the same fail-closed posture as an
// unclassified route, so a method nobody thought about (PATCH, a WebDAV verb)
// is never the cheap way to mutate with a read-only grant.
//
// What this CANNOT catch: a provider that mutates state on GET. That is a bug in
// the provider, invisible to the core, and the seam's doc comment says so.
func extSettingsPerm(method string) ext.Permission {
	if method == http.MethodGet || method == http.MethodHead {
		return ext.PermSettingsRead
	}
	return ext.PermSettingsWrite
}

// permForRoute returns the permission a (method, path) requires and whether the
// route is classified at all. An unclassified route (classified=false) is a
// programming error — every registered /api route must appear in apiRoutePerms
// or under one of the ext prefixes — and authzMiddleware fails it CLOSED for a
// policy session rather than granting it.
func permForRoute(method, path string) (perm ext.Permission, classified bool) {
	// Checked before extAPIPrefix would be: "/api/ext-settings/" does not share
	// that prefix today ("/api/ext/" ends in a slash), but the two live one
	// character apart, so ordering them explicitly keeps a future edit to either
	// constant from silently routing every settings write through the view
	// permission.
	if strings.HasPrefix(path, extSettingsAPIPrefix) {
		return extSettingsPerm(method), true
	}
	if strings.HasPrefix(path, extAPIPrefix) {
		return ext.PermExtViewRead, true
	}
	segs := strings.Split(strings.Trim(path, "/"), "/")
	for _, rp := range apiRoutePerms {
		if rp.method == method && segmentsMatch(rp.pattern, segs) {
			return rp.perm, true
		}
	}
	return permAny, false
}

// segmentsMatch reports whether path segments segs match a pattern, where a "{}"
// pattern segment matches any single non-empty segment and every other segment
// must be equal. The segment counts must be identical — the exact-depth rule that
// stops a placeholder pattern from matching a longer or shorter sibling path.
func segmentsMatch(pattern string, segs []string) bool {
	ps := strings.Split(strings.Trim(pattern, "/"), "/")
	if len(ps) != len(segs) {
		return false
	}
	for i, p := range ps {
		if p == "{}" {
			if segs[i] == "" {
				return false
			}
			continue
		}
		if p != segs[i] {
			return false
		}
	}
	return true
}

// authzMiddleware enforces the session's access policy on every wrapped /api
// request. It runs INSIDE tokenMiddleware, so the request is already
// authenticated and any policy is already on the context.
//
//   - No policy (nil) → full access: the static token, the password login, and
//     every OSS session land here, so OSS behavior is unchanged.
//   - A policy-carrying session → the route's permission is looked up; a route
//     with no classification is refused (fail closed), a permAny route is always
//     allowed, and otherwise the policy must grant the permission or the request
//     gets a 403 that names the missing permission (never a silent empty result).
func (s *Server) authzMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pol := policyFrom(r.Context())
		if pol == nil {
			next.ServeHTTP(w, r)
			return
		}
		perm, classified := permForRoute(r.Method, r.URL.Path)
		if !classified {
			// Every /api route must be classified; an omission must not silently
			// grant a scoped session access it was never evaluated for.
			slog.Error("console: unclassified /api route refused for a scoped session (fail closed)", "method", r.Method, "path", r.URL.Path)
			recordConsoleDeny(r, "authz.denied", "", map[string]string{"reason": "unclassified_route"})
			writeJSONError(w, http.StatusForbidden, "forbidden: this route has no authorization policy")
			return
		}
		if perm == permAny || pol.Allows(perm) {
			next.ServeHTTP(w, r)
			return
		}
		slog.Warn("console: request denied by session policy", "method", r.Method, "path", r.URL.Path, "missing_permission", string(perm))
		recordConsoleDeny(r, "authz.denied", string(perm), nil)
		writeJSONError(w, http.StatusForbidden, "forbidden: your role lacks the "+string(perm)+" permission")
	})
}

// recordConsoleDeny emits an authorization denial on the audit seam (a no-op
// with no sink installed — the OSS default). Actor is the session's verified
// login identity when known; Detail carries the route and, for a permission
// denial, the missing permission. Never blocks the response path (the sink
// contract) and never carries request bodies or row data.
func recordConsoleDeny(r *http.Request, action, missingPermission string, extra map[string]string) {
	detail := map[string]string{"method": r.Method, "path": r.URL.Path}
	if missingPermission != "" {
		detail["missing_permission"] = missingPermission
	}
	for k, v := range extra {
		detail[k] = v
	}
	ext.Record(r.Context(), ext.AuditEvent{
		Surface: "console",
		Action:  action,
		Actor:   consoleActor(r),
		Detail:  detail,
	})
}

// permissionsForPolicy reports the session's effective grant of every permission
// the core defines, for the SPA to gate its UI. A nil policy (OSS, or any
// full-access session) grants everything, so the map is all-true and the UI hides
// nothing — the same UI the console has always shown.
func permissionsForPolicy(pol *ext.AccessPolicy) map[string]bool {
	perms := ext.AllPermissions()
	m := make(map[string]bool, len(perms))
	for _, p := range perms {
		m[string(p)] = pol.Allows(p)
	}
	return m
}
