package ext

import "slices"

// Permission is a `verb:noun` capability string. Each authenticated console
// route requires exactly one of these; a session's AccessPolicy carries the set
// it holds. The strings are the stable contract between this OSS core (which
// defines the permissions and enforces them per route) and an embedding EE build
// (which maps its own role vocabulary — admin/operator/… — onto these strings
// and attaches a policy via the session-issuer seam). The core deliberately
// knows nothing about roles: it enforces permission SETS, never named roles.
type Permission string

// The initial permission set. Adding a permission here is additive; adding a new
// authenticated /api route means classifying it against one of these (or marking
// it as needing none) in the console's route table — an unclassified route is
// refused for any policy-carrying session, never silently granted.
const (
	// PermStatusRead — read the health/status surface. The floor: a session with
	// only this (and PermServersRead) sees operational state but no row data.
	PermStatusRead Permission = "status:read"
	// PermQueryExecute — read indexed row events (the events surface and the
	// schema listing that drives it).
	PermQueryExecute Permission = "query:execute"
	// PermRecoverExecute — generate reversal SQL (recover and recover-cascade).
	PermRecoverExecute Permission = "recover:execute"
	// PermReconstructExecute — point-in-time reconstruction (time-travel).
	PermReconstructExecute Permission = "reconstruct:execute"
	// PermBaselineCreate — trigger operator maintenance actions that write no
	// customer data but change server state (baseline snapshots, verify runs).
	PermBaselineCreate Permission = "baseline:create"
	// PermServersRead — read the server registry and per-server status, which
	// includes listing the snapshots taken of a server: what copies exist is a
	// read about that server, not console administration. Listing them is on
	// this floor; DOWNLOADING one serves unredacted rows and needs
	// PermQueryExecute instead.
	PermServersRead Permission = "servers:read"
	// PermServersWrite — mutate the server registry and control-plane state
	// (create/update entries, start/stop monitoring, rotation override).
	PermServersWrite Permission = "servers:write"
	// PermServersDelete — remove a server registry entry.
	PermServersDelete Permission = "servers:delete"
	// PermSettingsRead — reach the settings/administration surfaces (the storage
	// listing, the backup settings, telemetry opt-out, the managed MCP token) and
	// READ an installed settings panel's data routes. It does NOT carry the
	// snapshot listings; those are a read about a server — see PermServersRead.
	PermSettingsRead Permission = "settings:read"
	// PermSettingsWrite — MUTATE through a settings surface. Split from
	// PermSettingsRead because an administration panel (ConsoleSettingsProvider)
	// can change configuration for everyone, so a session that may inspect the
	// settings surface must not thereby be able to rewrite it — the read-only
	// auditor role is the case this exists for.
	PermSettingsWrite Permission = "settings:write"
	// PermExtViewRead — reach an installed extension view's data routes.
	PermExtViewRead Permission = "extview:read"
)

// allPermissions is every permission the core defines, in a stable order. The
// console iterates it to report a session's effective grants to the SPA for UI
// gating; tests iterate it to guard against typos in the route table.
var allPermissions = []Permission{
	PermStatusRead,
	PermQueryExecute,
	PermRecoverExecute,
	PermReconstructExecute,
	PermBaselineCreate,
	PermServersRead,
	PermServersWrite,
	PermServersDelete,
	PermSettingsRead,
	PermSettingsWrite,
	PermExtViewRead,
}

// AllPermissions returns a copy of every permission the core defines. Callers
// may not mutate the returned slice's effect on the package (it is a fresh copy).
func AllPermissions() []Permission {
	out := make([]Permission, len(allPermissions))
	copy(out, allPermissions)
	return out
}

// TableRef names one table by schema and table name.
type TableRef struct{ Schema, Table string }

// TableColumnRef names one column by schema, table and column name.
type TableColumnRef struct{ Schema, Table, Column string }

// SessionRestrictions scopes what ROW DATA a session reads, attached directly to
// its AccessPolicy by the auth provider that minted it (#1449). It is the
// label-free sibling of the Profile name: a provider whose authorization source
// already knows the concrete tables and columns can say so here, without first
// materializing flag/profile/access labels into every server's index.
//
// Enforcement is the console's, in the SAME pass that applies session profiles,
// with the same consequences: deny/allow at the query layer, column values
// nulled in row images, query_text/query_hash withheld (#699), archives off,
// and every surface that cannot honor per-column redaction refused outright. A
// session carrying a non-empty restriction set counts as restricted exactly as
// a profiled session does.
//
// Deny composes over allow: a table both allowed and denied yields nothing.
// AllowTables non-empty switches the session to allow-list mode — tables not
// listed are withheld — so a provider expressing "only these tables" does not
// need to enumerate the universe it wants excluded.
type SessionRestrictions struct {
	// DenyTables withholds every row event of these tables.
	DenyTables []TableRef
	// RedactColumns nulls these column values in returned row images.
	RedactColumns []TableColumnRef
	// AllowTables, when non-empty, withholds every table NOT listed.
	AllowTables []TableRef
	// AllowColumns, for each table appearing in it, nulls every column of that
	// table's row images NOT listed for it. It scopes COLUMNS of the tables
	// it names and does not by itself withhold other tables — a provider
	// wanting table-level allow-listing sets AllowTables as well (the usual
	// pairing: AllowTables says which tables, AllowColumns narrows within
	// them).
	AllowColumns []TableColumnRef
}

// Empty reports whether r restricts nothing. Nil-safe, so callers can hold a
// nil *SessionRestrictions and never branch on it.
func (r *SessionRestrictions) Empty() bool {
	return r == nil ||
		(len(r.DenyTables) == 0 && len(r.RedactColumns) == 0 &&
			len(r.AllowTables) == 0 && len(r.AllowColumns) == 0)
}

// AccessPolicy is the optional authorization an external auth provider attaches
// to the console session it mints (see ConsoleSessionIssuer). It has three
// dimensions; the first gates ROUTES, the other two scope DATA:
//
//   - Permissions — the capability strings this session holds, gating which
//     routes it may call. Enforced by the OSS core, per route.
//   - Profile — the name of an existing RBAC data profile (the profiles/flags/
//     access_rules the `bintrail flag|profile|access` commands manage), scoping
//     what DATA the session sees. This struct only CARRIES the name; enforcing it
//     (deny/redact, archive and reconstruct gates) is a separate concern the
//     console wires per request.
//   - Restrictions — direct table/column restrictions carried by the policy
//     itself (#1449), for providers whose rules do not live in the index as
//     labels. Enforced alongside Profile; both apply when both are set.
//
// A nil *AccessPolicy means "no policy" — a full-access session, exactly what the
// password login and the static token mint today. The OSS build never constructs
// one, so its behavior is unchanged; only an installed provider (an EE build)
// attaches a policy, and only then does per-route enforcement bite.
type AccessPolicy struct {
	// Permissions is the set this session holds. Order is irrelevant; duplicates
	// are harmless. An empty (but non-nil policy) grants nothing beyond the
	// permission-free routes.
	Permissions []Permission
	// Profile is the data-profile name to enforce, or "" for none.
	Profile string
	// Restrictions are direct data restrictions, or nil for none.
	Restrictions *SessionRestrictions
}

// DataRestricted reports whether the policy scopes what data the session sees —
// a data profile name, direct restrictions, or both. It is the predicate the
// console's raw-data surface gates key on; nil-safe (no policy restricts
// nothing).
func (p *AccessPolicy) DataRestricted() bool {
	return p != nil && (p.Profile != "" || !p.Restrictions.Empty())
}

// Allows reports whether the policy grants p. A nil policy allows everything (no
// policy = full access), so callers may invoke it on the nil value.
func (p *AccessPolicy) Allows(perm Permission) bool {
	if p == nil {
		return true
	}
	return slices.Contains(p.Permissions, perm)
}
