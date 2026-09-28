package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Per-session data-profile enforcement (#1075). A session may carry a data
// profile name (ext.AccessPolicy.Profile, attached by an EE build via the
// session-issuer seam — #1074). When it does, the console resolves that profile
// against the SELECTED server's index at request time and enforces it: redaction
// on the data-read paths, and a hard refusal of every surface that reads raw or
// baseline data the redaction pass cannot cover.
//
// The OSS build attaches no policy, so sessionProfileName is always "" and every
// helper here is inert — behavior is unchanged.

// sessionProfileTTL bounds how long a resolved (server, profile) rule set is
// cached. Short on purpose: a profile edited via the CLI takes effect within one
// TTL, and the rules are tiny so re-loading is cheap.
const sessionProfileTTL = 30 * time.Second

// sessionProfileName returns the data-profile name the request's session
// carries, or "" when the session has no policy or an empty profile (the OSS
// case, and any full-access session).
func sessionProfileName(r *http.Request) string {
	if p := policyFrom(r.Context()); p != nil {
		return p.Profile
	}
	return ""
}

// sessionRestrictions returns the direct table/column restrictions the
// request's session policy carries (#1449), or nil. Nil-safe on both ends —
// SessionRestrictions methods accept a nil receiver.
func sessionRestrictions(r *http.Request) *ext.SessionRestrictions {
	if p := policyFrom(r.Context()); p != nil {
		return p.Restrictions
	}
	return nil
}

// sessionRestricted reports whether the request's session scopes what data it
// may read — a data profile, direct policy restrictions (#1449), or both. It
// is the single predicate the raw/baseline-data gates key on (reconstruct,
// baselines, recover-cascade, verify, extension views): those surfaces cannot
// honor per-column redaction, so a restricted session is refused outright
// rather than shown unredacted data.
func sessionRestricted(r *http.Request) bool {
	return policyFrom(r.Context()).DataRestricted()
}

// rbacActiveFor folds the request's session profile into the process-global
// rbacActive() signal. Gate sites that refused a surface under a startup profile
// (recover-cascade, verify, reconstruct/verify capabilities) call this so they
// refuse it for a profiled session too. Inert in OSS: sessionRestricted is false,
// so it equals rbacActive().
func (s *Server) rbacActiveFor(r *http.Request) bool {
	return s.rbacActive() || sessionRestricted(r)
}

// profileActiveFor folds the request's session profile into the process-global
// profileActive signal (a NAMED profile was supplied). The extension-view guard
// and the extension-view capability suppression key on this, so a profiled
// session is refused/hidden the raw-*sql.DB extension surface exactly as a
// startup profile is. Inert in OSS.
func (s *Server) profileActiveFor(r *http.Request) bool {
	return s.profileActive || sessionRestricted(r)
}

// selectedID returns the server-selection key for the request (the
// X-Bintrail-Server header, or the default). It is the per-server component of
// the profile-rule cache key: the SAME profile name can resolve to DIFFERENT
// rules on different servers' indexes, so the cache must not share entries
// across servers.
func (s *Server) selectedID(r *http.Request) string {
	id := r.Header.Get(serverHeader)
	if id == "" {
		id = s.cm.defaultID()
	}
	return id
}

// writeSessionProfileError maps an applySessionProfile failure to a response: a
// nonexistent profile is a 403 with an actionable message (the session's profile
// is misconfigured — never silently enforce nothing); anything else is a logged
// 500.
func writeSessionProfileError(w http.ResponseWriter, r *http.Request, err error) {
	var nf *profileNotFoundError
	if errors.As(err, &nf) {
		recordConsoleDeny(r, "profile.denied", "", map[string]string{"reason": "profile_not_found", "profile": nf.profile})
		writeJSONError(w, http.StatusForbidden, nf.Error())
		return
	}
	slog.Error("console: session profile enforcement failed", "error", err)
	writeJSONError(w, http.StatusInternalServerError, "couldn't apply your access profile; check the server log")
}

// recordProfileGateDeny audits a raw/baseline-data surface refused because the
// session carries a data profile (reconstruct, baselines, recover-cascade,
// verify, extension views). No-op with no sink installed.
func recordProfileGateDeny(r *http.Request, surface string) {
	recordConsoleDeny(r, "profile.denied", "", map[string]string{"reason": "unredactable_surface", "surface_gate": surface})
}

// profileNotFoundError signals that a session's data profile does not exist on
// the selected server's index. Handlers map it to 403 — never "enforce nothing"
// on a typo (the fail-loud posture the CLI/startup check takes, #838).
type profileNotFoundError struct{ profile string }

func (e *profileNotFoundError) Error() string {
	return fmt.Sprintf("the data profile %q is not defined on this server", e.profile)
}

// profileRuleEntry is one cached (server, profile) resolution.
type profileRuleEntry struct {
	deny     []query.SchemaTable
	redact   []query.SchemaTableColumn
	exists   bool
	loadedAt time.Time
}

// profileRuleCache caches resolved profile rules per (server, profile) with a
// short TTL, so per-request enforcement does not re-query the index on every
// events/recover call. Safe for concurrent use.
type profileRuleCache struct {
	mu  sync.Mutex
	m   map[string]profileRuleEntry
	now func() time.Time // injectable for tests
}

func newProfileRuleCache() *profileRuleCache {
	return &profileRuleCache{m: make(map[string]profileRuleEntry), now: time.Now}
}

// load resolves the deny/redact rules for profile on db (the selected server's
// index), caching the result under key for sessionProfileTTL. A cache entry
// records existence too, so a nonexistent profile is cached (and re-checked on
// expiry) rather than re-probed every request.
func (c *profileRuleCache) load(ctx context.Context, key string, db *sql.DB, profile string) (profileRuleEntry, error) {
	now := c.now()
	c.mu.Lock()
	if e, ok := c.m[key]; ok && now.Sub(e.loadedAt) < sessionProfileTTL {
		c.mu.Unlock()
		return e, nil
	}
	c.mu.Unlock()

	// Resolve outside the lock (a DB round trip). A concurrent miss on the same
	// key resolves twice; harmless (same result) and simpler than single-flight.
	exists, err := query.ProfileExists(ctx, db, profile)
	if err != nil {
		return profileRuleEntry{}, err
	}
	e := profileRuleEntry{exists: exists, loadedAt: now}
	if exists {
		deny, redact, err := query.LoadProfileRules(ctx, db, profile)
		if err != nil {
			return profileRuleEntry{}, err
		}
		e.deny, e.redact = deny, redact
	}

	c.mu.Lock()
	c.m[key] = e
	c.mu.Unlock()
	return e, nil
}

// invalidate drops every cached entry for a server (all its (server, profile)
// keys), so a subsequent request re-resolves rules against that server's CURRENT
// index. The console calls it wherever it evicts a server's connection bundle —
// a DSN edit (the index may now be a DIFFERENT database) or a delete — so a
// profiled session can never be enforced with rules resolved against the old
// index against the new one (a stale under-redaction window otherwise).
func (c *profileRuleCache) invalidate(id string) {
	if c == nil {
		return
	}
	prefix := id + "\x00"
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.m {
		if strings.HasPrefix(k, prefix) {
			delete(c.m, k)
		}
	}
}

// applySessionProfile unions the request session's data restrictions onto opts,
// on top of the startup floor buildOptions already set, and forces
// ProfileActive so query_text/query_hash stay withheld (#699). Two sources,
// both applied when both are present:
//
//   - a data-profile NAME (resolved against the selected server's index at
//     request time; nonexistent → *profileNotFoundError, handler → 403);
//   - direct policy restrictions (#1449), carried by the session itself and
//     needing no index resolution.
//
// Neither present → opts unchanged.
func (s *Server) applySessionProfile(ctx context.Context, r *http.Request, b *bundle, opts query.Options) (query.Options, error) {
	profile := sessionProfileName(r)
	rest := sessionRestrictions(r)
	if profile == "" && rest.Empty() {
		return opts, nil
	}
	// Union with the startup floor: the startup profile is the floor, a session
	// profile can only narrow further. Copy rather than append-in-place so the
	// startup slices (shared across requests) are never mutated.
	opts.DenyTables = append([]query.SchemaTable(nil), opts.DenyTables...)
	opts.RedactColumns = append([]query.SchemaTableColumn(nil), opts.RedactColumns...)
	if profile != "" {
		e, err := s.sessionProfiles.load(ctx, s.selectedID(r)+"\x00"+profile, b.db, profile)
		if err != nil {
			return opts, fmt.Errorf("resolve session profile %q: %w", profile, err)
		}
		if !e.exists {
			return opts, &profileNotFoundError{profile}
		}
		opts.DenyTables = append(opts.DenyTables, e.deny...)
		opts.RedactColumns = append(opts.RedactColumns, e.redact...)
	}
	if !rest.Empty() {
		for _, t := range rest.DenyTables {
			opts.DenyTables = append(opts.DenyTables, query.SchemaTable{Schema: t.Schema, Table: t.Table})
		}
		for _, c := range rest.RedactColumns {
			opts.RedactColumns = append(opts.RedactColumns, query.SchemaTableColumn{Schema: c.Schema, Table: c.Table, Column: c.Column})
		}
		// The allow lists have no startup-floor counterpart today (nothing
		// else sets them on console options), but they get the same
		// copy-before-append discipline as the deny slices anyway: the next
		// caller who DOES pre-populate them from a shared slice must not
		// discover cross-request mutation the hard way.
		opts.AllowTables = append([]query.SchemaTable(nil), opts.AllowTables...)
		opts.AllowColumns = append([]query.SchemaTableColumn(nil), opts.AllowColumns...)
		for _, t := range rest.AllowTables {
			opts.AllowTables = append(opts.AllowTables, query.SchemaTable{Schema: t.Schema, Table: t.Table})
		}
		for _, c := range rest.AllowColumns {
			opts.AllowColumns = append(opts.AllowColumns, query.SchemaTableColumn{Schema: c.Schema, Table: c.Table, Column: c.Column})
		}
	}
	opts.ProfileActive = true
	return opts, nil
}

// fetchRestricted runs the shared cross-source fetch like fetch, but forces
// archives OFF when the request's session carries a data profile: Parquet
// archives do not run the redaction pass, so a profiled session must read live
// MySQL only (the same reason a startup profile forces --no-archive process-wide).
//
// diverged is the count of duplicate event_ids whose live-index and archive
// copies DISAGREED during the merge (#1325); the handlers put it in the
// response warnings, since a console operator never sees the server log where
// the merge already warns. The skipped-sources list FetchMergedFull also
// returns stays deliberately unused here: for these browsing endpoints a
// partially-failing archive source remains a log-only condition (the
// documented trade-off on Server.fetch), and adopting it is a separate
// decision from surfacing divergence.
//
// archivesElided reports the newest-first short-circuit (#1353): registered
// archives were deliberately not read because the live index filled the page
// and every archived hour sits below the live rotation floor — they could not
// have changed the answer. The handlers surface it as a response info note
// (archiveElisionNotes → the `notes` list, #1365) so the skip is auditable,
// per the #1311/#1321 contract that a result always says what scope was read;
// it is a note and not a warning because the skip is correctness-preserving
// ("nothing is missing"), while warnings stay for cautionary facts. The flag
// is structurally always false for a profiled session or a --no-archive
// console: there the archives are EXCLUDED (excl says so and its notice
// speaks), not elided — nothing was resolved that could have been skipped.
func (s *Server) fetchRestricted(ctx context.Context, r *http.Request, b *bundle, opts query.Options) ([]query.ResultRow, *query.QueryPlan, archiveExclusion, []string, int, bool, error) {
	return s.fetchRestrictedScoped(ctx, r, b, opts, false)
}

// fetchRestrictedScoped is fetchRestricted with the caller-requested live-only
// scope (#1414, `GET /api/events?scope=live`): liveOnly forces the archives
// off for THIS request the same way a profile does mechanically, but the two
// must not share bookkeeping — excl records exclusions the operator did not
// choose and must be told about, while scope=live is the caller's own phase-1
// request, disclosed through liveScopeAdvisories instead.
// skipped is FetchMergedFull's incompleteness inventory — failed archive
// sources plus the DiscoveryFailedSource sentinel — and it is now RETURNED
// rather than discarded (#1414 review pass 2): the scope=live phase-1 warning
// promises "a full read will report it", and the phase-2 response is what
// sweeps that warning away, so it must be able to keep the promise. The old
// log-only trade-off (Server.fetch's doc) predates a phase-1 marker whose
// removal asserts completeness.
func (s *Server) fetchRestrictedScoped(ctx context.Context, r *http.Request, b *bundle, opts query.Options, liveOnly bool) ([]query.ResultRow, *query.QueryPlan, archiveExclusion, []string, int, bool, error) {
	excl := archiveExclusionFor(r, b)
	rows, plan, skipped, diverged, archivesElided, err := query.FetchMergedFull(ctx, b.db, b.engine, query.FetchMergedOptions{
		Opts:           opts,
		DBName:         b.dbName,
		NoArchive:      excl.any() || liveOnly,
		AllowGaps:      true,
		ArchiveFetcher: s.archiveFetch(),
	})
	return rows, plan, excl, skipped, diverged, archivesElided, err
}

// archiveExclusion records WHY a fetch left the Parquet archives out (#1311),
// tracking the two causes SEPARATELY rather than collapsing them.
//
// Excluding them is correct — the archive path runs no redaction, so a
// profiled session must not be served from it — and it fails in the safe
// direction: the session is shown less, never unredacted more. What was
// missing is that the RESULT never said so. Hours rotated out of live MySQL
// still exist; the session simply does not read them. An operator sees a short
// or empty result and reads it as "nothing happened in that window", which is
// the one conclusion the data does not support.
//
// The two causes are kept apart because collapsing them to one winner made a
// profiled session on a --no-archive console silent: the server-wide cause
// won, and the server-wide notice is deliberately conditional on the planner
// finding gaps, which the default browse never produces (no time range, no
// plan). Session and console are different facts about the same read and both
// have to survive.
type archiveExclusion struct {
	// server: the whole console excludes archives (--no-archive, or a startup
	// --profile, which implies it — see consoleapp/serve.go).
	server bool
	// profile: THIS session carries a data access policy — a profile name,
	// direct restrictions (#1449), or both (sessionRestricted's verdict; the
	// field keeps its original name).
	profile bool
}

func (e archiveExclusion) any() bool { return e.server || e.profile }

// archiveExclusionFor reports why this request will not read archives.
func archiveExclusionFor(r *http.Request, b *bundle) archiveExclusion {
	return archiveExclusion{server: b.noArchive, profile: sessionRestricted(r)}
}

// announce reports whether the operator must be told, given whether the
// planner found hours this read could not see.
//
// A session data profile is ALWAYS announced: it is invisible to the person
// reading the screen — they did not set it, the UI does not show it, and
// nothing else in the response hints that half the index is out of scope.
//
// A console-wide --no-archive alone is announced only once there is a gap to
// point at. Putting it on every response would be a permanent banner on every
// page of that console, and a banner that is always there is read by nobody —
// including on the day it matters, and it would train users straight past the
// profile notice.
func (e archiveExclusion) announce(gaps bool) bool {
	return e.profile || (e.server && gaps)
}

// notice is the operator-facing sentence for an exclusion. It states the scope
// of the result and explicitly denies the wrong inference, because the wrong
// inference is the whole failure mode: a short result under an unstated
// restriction reads as an answer about the data.
// The wording names the cause whose removal would actually change the result.
// When the whole console excludes archives, saying "your data profile" would
// point the operator at something that is not the reason and whose removal
// would change nothing — so the console-wide sentence wins the WORDING even
// though the profile wins the decision to speak.
func (e archiveExclusion) notice() string {
	switch {
	case e.server:
		return "This DBTrail reads the LIVE INDEX ONLY (started with --no-archive, or with a profile that " +
			"implies it), so archived (rotated) hours are not searched. A short or empty result does not " +
			"mean nothing happened in that window."
	case e.profile:
		return "Your session carries a data access policy, so these results come from the LIVE INDEX ONLY: " +
			"archived (rotated) hours are not searched, because the archive path cannot apply the " +
			"redaction your policy requires. A short or empty result does not mean nothing happened " +
			"in that window; ask an operator without data restrictions, or use the CLI, to search the archives."
	}
	return ""
}
