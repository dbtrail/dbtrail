package ext

import (
	"context"
	"database/sql"
	"net/http"
	"regexp"

	"github.com/dbtrail/dbtrail/indexquery"
)

// ConsoleQueryContext is the per-request, per-selected-server index access a
// ConsoleViewProvider's data handler receives. The console resolves it from the
// request's selected server (the X-Bintrail-Server header, or the default) the
// same way its own endpoints do, so an extension view reads exactly the index
// the operator is looking at — never a different server.
type ConsoleQueryContext struct {
	// DB is the selected server's index connection. It is owned by the console
	// (pooled and lazily opened per server); the provider must NOT close it, and
	// must not assume it outlives the request.
	//
	// DB is NIL when the index cannot be opened (a dead entry, or a per-source
	// index that does not exist yet) but the selected server DOES have a source
	// configured — so a source-only view (one that reads only the live source,
	// never the index) still resolves and runs during an index outage. An
	// index-backed view MUST nil-check DB (or rely on Fetch, below) rather than
	// dereference it. When the index opened successfully, DB is non-nil; when no
	// source is configured AND the index cannot be opened, the resolve func
	// returns an error instead of this struct.
	DB *sql.DB
	// Fetch runs the console's own cross-source read pipeline (live MySQL
	// partitions plus Parquet archives, merged, sorted, gap-aware) already bound
	// to the selected server. It applies whatever RBAC redaction the console was
	// started with, so a provider that fetches through it inherits the operator's
	// profile rather than having to reimplement redaction. Prefer it over
	// querying DB directly whenever archive coverage or redaction matters.
	//
	// Fetch is always non-nil (never call it on a nil value), but when DB is nil
	// — the index-unreachable-but-source-configured case above — Fetch returns
	// the underlying index error on every call. An index-backed view thus
	// degrades with a real error instead of a nil-pointer panic or a false empty
	// result.
	Fetch func(ctx context.Context, opts indexquery.Options) ([]indexquery.ResultRow, *indexquery.QueryPlan, error)
	// SourceDSN is the captured-source DSN of the selected registry entry, when
	// one is configured — a provider that needs to reach the live source (not
	// just the index) can use it. Empty for the boot/command-line entry and for a
	// selection with no source configured; treat empty as "no source available",
	// not an error. It is read from the registry WITHOUT opening the index, so it
	// is populated even when the index is unreachable (the DB-nil case above) —
	// that is what keeps a source-only view working during an index outage.
	SourceDSN string
	// SourceTLS is the selected entry's source TLS (its ssl_* settings), the
	// TLS capture uses for SourceDSN; zero means "preferred". Open the source
	// with OpenSource(SourceDSN, SourceTLS). Populated whenever SourceDSN is.
	SourceTLS SourceTLS
}

// ConsoleQueryContextFunc resolves the ConsoleQueryContext for the selected
// server behind a request. It mirrors the console's per-request bundle
// resolution and returns the same errors: an unresolvable selection yields an
// error whose message is already scrubbed of DSN secrets, safe to surface to
// the client.
type ConsoleQueryContextFunc func(r *http.Request) (ConsoleQueryContext, error)

// ConsoleViewProvider injects one additional view into the web console: a nav
// item, a capability entry, an authenticated data API, and a frontend module
// that renders the view. It lets an embedding distribution — a build that
// imports consoleapp and wraps the OSS core — add a console view the stock
// binary does not ship, without forking the console.
//
// The console mounts a provider like this:
//
//   - Static assets at "/ext/<ID>/" — served UNAUTHENTICATED behind the host
//     guard and security headers only, exactly like index.html and app.js. Code
//     always ships; only DATA is gated. A browser must be able to load the
//     module before it has authenticated.
//   - Data routes at "/api/ext/<ID>/" — mounted behind the console's bearer-token
//     middleware (so every data route inherits authentication) AND refused with
//     403 before the provider's handler runs whenever an RBAC access-control
//     profile is active (the console cannot guarantee a third-party handler
//     honors table-deny / column-redaction rules, so it withholds the whole
//     surface rather than risk a leak — the same posture recover-cascade takes).
//   - A nav item labeled Label(), and a capability entry the SPA uses to reveal
//     the nav item and route to the view.
type ConsoleViewProvider interface {
	// ID is a STABLE, lowercase, URL-safe key for this view. It is used three
	// ways — as the capability key, as the "/ext/<ID>/" + "/api/ext/<ID>/" mount
	// segment, and as the SPA route "ext-<ID>" — so it must match
	// ^[a-z0-9-]+$ (see ValidConsoleViewID). It must not change across releases:
	// the operator's bookmarks and the SPA route both key on it. A provider whose
	// ID fails validation is skipped by the console (logged, not mounted) so a
	// typo degrades to "no view" instead of a broken or injectable route.
	ID() string
	// Label is the human-readable nav text for the view.
	Label() string
	// Script is the URL of an ES module the SPA import()s and then calls
	// render(mount, {apiBase, api}) on. It renders into the same document,
	// same-origin — NOT an iframe (X-Frame-Options: DENY is global) — so the
	// module runs with the console's own origin and shares its bearer credential
	// via the api primitive the SPA passes in. Serve it from this provider's own
	// StaticHandler subtree (e.g. "/ext/<ID>/view.js").
	Script() string
	// StaticHandler serves the view's frontend assets. The console mounts it at
	// prefix ("/ext/<ID>/") UNAUTHENTICATED — it must expose only static,
	// non-secret files (the module named by Script and whatever it loads). The
	// handler sees requests whose path still carries prefix; strip it if needed.
	StaticHandler(prefix string) http.Handler
	// DataHandler serves the view's authenticated data routes. The console mounts
	// it at prefix ("/api/ext/<ID>/") behind the bearer-token middleware and the
	// RBAC guard described above. resolve yields the per-request, per-server
	// ConsoleQueryContext; the handler calls it to read the index the operator
	// has selected. The handler sees requests whose path still carries prefix.
	DataHandler(prefix string, resolve ConsoleQueryContextFunc) http.Handler
}

// consoleViewIDRE constrains a provider ID to a lowercase, URL-safe token: the
// ID flows into an HTTP path segment AND a DOM route/data-attribute, so an ID
// with a slash, a space, or markup could produce a broken mount or an injected
// route. Anchored, non-empty.
var consoleViewIDRE = regexp.MustCompile(`^[a-z0-9-]+$`)

// ValidConsoleViewID reports whether id is an acceptable ConsoleViewProvider ID
// (matches ^[a-z0-9-]+$). The console calls it at mount time and skips a
// provider that fails, rather than mounting a malformed route.
func ValidConsoleViewID(id string) bool { return consoleViewIDRE.MatchString(id) }

// consoleView is the ORIGINAL single-provider slot, kept because an embedding
// build already calls SetConsoleView. Nil in the OSS build — the console ships
// no extension views.
var consoleView ConsoleViewProvider

// registeredConsoleViews holds the providers installed additively through
// RegisterConsoleView. Separate from the slot above so SetConsoleView keeps its
// documented replace-the-slot behavior (an existing caller that installs twice
// must not suddenly mount two copies) while a new caller can install alongside
// one without knowing whether the slot is taken. Empty in the OSS build.
var registeredConsoleViews []ConsoleViewProvider

// SetConsoleView installs the process-wide console extension-view provider in
// the legacy single slot, replacing whatever was there (nil clears it). Call
// once from main() before command dispatch, like SetConsoleAuth: the console
// reads it when the server is constructed, so a later install is never picked
// up. A bad ID is not rejected here — the console validates it at mount time and
// skips the provider — so this setter cannot panic on a caller's typo.
//
// Prefer RegisterConsoleView for new code: this setter can hold exactly one
// provider, so two independent installs silently reduce to whichever ran last.
func SetConsoleView(p ConsoleViewProvider) {
	consoleView = p
}

// ConsoleView returns the provider installed in the legacy slot, or nil when
// none is (the OSS build). It does NOT see providers installed with
// RegisterConsoleView — a caller that must act on every installed view (the
// console's mount and capabilities paths do) must range over ConsoleViews.
func ConsoleView() ConsoleViewProvider {
	return consoleView
}

// RegisterConsoleView installs an additional console extension-view provider.
// Additive by contract: two calls install two views, which is the whole reason
// this exists next to SetConsoleView. Call from main() before command dispatch
// — the console reads the registry when the server is constructed. A nil
// provider is ignored rather than appended, so an unconditional call in a build
// that computed no provider cannot produce a nil-dereference at mount time.
//
// IDs are neither validated nor deduplicated here (a setter that panicked on a
// caller's typo would take down the daemon at startup); the console skips an
// invalid or already-mounted ID at mount time and logs it.
func RegisterConsoleView(p ConsoleViewProvider) {
	if p == nil {
		return
	}
	registeredConsoleViews = append(registeredConsoleViews, p)
}

// ConsoleViews returns every installed extension-view provider — the legacy slot
// first, then the registered ones in install order — as a fresh slice the caller
// may keep. Empty in the OSS build. The order is stable so the console's nav
// items do not shuffle between restarts.
func ConsoleViews() []ConsoleViewProvider {
	out := make([]ConsoleViewProvider, 0, len(registeredConsoleViews)+1)
	if consoleView != nil {
		out = append(out, consoleView)
	}
	return append(out, registeredConsoleViews...)
}

// Content-Security-Policy invariant: the console sets NO Content-Security-Policy
// header today (see internal/console securityHeaders — Referrer-Policy,
// X-Content-Type-Options, and X-Frame-Options only). That is what lets the SPA
// dynamically import() a provider's Script and lets the provider module fetch()
// its own "/api/ext/<ID>/" data routes. If a CSP is ever added, its script-src
// and connect-src MUST include 'self' (the modules and their data routes are
// same-origin), or every extension view goes dark. Do not add a CSP without
// accounting for this.
