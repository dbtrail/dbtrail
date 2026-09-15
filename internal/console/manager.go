package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// errNoServers is returned when a request arrives with no selectable server:
// no --index-dsn boot entry and an empty registry.
var errNoServers = errors.New("no servers configured: pass --index-dsn or add a server in the UI")

// bundle is the per-server connection state: everything that used to live on
// Server when the console spoke to exactly one index. One bundle per selected
// server, built lazily and cached by connManager.
type bundle struct {
	db     *sql.DB
	dbName string
	// dsn is the connection's own DSN, kept for the capacity probe's
	// locality check (is the index datadir on THIS host?) and never
	// serialized — the masked DTOs are built elsewhere (#1444).
	dsn      string
	engine   *query.Engine
	resolver *metadata.Resolver
	// resolverUnavailable records that loadResolver failed for a reason OTHER
	// than ErrNoSnapshots (permissions, un-migrated index, transient error at
	// open). resolver is nil either way; this distinguishes "no snapshots
	// exist" from "the snapshot half was silently skipped", which /api/schemas
	// reports as snapshot_unavailable (#1071).
	resolverUnavailable bool
	// noArchive disables Parquet archive auto-discovery for this server. It is
	// the entry's own flag OR'd with the process-global profileActive: archives
	// do not enforce RBAC, so an active profile forces it on every server.
	noArchive bool
	// baselineSrc is the resolved reconstruct baseline source (local dir wins
	// over s3:// prefix); empty when reconstruct is not configured.
	baselineSrc string
	// baselineFallbackSrc is the S3 prefix to retry on ErrNoBaseline when a
	// local dir was preferred as baselineSrc AND an S3 prefix is also
	// configured; empty otherwise. Local baselines can be pruned by retention
	// (#616) while a durable S3 copy remains — without this, a request for a
	// table/time only covered by the S3 copy silently degraded to "no
	// baseline" instead of finding it (#766).
	baselineFallbackSrc string
	// baselineConfigured gates the reconstruct surface per server: a baseline
	// is present AND archives are enabled AND no RBAC profile is active. See
	// the rationale on newBundleDerived.
	baselineConfigured bool
	// activity is this server's materialized Overview aggregate (#1352). It
	// lives on the bundle so evicting the server drops the materialization
	// with the connection. The POINTER is what rebuildDerived carries across a
	// derived-only rebuild — the bundle snapshot stays immutable; the cache's
	// interior mutability is its own, behind its own lock.
	activity *activityCache
}

// connManager owns the per-server connection lifecycle: lazy open on first
// selection, caching, single-flight, eviction on edit/delete, and shutdown.
//
// Critically, it NEVER runs indexer.EnsureSchema on a registry DSN: schema
// migration is an ALTER (DDL), and the console's read-only contract only
// tolerates it on the DSN the operator typed on the command line (the boot
// entry, migrated by cmd/bintrail before the server starts). A registry index
// missing a newer column surfaces as an actionable error instead — see
// writeFetchError in api.go.
type connManager struct {
	reg *Registry
	// profileActive mirrors "any RBAC rule is loaded" — it forces noArchive
	// (and thus disables reconstruct) on every bundle, because neither archives
	// nor baseline reads apply RBAC redaction.
	profileActive bool

	// defaultBaselineDir / defaultBaselineS3 are the process-wide
	// --baseline-dir / --baseline-s3 flags, used as the baseline source for
	// registry entries that carry none of their own (#1010). Without this
	// fallback, a server added from the UI (whose add form has no baseline
	// field) could never enable Time-travel/reconstruct/verify even though the
	// daemon was started with a usable --baseline-dir — the common
	// single-baseline-dir deployment. Written once before serving
	// (console.New), like profileActive; read-only afterwards.
	defaultBaselineDir string
	defaultBaselineS3  string

	// hideBoot removes the boot entry from the UI entirely. Set by
	// source-less `bintrail-console watch`: its boot index is only the
	// control plane's anchor database — no stream ever writes to it — so a
	// fresh install must list NO servers (showing the internal index as a
	// "server" reads as a phantom entry). Header-less requests still resolve
	// to the boot bundle underneath, so the views render (empty) before the
	// first server is added. Written once before serving (console.New);
	// read under mu.
	hideBoot bool

	mu      sync.Mutex
	bundles map[string]*bundle
	locks   map[string]*sync.Mutex // per-id single-flight for lazy opens
	// boot is the ephemeral command-line entry (--index-dsn / `up`'s stream
	// DSN): opened eagerly by the cmd layer (which also owns closing its db),
	// never persisted, never evicted. nil when the console is registry-only.
	boot *bundle
	// bootDSN is the boot entry's DSN, kept ONLY to render the masked DTO in
	// /api/servers (host/user/dbname). Never serialized whole.
	bootDSN string
}

func newConnManager(reg *Registry, profileActive bool) *connManager {
	if reg == nil {
		reg, _ = LoadRegistry("") // in-memory; "" never errors
	}
	return &connManager{
		reg:           reg,
		profileActive: profileActive,
		bundles:       map[string]*bundle{},
		locks:         map[string]*sync.Mutex{},
	}
}

// Resolve returns the bundle for a server id, lazily opening the connection on
// first selection. id == "" selects the default: the boot entry when present,
// else the first registry entry. The open is single-flighted per id so the
// fan-out a tab switch fires (capabilities + schemas + events) opens ONE
// connection, not four.
func (cm *connManager) Resolve(ctx context.Context, id string) (*bundle, error) {
	if id == "" {
		// The no-header default must match defaultID() exactly — /api/servers
		// reports that id as default_id and the switcher renders it selected,
		// so resolving "" anywhere else would render one server while
		// querying another.
		if id = cm.defaultID(); id == "" {
			// Source-less watch, empty registry: the boot entry is hidden
			// from every listing but still backs header-less requests, so a
			// fresh install renders (empty) views instead of a 404. The
			// bundle's db is non-nil by construction: only watch sets
			// HideBoot, and it connects the boot DB before console.New.
			cm.mu.Lock()
			if b := cm.boot; b != nil {
				cm.mu.Unlock()
				return b, nil
			}
			cm.mu.Unlock()
			return nil, errNoServers
		}
	}
	cm.mu.Lock()
	if id == bootServerID {
		if b := cm.boot; b != nil {
			cm.mu.Unlock()
			return b, nil
		}
		cm.mu.Unlock()
		return nil, ErrUnknownServer
	}
	if b, ok := cm.bundles[id]; ok {
		cm.mu.Unlock()
		return b, nil
	}
	lock, ok := cm.locks[id]
	if !ok {
		lock = &sync.Mutex{}
		cm.locks[id] = lock
	}
	cm.mu.Unlock()

	// Single-flight: only one goroutine builds the bundle for this id; the
	// rest block here and find it cached on re-check.
	lock.Lock()
	defer lock.Unlock()
	cm.mu.Lock()
	if b, ok := cm.bundles[id]; ok {
		cm.mu.Unlock()
		return b, nil
	}
	cm.mu.Unlock()

	// Build outside cm.mu (network I/O), then publish under ONE critical
	// section that re-validates the entry. An edit racing this open
	// (handleServersUpdate runs reg.Update→evict WITHOUT the per-id lock)
	// would otherwise leave a bundle for the OLD DSN cached forever: evict
	// only closes what is already in the map, and this open isn't yet.
	// Re-reading under cm.mu pairs with evict's cm.mu — the edit either
	// happened before our re-read (we rebuild from the current entry) or its
	// evict runs after our publish and closes what we cached.
	for attempt := 0; ; attempt++ {
		entry, ok := cm.reg.Get(id)
		if !ok {
			return nil, ErrUnknownServer
		}
		b, err := cm.buildBundle(entry)
		if err != nil {
			// Not cached: a failed open is retried on the next selection rather
			// than poisoning the entry until restart.
			return nil, err
		}
		cm.mu.Lock()
		cur, stillThere := cm.reg.Get(id)
		if stillThere && cur.DSN == entry.DSN {
			// Derive the published gates from the CURRENT entry: a
			// baseline/no-archive-only edit during the open keeps this db but
			// must not publish the stale entry's reconstruct gate.
			nb := newBundleDerived(b.db, b.dbName, cm.withBaselineDefaults(cur), cm.profileActive)
			nb.dsn = b.dsn
			nb.resolver = b.resolver
			nb.resolverUnavailable = b.resolverUnavailable
			cm.bundles[id] = nb
			cm.mu.Unlock()
			return nb, nil
		}
		cm.mu.Unlock()
		_ = b.db.Close() // built against a deleted or DSN-edited entry
		if !stillThere {
			return nil, ErrUnknownServer
		}
		if attempt >= 2 {
			return nil, fmt.Errorf("server %q is being edited concurrently; retry", cur.Name)
		}
	}
}

// buildBundle opens a registry server's connection and derives its per-server
// state. config.Connect Pings eagerly, so a dead entry fails here — on
// selection, exactly when the operator switches to it — with a scrubbed error.
// NO EnsureSchema: see the connManager doc comment.
func (cm *connManager) buildBundle(entry ServerEntry) (*bundle, error) {
	cfg, err := mysql.ParseDSN(entry.DSN)
	if err != nil {
		// The driver's ParseDSN messages are static today, but scrub anyway so
		// DSN secrecy holds on every error path, not by driver-version luck.
		return nil, fmt.Errorf("server %q: invalid DSN: %s", entry.Name, scrubDSNError(err, entry.DSN))
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("server %q: DSN must include a database name", entry.Name)
	}
	db, err := config.Connect(entry.DSN)
	if err != nil {
		return nil, fmt.Errorf("server %q: %s", entry.Name, scrubDSNError(err, entry.DSN))
	}
	b := newBundleDerived(db, cfg.DBName, cm.withBaselineDefaults(entry), cm.profileActive)
	b.dsn = entry.DSN
	b.resolver, b.resolverUnavailable = loadResolver(db)
	return b, nil
}

// withBaselineDefaults returns the entry with the process-wide
// --baseline-dir / --baseline-s3 filled in when it carries no baseline source
// of its own (#1010). The fallback is all-or-nothing: an entry with its OWN
// dir or S3 configured chose its baseline location explicitly, and mixing a
// per-entry S3 with the process dir (or vice versa) would make findBaseline's
// dir-over-S3 preference and #766 S3 retry read from a location the operator
// never associated with that server.
func (cm *connManager) withBaselineDefaults(entry ServerEntry) ServerEntry {
	if entry.BaselineDir == "" && entry.BaselineS3 == "" {
		entry.BaselineDir = cm.defaultBaselineDir
		entry.BaselineS3 = cm.defaultBaselineS3
	}
	return entry
}

// newBundleDerived computes the pure-config per-server state shared by lazy
// opens and derived-only rebuilds. The reconstruct gate mirrors what New()
// enforced process-globally before multi-server:
//  1. a baseline must be configured (dir wins over s3);
//  2. no RBAC profile may be active — baseline reads bypass redaction; and
//  3. archives must be enabled — the planner can only fail loud on coverage
//     gaps of rotated-out hours if their archives are actually fetched.
//
// Conditions 2 and 3 collapse into !noArchive, exactly as in New().
func newBundleDerived(db *sql.DB, dbName string, entry ServerEntry, profileActive bool) *bundle {
	noArchive := entry.NoArchive || profileActive
	src := entry.BaselineDir
	fallback := ""
	if src == "" {
		src = entry.BaselineS3
	} else if entry.BaselineS3 != "" {
		fallback = entry.BaselineS3
	}
	return &bundle{
		db:                  db,
		dbName:              dbName,
		engine:              query.New(db),
		noArchive:           noArchive,
		baselineSrc:         src,
		baselineFallbackSrc: fallback,
		baselineConfigured:  src != "" && !noArchive,
		activity:            newActivityCache(),
	}
}

// findBaseline locates a baseline for schema.table at-or-before at via
// b.baselineSrc, falling back to b.baselineFallbackSrc (the durable S3 copy,
// when both a local dir and an S3 prefix are configured) on ErrNoBaseline —
// see the baselineFallbackSrc field doc (#766).
func (b *bundle) findBaseline(ctx context.Context, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
	path, snapshotTime, stale, err := reconstruct.FindBaseline(ctx, b.baselineSrc, schema, table, at)
	if b.baselineFallbackSrc == "" {
		return path, snapshotTime, stale, err
	}
	switch {
	case errors.Is(err, reconstruct.ErrNoBaseline):
		return reconstruct.FindBaseline(ctx, b.baselineFallbackSrc, schema, table, at)
	case errors.Is(err, reconstruct.ErrUnreadableSnapshot), err == nil && stale.Unreadable:
		// #1639: a local folder could not be read. The durable copy may hold
		// the snapshot it hides; ask it before refusing or settling for an
		// older local one, and say why the answer came from there.
		fpath, ftime, fstale, ferr := reconstruct.FindBaseline(ctx, b.baselineFallbackSrc, schema, table, at)
		if ferr != nil && !errors.Is(ferr, reconstruct.ErrNoBaseline) {
			// Kept to the log: the local answer (or its refusal) still stands
			// and says why, but a destination that cannot be read is a second
			// problem nobody would otherwise see.
			slog.Warn("backup lookup: the backup destination could not be read either",
				"table", schema+"."+table, "destination", b.baselineFallbackSrc, "err", ferr)
		}
		if ferr != nil || (err == nil && !ftime.After(snapshotTime)) {
			return path, snapshotTime, stale, err
		}
		cause := stale.Message
		if err != nil {
			cause = err.Error()
		}
		why := "read from the backup destination because a local backup folder could not be read, so a newer backup may exist there: " + cause
		if fstale.Stale() {
			// The destination's own answer is stale too: keep both, or the
			// operator sees a stale warning without the reason it came from
			// the destination.
			fstale.Message = why + "; " + fstale.Message
			fstale.Unreadable = true
		} else {
			fstale = reconstruct.StaleWarning{
				Message:        why,
				UsingSnapshot:  ftime,
				NewestSnapshot: ftime,
				Unreadable:     true,
			}
		}
		return fpath, ftime, fstale, nil
	}
	return path, snapshotTime, stale, err
}

// loadResolver loads the latest schema snapshot, best-effort: a missing
// snapshot just means recovery falls back to all-column WHERE clauses.
// unavailable reports a failure OTHER than "no snapshots exist" — permissions
// on schema_snapshots, an un-migrated index, a transient error at open — so
// /api/schemas can flag that its snapshot half was skipped instead of
// answering an indistinguishable empty list (#1071).
func loadResolver(db *sql.DB) (r *metadata.Resolver, unavailable bool) {
	if db == nil {
		return nil, false
	}
	r, err := metadata.NewResolver(db, 0)
	switch {
	case err == nil:
		return r, false
	case errors.Is(err, metadata.ErrNoSnapshots):
		slog.Debug("console: no schema snapshots; recovery will use all-column WHERE clauses")
		return nil, false
	default:
		slog.Warn("console: failed to load schema resolver; recovery will use all-column WHERE clauses", "error", err)
		return nil, true
	}
}

// seedBoot registers the ephemeral command-line bundle. The cmd layer built it
// eagerly (config.Connect + EnsureSchema, fail-fast preserved) and keeps
// ownership of closing its db.
func (cm *connManager) seedBoot(b *bundle, dsn string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	b.dsn = dsn
	cm.boot = b
	cm.bootDSN = dsn
}

// bootInfo returns the boot bundle and its display DSN under the lock —
// seedBoot runs once before serving, but readers stay uniformly synchronized.
func (cm *connManager) bootInfo() (*bundle, string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.boot, cm.bootDSN
}

// evict drops and closes a cached bundle (DSN edit or delete). The boot entry
// is not evictable. Closing while a request is in flight is acceptable for a
// single-operator tool: the in-flight query errors, the next selection reopens.
func (cm *connManager) evict(id string) {
	cm.mu.Lock()
	b := cm.bundles[id]
	delete(cm.bundles, id)
	cm.mu.Unlock()
	if b != nil && b.db != nil {
		if err := b.db.Close(); err != nil {
			slog.Warn("console: closing evicted server connection", "server", id, "error", err)
		}
	}
}

// rebuildDerived recomputes a cached bundle's derived flags after a
// baseline/no-archive-only edit, keeping the open db (no needless re-Ping).
// The bundle is replaced wholesale (handlers hold *bundle snapshots, which
// must stay immutable once published). No-op when the bundle isn't cached —
// the next lazy open reads the updated entry anyway.
func (cm *connManager) rebuildDerived(entry ServerEntry) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	old, ok := cm.bundles[entry.ID]
	if !ok {
		return
	}
	nb := newBundleDerived(old.db, old.dbName, cm.withBaselineDefaults(entry), cm.profileActive)
	nb.dsn = old.dsn
	nb.engine = old.engine
	nb.resolver = old.resolver
	nb.resolverUnavailable = old.resolverUnavailable
	// A baseline/no-archive edit changes nothing the activity materialization
	// read (live binlog_events under this same db), so keep it warm.
	nb.activity = old.activity
	cm.bundles[entry.ID] = nb
}

// cached reports whether a live bundle exists for id (the UI's "connected" dot).
func (cm *connManager) cached(id string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	_, ok := cm.bundles[id]
	return ok
}

// capability reports the reconstruct gate for a registry entry as pure config —
// no connection is opened, so /api/servers can label every entry instantly.
func (cm *connManager) capability(entry ServerEntry) bool {
	entry = cm.withBaselineDefaults(entry)
	src := entry.BaselineDir
	if src == "" {
		src = entry.BaselineS3
	}
	return src != "" && !(entry.NoArchive || cm.profileActive)
}

// defaultID returns the id the browser falls back to with no header: the boot
// entry when present and not hidden; under hideBoot, the first entry with a
// source CONFIGURED (typically the monitored one), else the first entry,
// else "" — a fresh hidden-boot install reports no default at all, and
// Resolve("") falls back to the hidden boot bundle so the views still
// render. Registry-only `serve` (boot == nil) keeps its longstanding
// first-entry default: the sourced preference applies ONLY when a hidden
// boot forces a choice — applying it to serve would silently change which
// server header-less tabs query on registries shared with watch.
func (cm *connManager) defaultID() string {
	cm.mu.Lock()
	boot := cm.boot
	hide := cm.hideBoot
	cm.mu.Unlock()
	entries := cm.reg.List()
	if boot != nil && !hide {
		return bootServerID
	}
	if boot != nil { // hidden boot: prefer the entry events actually land in
		for _, e := range entries {
			if e.SourceDSN != "" {
				return e.ID
			}
		}
	}
	if len(entries) > 0 {
		return entries[0].ID
	}
	return ""
}

// bootHidden reports whether the boot entry exists but must not appear in any
// server listing (source-less watch).
func (cm *connManager) bootHidden() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.hideBoot && cm.boot != nil
}

// bootSelectable reports whether the boot entry may be picked as a flashback
// target: it exists AND is not hidden. Mirrors what the console's server
// switcher lists, so `USE`-by-username on the flashback port only reaches the
// same servers the operator sees in the UI (a hidden boot is the empty
// control-plane anchor of a source-less watch — never a useful time-travel
// source).
func (cm *connManager) bootSelectable() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.boot != nil && !cm.hideBoot
}

// CloseAll closes every cached registry connection. The boot entry's db is
// NOT closed here — the cmd layer opened it and owns its defer Close().
func (cm *connManager) CloseAll() {
	cm.mu.Lock()
	bundles := cm.bundles
	cm.bundles = map[string]*bundle{}
	cm.mu.Unlock()
	for id, b := range bundles {
		if b.db != nil {
			if err := b.db.Close(); err != nil {
				slog.Warn("console: closing server connection at shutdown", "server", id, "error", err)
			}
		}
	}
}
