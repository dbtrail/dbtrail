package console

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pgShapeRetryAfter is how long a reload that failed is not tried again for
// the same newest snapshot. The load scans schema_snapshots (with its own 15 s
// bound), so retrying it on every request of a server whose index refuses it
// would put that scan, and a warning, behind each one.
const pgShapeRetryAfter = 30 * time.Second

// pgShapeNow is the clock the retry window reads; tests move it.
var pgShapeNow = time.Now

// pgShapeState is what one PostgreSQL index connection's published resolver
// was loaded at, and its last failed reload.
type pgShapeState struct {
	// loadMu is held for a reload. A request that finds it taken keeps the
	// bundle it has instead of waiting: shapes a moment old beat a request
	// held for a load another one is already doing.
	loadMu sync.Mutex

	mu       sync.Mutex // guards the fields below
	known    bool       // loaded holds a value
	loaded   int        // newest snapshot_id when the published resolver was loaded
	failedID int        // newest snapshot_id at the last failed reload
	failedAt time.Time  // when it failed; zero: no failure to wait out
}

// pgShapes holds one pgShapeState per index connection. Keyed by the *sql.DB:
// a bundle rebuilt over the same connection (rebuildDerived, a re-publish)
// keeps its resolver, and an edited DSN opens a new connection. Entries are
// dropped when the connection is closed (forgetPGShapes).
var pgShapes sync.Map // *sql.DB -> *pgShapeState

func pgShapeStateFor(db *sql.DB) *pgShapeState {
	v, _ := pgShapes.LoadOrStore(db, &pgShapeState{})
	return v.(*pgShapeState)
}

// forgetPGShapes drops what is recorded for a connection being closed.
func forgetPGShapes(db *sql.DB) {
	if db != nil {
		pgShapes.Delete(db)
	}
}

// openPGShapes loads a PostgreSQL index's table shapes when its bundle is
// opened, and records the newest snapshot they were loaded at, so the first
// request does not load them again. The newest snapshot is read BEFORE the
// load: a shape saved in between makes the next request see a newer one and
// reload, never the reverse.
func openPGShapes(ctx context.Context, db *sql.DB) (*metadata.Resolver, bool) {
	maxID, maxErr := pgNewestSnapshotID(ctx, db)
	r, unavailable := loadResolverForFlavor(db, "postgres")
	if maxErr == nil && !unavailable {
		st := pgShapeStateFor(db)
		st.mu.Lock()
		st.known, st.loaded = true, maxID
		st.mu.Unlock()
	}
	return r, unavailable
}

// freshPGShapes returns b with a resolver that knows every table shape its
// index holds, for a PostgreSQL server (#3110). A PostgreSQL index receives
// shapes one table at a time for as long as capture runs (a table's first
// change, a column added or dropped), so a resolver loaded when the bundle was
// opened goes stale: a table first seen since is missing from the table list,
// and an undo is checked against the shape from before a DROP COLUMN.
//
// It costs one MAX(snapshot_id) read per request and loads the resolver again
// only when that changes, the rule indexSchema (tail_source.go) follows. Any
// other source returns b untouched without reading anything: a MySQL or
// MariaDB snapshot is the whole schema at once and its bundle keeps the
// resolver it opened with, as before. So does a server whose index is opened
// lazily (the router), which never loads one.
//
// A read or load that fails keeps the bundle it was given. A failed load is
// not tried again for the same newest snapshot within pgShapeRetryAfter, and
// warns once per such window.
func (cm *connManager) freshPGShapes(ctx context.Context, b *bundle) *bundle {
	if b == nil || b.db == nil || cm.lazyIndex || !sourceIsPostgres(b.serverID, b.pgSource, cm.reg.Get) {
		return b
	}
	maxID, err := pgNewestSnapshotID(ctx, b.db)
	if err != nil {
		slog.Debug("console: could not read the newest schema snapshot; keeping the loaded table shapes", "server", b.serverID, "error", err)
		return b
	}
	st := pgShapeStateFor(b.db)
	if !st.due(maxID) {
		return b
	}
	if !st.loadMu.TryLock() {
		return b // another request is loading them
	}
	defer st.loadMu.Unlock()
	if !st.due(maxID) {
		return cm.publishedOr(b)
	}
	cur := cm.publishedOr(b)
	if cur != cm.publishedBundle(b) {
		return b // evicted or reopened meanwhile: the next request resolves anew
	}

	r, err := query.LatestSchemaResolverForFlavor(cur.db, "postgres")
	switch {
	case err == nil:
	case errors.Is(err, metadata.ErrNoSnapshots):
		r = nil
	default:
		st.mu.Lock()
		st.failedID, st.failedAt = maxID, pgShapeNow()
		st.mu.Unlock()
		slog.Warn("console: could not reload the table shapes of a PostgreSQL index; keeping the ones loaded before, and trying again in 30s or when a newer shape is saved",
			"server", cur.serverID, "snapshot", maxID, "error", err)
		return cur
	}
	nb := *cur
	nb.resolver, nb.resolverUnavailable = r, false
	if !cm.republish(cur, &nb) {
		return cur
	}
	st.mu.Lock()
	st.known, st.loaded, st.failedAt = true, maxID, time.Time{}
	st.mu.Unlock()
	return &nb
}

// due reports whether the shapes need loading for maxID: not loaded at it,
// and not a load that failed at it less than pgShapeRetryAfter ago.
func (st *pgShapeState) due(maxID int) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.known && st.loaded == maxID {
		return false
	}
	return st.failedAt.IsZero() || st.failedID != maxID || pgShapeNow().Sub(st.failedAt) >= pgShapeRetryAfter
}

// pgNewestSnapshotID is the newest snapshot_id of the index, 0 when it has
// none.
func pgNewestSnapshotID(ctx context.Context, db *sql.DB) (int, error) {
	var id int
	err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(snapshot_id), 0) FROM schema_snapshots").Scan(&id)
	return id, err
}

// publishedOr is the bundle published in b's place when it is over b's
// connection, else b.
func (cm *connManager) publishedOr(b *bundle) *bundle {
	if cur := cm.publishedBundle(b); cur != nil && cur.db == b.db {
		return cur
	}
	return b
}

// publishedBundle is the bundle currently published in b's place (the boot
// bundle, or the cached one for b's server), nil when b's place is gone.
func (cm *connManager) publishedBundle(b *bundle) *bundle {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if b.serverID == "" {
		return cm.boot
	}
	return cm.bundles[b.serverID]
}

// republish replaces old with nb where old is published, and reports whether
// it was still there. A bundle is never mutated once published (handlers hold
// it), so a reload publishes a copy.
func (cm *connManager) republish(old, nb *bundle) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if old.serverID == "" {
		if cm.boot != old {
			return false
		}
		cm.boot = nb
		return true
	}
	if cm.bundles[old.serverID] != old {
		return false
	}
	cm.bundles[old.serverID] = nb
	return true
}
