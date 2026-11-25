package console

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// negativeDecimalTTL bounds how long a FAILED footer resolution is remembered,
// for the same reason negativeRegionTTL exists: a failure is not a property of
// the snapshot, so a blip must not cost every file this daemon hands out its
// casts until someone restarts it.
const negativeDecimalTTL = 5 * time.Minute

type baselineDecimalEntry struct {
	decimals map[string]baseline.TableFooter
	at       time.Time
	failed   bool
	// unread are the files a read that otherwise worked could not look at
	// (baseline.FooterRead.Unread). The retry asks for these alone.
	unread []string
}

// resolveBaselineDecimals fills in the state views' decimal casts, memoized per
// snapshot.
//
// The memoization is not an optimization, it is the rule this path is under.
// Server.bucketRegions carries it in so many words: buildViewsInput runs on
// EVERY SQL panel query, so a network round trip does not belong there in the
// steady state. Reading a footer per table would be N of them, unbounded in the
// table count, inside the panel's setup deadline and behind its single-flight
// latch, which turns one slow bucket into 429s for every other panel user.
//
// A snapshot directory is immutable once complete, so a SUCCESSFUL answer never
// expires: the cache key carries the snapshot timestamp, and taking a new
// baseline produces a new key rather than a stale hit. A resolution that FAILED
// expires after negativeDecimalTTL, and so does one that failed IN PART
// (baseline.FooterRead.Unread): what it did read is served meanwhile.
//
// A table that is merely ABSENT from a successful answer is cached with it, and
// deliberately so: that means the file carries no embedded schema, which for an
// old baseline or a PostgreSQL-source one is a permanent fact about an
// immutable file, not a fault to retry on every query.
func (s *Server) resolveBaselineDecimals(ctx context.Context, in *views.Input) {
	if len(in.Baselines) == 0 {
		return
	}
	key := fmt.Sprintf("%s@%s", in.BaselineSource, in.BaselineSnapshot.UTC().Format(time.RFC3339))

	s.baselineDecimalMu.Lock()
	e, ok := s.baselineDecimals[key]
	s.baselineDecimalMu.Unlock()
	if ok && (!e.failed || time.Since(e.at) < negativeDecimalTTL) {
		in.ApplyFooters(e.decimals)
		return
	}

	// The lock is held only around the map, never across the read: sync.Mutex
	// is not context-aware, so a goroutine blocked on it could not be released
	// by the panel's setup deadline. Two callers racing the same snapshot just
	// do the same read twice, which is harmless.
	// A retry after a PARTIAL read asks only for the files that went unread
	// and adds them to what is already known. Asking for everything again
	// would send a snapshot with one permanently unreadable file down the
	// one-query-per-table path every negativeDecimalTTL, inside whichever
	// statement arrives first.
	paths := in.BaselinePaths()
	known := map[string]baseline.TableFooter{}
	if ok && len(e.unread) > 0 {
		paths = e.unread
		for k, v := range e.decimals {
			known[k] = v
		}
	}
	read, err := baseline.ReadTableFooters(ctx, paths)
	decimals := read.Footers
	if err == nil {
		for k, v := range decimals {
			known[k] = v
		}
		decimals = known
		in.ApplyFooters(decimals)
	} else if len(known) > 0 {
		// The retry itself failed outright: keep serving the earlier part.
		decimals, read.Unread = known, e.unread
		in.ApplyFooters(decimals)
	}
	// A canceled or expired context is the CALLER's state, never a fact about
	// the snapshot, and this cache never forgets a "successful" answer. One
	// browser hitting Stop mid-download (#1583 put a cancelable caller in
	// front of this) would otherwise memoize either a negative entry or a
	// PARTIAL positive one — five minutes, or forever, of silently uncast
	// decimals for every later file of this snapshot — under a warning that
	// sends the operator chasing a storage fault that never happened.
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		slog.Warn("console: could not read baseline column types from the Parquet footers; "+
			"the state views "+baseline.SchemaLossConsequence, "error", err)
	}
	// A read that left SOME files unread is not a successful answer either:
	// it is served as far as it got (the readable tables keep their casts and
	// collations) and tried again after the same delay as a total failure, or
	// one transient fault on one object would cost that table its columns
	// until the daemon restarts. The read itself has logged the count.
	s.rememberBaselineDecimals(key, decimals, err != nil || len(read.Unread) > 0, read.Unread)
}

func (s *Server) rememberBaselineDecimals(key string, decimals map[string]baseline.TableFooter, failed bool, unread []string) {
	s.baselineDecimalMu.Lock()
	defer s.baselineDecimalMu.Unlock()
	if s.baselineDecimals == nil {
		s.baselineDecimals = map[string]baselineDecimalEntry{}
	}
	s.baselineDecimals[key] = baselineDecimalEntry{decimals: decimals, at: time.Now(), failed: failed, unread: unread}
}

// CopyTable is one table of the copy as the PostgreSQL-protocol port's
// catalog describes it (#2769): where it is, the columns its view hides, and
// the shape the source recorded for it in the snapshot (nil when the
// snapshot predates that record).
type CopyTable struct {
	Schema, Table string
	Hidden        []string
	Relation      *baseline.PGRelation
	// Trimmed are the columns of Relation the view reads without their
	// padding (baseline.PGPaddedToTrim): what the PostgreSQL port's catalog
	// says of each (TrimmedOnCopy).
	Trimmed map[string]bool
}

// CopyTables are the tables the copy serves this connection, after the
// server's hidden rules: the ones a statement can read, no more.
func (q *SQLOnCopy) CopyTables(ctx context.Context) ([]CopyTable, error) {
	out, _, err := q.CopyTablesStamped(ctx)
	return out, err
}

// CopyTablesStamped is CopyTables with the stamp (PGCopyStamp) of the
// snapshot files they were read from.
func (q *SQLOnCopy) CopyTablesStamped(ctx context.Context) ([]CopyTable, string, error) {
	in, err := q.s.buildViewsInput(ctx, q.b, pgCopyRequest)
	if err != nil {
		return nil, "", err
	}
	out := make([]CopyTable, 0, len(in.Baselines))
	for _, t := range in.Baselines {
		out = append(out, CopyTable{Schema: t.Schema, Table: t.Table, Hidden: t.HiddenColumns, Relation: t.PGRelation, Trimmed: pgTrimmedOnCopy(t.PGRelation)})
	}
	stamp := pgCopyStamp(in)
	q.s.pgStampOf(q.id).put(time.Now(), stamp)
	return out, stamp, nil
}

// PGCopyStamp names the snapshot files a statement on the copy reads now,
// and the columns the server hides of each: two calls give the same stamp
// only when the copy's tables, their columns and their types are the same.
// It reads no table and needs none of the copy's workers. The PostgreSQL
// port reads it before reusing the catalog it keeps, so that a new snapshot
// (a column's type changed with it, #3251) is not answered with the old
// one's list of columns. It is read before nearly every statement, and with
// the snapshots in S3 and no local mirror yet each read lists S3, so one is
// shared by the server's connections for pgStampFor: a new snapshot is seen
// that much later at most.
func (q *SQLOnCopy) PGCopyStamp(ctx context.Context) (string, error) {
	return q.s.pgStampOf(q.id).get(time.Now(), func() (string, error) { return q.PGCopyStampFresh(ctx) })
}

// PGCopyStampFresh is PGCopyStamp read now: what reading the catalog
// compares with the stamp of its tables (CopyTablesStamped), which is never
// an older one.
func (q *SQLOnCopy) PGCopyStampFresh(ctx context.Context) (string, error) {
	in, err := q.s.buildViewsInput(ctx, q.b, pgCopyRequest)
	if err != nil {
		return "", err
	}
	stamp := pgCopyStamp(in)
	q.s.pgStampOf(q.id).put(time.Now(), stamp)
	return stamp, nil
}

// pgStampOf is server id's stamp memo, made on first use.
func (s *Server) pgStampOf(id string) *pgStampMemo {
	m, _ := s.pgStamps.LoadOrStore(id, &pgStampMemo{})
	return m.(*pgStampMemo)
}

// pgStampFor is how long a stamp is shared (PGCopyStamp).
const pgStampFor = 250 * time.Millisecond

// pgStampMemo is one server's last stamp and when it was read.
type pgStampMemo struct {
	mu  sync.Mutex
	at  time.Time
	val string
}

// get is the stamp read at most pgStampFor before now, or read reads it.
// An error is not kept.
func (m *pgStampMemo) get(now time.Time, read func() (string, error)) (string, error) {
	m.mu.Lock()
	if !m.at.IsZero() && now.Sub(m.at) >= 0 && now.Sub(m.at) < pgStampFor {
		v := m.val
		m.mu.Unlock()
		return v, nil
	}
	m.mu.Unlock()
	return read()
}

// put keeps a stamp just read, unless a later one is kept already.
func (m *pgStampMemo) put(at time.Time, val string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if at.After(m.at) {
		m.at, m.val = at, val
	}
}

// pgCopyRequest is the views input the port's catalog is read from.
var pgCopyRequest = viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true, LocalCopy: true, ApplyHidden: true}

// pgCopyStamp is in's stamp: each table's name, file and hidden columns.
// Names and paths are hashed whole, each with its length, so that no two
// lists run together into the same text.
func pgCopyStamp(in views.Input) string {
	rows := make([]string, 0, len(in.Baselines))
	for _, t := range in.Baselines {
		parts := append([]string{t.Schema, t.Table, t.Path}, t.HiddenColumns...)
		var b strings.Builder
		for _, p := range parts {
			fmt.Fprintf(&b, "%d:%s,", len(p), p)
		}
		rows = append(rows, b.String())
	}
	sort.Strings(rows)
	h := sha256.New()
	for _, r := range rows {
		fmt.Fprintf(h, "%d:%s\n", len(r), r)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// pgTrimmedOnCopy are the char(n) columns of rel the copy's view reads
// without their padding (baseline.PGPaddedToTrim), the one call the console
// makes for it.
func pgTrimmedOnCopy(rel *baseline.PGRelation) map[string]bool {
	return baseline.PGPaddedToTrim(rel)
}
