package console

import (
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// coverageResponse is GET /api/coverage — the live RPO statement behind the
// overview card (#1194): "any point between delta_from and delta_to is
// restorable", plus how far behind capture is and whether the range has
// holes. The DELTA half is metadata-only (timestamps and verdicts, no row
// data) and carries no profile gate (/api/status keeps its verdict for every
// session and scopes only its capture-health table names, #1452). The
// FULL-TABLE half is derived from the baseline listing — the surface /api/baselines refuses
// to a profiled session (#1075: baseline reads aren't redacted, and
// broken_tables is a table-name inventory) — so it is gated by the SAME
// sessionRestricted rule. Capture-health drops (#1034) are deliberately not
// consulted — they have their own surface (status, --fail-on-gap).
type coverageResponse struct {
	// Delta window: any row/point in [delta_from, delta_to] is recoverable
	// from indexed deltas. delta_from omitted = unknown floor (never
	// assumed); delta_to omitted = empty index.
	DeltaFrom string `json:"delta_from,omitempty"`
	DeltaTo   string `json:"delta_to,omitempty"`
	// LagSeconds = now − delta_to, present only when a capture stream exists
	// AND at least one event is indexed. The window's upper edge is the last
	// INDEXED event, never the wall clock — the lag is what says how close
	// to "now" that edge is.
	LagSeconds *int64 `json:"lag_seconds,omitempty"`
	// Continuity: ok | gap_lost | unknown | unavailable | none — the exact
	// status.ContinuityStatus rule, never recomputed here.
	Continuity string `json:"continuity"`
	// Freshness: current | idle | stalled | unknown | unavailable | none — the
	// exact status.FreshnessStatus rule, never recomputed here either (#1227).
	// It is the LIVENESS half continuity is not, and it is what makes
	// lag_seconds readable: the same number means a dead daemon under
	// "stalled" and a quiet source under "idle". Note the offline limit
	// FreshnessStatus documents — "idle" cannot separate a quiet source from
	// one whose capture is far behind, and the card must not imply either.
	Freshness string `json:"freshness"`
	// CheckpointAgeSeconds is how long ago the daemon last wrote stream_state;
	// omitted (never 0) when there is no checkpoint to age. Under "stalled"
	// this is the number that says how long it has been down.
	CheckpointAgeSeconds *int64 `json:"checkpoint_age_seconds,omitempty"`
	// BaselineConfigured mirrors /api/baselines' "configured" — a baseline
	// SOURCE exists. It is NOT the reconstruct gate (/api/capabilities), the
	// same configured-vs-reconstruct split baselines_api documents.
	BaselineConfigured bool `json:"baseline_configured"`
	// FullTableStatus discriminates the full-table half when a source is
	// configured: "ok" = evaluated (fields below are meaningful, possibly
	// empty because no usable baseline exists yet), "unknown" = could NOT be
	// evaluated (unknown floor or a failed listing) — absent fields must
	// never read as "nothing broken". Omitted when no source is configured
	// or the session is profile-restricted.
	FullTableStatus string `json:"full_table_status,omitempty"`
	// FullTableFrom: from this instant onwards every table with a usable
	// backup WHERE THE RESTORE BUTTON READS (see RestoreReads) is fully
	// reconstructable (the latest usable newest-per-table anchor). Tables in
	// broken_tables are excluded — they are not reconstructable at all — as
	// are those in unreachable_tables, whose anchor the Restore button cannot
	// open; never-baselined tables are invisible here.
	// Omitted when full_table_status != "ok" or no usable baseline exists.
	FullTableFrom string `json:"full_table_from,omitempty"`
	// BrokenTables: tables whose NEWEST baseline predates the delta floor —
	// full-table restore through that hole is impossible (#1193's verdict).
	BrokenTables []string `json:"broken_tables,omitempty"`
	// RestoreReads names the location the Restore button folds from: "s3"
	// when the server has an S3 destination (the same rule the scheduled
	// update follows, BaselineFoldSource, #1541), "dir" when it has only a
	// local directory, "" when Restore refuses the server outright — no
	// local directory of its own to fold INTO, whatever it reads from — and
	// "inherited" when the server names no location of its own and the card
	// graded the daemon-wide ones its bundle resolved to: a backup is there
	// and time-travel reads it, but Restore refuses that server too (#1602),
	// so the window is real and the button is not.
	// Time-travel reads the other way round (local first, bucket on a miss,
	// #766), which is why this card says which location it graded against.
	RestoreReads string `json:"restore_reads,omitempty"`
	// UnreachableTables: tables whose only usable baseline lives in a location
	// the Restore button does NOT read: on a server whose backups go to S3,
	// a snapshot that exists only on this host (the daemon-wide refresh loop
	// writes those, and so does an upload that failed); on a dir-only server,
	// a snapshot that exists only in a daemon-wide bucket.
	//
	// Its own bucket rather than broken_tables: broken drives an alarm, and a
	// backup that exists is not broken. Its own bucket rather than silence,
	// too — the listing behind this verdict reads every location (#1571), so
	// folding these into full_table_from would promise a restore the button
	// then refuses with "no backup exists at or before".
	UnreachableTables []string `json:"unreachable_tables,omitempty"`
	// RestoreNeedsLocal marks a server whose backups go ONLY to S3: the Restore
	// button needs a local backup directory to fold INTO and refuses outright
	// without one, so no per-table finding applies and unreachable_tables is
	// left empty rather than naming the entire schema.
	RestoreNeedsLocal bool `json:"restore_needs_local,omitempty"`
	// UnevaluableTables: tables whose coverage could not be decided. They are
	// why full_table_status is "unknown", and naming them is the whole point.
	//
	// The routine producer is the ambiguity demotion (#1219): on an index whose
	// archives cannot be attributed to a source, DeltaFloor.Grade turns
	// "broken" into "unknown", because a snapshot below the LIVE floor may
	// still be covered by that source's own archives. That is the right call
	// for the verdict and the wrong one for the inventory -- without the names
	// the card says only "could not be checked", and the operator cannot tell
	// which table to look at, or that any table is involved at all.
	UnevaluableTables []string `json:"unevaluable_tables,omitempty"`
}

// tableAnchors holds the two baseline anchors of one table, which answer two
// different questions and must not be conflated.
//
// newest decides whether the table is BROKEN: if even its most recent baseline
// predates the delta floor, no point in time is fully reconstructable for it.
//
// earliestUsable decides where the table's restorable window STARTS.
// reconstruct.FindBaseline does not use a table's newest baseline — it picks
// the newest snapshot AT OR BEFORE the requested instant — so an instant older
// than the newest baseline is served from an older one, provided that one's
// own anchor is inside delta coverage. Taking the newest here understated the
// window by however long ago the last baseline ran, and got worse the more
// diligently an operator snapshotted: every new baseline pushed the reported
// floor forward (#1294).
//
// earliestUsableReachable is the same anchor restricted to files in the
// location the Restore button reads (restoreReach), which is what
// full_table_from is reported from. The two differ because the listing behind
// this card reads every configured location (#1571) while the Restore button
// beside it folds from ONE: the bucket on a server with an S3 destination, the
// local directory otherwise (#1541, the same rule as the scheduled update).
// Reconstruct and time-travel DO reach both through bundle.findBaseline
// (#766), so an unreachable anchor is real coverage — it just cannot be the
// number printed next to a button that will refuse it.
type tableAnchors struct {
	newest                  time.Time
	earliestUsable          time.Time
	earliestUsableReachable time.Time
	// hasSource and newestSource describe the copies in the location the
	// Restore reads (restoreReach), usable or not. They decide the verdict for
	// a table whose SOURCE copy is stale while a fresh copy sits elsewhere:
	// the fold anchors on the newest source copy at-or-before the instant, so
	// a stale one refuses the whole restore (capture gap), and the fresh copy
	// in the other location does not help the button — under "dir" because
	// bundle.findBaseline falls back to the bucket only on ErrNoBaseline
	// (#766), so time travel resolves the stale local copy too and no console
	// surface reaches the bucket; under "s3" because the fold reads the bucket
	// and the local-only fresh copy (a failed upload, the daemon-wide refresh)
	// was never sent there. Both are broken, not unreachable: the remedy is a
	// fresh backup, which is what broken advises.
	hasSource    bool
	newestSource time.Time
}

// observe folds one baseline file's anchor in, with the verdict already graded
// against the delta floor. Usable means at or above the floor — ok and aging
// both qualify; aging says the window is shrinking, not that it is gone.
func (a *tableAnchors) observe(ts time.Time, v status.BaselineStalenessVerdict, reachable bool) {
	if ts.After(a.newest) {
		a.newest = ts
	}
	if reachable {
		a.hasSource = true
		if ts.After(a.newestSource) {
			a.newestSource = ts
		}
	}
	switch v {
	case status.BaselineOK, status.BaselineAging:
		if a.earliestUsable.IsZero() || ts.Before(a.earliestUsable) {
			a.earliestUsable = ts
		}
		if reachable && (a.earliestUsableReachable.IsZero() || ts.Before(a.earliestUsableReachable)) {
			a.earliestUsableReachable = ts
		}
	}
}

// fullTableVerdict is the full-table half of the coverage card, decided from a
// baseline listing and the delta floor.
type fullTableVerdict struct {
	from        time.Time
	broken      []string
	unreachable []string
	// unevaluable lists the tables that could not be graded, and is what makes
	// the whole card "unknown". A bare bool erased the names.
	unevaluable []string
}

// restoreReach is the location the Restore button folds from, and the
// evidence needed to tell whether a listed file is in it.
//
// kind follows BaselineFoldSource's rule on the server's OWN locations, and
// restoreReadsFrom's refusal rule on top: "s3" when it has a local directory
// AND an S3 destination, "dir" when only a local directory, "" when it has no
// local directory of its own — Restore refuses that server outright, so
// nothing is reachable and the unreachable list is suppressed
// (restore_needs_local, decided from the bundle's sources, says it once).
//
// inS3 is needed because the merged listing keeps the LOCAL path for a file
// present in both locations (the footer read wants it), so under "s3" the
// path alone would call every uploaded-and-kept snapshot unreachable.
type restoreReach struct {
	kind string
	inS3 map[baselineFileKey]bool
}

// restoreReadsFrom classifies where the Restore button reads for a server
// with these locations of its own: the precedence of BaselineFoldSource,
// after handleBaselineRestore's own gate. That gate comes first: with no
// local directory the fold has nowhere to WRITE and the button refuses, so
// an S3-only server reads from nothing, not from the bucket — grading it as
// "s3" would print a restore window next to a refusal. The card and the fold
// are separate functions and MUST agree; TestRestoreReadsFrom pins them
// against each other.
func restoreReadsFrom(dir, s3 string) string {
	switch {
	case dir == "":
		return ""
	case s3 != "":
		return "s3"
	}
	return "dir"
}

func (r restoreReach) reaches(f reconstruct.BaselineFile) bool {
	switch r.kind {
	case "dir":
		return baselineKindOf(f.Path) == "dir"
	case "s3":
		return baselineKindOf(f.Path) == "s3" || r.inS3[keyOf(f)]
	}
	return false
}

// gradeFullTable folds a merged baseline listing into that verdict.
//
// Pure, and separated from the handler for one reason the handler cannot give
// a test: an s3:// location is not listable from a unit test, so driving the
// endpoint can only ever produce local anchors, and the whole point of the
// reachable/unreachable split is what happens when an anchor is NOT where the
// Restore reads. Taking the files as an argument is what lets a test state
// that shape at all.
//
// Graded THROUGH the floor, never against its hour alone: on a multi-source
// index the hour is the live floor and an anchor below it is unattributable,
// not broken (#1219). Grading with the bare hour would name healthy tables in
// broken, the false alarm the floor's own narrowing exists to avoid.
func gradeFullTable(files []reconstruct.BaselineFile, reach restoreReach, floor status.DeltaFloor, now time.Time) fullTableVerdict {
	anchors := make(map[string]*tableAnchors, len(files))
	for _, f := range files {
		k := f.Schema + "." + f.Table
		a := anchors[k]
		if a == nil {
			a = &tableAnchors{}
			anchors[k] = a
		}
		// A file present in BOTH locations kept its LOCAL path in the merge;
		// reach.reaches consults reach.inS3 for that, so the kind of the path
		// alone never decides.
		a.observe(f.SnapshotTime, floor.Grade(f.SnapshotTime, now), reach.reaches(f))
	}

	var v fullTableVerdict
	for k, a := range anchors {
		if !a.earliestUsableReachable.IsZero() {
			// The window covering ALL usable tables starts at the LATEST of
			// their individual starts — and a table's own start is its
			// EARLIEST usable anchor, not its newest (see tableAnchors).
			if a.earliestUsableReachable.After(v.from) {
				v.from = a.earliestUsableReachable
			}
			continue
		}
		if a.hasSource {
			// The source has copies of this table and none is usable (branch 1
			// took the usable ones): the fold would anchor on the newest of
			// them and refuse. A fresh copy in the OTHER location does not
			// rescue the button (see tableAnchors), so this is broken on the
			// newest SOURCE anchor — under "dir" exactly the verdict this card
			// gave before it read both locations. Calling it unreachable
			// would trade a red "take a fresh backup" for a warning that says
			// the backup merely lives elsewhere.
			if floor.Grade(a.newestSource, now) == status.BaselineBroken {
				v.broken = append(v.broken, k)
				continue
			}
			// Only the ambiguity demotion reaches here: every source anchor is
			// below the floor, so the grade is Broken unless BelowIsUnknown
			// turned it into Unknown. Named rather than dropped -- on a
			// multi-source index it is the ROUTINE verdict, so silently
			// unevaluable would erase it most of the time.
			v.unevaluable = append(v.unevaluable, k)
			continue
		}
		if !a.earliestUsable.IsZero() {
			// A usable backup exists, and the Restore's source holds NO copy
			// of this table at all: only in a bucket on a dir-only server,
			// only on this host on an S3-backed one. Named, not folded into the window and
			// not called broken: counting this anchor would print a start the
			// button then refuses, while broken drives an alarm a backup that
			// exists does not deserve. Time-travel reads it either way (#766).
			// Suppressed wholesale when the server has no local directory of
			// its own to restore into: Restore refuses it outright, every
			// usable table is unreachable by construction, and the list would
			// name the whole schema and say nothing the single
			// restore_needs_local sentence does not.
			// Decided here rather than blanked in the handler, so it is a
			// property of the fold and a pure test can state it.
			if reach.kind != "" {
				v.unreachable = append(v.unreachable, k)
			}
			continue
		}
		// No usable anchor at all: the newest one says which kind of nothing
		// this is.
		if floor.Grade(a.newest, now) == status.BaselineBroken {
			v.broken = append(v.broken, k)
			continue
		}
		// Neither broken nor usable: it must not join broken AND must not
		// define the window, or "ok" would assert restorability from an anchor
		// whose coverage is unknown.
		v.unevaluable = append(v.unevaluable, k)
	}
	sort.Strings(v.broken)
	sort.Strings(v.unreachable)
	sort.Strings(v.unevaluable)
	return v
}

// serverID labels log lines with the request's selected server — a
// multi-server console emitting unattributed warns is undebuggable.
func serverID(r *http.Request) string {
	if id := r.Header.Get(serverHeader); id != "" {
		return id
	}
	return "default"
}

func (s *Server) handleCoverage(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	resp := coverageResponse{BaselineConfigured: b.baselineSrc != ""}
	if b.db == nil {
		// An unopened bundle connection degrades to an explicit
		// "unavailable" card, never a fabricated window.
		slog.Warn("console: coverage not evaluated — the server's index connection is not open", "server", serverID(r))
		resp.Continuity = "unavailable"
		resp.Freshness = status.FreshnessUnavailable
		writeJSON(w, http.StatusOK, resp)
		return
	}
	now := time.Now().UTC()
	sum, err := status.CollectCoverageSummary(r.Context(), b.db, b.dbName, now)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp.Continuity = sum.Continuity
	resp.Freshness = sum.Freshness
	resp.CheckpointAgeSeconds = sum.CheckpointAgeSeconds
	resp.LagSeconds = sum.LagSeconds
	if !sum.Floor.Hour.IsZero() {
		resp.DeltaFrom = sum.Floor.Hour.Format(consoleTSFormat)
	}
	if !sum.DeltaTo.IsZero() {
		resp.DeltaTo = sum.DeltaTo.Format(consoleTSFormat)
	}
	// Full-table half: newest-per-table baseline anchors graded against the
	// floor — #1193's verdict via the status package, no second
	// implementation. Gated exactly like /api/baselines (#1075): the listing
	// this is derived from bypasses redaction, and broken_tables is a
	// table-name inventory a deny-profile withholds elsewhere. A failed
	// listing or an unknown floor is "unknown", never a silently-empty "ok"
	// — a broken-table warning must not vanish because of an error.
	switch {
	case b.baselineSrc == "":
	case sessionRestricted(r):
		recordProfileGateDeny(r, "coverage")
	case sum.Floor.Hour.IsZero():
		resp.FullTableStatus = "unknown"
	default:
		// EVERY configured location, not b.baselineSrc alone (#1571). This is
		// a verdict about what can be restored, so deriving it from one of two
		// places answers about a subset: on a server with a local directory
		// and an S3 destination, a snapshot that survives only in the bucket
		// would be graded as if it were gone, and the panel would report a
		// shorter restorable window than the one that exists.
		// Decided from the CONFIGURATION, ahead of any listing: on a server that
		// backs up ONLY to S3 there is no local directory to fold into, so
		// Restore refuses outright and every table would be unreachable; the
		// card would print one warn line naming the whole schema. That is a
		// configuration fact, stated once. A list of every table is the kind
		// of line operators learn to skip, and the next one they skip is a
		// real one. Ahead of the listing because an unreachable bucket and an
		// S3-only server must not look the same.
		restoreNeedsLocal := !slices.ContainsFunc(baselineSourcesOf(b),
			func(s string) bool { return baselineKindOf(s) == "dir" })
		resp.RestoreNeedsLocal = restoreNeedsLocal
		// Where the Restore button reads, classified from the bundle's
		// locations. withBaselineDefaults is all-or-nothing, so for a server
		// that names locations of its own these ARE its own, the ones
		// handleBaselineRestore uses; for one that names none — the boot
		// entry, or a registry entry inheriting --baseline-dir/--baseline-s3 —
		// they are the daemon-wide ones. That server is graded against them
		// anyway, because Restore refuses it for a different reason (#1602)
		// and grading it as reading nothing would render the "no usable
		// baseline exists yet" shape over a backup that is there; the card
		// says "inherited" instead of the kind so it does not claim a button
		// the page does not render. Stays "" when even those locations give
		// the fold nowhere to write, which is the restore_needs_local shape.
		reads := restoreReadsFrom(bundleBaselineDir(b), bundleBaselineS3(b))
		resp.RestoreReads = reads
		if e, ok := s.selectedEntry(r); reads != "" && (!ok || (e.BaselineDir == "" && e.BaselineS3 == "")) {
			resp.RestoreReads = "inherited"
		}

		merged := listBaselinesMerged(r.Context(), baselineSourcesOf(b), reconstruct.ListBaselines)
		// ANY location that FAILED TO LIST makes the verdict unknown, not just
		// all of them. A partial listing can only understate coverage, and an
		// understated coverage window names healthy tables as broken -- the
		// cry-wolf failure status.DeltaFloor already refuses when archives
		// cannot be attributed. Unknown is the honest third state.
		//
		// Bounded deliberately at whole-location failures: listBaselinesLocal
		// warns and skips a snapshot subdirectory it cannot read and returns a
		// nil error, so that location still counts as answered and this guard
		// does not see it. Closing that hole means propagating a partial
		// signal out of reconstruct.ListBaselines, which is upstream of here.
		if merged.Listed < len(merged.Sources) || merged.Listed == 0 {
			slog.Warn("console: coverage card could not list every backup location; the verdict is unknown rather than graded against a partial view",
				"server", serverID(r), "listed", merged.Listed, "configured", len(merged.Sources))
			resp.FullTableStatus = "unknown"
			break
		}
		v := gradeFullTable(merged.Files, restoreReach{kind: reads, inS3: merged.InS3}, sum.Floor, now)
		resp.FullTableStatus = "ok"
		resp.BrokenTables, resp.UnreachableTables = v.broken, v.unreachable
		if len(v.unevaluable) > 0 {
			resp.FullTableStatus = "unknown"
			resp.UnevaluableTables = v.unevaluable
			// The card tells the operator to check the daemon log, and this is
			// the only producer of "unknown" that was not writing one: the
			// listing failure and the missing floor both log already.
			slog.Warn("console: coverage could not grade every table; their newest backup sits below a floor whose archives cannot be attributed to one source",
				"server", serverID(r), "tables", v.unevaluable)
			break
		}
		if !v.from.IsZero() {
			resp.FullTableFrom = v.from.Format(consoleTSFormat)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
