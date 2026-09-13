// Package views generates DuckDB view definitions over a bintrail archive
// layout.
//
// The output is SQL TEXT and nothing else: this package never opens DuckDB, and
// `bintrail views` never executes what it prints. That is the whole design
// stance — the Parquet tier is an open layout the operator already owns, so the
// product's job is to hand them a correct schema for it, not to become the
// engine that reads it. Whatever DuckDB they already have (CLI, a notebook, an
// embedded process) runs the result, on their machine, with no result caps, no
// server and no new surface to secure.
//
// Everything here is a pure function of its Input, which is what makes the
// generated SQL golden-file testable and what keeps the discovery (index reads,
// baseline listing) in the command layer where it can be seen.
package views

import (
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// BaselineTable names one table's Parquet file inside a baseline snapshot.
// FollowMode says how the state views reach a snapshot published after this
// file was generated. The two following modes buy the same thing by different
// means, and they do NOT cost the reader the same, which is why they are
// distinct values rather than one bool.
type FollowMode int

const (
	// FollowNone pins the views to the snapshot discovered at generation time.
	// Rows stop changing; regenerating the file is what moves them.
	FollowNone FollowMode = iota
	// FollowPointer names the baselines root's `current` symlink in every
	// path. One rename(2) moves every table at once, so no query can resolve
	// into a half-published snapshot and no two views can disagree about which
	// snapshot they are reading.
	FollowPointer
	// FollowNewest resolves the newest `_SUCCESS`-marked snapshot into a session
	// variable that every state view reads through. It is the only following
	// available on an S3 root, which has no pointer to rewrite.
	//
	// One variable, so the views agree with each other the way the pointer makes
	// them agree. What differs is WHEN: the pointer is followed per query, this
	// is resolved when the file is read. In practice that is the same session
	// boundary an S3 file already has, since its credentials do not persist
	// either.
	FollowNewest
)

// follows reports whether the state views move on their own, for the wording
// both following modes share.
func (m FollowMode) follows() bool { return m != FollowNone }

type BaselineTable struct {
	Schema string
	Table  string
	Path   string // local path or s3:// URL of the table's .parquet file
	// Rel is Path relative to its snapshot directory ("schema/table.parquet"),
	// set by the producer under BOTH following modes. FollowNewest needs it to
	// build its view bodies at all: the following there is a SQL shape rather
	// than a path, so it rebuilds the file name against whichever snapshot the
	// query picks. FollowPointer's bodies do not need it, but the preflight
	// (#1558) does, and it asks the same question of both files, so deriving
	// the tail one way here and another way there is how the two renders would
	// come to disagree about the layout. Deriving it back out of Path in the
	// generator would be a second opinion for the same reason: the producer
	// already split it once to decide it could follow at all.
	Rel string

	// Decimals are the table's DECIMAL and NUMERIC columns, which the baseline
	// writer stores as text (internal/baseline.MysqlToParquetNode says why).
	// The state view casts each one back to a number so arithmetic works
	// without the reader having to discover the storage choice through a
	// failed sum().
	Decimals []DecimalColumn
	// SchemaKnown records whether the table's embedded CREATE TABLE was read at
	// all. It separates "this table has no decimal columns" from "we could not
	// find out", which are the same empty Decimals slice and very different
	// facts to state in a file someone reads to understand their own layout.
	SchemaKnown bool
}

// ArchiveGroup is one set of archived files that share a column set, as read
// back from archive_state by query.ArchiveGroups (#1535). Restated here as a
// plain struct so this package keeps generating text out of values and links
// nothing to reach the registry.
type ArchiveGroup struct {
	// Columns is the group's shared column set, lowercase. A column another
	// group has and this one does not is emitted as NULL.
	Columns []string
	// Files are the group's archived files: full local paths or s3:// URLs.
	Files []string
}

// LiveIndex names the index whose `binlog_events` the events view unions in as
// its hot leg, so a query is fresh to capture lag instead of to the archive
// retention window (#1480).
//
// The archives only hold partitions rotation has already archived. On a
// deployment measured for #1465 that left the most recent ~12 hours visible
// nowhere in the Parquet tier, which a reader of an archives-only view cannot
// distinguish from those rows not existing.
//
// It carries HOST, PORT, DATABASE and USER, which are configuration, and NEVER
// a password: this file is meant to be shared, and the header says it holds no
// credentials. The generated preamble emits the password slot empty for the
// operator to fill in their own session.
type LiveIndex struct {
	Host     string
	Port     int
	Database string
	User     string

	// BintrailID attributes the hot leg's rows, and is the ONLY signal that
	// they are attributed at all. Live binlog_events has no per-row source
	// column (the archives get theirs from the Hive path), so this can only be
	// filled when exactly one source was observed. Empty = the hot rows carry
	// NULL, and Attribution below says what was observed instead.
	//
	// One field, not a pair: an "it is attributed" flag alongside the value is
	// a pair that can desync, and the desynced state would emit an
	// attribution nobody established.
	BintrailID string

	// Attribution is consulted ONLY when BintrailID is empty, to say what was
	// observed. Its zero value is the undetermined one on purpose: a producer
	// that fills nothing must not make the file claim anything.
	Attribution LiveAttribution

	// TableColumns is the live binlog_events column set as OBSERVED on the
	// index. A column named here is selected; one that is absent is emitted as
	// NULL, because naming it would make DuckDB refuse the whole file with a
	// binder error and define no view at all.
	//
	// This is the hot leg's union_by_name: the cold leg tolerates archives
	// written before a column existed, and an index migrated to a different
	// point (the console sets EnsureSchema: false and never migrates registry
	// servers) needs exactly the same tolerance.
	//
	// Empty means NOT OBSERVED, and every column is named — the behaviour of a
	// producer that cannot probe. `bintrail views` always probes.
	TableColumns []string
}

// LiveAttribution records what the producer OBSERVED about which source the
// index serves, for the one line of the generated file that must not guess.
//
// The rule this type exists to keep is the same one Input.ArchiveDiscoveryFailed
// and Input.RegionAmbiguous keep: never state a cause the caller does not know.
// A single "unattributed" value collapsed four different observations —
// several sources, none registered, an unreadable list, a disagreement — into
// one sentence that was false for three of them.
//
// It names the OBSERVATION, never the inference. "No source is registered" is
// what the count reports; that this is a file-mode or legacy index is a guess
// about why, and it stays out of the artifact.
type LiveAttribution int

const (
	// AttributionUndetermined: nothing was established. The zero value, so an
	// Input that fills none of this says so rather than asserting.
	AttributionUndetermined LiveAttribution = iota
	// AttributionMultiSource: more than one source is registered, and a live
	// row carries no identity of its own to tell them apart by.
	AttributionMultiSource
	// AttributionUnregistered: the index registers no source id — a zero count,
	// a NULL id, or no such table at all. Every one of those is "there is no id
	// to attribute a row to", which is what the file says.
	AttributionUnregistered
)

// Input is everything Generate needs. The command layer resolves it; nothing in
// this package performs IO.
type Input struct {
	// GeneratedAt stamps the header. Passed in rather than read from the clock
	// so the output is reproducible under test.
	GeneratedAt time.Time
	// Version is the bintrail build that generated the file, for the header.
	Version string

	// ArchiveSources are the archive base paths discovered from archive_state
	// (each ending in the `bintrail_id=<id>` segment), local or s3://.
	ArchiveSources []string
	// PortableRouting is true when ArchiveSources were resolved by
	// query.PortableArchiveSources (S3 wherever the registry has one), which
	// is the only case in which the header may state that rule. It names the
	// ROUTING, not the provenance: the console SQL panel's sources also come
	// out of the registry, local-first, and leave this false. An operator who
	// named the roots with --archive-dir/--archive-s3 gets exactly what they
	// named, both of them if they passed both (#1456).
	PortableRouting bool
	// ArchiveGroups, when non-empty, REPLACES the single globbed cold leg with
	// one leg per column set (#1535). Each group names its files explicitly and
	// reads them with union_by_name = false, so the bind opens one footer per
	// GROUP instead of one per file — the cost that made a statement over the
	// events view wait on every archive ever written.
	//
	// Correctness is preserved by writing out what union_by_name was doing: a
	// column a group does not have is emitted as NULL. Absent groups keep the
	// globbed form, which is what every caller did before this existed.
	ArchiveGroups []ArchiveGroup
	// UngroupedPartitions is how many registered partitions have no recorded
	// column set. It must be zero for ArchiveGroups to be used — see
	// query.ArchiveGroups — and it is carried here so the file can SAY why it
	// still binds the slow way instead of leaving the operator to wonder.
	UngroupedPartitions int

	// ArchiveDiscoveryFailed is set when the registry could not be read at
	// all. The file then says so instead of "none registered", which would
	// state a cause the caller does not know. A bool, not the error: the
	// error text names the index host and the DB user (a dial error, a 1142),
	// and this file is meant to be shared. The text goes to the console log,
	// and to the 502 body when there is no baseline half to serve; `bintrail
	// views` fails the command instead of setting this.
	ArchiveDiscoveryFailed bool
	// ArchiveRegion pins REGION in the S3 secret. Empty = let the credential
	// chain resolve it, which is what every other bintrail S3 read does by
	// default.
	ArchiveRegion string

	// RegionAmbiguous is set when two buckets were each DETECTED in a
	// different region, so ArchiveRegion was left empty rather than pinned to
	// one of them: one secret and one s3_region cannot describe two. The file
	// states that as a fact, so the producer must not set it for a bucket that
	// merely could not be asked — an undetectable bucket is not evidence that
	// the regions differ, and claiming it would send the reader off to split a
	// file that is fine. Same rule as ArchiveDiscoveryFailed above: never state
	// a cause the caller does not know.
	RegionAmbiguous bool
	// S3Endpoint is the S3-compatible store the paths live in, when the
	// generating process was configured with one (#1454). It is a location,
	// not a credential, and belongs in the file: a reader on another machine
	// has no other way to learn that s3:// here does not mean AWS.
	S3Endpoint storage.S3Endpoint

	// BaselineSource is the root the snapshot below was discovered under, for
	// the header. BaselineSnapshot is that snapshot's timestamp.
	BaselineSource   string
	BaselineSnapshot time.Time
	// NewerElsewhere names a snapshot that is more recent than the one this
	// file pins, found in the server's OTHER backup location (#1571).
	//
	// It is a warning and deliberately NOT a correction. This file names ONE
	// root, and every state view resolves a path under it: a reader who has
	// the local directory cannot open an s3:// path and the reverse is just
	// as true, so merging the two locations would produce views that half of
	// the readers cannot resolve. What silence cost instead was worse in a
	// quieter way — a file pinned to a week-old snapshot because the newest
	// one had already aged out of local retention, reading as current.
	NewerElsewhere       time.Time
	NewerElsewhereSource string
	// NewerElsewhereHowTo is the route THIS producer's reader can take to get
	// that snapshot instead, rendered verbatim as one comment line. Same rule
	// as LiveLegHowTo: this package states no route it cannot see, so an empty
	// value leaves the note stating the fact alone. A note that names a
	// problem and no way to act on it is where the reader stops.
	NewerElsewhereHowTo string
	// NewerElsewhereUnchecked names the other location when the look at it did
	// NOT answer, so the header can say the question is open instead of
	// rendering a file indistinguishable from one whose other location was
	// read and held nothing newer. Set only when NewerElsewhere is zero.
	NewerElsewhereUnchecked string
	// Follow records how, if at all, the state views reach a snapshot published
	// after this file was generated (#1484, #1550). ApplyFollow is what sets
	// it; see FollowMode for what each mode costs the reader.
	Follow FollowMode
	// Baselines are the tables of the NEWEST discoverable snapshot only. An
	// older snapshot's rows are a different point in time, and a view union-ing
	// two of them would silently mix states that never coexisted.
	Baselines []BaselineTable

	// LiveIndex, when set, adds the hot leg to the events view. Nil keeps the
	// archives-only view, which is what `bintrail views` emitted before #1480
	// and what a file generated with no reachable index must still emit.
	LiveIndex *LiveIndex

	// LiveLegUnavailable is set by a producer that has no way to offer the hot
	// leg at all, so the archives-only note does not send its reader to a
	// route that surface cannot take. The console download sets it per SERVER:
	// one whose index connection is not open, or whose index DSN names a unix
	// socket, cannot carry the leg however the reader asks.
	LiveLegUnavailable bool

	// LiveLegHowTo replaces the archives-only note's "regenerate with
	// --include-live" line with the route THIS producer's reader can take. The
	// console sets it because that reader has a checkbox, not a command line,
	// and a flag they cannot pass is not remediation. One line of plain text,
	// rendered as a comment; empty keeps the CLI wording, and it is ignored
	// when LiveLegUnavailable is set (a producer with no route names none).
	LiveLegHowTo string

	// ExcludeEventColumns drops the named columns (matched case-insensitively
	// against archive.BinlogEventColumns) from the `events` view. Purely
	// mechanical here — the views package names no policy. The console SQL panel
	// (#1177) uses it to withhold the paid forensics columns (connection_id,
	// query_text, query_hash) it must not serve, the same set eventDTO omits.
	// Empty for the downloadable `bintrail views` file, which describes the
	// operator's OWN Parquet in full.
	ExcludeEventColumns []string

	// OnlyViews limits the render to the views it names. Mechanical, like
	// ExcludeEventColumns above: this package decides nothing about WHICH views
	// a caller wants, it only leaves out the ones the caller did not ask for.
	// The console SQL panel (#1526) sets it to what one statement references,
	// because defining a view over Parquet binds its columns — a file read, and
	// a network round trip per file on an S3 layout — so a statement that names
	// none of them should pay for none of them.
	OnlyViews ViewSet

	// SnapshotScoped marks the file that is PUBLISHED INSIDE a snapshot,
	// beside its _SUCCESS marker (#1583). Such a file describes the one
	// snapshot it sits in and nothing else: its producer never read
	// archive_state, so the header must not claim anything about it, and the
	// events section names the wider file instead of a registry nobody
	// checked — the ArchiveDiscoveryFailed rule ("never state a cause the
	// caller does not know") applied to a producer that never asked.
	// Producers set it with Follow == FollowNone and no ArchiveSources; it
	// changes wording, never mechanism.
	SnapshotScoped bool

	// OmitEvents leaves the events view out even when archive sources ARE
	// available. It is the DEFAULT for both file producers since #1535: binding
	// that view opens one Parquet footer per archived file at CREATE VIEW time,
	// a cost that is O(archived files) and grows forever, and it is paid before
	// the first row comes back — so a reader who only wanted their tables paid
	// the whole change log to get them.
	//
	// A separate field rather than an OnlyViews entry, because OnlyViews names
	// views INDIVIDUALLY and the state view names are manufactured in this
	// package (sanitized, deduped). A caller outside it cannot spell "every
	// state view but not events" without duplicating that naming.
	//
	// Composes with OnlyViews rather than replacing it: events renders only if
	// OnlyViews wants it AND this is false. The zero value is the old
	// behaviour, so the SQL panel — which decides through OnlyViews and needs
	// events whenever a statement names it — is unaffected.
	OmitEvents bool
}

// ViewSet names a subset of the views an Input defines.
//
// The nil/empty distinction is the whole type: a NIL set means every view,
// which is what the downloadable file has always emitted and what any caller
// that never heard of this field gets; a non-nil set means exactly the names it
// holds, and an EMPTY non-nil set therefore means NO views at all, which is
// what `SELECT 1` needs. A bare []string could not tell those two apart.
type ViewSet map[string]bool

// wants reports whether name is in the set.
//
// The lookup is lowercased because DuckDB identifiers are case-insensitive and
// a set is built from a statement a human typed; the set's KEYS are therefore
// expected lowercase, which is how its one producer builds them. An
// unrecognized name is simply not wanted: this is a filter, never a validator,
// so it neither errors nor logs on one.
func (v ViewSet) wants(name string) bool {
	if v == nil {
		return true
	}
	return v[strings.ToLower(name)]
}

// Generate renders the complete .sql file: the explanatory header, the S3
// credential preamble when needed, and the view definitions. This is the
// artifact an operator downloads and runs in their OWN DuckDB, so the preamble
// creates a credential_chain secret and INSTALLs httpfs inline.
func Generate(in Input) string {
	var b strings.Builder
	writeHeader(&b, in)
	writeTimeZone(&b)
	if in.NeedsS3() {
		region := in.ArchiveRegion
		if in.RegionAmbiguous {
			// Enforced here, not only trusted from the producer: the file
			// STATES that no region is pinned, so emitting one anyway would
			// make the artifact contradict itself in the one place a reader
			// looks to understand why their read failed.
			region = ""
		}
		// Stays ahead of every view, and is the one statement the reordering
		// below does NOT protect anyone from: baselines can live on S3 too, so
		// a secret emitted after the state views would break the layout this
		// preamble exists to serve. Its CREATE SECRET aborts the session when
		// no credential resolves, which costs the reader every view emitted
		// after it — including purely local state views, on a layout whose
		// baselines are local and whose archives are on S3. The degrade
		// documented below is about the ATTACH, not about credentials.
		writeS3Preamble(&b, region, in.S3Endpoint, in.RegionAmbiguous)
	}
	// The state views FIRST, then the ATTACH, then the events view that needs
	// it.
	//
	// `duckdb -init` aborts the session at the first error, and the ATTACH is
	// the one statement in this file that depends on reaching another machine:
	// a host that does not resolve, a password left blank, an index that is
	// down. Emitted ahead of the views, as it used to be, that single failure
	// cost the reader the whole file — no events view, no state views, nothing
	// — even though the state views read one Parquet file per table and needed
	// nothing from the index. DuckDB commits what ran before the aborting
	// statement, so moving them ahead of the ATTACH turns that total loss into
	// a degrade: the table snapshots are already created and the reader loses
	// the events view plus a message naming what could not be reached.
	//
	// "Already created" is not the same as "still reachable", and
	// writeAttachDegradeNote is where that distinction is spelled out for the
	// reader rather than assumed here.
	//
	// The events view is defined ONCE, here at the end, rather than
	// archives-only above the ATTACH and CREATE OR REPLACEd below it. Two
	// definitions would bind the archive file list twice, and that bind is the
	// expensive statement in this file: `union_by_name` opens one Parquet
	// footer per archived file before the view returns a row (#1535). Measured
	// over 120 files, the second bind costs about half the first (7.1s then
	// 3.6s), and writing it as a reference to the first view instead of
	// repeating the literal costs the same 3.6s — DuckDB re-binds a view per
	// statement, so there is no cheap way to say it twice.
	//
	// The deliberate trade: under --include-live an unreachable index costs the
	// events view ENTIRELY, not just its hot leg. The state views are what
	// survive. A leg over the archives alone would have to be paid for with a
	// second full bind on every generated file, including the ones whose index
	// is reachable.
	//
	// "Survive" is conditional on HOW the file is run, and the note the events
	// view carries says so rather than asserting the good case. Verified in
	// DuckDB v1.5.5 with a failing ATTACH: `.read` in an open session reports
	// the error and keeps going; `duckdb -init file.sql your.db` exits but the
	// views are in your.db when it is reopened; a bare `duckdb -init file.sql`
	// exits and the in-memory database dies with it, so nothing survives —
	// and that last one is an invocation `bintrail views --help` names.
	stateSurvives := writeStateViews(&b, in)
	// The preamble is gated on the events view being RENDERED, not merely on an
	// index being configured: under OnlyViews the ATTACH would otherwise be
	// emitted with no view reading through it, above a comment introducing a
	// hot leg that is not there.
	if in.LiveIndex != nil && in.OnlyViews.wants(eventsViewName) {
		writeLivePreamble(&b, in.LiveIndex)
	}
	writeEventsView(&b, in, stateSurvives)
	return b.String()
}

// GenerateViews renders ONLY the view definitions — no header, no S3 preamble.
// It is for a caller that runs the views in a DuckDB session it already set up:
// bintrail's own S3 credential wiring (duckdbutil.EnableS3CredentialChain) is
// best-effort and tolerates an unresolved credential chain, whereas the
// download preamble's `CREATE SECRET` aborts the whole script when no
// credentials resolve. The console SQL panel (#1177) uses this so its session
// setup does not hinge on the human-facing preamble.
func GenerateViews(in Input) string {
	var b strings.Builder
	// The hot leg is dropped here rather than documented as unsupported. This
	// entry point emits no preamble, so it emits no ATTACH, so a two-leg view
	// rendered through it would reference a catalog that does not exist and
	// fail at CREATE VIEW. Nilling it makes that divergence impossible instead
	// of leaving Generate and GenerateViews with different preconditions on
	// the same Input; the caller gets the archives-only view, which is what
	// this entry point can actually back.
	in.LiveIndex = nil
	// stateSurvives is false because this entry point emits no ATTACH: with
	// nothing that can abort, there is no degrade to describe. (It also emits
	// the state views AFTER this call, so the good answer is not available
	// here either.)
	events := writeEventsView(&b, in, false)
	state := writeStateViews(&b, in)
	if !events && !state {
		// Not one view was emitted, so whatever is in the builder is comments.
		// The caller EXECUTES this text and DuckDB answers a comment-only script
		// with "empty query", which would turn "this statement needs no view"
		// into an error. An empty string is the honest instruction to run
		// nothing.
		return ""
	}
	return b.String()
}

// NeedsS3 reports whether the rendered file will read any s3:// path. Callers
// use it to decide whether a broken S3 endpoint configuration is worth
// refusing over: a layout that is entirely local reads nothing through httpfs,
// so failing it on an unrelated environment variable blocks a render that
// would have been correct.
//
// It answers for what will actually be RENDERED, so a set OnlyViews narrows it
// the same way it narrows the output: a session built for a statement that
// names only a local state view needs no S3 credentials, even though the layout
// around it has S3 in it. With OnlyViews nil this is the whole layout, which is
// every caller outside the SQL panel.
func (in Input) NeedsS3() bool {
	// rendersEvents, not OnlyViews alone: a file that leaves the events view out
	// reads no archive path, so emitting an S3 secret for one would make the
	// whole script abort on an unresolvable credential chain for a bucket it was
	// never going to touch.
	if in.rendersEvents() {
		for _, s := range in.ArchiveSources {
			if isS3(s) {
				return true
			}
		}
	}
	for _, p := range stateViewPlan(in) {
		if isS3(p.table.Path) && in.OnlyViews.wants(p.name) {
			return true
		}
	}
	return false
}

// DefinedViews names every view this Input defines, ignoring OnlyViews: it
// answers "what could this layout define", which is what a caller filtering by
// name needs in order to tell a name it can build from one it cannot.
//
// LiveIndex is read as Generate reads it. GenerateViews nils it before
// rendering, so a caller that pairs this with THAT entry point must not set it
// either, or this would claim an events view a live-only render would emit and
// GenerateViews would not.
func (in Input) DefinedViews() []string {
	var names []string
	if in.definesEvents() {
		names = append(names, eventsViewName)
	}
	for _, p := range stateViewPlan(in) {
		names = append(names, p.name)
	}
	return names
}

// SelectedBaselines returns the baseline tables whose state view this Input
// would render, honoring OnlyViews. It answers "which baseline files does this
// render actually read", which is what a caller that reports on those files has
// to count over: a session built for `SELECT 1` opens none of them, so a note
// about their column types would be describing files that query never touched.
func (in Input) SelectedBaselines() []BaselineTable {
	var out []BaselineTable
	for _, p := range stateViewPlan(in) {
		if in.OnlyViews.wants(p.name) {
			out = append(out, p.table)
		}
	}
	return out
}

// rendersEvents reports whether THIS render will contain an events view: the
// view has to be defined at all (definesEvents) and the caller must not have
// left it out. Every sentence elsewhere in the file that points at that view
// asks this, so a filtered or state-only render cannot describe a neighbour it
// does not have.
func (in Input) rendersEvents() bool {
	return in.definesEvents() && !in.OmitEvents && in.OnlyViews.wants(eventsViewName)
}

// RendersEventsView is rendersEvents for callers outside this package: a
// producer that reports what it wrote needs the same answer the renderer used,
// or its summary line describes a different file from the one on disk.
func (in Input) RendersEventsView() bool { return in.rendersEvents() }

// RendersAnyView reports whether this Input would produce a file with at least
// one view definition in it.
//
// The two FILE producers ask it and refuse when it is false. A refusal keyed on
// the OUTCOME rather than on a flag combination is the point: `bintrail views`
// already refuses `--no-baselines` without `--include-events`, but the
// identical empty file is reached by simply not naming a baseline location, and
// that path used to render the events view, so the flip turned a useful command
// into a silently useless one (exit 0, "wrote views.sql", nothing in it).
//
// GenerateViews has its own empty-string answer for the same situation and does
// NOT go through here: its caller executes the text, so "run nothing" is a
// legitimate result there, not a mistake to report.
func (in Input) RendersAnyView() bool {
	if in.rendersEvents() {
		return true
	}
	for _, p := range stateViewPlan(in) {
		if in.OnlyViews.wants(p.name) {
			return true
		}
	}
	return false
}

// definesEvents reports whether the events view is emitted at all: it needs a
// leg, and a failed discovery leaves no usable archive list whatever
// ArchiveSources holds. One predicate, shared by writeEventsView and
// DefinedViews, so the two cannot disagree about whether the view exists.
func (in Input) definesEvents() bool {
	cold := !in.ArchiveDiscoveryFailed && len(in.ArchiveSources) > 0
	return cold || in.LiveIndex != nil
}

func isS3(p string) bool { return strings.HasPrefix(p, "s3://") }

func writeHeader(b *strings.Builder, in Input) {
	fmt.Fprintf(b, "-- DuckDB views over a bintrail archive layout.\n")
	fmt.Fprintf(b, "-- Generated by bintrail %s at %s.\n",
		orUnknown(in.Version), in.GeneratedAt.UTC().Format(time.RFC3339))
	b.WriteString("--\n")
	b.WriteString("-- THIS FILE IS A SNAPSHOT OF THE LAYOUT, NOT A LIVE BINDING. ")
	// The events view follows the layout only when it is GLOBBED. A grouped one
	// (#1535) names its archived files one by one, which is what buys the cheap
	// bind and what costs the self-updating: partitions archived later are not
	// in it. Every sentence below that says the file keeps up with rotation is
	// gated on this, because getting it wrong is worse than saying nothing —
	// the reader keeps a file that quietly stopped covering the recent end of
	// their history.
	grouped := len(in.ArchiveGroups) > 0
	// checksSnapshot, not Follow.follows(), for the sentence below that names
	// the dropped-table check. The two are the same today for every render a
	// producer makes, and they come apart for a filtered one: writeStateViews
	// returns before the preflight when OnlyViews selected no state view, so a
	// file could promise a check it does not contain. Deriving the promise from
	// the same plan the writer uses is what keeps them coextensive rather than
	// merely equal today.
	checks := in.checksSnapshot()
	if in.Follow.follows() {
		// The state views follow too, so the sentence above is now only about
		// SHAPE. Say which shape, concretely: a reader who knows a new table
		// will not appear on its own can schedule a regeneration; one told
		// only "not a live binding" cannot tell what still needs doing.
		//
		// Which MECHANISM follows is named rather than glossed as "follow":
		// the two do not fail the same way, and the reader who has to reason
		// about a mid-session refresh needs to know which one is in the file.
		how := "follow the `" + baseline.CurrentLinkName + "` pointer"
		if in.Follow == FollowNewest {
			how = "select the newest completed snapshot"
		}
		if in.rendersEvents() && grouped {
			b.WriteString("The events view below\n")
			b.WriteString("-- names the archived files it reads one by one, so partitions archived\n")
			b.WriteString("-- after this file was written are NOT in it, with no error and no warning:\n")
			b.WriteString("-- a query over the most recent hours simply returns nothing for them.\n")
			b.WriteString("-- Regenerate on the schedule your rotation archives on. The baseline state\n")
			b.WriteString("-- views do " + how + ", so a refreshed baseline reaches them\n")
		} else if in.rendersEvents() {
			b.WriteString("The globs below\n")
			b.WriteString("-- keep picking up newly rotated partitions, and the baseline state views\n")
			b.WriteString("-- " + how + ", so a refreshed baseline reaches them\n")
		} else {
			b.WriteString("The baseline state\n")
			b.WriteString("-- views " + how + ", so a refreshed baseline reaches them\n")
		}
		b.WriteString("-- on its own. What does NOT follow is this file's idea of the SHAPE of the\n")
		b.WriteString("-- data: which views exist, and how each DECIMAL column is read, were decided\n")
		b.WriteString("-- from the snapshot named below. Re-run `bintrail views` (or download the file\n")
		b.WriteString("-- again from the console) after a table is added or dropped, after a column\n")
		b.WriteString("-- changes type, and whenever archive sources are added or removed.\n")
		b.WriteString("--\n")
		b.WriteString("-- Which of those you will notice follows one rule: this file names only the\n")
		b.WriteString("-- PATHS and the DECIMAL columns, so only those two can fail. ")
		if checks {
			b.WriteString("A table that\n")
			b.WriteString("-- leaves the newest snapshot is caught by the check below, which names the\n")
			b.WriteString("-- table and stops before any view is created; a DECIMAL column renamed or\n")
			b.WriteString("-- dropped fails the script at its own view.\n")
		} else {
			b.WriteString("A table that\n")
			b.WriteString("-- leaves the newest snapshot stops resolving, and the read fails at its own\n")
			b.WriteString("-- view; so does a DECIMAL column renamed or dropped.\n")
		}
		b.WriteString("-- Everything else is QUIET, because `SELECT *` passes it through untouched:\n")
		b.WriteString("-- a table added to the source has no view here, a DECIMAL whose scale grew is\n")
		b.WriteString("-- read at the old scale with the extra digits rounded away, a column that\n")
		b.WriteString("-- changes to some other type arrives as whatever the new file holds (an\n")
		b.WriteString("-- ORDER BY can start sorting text), and an archive source added later is\n")
		b.WriteString("-- simply not here. None of those raise an error.\n")
	} else if in.rendersEvents() && grouped {
		b.WriteString("The events view below\n")
		b.WriteString("-- names the archived files it reads, one by one, and the baseline state views\n")
		b.WriteString("-- point at ONE snapshot. NOTHING here updates itself: partitions archived\n")
		b.WriteString("-- after this file was written are not in it, with no error and no warning —\n")
		b.WriteString("-- a query over the most recent hours simply returns nothing for them.\n")
		b.WriteString("-- Regenerate this file on the schedule your rotation archives on, and after\n")
		b.WriteString("-- taking or refreshing a baseline, and whenever archive sources are added or\n")
		b.WriteString("-- removed. Re-run `bintrail views`, or download the file again from the\n")
		b.WriteString("-- console.\n")
	} else if in.rendersEvents() {
		// The one self-following half of the file, and only the events view
		// has it: its globs are evaluated per query. Claimed only when that
		// view is actually here, or a state-only file would promise a
		// following it does no part of.
		b.WriteString("The globs below\n")
		b.WriteString("-- keep picking up newly rotated partitions on their own, but the baseline\n")
		b.WriteString("-- state views point at ONE snapshot. Re-run `bintrail views` (or download the\n")
		b.WriteString("-- file again from the console) after taking or refreshing a baseline, and\n")
		b.WriteString("-- whenever archive sources are added or removed.\n")
	} else if in.SnapshotScoped {
		b.WriteString("The baseline state\n")
		b.WriteString("-- views point at the snapshot this file was published with, and that is the\n")
		b.WriteString("-- point of this copy: it sits beside the files it names, so it cannot\n")
		b.WriteString("-- disagree with them. A newer snapshot carries its own copy of this file,\n")
		b.WriteString("-- beside its own data.\n")
	} else {
		b.WriteString("The baseline state\n")
		b.WriteString("-- views point at ONE snapshot. Re-run `bintrail views` (or download the file\n")
		b.WriteString("-- again from the console) after taking or refreshing a baseline.\n")
	}
	// The sentence above covers an operator who takes baselines by hand: the
	// refresh is something they DO, so regenerating is the next thing they do.
	// It does not cover --baseline-refresh-interval, where the snapshot is
	// published on a timer and nobody performs an action this advice can attach
	// to (#1484). Naming the flag WITH its binary is what separates the two
	// readers, and `bintrail views` is not that binary.
	//
	// Two renderings, one for each behaviour, rather than one paragraph plus a
	// caveat: the pinned half warns that the rows stop changing, and under
	// following that warning is simply false. The pinned arm stays phrased so
	// it holds with no state views at all ("ANY state view below"); the
	// following arms may assert they exist, because ApplyFollow only ever
	// leaves Follow set alongside a non-empty Baselines.
	//
	// "nothing regenerates this file", NOT "nothing re-runs this command": the
	// console download is produced by a page, not a command, which is the same
	// distinction Input.LiveLegUnavailable exists to make. The paragraph above
	// already names both routes.
	b.WriteString("--\n")
	switch {
	case in.Follow == FollowNewest:
		// The same paragraph as the pointer arm up to the last two sentences,
		// which is where the two mechanisms actually differ. Stating the
		// weaker guarantee is not optional: a reader who was told "a single
		// step" would reasonably assume the whole file moves together, and
		// here it does not.
		b.WriteString("-- A daemon running `bintrail-console watch --baseline-refresh-interval`\n")
		b.WriteString("-- publishes a new snapshot every interval, and the state views below move to\n")
		b.WriteString("-- it once it completes. They resolve which one ONCE, when this file is read,\n")
		b.WriteString("-- so every state view shows the same snapshot and none of them can drift\n")
		b.WriteString("-- apart mid-session. A refresh published while you are working is picked up\n")
		b.WriteString("-- by reading this file again, which is already what an S3 session needs to do\n")
		b.WriteString("-- for the secret above.\n")
		b.WriteString("-- That choice lives in a SESSION variable, and views persist while a session\n")
		b.WriteString("-- variable does not: a database file that saved these views lists them all in\n")
		b.WriteString("-- a new session, and every read raises until this file's SET VARIABLE\n")
		b.WriteString("-- statement is run again in that session.\n")
	case in.Follow == FollowPointer:
		// The paragraph this replaces exists because a timer-published
		// snapshot has no operator action to attach "regenerate" to. Following
		// removes that need for ROWS and leaves it for SHAPE, so the same
		// reader is now told the one thing still on their plate.
		b.WriteString("-- A daemon running `bintrail-console watch --baseline-refresh-interval`\n")
		b.WriteString("-- publishes a new snapshot every interval, and the state views below move to\n")
		b.WriteString("-- it when it completes. Replacing the pointer is a single step, so no path\n")
		b.WriteString("-- ever resolves into a half-published snapshot, and a query already running\n")
		b.WriteString("-- finishes against the files it opened. Two views resolved either side of that\n")
		b.WriteString("-- one step can still land on different snapshots; the window is the swap\n")
		b.WriteString("-- itself, not the hours a file left behind would span.\n")
	case in.SnapshotScoped:
		b.WriteString("-- A daemon running `bintrail-console watch --baseline-refresh-interval`\n")
		b.WriteString("-- publishes a new snapshot every interval, and each one is published with\n")
		b.WriteString("-- its own copy of this file. This copy stays bound to the snapshot it sits\n")
		b.WriteString("-- in: its rows never change, and the newer rows are beside the newer\n")
		b.WriteString("-- snapshot's own copy.\n")
	default:
		b.WriteString("-- A daemon running `bintrail-console watch --baseline-refresh-interval`\n")
		b.WriteString("-- publishes a new snapshot every interval, and nothing regenerates this file.\n")
		b.WriteString("-- Any state view below stays bound to the snapshot it was generated against,\n")
		b.WriteString("-- with no error and no warning: its rows just stop changing. Regenerate this\n")
		b.WriteString("-- file on the same schedule that refresh runs on.\n")
	}
	b.WriteString("--\n")
	b.WriteString("-- Nothing here writes: every view is a read over Parquet files you already own.\n")
	b.WriteString("--\n")

	// The path choice is stated where the paths are listed, and only when a
	// choice was made over a real listing: portable routing names an archive
	// registered with both a local path and an S3 location by the S3 one,
	// because this file is meant to run on a machine that is not the one the
	// local path belongs to (#1456). Explicitly named roots are listed as
	// named, and a failed read lists nothing, so neither gets the sentence.
	// Baseline state views are out of it on purpose: they point wherever the
	// baseline root points.
	if in.PortableRouting && !in.ArchiveDiscoveryFailed {
		b.WriteString("-- Archive sources (an archive registered with both a local path and an S3\n")
		b.WriteString("-- location is listed by its S3 location, so those reads work from another\n")
		b.WriteString("-- machine; a local path below means the registry holds no S3 location this\n")
		b.WriteString("-- file can use):\n")
	} else {
		b.WriteString("-- Archive sources:\n")
	}
	switch {
	case in.SnapshotScoped:
		// This producer never asked archive_state, so the two claims below are
		// not available to it — the same never-state-a-cause-you-do-not-know
		// rule ArchiveDiscoveryFailed keeps, from the other side.
		b.WriteString("--   (out of scope: this file describes the one snapshot it was published\n")
		b.WriteString("--   with; `bintrail views` writes the file that also reads the archived\n")
		b.WriteString("--   change log)\n")
	case in.ArchiveDiscoveryFailed:
		b.WriteString("--   (could not be read from archive_state; the console log has the error)\n")
	case len(in.ArchiveSources) == 0:
		b.WriteString("--   (none registered in archive_state: no rotated partitions have been archived yet)\n")
	}
	for _, s := range in.ArchiveSources {
		fmt.Fprintf(b, "--   %s\n", commentSafe(s))
	}
	b.WriteString("-- Baseline snapshot:\n")
	switch {
	case len(in.Baselines) == 0 && in.BaselineSource == "":
		b.WriteString("--   (no baseline source given: pass --baseline-dir or --baseline-s3)\n")
	case len(in.Baselines) == 0:
		fmt.Fprintf(b, "--   (none discoverable under %s)\n", commentSafe(in.BaselineSource))
	case in.SnapshotScoped && in.BaselineSource == ".":
		// The tarball's copy (#1583): paths spelled "./schema/table.parquet".
		// Guarded by INTENT, not by the path value alone: `bintrail views
		// --baseline-dir .` reaches this function with BaselineSource "." and
		// SnapshotScoped false, and the tarball wording would misdescribe it
		// three ways at once — "this archive" for a CLI file, a
		// one-level-too-deep "run from inside" remediation, and a followed
		// file stripped of the pointer line that separates it from a pinned
		// one. Only snapshotViewsRelative pairs the dot root with the scope.
		// Relative is the point — it survives being unpacked anywhere, moved
		// later, or handed to someone else, because nothing inside names a
		// place — and the price is stated with it: DuckDB resolves relative
		// paths against the PROCESS working directory, not against this file.
		fmt.Fprintf(b, "--   this archive, taken at %s (%d table(s))\n",
			in.BaselineSnapshot.UTC().Format(time.RFC3339), len(in.Baselines))
		b.WriteString("--   Paths are relative: run DuckDB from inside the unpacked folder (the one\n")
		b.WriteString("--   holding the schema directories). From anywhere else the reads fail with\n")
		b.WriteString("--   DuckDB's own \"No files found\" naming the exact relative path.\n")
	default:
		// commentSafe on the SOURCE as well as the sentence: a registry entry's
		// baseline path is operator-supplied and unvalidated for newlines, and
		// one here would end the comment and leave the rest of the path on a
		// line DuckDB executes. The HowTo beside it is a compile-time
		// constant; this one is not, which is the half that needed it.
		fmt.Fprintf(b, "--   %s at %s (%d table(s))\n",
			commentSafe(in.BaselineSource), in.BaselineSnapshot.UTC().Format(time.RFC3339), len(in.Baselines))
		switch {
		case !in.NewerElsewhere.IsZero():
			fmt.Fprintf(b, "--   NOTE: a newer snapshot (%s) exists under %s, which this file does\n"+
				"--   not read. A file names one location, and its paths only resolve there.\n",
				in.NewerElsewhere.UTC().Format(time.RFC3339), commentSafe(in.NewerElsewhereSource))
			if in.NewerElsewhereHowTo != "" {
				fmt.Fprintf(b, "--   %s\n", commentSafe(in.NewerElsewhereHowTo))
			}
		case in.NewerElsewhereUnchecked != "":
			// Say the check did not answer. Silence here is indistinguishable
			// from "the other location holds nothing newer", and the two lead
			// to opposite actions. Same shape as the archive half above.
			fmt.Fprintf(b, "--   (whether %s holds a newer snapshot could not be read; the\n"+
				"--   console log has the error)\n", commentSafe(in.NewerElsewhereUnchecked))
		}
		switch in.Follow {
		case FollowPointer:
			// Name the timestamp AND the pointer: the timestamp is what the
			// column types were read from, the pointer is what the views
			// actually open. Reporting only one of the two would make a
			// followed file indistinguishable from a pinned one.
			fmt.Fprintf(b, "--   read through %s/%s, which is that snapshot right now\n",
				commentSafe(strings.TrimSuffix(in.BaselineSource, "/")), baseline.CurrentLinkName)
		case FollowNewest:
			// Same job as the pointer line, and the same reason: the timestamp
			// above is where the column types came from, not what the views
			// open. What they open is decided per read.
			b.WriteString("--   read through the newest `_SUCCESS` snapshot, which is that one right now\n")
		}
		// A local root resolves on ONE machine, and this file travels: the
		// console serves it to a browser, and a generated file is meant to be
		// copied around. Said here, beside the path, because without it the
		// mismatch surfaces as DuckDB's "No files found", which reads as a
		// missing or corrupt backup rather than as the right file on the wrong
		// host. An s3:// root needs no such line; that is what it is for.
		if !isS3(in.BaselineSource) {
			b.WriteString("--   a directory, so the state views resolve only where it is mounted,\n")
			b.WriteString("--   at exactly this path\n")
		}
	}
	b.WriteString("\n")
}

func orUnknown(s string) string {
	if strings.TrimSpace(s) == "" {
		return "(unknown version)"
	}
	return s
}

// writeS3Preamble mirrors what duckdbutil.EnableS3CredentialChain configures on
// every DuckDB session bintrail opens, so a query that works through `bintrail
// query --archive-s3` works the same way pasted into a bare DuckDB.
//
// The credential-chain form is deliberate and is the reason this file is safe to
// paste into a notebook or share with a colleague: it resolves whatever the
// environment already has (instance role, SSO profile, env vars) and puts NO key
// material in the generated text.
// writeTimeZone pins the session to UTC, which is the one statement in this file
// that is neither a view nor a way to reach storage.
//
// It is here because leaving it out is wrong QUIETLY. The archives store
// event_timestamp as TIMESTAMP WITH TIME ZONE, so the session's zone decides
// both how it prints and where date_trunc puts a day boundary; DuckDB defaults
// to the machine's zone, and a reader west of UTC gets buckets that disagree
// with the console, with the MCP tools and with every timestamp dbtrail records,
// without anything failing. The guide used to carry this as a step the reader
// performed by hand, which made a silent wrong answer the default for anyone who
// skipped it.
//
// Emitted, not enforced: the comment says what it did so a reader who would
// rather work in their own zone can change one line.
func writeTimeZone(b *strings.Builder) {
	b.WriteString("-- Timestamps are recorded in UTC, and the archives carry the zone, so the\n")
	b.WriteString("-- session's setting decides how they print and where date_trunc puts a day\n")
	b.WriteString("-- boundary. Pinned to UTC here so the numbers match the console. Change it if\n")
	b.WriteString("-- you would rather read in your own zone.\n")
	b.WriteString("SET TimeZone = 'UTC';\n\n")
}

func writeS3Preamble(b *strings.Builder, region string, ep storage.S3Endpoint, ambiguousRegion bool) {
	b.WriteString("-- S3 setup, mirroring what bintrail's own DuckDB sessions configure.\n")
	if ep.Set() {
		fmt.Fprintf(b, "-- s3:// paths here live in an S3-compatible store at %s, not in AWS.\n", ep.URL)
	}
	if ambiguousRegion {
		b.WriteString("-- No region is pinned below: two of the buckets this file reads were\n")
		b.WriteString("-- each detected in a DIFFERENT region, and one secret cannot name two.\n")
		b.WriteString("-- Your own AWS configuration resolves one of them; the other rejects a\n")
		b.WriteString("-- request signed for it. Split those reads into one file per region, or\n")
		b.WriteString("-- add a second scoped secret for the odd bucket out.\n")
	}
	b.WriteString("INSTALL httpfs; LOAD httpfs;\n")
	b.WriteString("INSTALL aws; LOAD aws;\n")
	// Ahead of the secret, and not only inside it: an interactive DuckDB
	// CONTINUES past a failed statement, so a session where the credential
	// chain resolves nothing (an offline laptop, expired SSO, no aws
	// extension) would skip the secret and read AWS with whatever ambient keys
	// it finds — the same hole the daemon's own routing closes. The two
	// mechanisms are independent: a secret's ENDPOINT does not set these, and
	// these do not populate the secret.
	//
	// From duckdbutil's own list, not a copy of it: the copy drifted once
	// already, losing s3_region.
	for _, stmt := range duckdbutil.S3SettingStatements(region, ep) {
		fmt.Fprintf(b, "%s;\n", stmt)
	}
	secret := "CREATE OR REPLACE SECRET bintrail_s3_chain (TYPE s3, PROVIDER credential_chain" +
		duckdbutil.S3SecretClauses(region, ep) + ");\n"
	b.WriteString(secret)
	// The secret is TEMPORARY on purpose, and the file says so where the
	// operator will look when a reopened database file cannot read S3. The
	// PERSISTENT form is not offered: DuckDB resolves the credential chain at
	// creation and writes the resulting keys to ~/.duckdb/stored_secrets, which
	// would make a file that promises "no credentials" plant them on disk
	// (#1456).
	b.WriteString("-- This secret lives only in this DuckDB session. Views persist in a database\n")
	b.WriteString("-- file; secrets do not. Reopening that file later and querying S3 fails with\n")
	b.WriteString("-- \"No credentials are provided\": run this file again in every session that\n")
	b.WriteString("-- reads S3 (`.read views.sql`, or `duckdb -init views.sql your.db`).\n")
	b.WriteString("-- Do not make it PERSISTENT: DuckDB would resolve your credential chain now\n")
	b.WriteString("-- and store the resulting keys on disk.\n")
	b.WriteString("-- No credentials appear in this file by design. If the credential chain is not\n")
	b.WriteString("-- available where you run this, replace the secret above with explicit keys:\n")
	b.WriteString("--   CREATE OR REPLACE SECRET bintrail_s3_chain (\n")
	if ep.Set() {
		b.WriteString("--     TYPE s3, KEY_ID '…', SECRET '…', REGION '…',\n")
		fmt.Fprintf(b, "--     ENDPOINT %s, URL_STYLE %s, USE_SSL %t);\n",
			sqlString(ep.Host()), sqlString(map[bool]string{true: "path", false: "vhost"}[ep.PathStyle]), ep.UseSSL())
	} else {
		b.WriteString("--     TYPE s3, KEY_ID '…', SECRET '…', REGION '…');\n")
	}
	b.WriteString("\n")
}

// eventTypeCase renders the numeric event_type as its name. The mapping is
// event.EventInsert/Update/Delete; an unknown code renders as its own number
// rather than NULL, so a future event type shows up as an unfamiliar value
// instead of silently disappearing from a filtered query.
const eventTypeCase = "CASE \"event_type\"\n" +
	"      WHEN 1 THEN 'INSERT'\n" +
	"      WHEN 2 THEN 'UPDATE'\n" +
	"      WHEN 3 THEN 'DELETE'\n" +
	"      ELSE CAST(\"event_type\" AS VARCHAR)\n" +
	"    END AS \"event_type\""

// writeEventsView emits the union view over every archived partition.
//
// The projected column list is derived from archive.BinlogEventColumns — the
// same slice the archiver writes with — so a column added or removed there
// changes this output and breaks the golden test, rather than leaving the
// generated schema quietly behind the files it describes.
// liveAttachAlias is the DuckDB catalog name the index is attached under. It is
// deliberately not "index": that is close enough to a reserved word in enough
// dialects that an operator pasting these views elsewhere hits an avoidable
// parse error, and a distinctive name makes the hot leg obvious in a plan.
const liveAttachAlias = "bintrail_live"

// eventsViewName is the one fixed view name in the output; every other name is
// derived from a baseline table. Named so the filter and the generator agree on
// its spelling.
const eventsViewName = "events"

// liveSecretName is the DuckDB secret the ATTACH reads credentials from.
const liveSecretName = "bintrail_index"

// writeLivePreamble emits the mysql extension setup for the hot leg.
//
// The password slot is left EMPTY on purpose. Everything else here (host, port,
// database, user) is configuration that a reader on another machine genuinely
// needs, but this file is meant to be shared and its header says it carries no
// credentials — so the one field that is a credential is the one the operator
// fills in their own session. Same stance as the S3 preamble, which reaches for
// a credential chain rather than writing keys into the file; MySQL has no such
// chain, so the slot is explicit instead.
func writeLivePreamble(b *strings.Builder, li *LiveIndex) {
	b.WriteString("-- Live index setup, for the hot leg of the events view.\n")
	b.WriteString("-- FILL IN THE PASSWORD BELOW before running: this file is shareable and\n")
	b.WriteString("-- carries none. Everything else is the index location, which is not secret.\n")
	b.WriteString("-- Read-only: nothing in this file writes, and the ATTACH enforces it.\n")
	b.WriteString("INSTALL mysql; LOAD mysql;\n")
	// icu is declared for the same reason mysql and httpfs are: this file runs
	// in a DuckDB nobody here controls, and it must not depend on an extension
	// that merely happens to be loaded. The index leg reads event_timestamp
	// AT TIME ZONE 'UTC', which is ICU's. Where ICU is built in, both
	// statements are a no-op that reports it already installed.
	b.WriteString("INSTALL icu; LOAD icu;\n")
	fmt.Fprintf(b, "CREATE OR REPLACE SECRET %s (\n", quoteIdent(liveSecretName))
	b.WriteString("    TYPE mysql,\n")
	fmt.Fprintf(b, "    HOST %s,\n", sqlString(li.Host))
	fmt.Fprintf(b, "    PORT %d,\n", li.Port)
	fmt.Fprintf(b, "    DATABASE %s,\n", sqlString(li.Database))
	fmt.Fprintf(b, "    USER %s,\n", sqlString(li.User))
	b.WriteString("    PASSWORD ''  -- <- your index password\n")
	b.WriteString(");\n")
	// Normalized ONCE for both predicates. A DNS root-anchored form
	// ("localhost.", from a DSN the driver resolves the same as "localhost")
	// used to reach the single-label branch, because only that predicate
	// trimmed the trailing dot: the reader was told their host resolves in one
	// network and nowhere else, about a name that resolves everywhere to the
	// wrong index. The two branches must classify the same string.
	host := strings.TrimSuffix(li.Host, ".")
	if isLoopbackHost(host) {
		// Observed: the host IS a loopback address. NOT stated: why it is one
		// (an omitted address in the DSN, an SSH tunnel, a sidecar). The file
		// says what it can see, and what that costs a reader elsewhere.
		b.WriteString("-- HOST above is a loopback address, which names only the machine this file\n")
		b.WriteString("-- was generated on. Run this file somewhere else and the ATTACH resolves to\n")
		b.WriteString("-- whatever that machine runs on the same port. That may answer, with\n")
		b.WriteString("-- entirely plausible rows from a different index. Change HOST to a name that\n")
		b.WriteString("-- resolves from where you run this.\n")
	} else if isSingleLabelHost(host) {
		// Observed: the host is a bare name with no domain. NOT stated: why
		// (a Docker Compose service, a Kubernetes service, a /etc/hosts entry,
		// a search-domain suffix that happens to be configured here). Each of
		// those resolves inside one network and nowhere else, and this file is
		// built to be carried out of it.
		//
		// A separate branch from the loopback one rather than a widened
		// predicate: loopback resolves everywhere and silently answers with the
		// WRONG index, while this one usually fails to resolve at all. The
		// costs differ, so the sentences do.
		b.WriteString("-- HOST above is a bare name with no domain, which usually resolves only\n")
		b.WriteString("-- inside the network this file was generated in — a Docker Compose or\n")
		b.WriteString("-- Kubernetes service name resolves for containers on that network and for\n")
		b.WriteString("-- nothing outside it. Run this file elsewhere and the ATTACH below fails\n")
		b.WriteString("-- with an unknown-host error. Change HOST to an address that resolves\n")
		b.WriteString("-- from where you run this, or open a tunnel to it first.\n")
	}
	writeLiveCaptureNote(b)
	fmt.Fprintf(b, "ATTACH '' AS %s (TYPE mysql, SECRET %s, READ_ONLY);\n\n",
		quoteIdent(liveAttachAlias), quoteIdent(liveSecretName))
}

// writeLiveCaptureNote says WHOSE server the hot leg reads, next to the ATTACH
// that opens it.
//
// The two legs are not two halves of one thing (#1483). The cold leg opens
// Parquet files on disk or in an object store, which competes with nothing; this
// one opens a connection to the index a running bintrail is writing captured
// events into. In the run that prompted this, an analytical query scanned a 15M
// row live binlog_events and capture on that server stopped minutes later, and
// the person writing the query had no signal that this was a possibility.
//
// The consequence is worded from what the code does, not from that run: #1482
// tagged the batch-INSERT deadline with indexer.ErrWriteDeadline, and
// consoleapp/mainstream.go is the ONLY place that re-arms on it. So
// `bintrail-console watch` restarts and replays from its checkpoint, while
// every standalone capture process ends the run: `bintrail stream`
// (cliapp/stream.go, a bare streamrun.One), `bintrail up` (cliapp/up.go, which
// delegates to the same runStream) and `bintrail-pg stream`
// (cmd/bintrail-pg/stream.go, a bare pgstreamrun.One whose flush writes through
// this same indexer). The note names the CLASS rather than one member, because
// an `up` operator handed a sentence about `stream` reads themselves out of it.
//
// And the standalone case has no "while": the process exits and stays exited
// until something outside it restarts it, which is the 41-minute outage
// consoleapp/mainstream.go's own comment records. Wording it as capture
// "falling behind while that happens" described a dip, not a stop.
func writeLiveCaptureNote(b *strings.Builder) {
	b.WriteString("-- SHARED WITH CAPTURE: this attaches the index bintrail writes captured\n")
	b.WriteString("-- events into, so a query over the events view reads the server capture is\n")
	b.WriteString("-- writing to. The view cannot push your filter down to it (see COST below),\n")
	b.WriteString("-- so every query is a full scan of binlog_events, and a big one competes with\n")
	b.WriteString("-- capture for that server's disk and buffer pool. One measured run: an\n")
	b.WriteString("-- analytical query over a 15 million row binlog_events, and capture on that\n")
	b.WriteString("-- server stopped minutes later. An index write that runs past its timeout\n")
	b.WriteString("-- ends the run in a standalone capture process (`bintrail stream`, `bintrail\n")
	b.WriteString("-- up`, `bintrail-pg stream`), which then stays down until something restarts\n")
	b.WriteString("-- it; `bintrail-console watch` restarts from its last checkpoint instead.\n")
	b.WriteString("-- Capture is behind either way.\n")
	// The remedy is the COST note's, not "filter the view": this same block has
	// just said a filter does not reach the index, so advising one would
	// contradict the sentence above it and send the reader to the one thing
	// that cannot help.
	b.WriteString("-- Two ways to keep it off capture: query `bintrail_live`.\"binlog_events\"\n")
	b.WriteString("-- directly with your own WHERE, which does reach the index, or point HOST\n")
	b.WriteString("-- above at a read replica, at the cost of that replica's own lag on top of\n")
	b.WriteString("-- capture lag.\n")
}

// isLoopbackHost reports whether the generated ATTACH points at the generating
// machine itself. "localhost" is included by name: it is not an IP literal, but
// it resolves to the loopback everywhere it resolves at all.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	// An empty host reaches the driver as localhost (a DSN like `tcp(:3306)`),
	// so it is a loopback for the reader's purposes and gets the loopback
	// warning rather than falling through to no warning at all.
	if host == "" {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// isSingleLabelHost reports whether the generated ATTACH names a host with no
// domain part: `index-mysql`, not `index.example.com` and not an address.
//
// Callers must ask isLoopbackHost FIRST — `localhost` is single-label too, and
// it has its own warning because it fails in the opposite way (it resolves, and
// answers with the wrong index).
//
// An IP literal is not single-label whatever it looks like, so it is excluded
// by parse rather than by counting dots: an IPv6 address has no dots at all,
// and a bracketed one is still an address.
func isSingleLabelHost(host string) bool {
	h := strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if h == "" || net.ParseIP(h) != nil {
		return false
	}
	return !strings.Contains(h, ".")
}

// writeAttachDegradeNote states what a reader is left holding when the ATTACH
// above this view fails, which is the whole point of the ordering (#1536) and
// the one thing that reader cannot work out for themselves — their session
// either died or is showing them one error and no view.
//
// stateSurvives says whether any state view was actually defined above the
// ATTACH. It has to be asked rather than assumed: a file generated with archive
// sources, an index, and no --baseline-dir defines none, and telling that
// reader their snapshots are safe would send them looking for views that were
// never written.
//
// coldLegAvailable says whether regenerating without the live index would still
// produce an events view. With no archive source it would not, so no remedy is
// offered instead of one that yields an empty file.
func writeAttachDegradeNote(b *strings.Builder, stateSurvives, coldLegAvailable bool) {
	b.WriteString("--\n")
	b.WriteString("-- Defined AFTER the ATTACH above, so an index this machine cannot reach\n")
	if stateSurvives {
		b.WriteString("-- leaves this view undefined and the state_ views above already created.\n")
		b.WriteString("-- Keeping them takes a session that outlives the error: `.read` this file\n")
		b.WriteString("-- from an open DuckDB, or run `duckdb -init <this file> your.db` and then\n")
		b.WriteString("-- reopen your.db. A bare `duckdb -init <this file>` exits on the error and\n")
		b.WriteString("-- the in-memory database goes with it, so nothing is left.\n")
	} else {
		b.WriteString("-- leaves you with no view at all: this file defines no state_ view either\n")
		b.WriteString("-- (it names no baseline snapshot), so there is nothing here to fall back\n")
		b.WriteString("-- on. Point the generator at a baseline location to get one.\n")
	}
	if coldLegAvailable {
		b.WriteString("-- Regenerating WITHOUT the live index gives an events view over the\n")
		b.WriteString("-- archives alone, which needs nothing from the index.\n")
	}
	b.WriteString("--\n")
	b.WriteString("-- It is deliberately not ALSO defined over the archives alone beforehand:\n")
	b.WriteString("-- union_by_name opens one Parquet footer per archived file at CREATE VIEW\n")
	b.WriteString("-- time, so a second definition pays that bind again in every generated\n")
	b.WriteString("-- file, whether or not the index turns out to be reachable.\n")
}

// writeEventsView returns whether it emitted a view definition (as opposed to
// nothing, or a comment explaining why there is none). It is called ONCE per
// rendered file — see Generate for why the view is never redefined.
//
// stateSurvives is Generate's answer for writeAttachDegradeNote; GenerateViews
// emits no ATTACH and passes false.
func writeEventsView(b *strings.Builder, in Input, stateSurvives bool) bool {
	// Not wanted: emit NOTHING, comments included. A filtered render is executed
	// by its caller, and a script of only comments is an error there.
	if !in.OnlyViews.wants(eventsViewName) {
		return false
	}
	// The in-snapshot file (#1583): the change log is a different tier, and
	// this producer never looked at the registry that lists it, so neither of
	// the skip sentences below may be borrowed — both name archive_state.
	if in.SnapshotScoped {
		b.WriteString("-- events: not part of this file.\n")
		b.WriteString("--\n")
		b.WriteString("-- This file describes the snapshot it sits in. The archived change log is a\n")
		b.WriteString("-- different tier; `bintrail views` (or the console's DuckDB schema download)\n")
		b.WriteString("-- writes the file that reads both.\n\n")
		return false
	}
	// Left out on purpose, which is a DIFFERENT fact from the skip branches
	// below and must not borrow their wording: "no archive sources are
	// registered" would send an operator to check a registry that is fine.
	// A comment, not silence, because the reader is looking at a file with no
	// events view in it and the file is the only thing that can tell them why.
	//
	// definesEvents is asked HERE, not only below: with no archive source there
	// would be no view to define anyway, and claiming a cost ("one footer per
	// archived file") plus a remedy ("--include-events") for a layout with no
	// archived files is advice that regenerates the same file. That shape falls
	// through to the skip branch, which states the real reason.
	if in.OmitEvents && in.definesEvents() {
		b.WriteString("-- events: not included in this file.\n")
		b.WriteString("--\n")
		b.WriteString("-- Defining it opens one Parquet footer per archived file before it returns\n")
		b.WriteString("-- a row, so it is left out unless asked for. Add it with `bintrail views\n")
		b.WriteString("-- --include-events`, or the matching box on the console download.\n\n")
		return false
	}
	// Whether the view EXISTS is definesEvents' question, asked below. These two
	// are the wording inputs: which legs it has decides what the comment says,
	// and a failed discovery leaves no usable archive list whatever
	// ArchiveSources holds, so it decides the cold leg here too.
	live := in.LiveIndex != nil
	cold := !in.ArchiveDiscoveryFailed && len(in.ArchiveSources) > 0

	switch {
	case live && !cold:
		b.WriteString("-- events: every binlog event the index still holds.\n")
	case live:
		b.WriteString("-- events: every binlog event, from the archives and from the index.\n")
	default:
		b.WriteString("-- events: every archived binlog event, across all archive sources.\n")
	}

	if !in.definesEvents() {
		if in.ArchiveDiscoveryFailed {
			// The header already names the failure; the body must not
			// contradict it with a cause nobody verified.
			b.WriteString("-- (skipped: archive_state could not be read; see the header)\n\n")
		} else {
			b.WriteString("-- (skipped: no archive sources are registered in archive_state)\n\n")
		}
		return false
	}

	if cold {
		b.WriteString("--\n")
		if len(in.ArchiveGroups) > 0 {
			fmt.Fprintf(b, "-- The archives are read in %d group(s), one per column set. Archives written\n", len(in.ArchiveGroups))
			b.WriteString("-- before a column existed simply lack it, so that column is selected as NULL\n")
			b.WriteString("-- for the group that lacks it. Files inside a group share a column set, which\n")
			b.WriteString("-- is why each read_parquet can say union_by_name = false: with it on, DuckDB\n")
			b.WriteString("-- opens EVERY file's footer to unify the schema, on every statement, and the\n")
			b.WriteString("-- wait grows with the archive. Grouping makes that one footer per group.\n")
			b.WriteString("--\n")
			b.WriteString("-- The file list is FIXED, and it comes from archive_state. Two consequences,\n")
			b.WriteString("-- both worth knowing before you save this file:\n")
			b.WriteString("--   - partitions archived after this was generated are not read. Regenerate.\n")
			b.WriteString("--   - a file under one of these roots with no archive_state row is not read.\n")
			b.WriteString("-- `bintrail archive reconcile` exits non-zero on both kinds of drift; it\n")
			b.WriteString("-- reports counts per partition, not file paths.\n")
		} else {
			b.WriteString("-- union_by_name is required, not cosmetic: archives written before a column\n")
			b.WriteString("-- existed simply lack it, and those files must read back with NULLs rather\n")
			b.WriteString("-- than failing the whole scan. A column absent from EVERY archived file is\n")
			b.WriteString("-- still an error here: drop it from the SELECT if you hit that on an old\n")
			b.WriteString("-- archive. (The grouped form this file is not using pads it with NULL.)\n")
			b.WriteString("--\n")
			b.WriteString("-- COST: union_by_name makes DuckDB open one Parquet footer per archived file\n")
			b.WriteString("-- to unify the schema, and a view re-binds on every statement, so the wait\n")
			b.WriteString("-- before the first row grows with the archive.\n")
			if in.UngroupedPartitions > 0 {
				fmt.Fprintf(b, "-- %d archived partition(s) cannot be grouped by schema: no recorded column\n", in.UngroupedPartitions)
				b.WriteString("-- set, or a registered file that is not on disk. Recording the set reads\n")
				b.WriteString("-- each footer once, offline:\n")
				if in.NeedsS3() {
					// --deep is not optional on S3: without it the scan never
					// opens a remote footer, so the repair finds nothing to
					// record and the wait stays exactly as it is.
					b.WriteString("--   bintrail archive reconcile --index-dsn ... --archive-s3 ... --deep --repair\n")
					b.WriteString("-- `--deep` is what reads the footers over S3; without it the repair records\n")
					b.WriteString("-- nothing and this note comes back unchanged.\n")
				} else {
					b.WriteString("--   bintrail archive reconcile --index-dsn ... --archive-dir ... --repair\n")
				}
				b.WriteString("-- Regenerating this file afterwards emits one read_parquet per column set,\n")
				b.WriteString("-- which binds one footer per group instead of one per file.\n")
			}
		}
	}

	switch {
	case cold && live:
		b.WriteString("--\n")
		b.WriteString("-- Two legs: the Parquet for everything rotation has archived, the index for\n")
		b.WriteString("-- events it has not. A partition that has been archived but not yet dropped\n")
		b.WriteString("-- exists on BOTH sides, so the index leg excludes any event_id the archives\n")
		b.WriteString("-- already returned.\n")
		b.WriteString("--\n")
		b.WriteString("-- The ARCHIVES win that overlap, which is the one place this differs from\n")
		b.WriteString("-- bintrail's own merge in Go. There the two copies of an event carry the same\n")
		b.WriteString("-- fields and the winner is immaterial; here they do not. An archived row\n")
		b.WriteString("-- knows its bintrail_id, event_date and event_hour from its path, and an\n")
		b.WriteString("-- index row has to derive or forgo them. Letting the index win would\n")
		b.WriteString("-- replace a known source with NULL for every event in the overlap, and a\n")
		b.WriteString("-- WHERE bintrail_id = ... would then miss rows the archives hold.\n")
		writeAttachDegradeNote(b, stateSurvives, true)
		writeLiveCostNote(b, true)
		b.WriteString("CREATE OR REPLACE VIEW \"events\" AS\n")
		b.WriteString("  WITH cold AS (\n")
		writeColdSide(b, in, "    ")
		b.WriteString("\n  ), hot AS (\n")
		writeEventSelect(b, in, true, nil, "    ")
		b.WriteString("\n  )\n")
		b.WriteString("  SELECT * FROM cold\n")
		b.WriteString("  UNION ALL BY NAME\n")
		b.WriteString("  SELECT * FROM hot\n")
		b.WriteString("   WHERE NOT EXISTS (SELECT 1 FROM cold WHERE cold.event_id = hot.event_id);\n\n")
	case live:
		b.WriteString("--\n")
		// "No archive source in this file", not "nothing has been archived":
		// this branch is also where a registry that could not be READ lands,
		// and the header is the one place that already distinguishes the two.
		b.WriteString("-- The index alone: this file names no archive source to read from, and the\n")
		b.WriteString("-- header above says why. It covers whatever the index still holds, and\n")
		b.WriteString("-- rotation dropping a partition removes those events from it. Archive and\n")
		b.WriteString("-- take a baseline before that matters, then regenerate this file.\n")
		// No cold leg to fall back to: the index IS the only source this file
		// names, so regenerating without it would define no events view at all.
		writeAttachDegradeNote(b, stateSurvives, false)
		writeLiveCostNote(b, false)
		b.WriteString("CREATE OR REPLACE VIEW \"events\" AS\n")
		writeEventSelect(b, in, true, nil, "  ")
		b.WriteString(";\n\n")
	default:
		b.WriteString("--\n")
		b.WriteString("-- SCOPE: these are the ARCHIVED events only. Partitions rotation has not\n")
		b.WriteString("-- archived yet exist solely in the index, so the most recent window is\n")
		b.WriteString("-- absent here and reads as if nothing happened.\n")
		if in.LiveLegUnavailable {
			b.WriteString("-- This download covers the archives; it has no way to reach the index.\n")
			b.WriteString("-- To add a leg over the index, run `bintrail views --index-dsn ...\n")
			b.WriteString("-- --include-live` from a host that can connect to it.\n")
		} else if in.LiveLegHowTo != "" {
			// The producer's own route. Rendered verbatim as one comment line
			// so this package states no route it cannot see (a page's control
			// is not something the generator can know about).
			b.WriteString("-- " + commentSafe(in.LiveLegHowTo) + "\n")
		} else {
			b.WriteString("-- Add a leg over the index by regenerating with --include-live:\n")
			b.WriteString("--   bintrail views --index-dsn ... --include-live\n")
		}
		b.WriteString("CREATE OR REPLACE VIEW \"events\" AS\n")
		writeColdSide(b, in, "  ")
		b.WriteString(";\n\n")
	}
	return true
}

// writeLiveCostNote states what a query against the two-leg view actually
// reads, because the operator otherwise measures it and blames the view.
//
// Measured, not assumed: event_date and event_hour are DERIVED on the index leg
// (the index has no partition path), so a predicate on them is evaluated above
// that scan and cannot become a partition filter the way it does on Parquet.
func writeLiveCostNote(b *strings.Builder, withCold bool) {
	b.WriteString("--\n")
	b.WriteString("-- COST: a filter on this view does not become a filter on the index. The\n")
	b.WriteString("-- index leg derives event_date and event_hour from event_timestamp, so a\n")
	b.WriteString("-- predicate on them is applied after the rows are read")
	if withCold {
		b.WriteString(", and the anti-join\n-- needs every archived event_id regardless of what the query asked for")
	}
	b.WriteString(".\n")
	b.WriteString("-- Every query therefore streams the whole live binlog_events (row_before,\n")
	b.WriteString("-- row_after and query_text included)")
	if withCold {
		b.WriteString(", and the archive scan behind the\n-- anti-join is not pruned by your filter either")
	}
	b.WriteString(".\n")
	fmt.Fprintf(b, "-- For a narrow read of recent events, query %s.\"binlog_events\" directly\n",
		quoteIdent(liveAttachAlias))
	b.WriteString("-- with your own WHERE: that one does reach the index, which is indexed on\n")
	b.WriteString("-- event_timestamp and on schema_name/table_name.\n")
}

// writeColdSide renders the archive half of the events view: one SELECT over
// the whole globbed layout, or — when the column sets are recorded — one per
// group, joined by UNION ALL BY NAME.
//
// BY NAME, not positional: the groups pad different columns, so their projected
// order is the same only because both walk archive.BinlogEventColumns. Relying
// on that would make a future reordering of that list silently misalign two
// groups' columns instead of failing, and a UNION that misaligns is a wrong
// answer, not an error.
func writeColdSide(b *strings.Builder, in Input, indent string) {
	if len(in.ArchiveGroups) == 0 {
		writeEventSelect(b, in, false, nil, indent)
		return
	}
	for i := range in.ArchiveGroups {
		if i > 0 {
			b.WriteString("\n" + indent + "UNION ALL BY NAME\n")
		}
		writeEventSelect(b, in, false, &in.ArchiveGroups[i], indent)
	}
}

// writeEventSelect renders one leg of the events view. Both legs go through
// here so the derived columns (the event_type label, commit_time) cannot drift
// between them: two hand-written copies of one projection is how a UNION starts
// reporting different things for the same event depending on its age.
func writeEventSelect(b *strings.Builder, in Input, live bool, group *ArchiveGroup, indent string) {
	exclude := make(map[string]bool, len(in.ExcludeEventColumns))
	for _, c := range in.ExcludeEventColumns {
		exclude[strings.ToLower(c)] = true
	}
	b.WriteString(indent + "SELECT\n")
	col := indent + "  "

	// The three hive_partitioning columns. The cold leg gets them synthesized
	// by DuckDB from the path; the hot leg has to produce them itself, because
	// live binlog_events carries no source column and no partition path.
	if live {
		id, note := liveSourceID(in)
		if id != "" {
			fmt.Fprintf(b, "%s%s AS \"bintrail_id\",\n", col, sqlString(id))
		} else {
			fmt.Fprintf(b, "%sNULL AS \"bintrail_id\", %s\n", col, note)
		}
		// Derived so the hot leg filters on the same predicates as the cold
		// one. These mirror the Hive path rotation writes, which is UTC.
		//
		// Run on the NAIVE column, before the UTC cast below: the index stores
		// DATETIME in UTC and rotation names the path from that same value, so
		// these strftimes agree with the archives exactly as they stand. The
		// CAST to DATE is what keeps event_date a DATE in the union — DuckDB
		// types the archives' hive column DATE and strftime returns VARCHAR, and
		// the widened VARCHAR silently breaks date_trunc, `- INTERVAL 1 DAY` and
		// any comparison against current_date on the SAME file regenerated with
		// this flag. event_hour needs no cast: the hive column reads back
		// VARCHAR ('03'), which is what strftime already produces.
		b.WriteString(col + "CAST(strftime(\"event_timestamp\", '%Y-%m-%d') AS DATE) AS \"event_date\",\n")
		b.WriteString(col + "strftime(\"event_timestamp\", '%H') AS \"event_hour\",\n")
	} else {
		b.WriteString(col + "\"bintrail_id\", \"event_date\", \"event_hour\",\n")
	}

	has := legColumnSet(in, live, group)
	for _, c := range archive.BinlogEventColumns {
		if exclude[strings.ToLower(c.Name)] {
			continue
		}
		if has != nil && !has[strings.ToLower(c.Name)] {
			// The leg's own union_by_name, written out. On the HOT leg the
			// index does not have the column (it was migrated to an older
			// point than this build's schema); naming it would fail the whole
			// file with a binder error and define no view at all. On a COLD
			// GROUP the archived files in it were written before the column
			// existed, which is precisely what union_by_name used to paper
			// over for the whole layout at the cost of a footer read per file.
			// Either way it reads back NULL.
			writeMissingColumn(b, col, c.Name, missingReason(live))
			continue
		}
		switch c.Name {
		case "event_id":
			// Cast on BOTH legs. The index column is BIGINT UNSIGNED and the
			// Parquet one is signed, and DuckDB reconciles that pair by
			// widening the union to HUGEINT — a 128-bit surprise in every
			// downstream join for a value that never leaves BIGINT range.
			b.WriteString(col + "CAST(\"event_id\" AS BIGINT) AS \"event_id\",\n")
		case "event_timestamp":
			if live {
				// The one cast that decides whether this view answers the
				// question it was asked. DuckDB reads the archives' Parquet
				// timestamp as TIMESTAMP WITH TIME ZONE and the index's
				// DATETIME as a naive TIMESTAMP; UNION reconciles that pair by
				// interpreting the naive value in the READER's session
				// timezone. The index stores UTC, so on any non-UTC session
				// every hot row lands off by that offset — the same event
				// returning two different instants depending on which leg
				// served it, and a UTC range filter returning nothing for
				// events that are in it. Correct on a UTC box, which is how it
				// goes unnoticed.
				b.WriteString(col + "\"event_timestamp\" AT TIME ZONE 'UTC' AS \"event_timestamp\",\n")
			} else {
				b.WriteString(col + "\"event_timestamp\",\n")
			}
		case "event_type":
			// INTEGER, not the narrower TINYINT the index stores it in: the
			// archives read back INTEGER, so this is also the type the
			// archives-only file has always produced. A narrowing cast would
			// make an unknown future event type fail the whole query, which
			// contradicts the ELSE branch below, whose entire purpose is that
			// such a code shows up as an unfamiliar value instead.
			b.WriteString(col + "CAST(\"event_type\" AS INTEGER) AS \"event_type_code\",\n")
			b.WriteString(col + eventTypeCase + ",\n")
		case "schema_version":
			b.WriteString(col + "CAST(\"schema_version\" AS INTEGER) AS \"schema_version\",\n")
		case "commit_ts_us":
			b.WriteString(col + "\"commit_ts_us\",\n")
			// Stored as epoch MICROSECONDS (#18); make the usable form the one
			// that reads like a timestamp next to event_timestamp.
			// The CAST is load-bearing: commit_ts_us is UNSIGNED (UBIGINT in the
			// Parquet schema) and make_timestamp only binds BIGINT, so the
			// uncast form is a binder error that would reach the operator's
			// DuckDB, not ours. Epoch microseconds stay far below 2^63 (year
			// 294247), so the narrowing cannot lose a real value.
			b.WriteString(col + "CASE WHEN \"commit_ts_us\" IS NULL THEN NULL\n")
			b.WriteString(col + "     ELSE make_timestamp(CAST(\"commit_ts_us\" AS BIGINT)) END AS \"commit_time\",\n")
		default:
			fmt.Fprintf(b, "%s%s,\n", col, quoteIdent(c.Name))
		}
	}
	trimTrailingComma(b)

	if live {
		// Only the columns above: live binlog_events also has pk_hash, a
		// generated column the archives do not carry, and selecting it would
		// give the two legs different shapes.
		fmt.Fprintf(b, "\n%sFROM %s.\"binlog_events\"", indent, quoteIdent(liveAttachAlias))
		return
	}
	paths := make([]string, 0, len(in.ArchiveSources))
	if group != nil {
		paths = append(paths, group.Files...)
	} else {
		for _, src := range in.ArchiveSources {
			paths = append(paths, archiveGlob(src))
		}
	}
	b.WriteString("\n" + indent + "FROM read_parquet(\n")
	b.WriteString(indent + "  [\n")
	for i, p := range paths {
		sep := ","
		if i == len(paths)-1 {
			sep = ""
		}
		fmt.Fprintf(b, "%s    %s%s\n", indent, sqlString(p), sep)
	}
	b.WriteString(indent + "  ],\n")
	b.WriteString(indent + "  hive_partitioning = true,\n")
	// The whole point of a group: its files share a column set, so there is
	// nothing to unify and DuckDB binds ONE footer for the list instead of
	// opening every one of them. hive_partitioning still reads bintrail_id,
	// event_date and event_hour off these explicit paths, and a filter on them
	// still prunes files, exactly as it does over a glob.
	if group != nil {
		b.WriteString(indent + "  union_by_name = false\n")
	} else {
		b.WriteString(indent + "  union_by_name = true\n")
	}
	b.WriteString(indent + ")")
}

// liveSourceID decides what the hot leg may claim about its rows' source, and
// when it may claim nothing, the end-of-line comment that says WHY — stating
// only what was observed, never a cause nobody established.
//
// It returns ("", note) far more often than it returns an id. That is the
// point: one sentence about several sources used to cover a file-mode index
// (which registers none and serves exactly one), an index too old to have the
// table, an account without SELECT on it, and a dropped connection.
func liveSourceID(in Input) (id, note string) {
	li := in.LiveIndex
	if li.BintrailID == "" {
		switch li.Attribution {
		case AttributionMultiSource:
			return "", "-- more than one source is registered in this index, and an index row carries none of its own"
		case AttributionUnregistered:
			return "", "-- this index registers no source id, so there is none to attribute an index row to"
		default:
			return "", "-- the index's registered sources could not be read, so these rows are left unattributed"
		}
	}
	// Cross-check against the identity the COLD leg will actually carry. The
	// two come from unrelated places — this one from bintrail_servers, the
	// other from the `bintrail_id=` path segment, which `rotate` takes verbatim
	// from --bintrail-id and never validates against the registry. When they
	// disagree, one source appears in the view as two servers and a
	// WHERE bintrail_id = ... returns half its rows, so assert neither.
	archived := archiveIDs(in.ArchiveSources)
	if len(archived) > 0 && !slices.Contains(archived, li.BintrailID) {
		return "", fmt.Sprintf(
			"-- the index reports source %s, these archives are written under %s: unattributed rather than assert either",
			commentSafe(li.BintrailID), commentSafe(strings.Join(archived, ", ")))
	}
	return li.BintrailID, ""
}

// archiveIDs pulls the `bintrail_id=<id>` identity out of each archive base
// path — the same segment archive.ParseArchivePath reads back, and the only
// place the cold leg's bintrail_id comes from.
func archiveIDs(sources []string) []string {
	var ids []string
	for _, s := range sources {
		const marker = "bintrail_id="
		i := strings.LastIndex(s, marker)
		if i < 0 {
			continue
		}
		id := strings.TrimRight(s[i+len(marker):], "/")
		if id = strings.SplitN(id, "/", 2)[0]; id != "" && !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return ids
}

// commentSafe keeps interpolated text inside the SQL comment it is written in.
// A bintrail_id is an operator-chosen string on the archive side, and a newline
// in one would end the comment and make everything after it statement text.
func commentSafe(s string) string {
	return strings.NewReplacer("\n", " ", "\r", " ").Replace(s)
}

// legColumnSet returns the lookup for which columns THIS leg may name, or nil
// when every column is nameable: a producer that observed no column set, and
// the globbed cold leg, whose union_by_name still handles its own absences.
//
// A cold GROUP always has one, even when it holds every column: the set is what
// the group was formed on, so trusting it is not an extra assumption, and a
// group that named a column its files lack would fail the whole view with a
// binder error — the failure union_by_name = false trades away.
func legColumnSet(in Input, live bool, group *ArchiveGroup) map[string]bool {
	var names []string
	switch {
	case group != nil:
		names = group.Columns
	case live:
		names = in.LiveIndex.TableColumns
	}
	if len(names) == 0 {
		return nil
	}
	has := make(map[string]bool, len(names))
	for _, c := range names {
		has[strings.ToLower(c)] = true
	}
	return has
}

// missingReason is the end-of-line comment a NULL placeholder carries, naming
// the leg it is missing FROM. Two legs put a NULL in the same output column for
// unrelated reasons, and a reader deciding whether to re-archive or re-migrate
// needs to know which one they are looking at.
func missingReason(live bool) string {
	if live {
		return " -- not a column of this index's binlog_events\n"
	}
	return " -- not a column of the archives in this group\n"
}

// writeMissingColumn emits the NULL placeholders for one column this leg does
// not have. It follows the projection's own shape: the two columns that render
// as two outputs must produce two NULLs, or the legs stop lining up.
func writeMissingColumn(b *strings.Builder, col, name, why string) {
	switch name {
	case "event_type":
		b.WriteString(col + "NULL AS \"event_type_code\"," + why)
		b.WriteString(col + "NULL AS \"event_type\",\n")
	case "commit_ts_us":
		b.WriteString(col + "NULL AS \"commit_ts_us\"," + why)
		b.WriteString(col + "NULL AS \"commit_time\",\n")
	default:
		fmt.Fprintf(b, "%sNULL AS %s,%s", col, quoteIdent(name), why)
	}
}

// archiveGlob turns an archive base path (…/bintrail_id=<id>) into the glob that
// matches every partition file under it — the same three-level Hive layout
// rotation writes and archive.ParseArchivePath reads back.
func archiveGlob(base string) string {
	return strings.TrimRight(base, "/") + "/event_date=*/event_hour=*/*.parquet"
}

// statePlan pairs one baseline table with the view name it is emitted under.
type statePlan struct {
	table BaselineTable
	name  string
}

// stateViewPlan assigns a view name to EVERY table of the snapshot, in emission
// order.
//
// Names are assigned over every table even when only some are rendered: a
// name's collision suffix depends on which names came before it, so choosing
// the tables first and naming them second would rename a view the moment its
// colliding sibling is left out — and a name that moves with the statement is a
// name nobody can write a query against.
func stateViewPlan(in Input) []statePlan {
	tables := append([]BaselineTable(nil), in.Baselines...)
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Schema != tables[j].Schema {
			return tables[i].Schema < tables[j].Schema
		}
		return tables[i].Table < tables[j].Table
	})
	used := map[string]bool{}
	plan := make([]statePlan, 0, len(tables))
	for _, t := range tables {
		plan = append(plan, statePlan{table: t, name: stateViewName(t.Schema, t.Table, used)})
	}
	return plan
}

// writeStateViews emits one view per table in the newest baseline snapshot, and
// returns whether it emitted any.
func writeStateViews(b *strings.Builder, in Input) bool {
	wanted := selectedStatePlans(in)
	// A filtered render that selected no state view emits NOTHING, comments
	// included, for the reason GenerateViews states: its caller executes this.
	if in.OnlyViews != nil && len(wanted) == 0 {
		return false
	}
	switch in.Follow {
	case FollowPointer:
		b.WriteString("-- state_<schema>_<table>: each table's full contents as of the snapshot the\n")
		b.WriteString("-- `" + baseline.CurrentLinkName + "` pointer names, which is whichever one completed most recently.\n")
	case FollowNewest:
		b.WriteString("-- state_<schema>_<table>: each table's full contents as of the newest snapshot\n")
		b.WriteString("-- carrying a `" + baseline.SuccessMarker + "` marker, chosen when the view is read.\n")
	default:
		b.WriteString("-- state_<schema>_<table>: each table's full contents as of the baseline snapshot.\n")
	}
	b.WriteString("--\n")
	b.WriteString("-- These are the SNAPSHOT's rows, not the table's current state: changes after\n")
	if in.rendersEvents() {
		b.WriteString("-- the snapshot live in the `events` view.\n")
	} else if in.OmitEvents && in.definesEvents() {
		// The view COULD be defined and was left out. Both routes named,
		// because the file is served by two producers and the console reader
		// has no command line to pass a flag on.
		b.WriteString("-- the snapshot are not in this file. `bintrail views --include-events`\n")
		b.WriteString("-- adds the view that holds them, as does the change-log box on the\n")
		b.WriteString("-- console download.\n")
	} else {
		// There is nothing to define the view FROM (no archive source, or a
		// registry that could not be read). Naming --include-events here would
		// be advice that produces the same file again: the events section
		// states the real reason, and repeating a guess at it would be a
		// second, possibly different, claim about one fact.
		b.WriteString("-- the snapshot are not in this file: it defines no `events` view.\n")
	}
	b.WriteString("-- To materialize a later point in time, use `bintrail reconstruct`. Folding\n")
	b.WriteString("-- the deltas back onto a baseline is what that command does, and it is not\n")
	b.WriteString("-- expressible as a view.\n")
	if len(in.Baselines) == 0 {
		b.WriteString("-- (skipped: no baseline snapshot was discovered)\n\n")
		return false
	}
	writeDecimalNote(b, in)
	if in.Follow == FollowNewest {
		writeNewestSnapshotVar(b, in)
	}
	writeSnapshotPreflight(b, in, wanted)

	for _, p := range wanted {
		t, name := p.table, p.name
		for _, line := range decimalComments(t) {
			fmt.Fprintf(b, "-- %s: %s\n", name, line)
		}
		fmt.Fprintf(b, "CREATE OR REPLACE VIEW %s AS\n", quoteIdent(name))
		if in.Follow == FollowNewest {
			writeNewestStateBody(b, t)
			continue
		}
		if replace := decimalReplaceClause(t); replace != "" {
			fmt.Fprintf(b, "  SELECT * REPLACE (%s)\n", replace)
			fmt.Fprintf(b, "  FROM read_parquet(%s);\n", sqlString(t.Path))
			continue
		}
		fmt.Fprintf(b, "  SELECT * FROM read_parquet(%s);\n", sqlString(t.Path))
	}
	b.WriteString("\n")
	return len(wanted) > 0
}

// newestVar is the session variable holding the snapshot directory every state
// view reads through, under FollowNewest.
//
// ONE variable for the whole file, which is what makes this mechanism cost what
// pinning costs. The obvious shape -- each view scanning every snapshot's copy
// of its table and filtering down to one -- was written, measured and rejected:
// DuckDB binds a view when it is CREATED, so a glob in the view body pays a
// storage listing per view, and that listing grows with the root. Against a real
// S3 root of 24 snapshots it opened a 47-view file in 170s where pinning took
// 31s, and the gap widens with every refresh published.
//
// Resolving once also restores what the `current` pointer gives and the rejected
// shape could not: every view agrees on one snapshot, because there is one value
// to disagree about.
const newestVar = "bintrail_newest_snapshot"

// newestVarUnsetMsg is what a state view raises when it is read in a session
// that never ran the SET VARIABLE statement above — the persisted-views case:
// lake.db keeps the views, the session variable dies with the session (#1583).
const newestVarUnsetMsg = "bintrail views: this file sets a session variable; " +
	"run its SET VARIABLE statement in this session first"

// missingVar and checkVar carry the preflight below. Two variables rather than
// one expression so the message can NAME the tables it found, and so a clean
// file prints nothing at all.
const (
	missingVar = "bintrail_missing_tables"
	checkVar   = "bintrail_tables_checked"
)

// writeNewestSnapshotVar emits the lookup that picks the snapshot, for a root
// with no `current` pointer to rewrite (#1550).
//
// The CASE is not decoration. With no marker under the root, max() is NULL, and
// a NULL variable reaches read_parquet as "cannot take NULL list as parameter",
// which is true and tells the reader nothing about baselines. Raising here names
// the root and the marker instead, and does it when the FILE IS READ rather than
// at the first query.
func writeNewestSnapshotVar(b *strings.Builder, in Input) {
	root := strings.TrimSuffix(in.BaselineSource, "/")
	b.WriteString("-- The snapshot every state view below reads through, resolved once, when this\n")
	b.WriteString("-- file is read. Re-run this statement to pick up a refresh without reopening\n")
	b.WriteString("-- the session; every view follows it, so they never disagree about which\n")
	b.WriteString("-- snapshot they are showing.\n")
	fmt.Fprintf(b, "SET VARIABLE %s = (\n", newestVar)
	b.WriteString("  SELECT CASE WHEN max(file) IS NULL\n")
	fmt.Fprintf(b, "    THEN error(%s)\n", sqlString(
		"bintrail views: no completed snapshot under "+root+"/ (nothing there carries a "+
			baseline.SuccessMarker+" marker). Take or refresh a baseline, then read this file again."))
	fmt.Fprintf(b, "    ELSE max(regexp_replace(file, %s, '')) END\n", sqlString(baseline.SuccessMarker+"$"))
	fmt.Fprintf(b, "  FROM glob(%s));\n\n", sqlString(root+"/*/"+baseline.SuccessMarker))
}

// selectedStatePlans is the state views this render will actually emit: the
// full plan, narrowed by OnlyViews.
//
// One function because two callers ask the same question — the writer, to emit
// them, and checksSnapshot, to decide whether the header may promise the
// dropped-table check. Narrowing the plan in one and re-deriving it in the
// other is how a file would come to advertise a guarantee it does not carry,
// which is the failure this whole PR is about.
func selectedStatePlans(in Input) []statePlan {
	plan := stateViewPlan(in)
	out := make([]statePlan, 0, len(plan))
	for _, p := range plan {
		if in.OnlyViews.wants(p.name) {
			out = append(out, p)
		}
	}
	return out
}

// checksSnapshot reports whether this render will emit the dropped-table check.
//
// It answers by running the writer's own conditions, not by restating them: the
// header's promise and the check's presence have to agree, and two lists of
// conditions maintained apart is how they would stop agreeing.
func (in Input) checksSnapshot() bool {
	if !in.Follow.follows() {
		return false
	}
	wanted := selectedStatePlans(in)
	if len(wanted) == 0 {
		return false
	}
	if _, _, ok := snapshotDirExpr(in); !ok {
		return false
	}
	for _, p := range wanted {
		if p.table.Rel == "" {
			return false
		}
	}
	return true
}

// writeSnapshotPreflight emits one check, ahead of every state view, that the
// tables this file names are still in the snapshot it will read (#1558).
//
// It exists because the alternative is the worst failure this file has. DuckDB
// binds a view when it is CREATED, so a table DROPped at the source takes the
// whole script down at its own statement: the views emitted before it exist, the
// ones after it never do, and against a persistent database what remains is a
// silently partial schema whose size depends on emission order. The error names
// a Parquet path, which reads as a corrupt snapshot rather than as a table
// somebody dropped on purpose.
//
// DuckDB has no tolerant read to lean on: union_by_name, filename and a
// zero-match glob all raise the same way, so "define the other views anyway" is
// not on the table. The choice is between failing confusingly and partially, and
// failing legibly and first. This is the second.
//
// Only for a file that FOLLOWS. A pinned file names a snapshot that already held
// every one of these tables when it was written, so the check would be asking
// whether someone deleted files by hand.
func writeSnapshotPreflight(b *strings.Builder, in Input, wanted []statePlan) {
	dir, globDir, ok := snapshotDirExpr(in)
	if !ok || len(wanted) == 0 {
		return
	}
	rels := make([]string, 0, len(wanted))
	for _, p := range wanted {
		if p.table.Rel == "" {
			// A producer that did not fill Rel cannot be described, and a
			// preflight over SOME of the tables would pass while the file still
			// broke on one it never checked.
			return
		}
		rels = append(rels, p.table.Rel)
	}

	b.WriteString("-- Every table below has to still be in that snapshot. A table dropped at the\n")
	b.WriteString("-- source leaves it, and DuckDB binds a view when it is created, so without\n")
	b.WriteString("-- this the script would stop at that one view and never define the rest.\n")
	b.WriteString("SET VARIABLE " + missingVar + " = (\n")
	b.WriteString("  SELECT string_agg(t, ', ' ORDER BY t) FROM (VALUES\n")
	for i, rel := range rels {
		sep := ","
		if i == len(rels)-1 {
			sep = ""
		}
		fmt.Fprintf(b, "    (%s)%s\n", sqlString(rel), sep)
	}
	b.WriteString("  ) AS x(t)\n")
	// `**` and not `*/`, even though every Rel in this layout is exactly
	// schema/table.parquet. The two would agree today and the recursion costs
	// nothing at this depth, but snapshotRel returns EVERYTHING after the
	// snapshot directory, so a layout that ever nested one level deeper would
	// leave the glob listing less than the list it is compared against. That
	// does not lose the check, it inverts it: every table looks missing and a
	// perfectly healthy file is refused before it creates anything, which is
	// the worst outcome this code has. `**` is a superset at every depth
	// (verified: it matches one, two and three levels), so it cannot.
	fmt.Fprintf(b, "  WHERE %s || t NOT IN (SELECT file FROM glob(%s || '**/*.parquet')));\n", dir, globDir)
	// Raised through a SET rather than a bare SELECT so a clean file prints
	// nothing. A SELECT here puts a one-row NULL table in front of every reader
	// who has nothing wrong.
	//
	// The condition is on missingVar and NOT inside error()'s argument, because
	// error() is a scalar function and PROPAGATES NULL: error('x ' || NULL)
	// returns NULL instead of raising, so a guard assembled by concatenation
	// disarms itself the moment any piece of it is NULL. That also decides what
	// happens under FollowNewest when nothing is marked and the directory is
	// NULL: the whole check evaluates to NULL and raises nothing. It fails OPEN
	// there on purpose, because the emitter above has already raised its own
	// refusal naming the root, and a second message about missing tables would
	// only describe the same absent snapshot in worse words.
	fmt.Fprintf(b, "SET VARIABLE %s = (SELECT CASE WHEN getvariable('%s') IS NOT NULL\n", checkVar, missingVar)
	fmt.Fprintf(b, "  THEN error(%s || getvariable('%s') ||\n",
		sqlString("bintrail views: these tables are not in the newest snapshot any more: "), missingVar)
	// Naming where it looked is what DuckDB's own "No files found" gave the
	// reader before this check existed, and it is the half that says whether the
	// table moved or the whole snapshot is the wrong one. Under FollowNewest the
	// file cannot know the directory until the query runs, so it has to come
	// from the variable rather than from a literal here.
	fmt.Fprintf(b, "    %s || %s ||\n", sqlString(". Looked in "), dir)
	fmt.Fprintf(b, "    %s) END);\n\n",
		sqlString(". If they were dropped or renamed, download this file again."))
}

// snapshotDirExpr returns TWO SQL expressions for the snapshot directory the
// state views read, both with a trailing separator, or ok=false for a file that
// does not follow one.
//
// They differ, and the difference is load-bearing. The first is compared
// against glob's OUTPUT, so it must be the LITERAL path. The second is
// concatenated into glob's PATTERN, where `[`, `*` and `?` are syntax.
//
// One of those is a real failure and the rest are hygiene, which is worth
// stating so nobody credits this with a guard it does not have. A `[` in the
// root makes the listing return NOTHING, so every table grades missing and a
// healthy file is refused before it creates anything — the worst outcome this
// code has. A `*` or `?` makes it match SIBLING directories, which cannot
// produce a wrong verdict: the comparison side is the literal, and an
// over-matched sibling is spelled with its own directory name, so it never
// equals the literal and the real file is still matched. They are escaped
// anyway because a pattern that means something other than its own path is a
// trap waiting for the next reader, not because a bug is behind them.
//
// DuckDB does not honour a backslash escape here; a single-character class does,
// which is what globLiteral builds. FollowNewest needs no escaping: its value is
// itself a glob RESULT, so a root that glob cannot express never reaches it —
// writeNewestSnapshotVar's own glob raises first, naming the root.
func snapshotDirExpr(in Input) (dir, globDir string, ok bool) {
	switch in.Follow {
	case FollowNewest:
		v := "getvariable('" + newestVar + "')"
		return v, v, true
	case FollowPointer:
		// filepath.Join, not a hand-built string off the raw BaselineSource.
		// Join CLEANS, which is what the paths in the view bodies were built
		// with, and trimming one trailing slash off the flag as typed does not:
		// a root ending "//" produced ".../001//current/" here while the bodies
		// read ".../001/current/...", so every table compared unequal and a
		// perfectly healthy file was refused. Same class as the Rel divergence
		// this file's preflight exists behind — one derivation, not two.
		lit := filepath.ToSlash(filepath.Join(in.BaselineSource, baseline.CurrentLinkName)) + "/"
		return sqlString(lit), sqlString(globLiteral(lit)), true
	}
	return "", "", false
}

// globLiteral makes s match itself under DuckDB's glob, by wrapping every
// pattern metacharacter in a single-character class.
//
// Verified against DuckDB rather than assumed: a backslash does NOT escape
// (`meta\[1\]` matches nothing), a class DOES (`meta[[]1]` matches), `[` alone
// under-matches to zero, and `*` and `?` over-match to siblings. `{` is wrapped
// too: alone it is inert, but a second one turns the pair into brace expansion.
//
// Built in ONE pass. Escaping `[` first and then the others would re-process the
// brackets this function just introduced.
func globLiteral(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '[', '*', '?', '{':
			b.WriteByte('[')
			b.WriteRune(r)
			b.WriteByte(']')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// writeNewestStateBody emits one state view's body under FollowNewest: a read of
// ONE Parquet file, whose directory comes from the session variable.
//
// A table that has left the newest snapshot fails here, loudly, with DuckDB's
// own "No files found that match the pattern" naming the exact path it looked
// for. That is the guarantee this mechanism exists to keep: reading as EMPTY a
// table that should have had rows is a worse answer than reading a stale one.
//
// The CASE around the variable is the other loud failure (#1583). Views
// PERSIST in a database file; the session variable does not. Reopening a
// lake.db in a new session (duckdb -ui included) lists every view and fails
// the first read with DuckDB's own "read_parquet cannot take NULL list as
// parameter", which names nothing the reader can act on. The CASE makes that
// read name its own cause instead. It costs nothing at CREATE time only
// because the generated file SETs the variable before it creates any view —
// error() raises when the CASE is BOUND with the variable still null, so a
// producer must never emit these bodies ahead of writeNewestSnapshotVar.
func writeNewestStateBody(b *strings.Builder, t BaselineTable) {
	read := fmt.Sprintf("read_parquet(CASE WHEN getvariable('%s') IS NULL\n    THEN error(%s)\n    ELSE getvariable('%s') || %s END)",
		newestVar, sqlString(newestVarUnsetMsg), newestVar, sqlString(t.Rel))
	if replace := decimalReplaceClause(t); replace != "" {
		fmt.Fprintf(b, "  SELECT * REPLACE (%s)\n", replace)
		fmt.Fprintf(b, "  FROM %s;\n", read)
		return
	}
	fmt.Fprintf(b, "  SELECT * FROM %s;\n", read)
}

// stateViewName builds the view identifier for a table and guarantees it is
// unique within the file.
//
// Sanitizing collapses distinct tables onto one name — `a_b`.`c` and `a`.`b_c`
// both want state_a_b_c — and since every statement is CREATE OR REPLACE, a
// collision would silently leave only the last one, with the earlier table's
// view pointing at the wrong Parquet file. Suffixing is not pretty; a view that
// reads someone else's table is worse.
func stateViewName(schema, table string, used map[string]bool) string {
	base := "state_" + sanitizeIdent(schema) + "_" + sanitizeIdent(table)
	name := base
	for i := 2; used[name]; i++ {
		name = fmt.Sprintf("%s_%d", base, i)
	}
	used[name] = true
	return name
}

// sanitizeIdent reduces a MySQL identifier to a bare word so the generated view
// name stays typeable without quoting in an interactive DuckDB session.
func sanitizeIdent(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// quoteIdent renders a SQL identifier with double quotes, doubling any embedded
// quote.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// sqlString renders a SQL string literal, doubling any embedded quote. Every
// path this package interpolates goes through it — a bucket or directory name
// containing an apostrophe is unusual but entirely legal, and the generated file
// is meant to be executed.
func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// trimTrailingComma removes the "," a projection loop just wrote, so the last
// column does not need a special case at every call site.
func trimTrailingComma(b *strings.Builder) {
	s := b.String()
	if strings.HasSuffix(s, ",\n") {
		b.Reset()
		b.WriteString(strings.TrimSuffix(s, ",\n"))
	}
}
