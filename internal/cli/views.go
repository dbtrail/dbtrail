package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/views"
)

var viewsCmd = &cobra.Command{
	Use:   "views",
	Short: "Generate DuckDB view definitions over your Parquet archive layout",
	Long: `Write a .sql file of DuckDB views over the archives and baseline snapshots
this index already knows about, so any DuckDB (the CLI, a notebook, an
embedded process) can query the Parquet tier without hand-writing
read_parquet globs.

The generated file defines:

  state_<schema>_<table>     each table's contents as of the newest
                             discoverable baseline snapshot
  events                     every archived binlog event across all archive
                             sources, with event_type decoded and the commit
                             timestamp typed -- only with --include-events

The events view is opt-in because defining it is expensive: DuckDB opens one
Parquet footer per archived file before the view returns a single row, so the
cost grows with the archive and is paid by every reader of the file, including
one who only wanted their tables. The state views read one file per table.

DECIMAL and NUMERIC columns are stored as text, so that a value MySQL can hold
is never rounded to fit a narrower type. The state views cast them back to
DECIMAL with the precision and scale the column was declared with, read from
each file's Parquet footer, so sum() and the rest work on them directly. A
column too wide for DuckDB, or a baseline with no schema in its footer, stays
text and is named in the file.

bintrail never runs what it prints. It does open DuckDB once while generating,
to read the column types out of the baseline files' Parquet footers, and that
read touches footers only. Nothing in the file writes, and no credentials
appear in it. S3 access goes through DuckDB's credential chain, the same way
bintrail's own S3 reads do. That S3 secret lives only in the DuckDB session
that runs the file: views persist in a database file, secrets do not, so run
the file in every session that reads S3 (.read views.sql, or duckdb -init
views.sql).

Archive sources discovered from the index are named so the file works from
another machine: an archive registered with both a local path and an S3
location is listed by its S3 location, and a local path appears only when
the registry holds no S3 location the file can use. State views point wherever --baseline-dir/--baseline-s3
points; a local baseline directory resolves only on the host that holds it.
With --archive-dir/--archive-s3 the file lists exactly what you named.

Archive sources come from the index's archive_state registry by default. Pass
--archive-dir/--archive-s3 with --bintrail-id to name one explicitly instead;
in that case --index-dsn is not needed at all.

Only the events view reads an archive source, so a baselines-only file needs
neither: --baseline-dir/--baseline-s3 on its own is enough. That is what a
snapshot downloaded from the console arrives as, and it can be queried on a
machine that cannot reach the index at all.

The file is a snapshot of the LAYOUT, not of the rows. The state views reach a
baseline published later on their own, by whichever route the root allows: a
--baseline-dir file reads through the current/ pointer, and a --baseline-s3
file, which has no pointer object to read through, has each view select the
newest snapshot carrying a _SUCCESS marker at the moment it is read. Pass
--pin-snapshot to bind them to the snapshot that exists now instead.

What does NOT follow is the shape: which views exist, and how each DECIMAL
column is read, were both decided from the snapshot named in the file.
Regenerate after a table is added or dropped, after a column changes type, and
whenever archive sources are added or removed. (With --include-events, that
view's globs also keep picking up newly rotated partitions on their own.)

Examples:
  # One view per table, as of the newest baseline snapshot
  bintrail views --index-dsn "..." --baseline-dir /data/baselines --out views.sql

  # Add the change log across every archive source
  bintrail views --index-dsn "..." --baseline-dir /data/baselines \
    --include-events --out views.sql

  # Open an interactive DuckDB with the views and the S3 secret loaded
  bintrail views --index-dsn "..." --baseline-dir /data/baselines --out views.sql
  duckdb -init views.sql lake.db

  # Without an index: name the archive and baseline locations directly
  bintrail views --archive-s3 s3://bucket/archives/ --bintrail-id <uuid> \
    --baseline-s3 s3://bucket/baselines/ --out views.sql`,
	RunE: runViews,
}

var (
	vIndexDSN      string
	vArchiveDir    string
	vArchiveS3     string
	vBintrailID    string
	vRegion        string
	vBaselineDir   string
	vBaselineS3    string
	vOut           string
	vNoBaselines   bool
	vIncludeLive   bool
	vIncludeEvents bool
	vPinSnapshot   bool
)

func init() {
	viewsCmd.Flags().StringVar(&vIndexDSN, "index-dsn", "", "DSN for the index MySQL database (used to discover archive sources; not needed with --archive-dir/--archive-s3, nor for a baselines-only file)")
	viewsCmd.Flags().StringVar(&vArchiveDir, "archive-dir", "", "Local root directory of Parquet archives (requires --bintrail-id)")
	viewsCmd.Flags().StringVar(&vArchiveS3, "archive-s3", "", "S3 root URL prefix of Parquet archives (requires --bintrail-id; e.g. s3://bucket/prefix/)")
	viewsCmd.Flags().StringVar(&vBintrailID, "bintrail-id", "", "Server identity UUID (required when --archive-dir or --archive-s3 is set)")
	viewsCmd.Flags().BoolVar(&vPinSnapshot, "pin-snapshot", false, "Bind the state views to the snapshot discovered now, so they keep returning today's rows after a baseline refresh (default: follow the newest snapshot, through the current/ pointer locally and through the newest _SUCCESS marker on S3)")
	viewsCmd.Flags().StringVar(&vRegion, "region", "", "AWS region to pin in the generated S3 secret (default: resolved by the credential chain)")
	viewsCmd.Flags().StringVar(&vBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots")
	viewsCmd.Flags().StringVar(&vBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline Parquet snapshots (e.g. s3://bucket/baselines/)")
	viewsCmd.Flags().BoolVar(&vNoBaselines, "no-baselines", false, "Skip the baseline state views (requires --include-events, or the file would define nothing)")
	viewsCmd.Flags().BoolVar(&vIncludeEvents, "include-events", false, "Add the events view over every archived binlog event. Left out by default because defining it opens one Parquet footer per archived file before it returns a row, a cost that grows with the archive and is paid by every reader of the file, including one who only wanted their tables")
	viewsCmd.Flags().BoolVar(&vIncludeLive, "include-live", false, "Add a live leg to the events view so it also covers events the index holds but rotation has not archived yet. Requires --index-dsn. The leg queries the index server directly, and the view cannot push a filter down to it, so every query is a full scan of binlog_events that competes with capture on that server: for a narrow read query the attached binlog_events directly, or point --index-dsn at a read replica of the index. The generated file carries the index host, port, database and user, and never its password: fill that in before running")
	viewsCmd.Flags().StringVar(&vOut, "out", "views.sql", "Output file, or - for stdout")
}

func runViews(cmd *cobra.Command, _ []string) error {
	if (vArchiveDir != "" || vArchiveS3 != "") && vBintrailID == "" {
		return fmt.Errorf("--bintrail-id is required when --archive-dir or --archive-s3 is set")
	}
	if vBaselineDir != "" && vBaselineS3 != "" {
		return fmt.Errorf("--baseline-dir and --baseline-s3 are mutually exclusive")
	}
	// Both refusals name a file that would define NOTHING or a leg with no view
	// to hang on. Refused rather than silently adjusted: turning on an expensive
	// view because the operator asked for its leg is a surprise they pay for on
	// every query, and an empty file is a successful command that produced no
	// schema.
	if vNoBaselines && !vIncludeEvents {
		return fmt.Errorf("--no-baselines leaves nothing to define: the events view is not included " +
			"by default. Add --include-events for an events-only file, or drop --no-baselines")
	}
	if vIncludeLive && !vIncludeEvents {
		return fmt.Errorf("--include-live adds a leg to the events view, which this file would not " +
			"define: pass --include-events with it")
	}
	explicitArchives := vArchiveDir != "" || vArchiveS3 != ""
	// An archive source is required only by the view that READS one. Since
	// #1546 the events view is opt-in, so the default file defines state views
	// over a baseline location and nothing else: demanding the index for it
	// asks for a host the reader may not reach, to discover paths the file
	// will never name. That is the shape a downloaded snapshot arrives in
	// (GET /api/baselines/download), and the old check sent that reader to
	// invent an --archive-dir to get the file it had just been refused.
	//
	// Keyed on --include-events rather than on "did anything get discovered":
	// the operator asked for the view, so the refusal names what they asked
	// for. A file that ends up defining nothing at all is still caught below
	// by RendersAnyView, which reports the reason that actually applies.
	if vIncludeEvents && vIndexDSN == "" && !explicitArchives {
		return fmt.Errorf("--include-events needs an archive source: pass --index-dsn (it is where " +
			"archive sources are discovered), or name one directly with --archive-dir/--archive-s3 " +
			"plus --bintrail-id")
	}

	in := views.Input{
		GeneratedAt: time.Now().UTC(),
		Version:     buildVersion,
		// Region is only meaningful for the S3 secret; a local-only layout
		// never emits the preamble that would use it.
		ArchiveRegion: vRegion,
		OmitEvents:    !vIncludeEvents,
	}

	switch {
	case explicitArchives:
		in.ArchiveSources = explicitArchiveSources()
	case vIndexDSN != "":
		sources, err := discoverArchiveSources(cmd.Context(), vIndexDSN)
		if err != nil {
			return err
		}
		in.ArchiveSources = sources
		in.PortableRouting = true
	default:
		// No index and nothing named: a baselines-only file. Discovery is
		// SKIPPED rather than run against an empty DSN, which would fail on
		// the connection and report it as an index fault.
	}

	// Grouping needs the registry, so it applies to operator-named roots too —
	// but only when there is an index to ask. `--archive-dir` alone describes a
	// layout with nothing to read column sets from, and the file then carries
	// the globbed form, which is what it has always carried.
	if vIndexDSN != "" {
		if err := resolveArchiveGroups(cmd.Context(), vIndexDSN, &in); err != nil {
			return err
		}
	}

	if vIncludeLive {
		li, err := resolveLiveIndex(cmd.Context(), vIndexDSN, vBintrailID)
		if err != nil {
			return err
		}
		in.LiveIndex = li
	}

	if !vNoBaselines {
		if err := resolveBaselineViews(cmd.Context(), &in); err != nil {
			return err
		}
	}

	// Keyed on what the file WOULD contain, not on the flags that got here: the
	// flag refusals above catch --no-baselines, and this catches the identical
	// empty file reached by simply not naming a baseline location. Before the
	// S3 probe, so a render with nothing in it is refused for the reason that
	// matters rather than over a bucket it would never read.
	if !in.RendersAnyView() {
		if in.OmitEvents {
			return fmt.Errorf("this would define no view at all: no baseline snapshot was " +
				"found to build state views from. Name one with --baseline-dir/--baseline-s3, " +
				"or pass --include-events for a file over the archived change log")
		}
		return fmt.Errorf("this would define no view at all: no baseline snapshot was found " +
			"and there is no archive source to build the events view from")
	}

	// After the layout is known, so a purely local one is not refused over an
	// S3 variable it will never read. When the file DOES carry s3:// paths, a
	// broken endpoint is fatal: the alternative is a file that silently sends
	// the operator's DuckDB to AWS.
	if in.NeedsS3() {
		ep, err := storage.S3EndpointFromEnv()
		if err != nil {
			return err
		}
		in.S3Endpoint = ep
	}

	sql := views.Generate(in)
	if err := writeViewsOutput(cmd, sql); err != nil {
		return err
	}

	// Deliberately NOT audited. ext.Record's contract covers surfaces that serve
	// historical ROW DATA; this one emits view DEFINITIONS — paths and column
	// names, no row ever read — which puts it in the same metadata-only class as
	// `status`. Auditing it would report a data access that did not happen.
	if vOut != "-" {
		// The events view is named either way. Reporting the archive-source
		// COUNT for a file that reads no archive path was the shape that made
		// the flip invisible to an unchanged scripted invocation: same exit
		// code, same-looking line, a view silently gone.
		events := "omitted (pass --include-events)"
		if in.RendersEventsView() {
			events = fmt.Sprintf("over %d archive source(s)", len(in.ArchiveSources))
		}
		fmt.Fprintf(cmd.OutOrStdout(), "wrote %s (%d state view(s), events view: %s)\n",
			vOut, len(in.Baselines), events)
	}
	return nil
}

// explicitArchiveSources scopes the operator-named roots to this server's
// bintrail_id subdirectory, exactly like `query`'s equivalent — a view over the
// unscoped root would mix servers' events under one name.
func explicitArchiveSources() []string {
	var sources []string
	if vArchiveDir != "" {
		sources = append(sources, filepath.Join(vArchiveDir, "bintrail_id="+vBintrailID))
	}
	if vArchiveS3 != "" {
		sources = append(sources, strings.TrimSuffix(vArchiveS3, "/")+"/bintrail_id="+vBintrailID)
	}
	return sources
}

// discoverArchiveSources reads the archive_state registry. An empty result is
// not an error: an index whose partitions have never been archived legitimately
// has no archive tier yet, and the generated file says so in a comment rather
// than failing a command whose whole job is to describe what exists.
//
// The PORTABLE routing (S3 wherever registered) is deliberate: the file is
// written to be run somewhere else, where this host's local copy does not
// resolve (#1456). `bintrail query` on this host keeps the local-first routing.
func discoverArchiveSources(ctx context.Context, dsn string) ([]string, error) {
	db, err := config.Connect(dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to index DB: %w", err)
	}
	defer db.Close()
	return discoverArchiveSourcesFrom(ctx, db)
}

// discoverArchiveSourcesFrom is the DB-taking half, split out so the routing
// choice (portable, not local-first) is pinned by a test on THIS surface: the
// console half has its own, and a revert here would otherwise ship unnoticed.
func discoverArchiveSourcesFrom(ctx context.Context, db *sql.DB) ([]string, error) {
	sources, err := query.PortableArchiveSources(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("resolve archive sources from archive_state: %w", err)
	}
	return sources, nil
}

// resolveArchiveGroups fills the per-column-set groups the events view is
// emitted from (#1535), leaving them empty when the registry cannot fully
// answer.
//
// EMPTY IS THE SAFE STATE and the reason this never fails the command: with no
// groups the file keeps the globbed union_by_name leg, which reads every file
// under the roots. Grouping reads the file list from archive_state instead, so
// it is used only when every registered partition has a recorded column set —
// otherwise the ungrouped ones would be missing from the view rather than
// merely slow to bind.
func resolveArchiveGroups(ctx context.Context, dsn string, in *views.Input) error {
	db, err := config.Connect(dsn)
	if err != nil {
		return fmt.Errorf("connect to index DB: %w", err)
	}
	defer db.Close()
	return resolveArchiveGroupsFrom(ctx, db, in)
}

// resolveArchiveGroupsFrom is the DB-taking half, split out so the all-or-
// nothing rule is pinned by a test rather than by the comment above it. The
// console half has its own; a revert on either surface would otherwise ship
// unnoticed, exactly as with discoverArchiveSourcesFrom.
func resolveArchiveGroupsFrom(ctx context.Context, db *sql.DB, in *views.Input) error {
	groups, ungrouped, err := query.ArchiveGroups(ctx, db, in.ArchiveSources)
	if err != nil {
		return fmt.Errorf("read archived column sets from archive_state: %w", err)
	}
	in.UngroupedPartitions = ungrouped
	if ungrouped == 0 {
		in.ArchiveGroups = toViewGroups(groups)
	}
	return nil
}

// toViewGroups converts the registry's groups to the generator's own struct.
// The two are separate types so internal/views links nothing that reaches a
// database; this is the only place they meet.
func toViewGroups(groups []query.ArchiveGroup) []views.ArchiveGroup {
	out := make([]views.ArchiveGroup, len(groups))
	for i, g := range groups {
		out[i] = views.ArchiveGroup{Columns: g.Columns, Files: g.Files}
	}
	return out
}

// resolveBaselineViews picks the NEWEST discoverable snapshot and takes its
// tables.
//
// Only one snapshot: rows from two snapshots are two different points in time,
// and a view spanning them would present a state that never existed. Which
// snapshot is newest per table is not a per-table question here either —
// ListBaselines already skips incomplete snapshots (#467), and taking the single
// newest snapshot timestamp keeps every state view in the file consistent with
// every other one.
func resolveBaselineViews(ctx context.Context, in *views.Input) error {
	src := vBaselineDir
	if src == "" {
		src = vBaselineS3
	}
	if src == "" {
		return nil
	}
	in.BaselineSource = src

	files, err := reconstruct.ListBaselines(ctx, src)
	if err != nil {
		return fmt.Errorf("list baseline snapshots under %s: %w", src, err)
	}
	if len(files) == 0 {
		return nil
	}
	// ListBaselines returns newest snapshot first.
	newest := files[0].SnapshotTime
	in.BaselineSnapshot = newest
	for _, f := range files {
		if !f.SnapshotTime.Equal(newest) {
			continue
		}
		in.Baselines = append(in.Baselines, views.BaselineTable{
			Schema: f.Schema,
			Table:  f.Table,
			Path:   f.Path,
		})
	}
	// Decimals BEFORE the rewrite, and the ordering is load-bearing in the
	// opposite direction from how it first looked. Following only happens when
	// the pointer names the snapshot just discovered, so both spellings resolve
	// to the SAME file and reading either is equivalent. What is not equivalent
	// is the console's memo, which is keyed by snapshot and matched back by
	// exact PATH: populate it with one spelling and the other silently matches
	// nothing, so every DECIMAL column ships uncast while the file blames an
	// unreadable footer that was never read.
	resolveBaselineDecimals(ctx, in)
	views.ApplyFollow(in, in.BaselineSource, vPinSnapshot)
	return nil
}

// resolveBaselineDecimals reads each table's column types out of its Parquet
// footer, so the state views can cast the decimal columns back to numbers.
//
// Best-effort on purpose: types make the generated file better, they are not
// what it is for. A footer that cannot be read costs those tables their casts
// and says so in the file, rather than failing a command whose whole job is to
// describe what exists.
func resolveBaselineDecimals(ctx context.Context, in *views.Input) {
	decimals, err := baseline.DecimalColumnsFor(ctx, in.BaselinePaths())
	if err != nil {
		slog.Warn("views: could not read baseline column types from the Parquet footers; "+
			"the state views will not cast decimal columns", "error", err)
		return
	}
	in.ApplyDecimals(decimals)
}

// writeViewsOutput sends the generated SQL to --out, or to stdout for "-".
//
// A file is written whole via os.WriteFile rather than streamed: the generator
// already produced the complete text, and a partial file left behind by a
// mid-write failure is a file someone would paste into DuckDB.
func writeViewsOutput(cmd *cobra.Command, sql string) error {
	if vOut == "-" {
		_, err := fmt.Fprint(cmd.OutOrStdout(), sql)
		return err
	}
	if err := os.WriteFile(vOut, []byte(sql), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", vOut, err)
	}
	return nil
}

// resolveLiveIndex decomposes the index DSN into the non-secret half the
// generated file can carry, and reads the two things about the index that the
// generated SQL cannot be correct without: which source it serves, and which
// columns its binlog_events actually has.
//
// The password is dropped HERE rather than in the generator. This is the only
// place it exists, so dropping it at the boundary means no later change inside
// the views package can start emitting it by accident.
//
// explicitID is --bintrail-id. When the operator named the source, that name
// also scoped the archive paths this same file reads, so nothing the registry
// reports could be more authoritative and the attribution query is not run.
func resolveLiveIndex(ctx context.Context, dsn, explicitID string) (*views.LiveIndex, error) {
	if dsn == "" {
		return nil, fmt.Errorf("--include-live needs --index-dsn: the live leg reads the index directly, " +
			"so there is nothing to attach without one")
	}
	li, err := liveIndexFromDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := config.Connect(dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to the index for the live leg: %w", err)
	}
	defer db.Close()

	if err := describeLiveIndex(ctx, db, li, explicitID); err != nil {
		return nil, err
	}
	return li, nil
}

// describeLiveIndex is the DB-taking half of resolveLiveIndex, split out so
// what the command asks the index — and what it does NOT ask it when the
// operator already named the source — is pinned without a MySQL.
func describeLiveIndex(ctx context.Context, db *sql.DB, li *views.LiveIndex, explicitID string) error {
	cols, err := liveTableColumns(ctx, db, li.Database)
	if err != nil {
		return err
	}
	li.TableColumns = cols

	if explicitID != "" {
		li.BintrailID = explicitID
		return nil
	}
	attributeLiveIndex(ctx, db, li)
	return nil
}

// attributeLiveIndex records what the index says about the sources it serves.
//
// Attribution is only possible with exactly ONE registered source: every source
// writes into the same binlog_events and the row itself carries no identity.
// Every other outcome leaves the rows unattributed, and this is where they stop
// being one outcome: the generated file states what was observed, so an
// unreadable list and a registry with nothing in it must not arrive as the same
// value. Best effort on purpose — none of these fail a file whose other half is
// fine.
func attributeLiveIndex(ctx context.Context, db *sql.DB, li *views.LiveIndex) {
	var n int
	var id string
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(MIN(bintrail_id), '') FROM bintrail_servers`).Scan(&n, &id)
	if err != nil {
		var myErr *drivermysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1146 {
			// No such table. Read the same way status.knownSourceCount reads
			// it: zero known sources, not an unreadable list. An index that
			// never ran the migration is not evidence of several sources, and
			// reporting it as one would be the exact inversion.
			li.Attribution = views.AttributionUnregistered
			return
		}
		li.Attribution = views.AttributionUndetermined
		return
	}
	switch {
	case n == 1 && id != "":
		li.BintrailID = id
	case n > 1:
		li.Attribution = views.AttributionMultiSource
	default:
		// No rows, or one row whose bintrail_id is NULL (the column is
		// nullable — see internal/status/staleness.go). Either way there is no
		// id to attribute a row to, which is what the file will say.
		li.Attribution = views.AttributionUnregistered
	}
}

// liveTableColumns reads the index's binlog_events column set, so the generated
// SQL names only columns that are there.
//
// The cold leg tolerates an archive written before a column existed
// (union_by_name); the hot leg has no such mechanism, and an index migrated to
// an earlier point than this build's schema — a legacy registry index the
// console never migrates, for one — turns the whole generated file into a
// binder error naming an internal column, with no events view created at all.
//
// Unlike attribution, this is NOT best effort: it decides whether the file
// binds. A file that cannot define its main view is worse than a refused
// command that says why.
func liveTableColumns(ctx context.Context, db *sql.DB, dbName string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT COLUMN_NAME FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events'`, dbName)
	if err != nil {
		return nil, fmt.Errorf("read the index's binlog_events columns for the live leg: %w", err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, fmt.Errorf("read the index's binlog_events columns for the live leg: %w", err)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the index's binlog_events columns for the live leg: %w", err)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("--index-dsn names database %q, which has no binlog_events table: "+
			"--include-live builds a view over that table, so there is nothing to add", dbName)
	}
	return cols, nil
}

// liveIndexFromDSN is the PURE half of resolveLiveIndex: DSN in, the non-secret
// connection facts out, no IO.
//
// Split out so the one property that matters can be tested without a database.
// The DSN carries the index password, this is the only place it is in scope,
// and everything downstream renders into a file meant to be shared. Dropping it
// here means no change further down can start emitting it.
//
// It does its own parsing rather than borrowing config.ParseSourceDSN: that
// function's refusals name --source-dsn and justify themselves with binlog
// replication, and a wrong flag with an irrelevant reason is worse guidance
// than none for an operator who typed --index-dsn at a command that generates
// text.
func liveIndexFromDSN(dsn string) (*views.LiveIndex, error) {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse --index-dsn for the live leg: %w", err)
	}
	if strings.EqualFold(cfg.Net, "unix") {
		// Refused rather than rendered. The generated file names the index by
		// host and port precisely so it can be read from another machine, and
		// a socket path is a name for one machine only. DuckDB's mysql
		// extension does take a SOCKET, so this is a choice about what the
		// artifact is for, not a limitation.
		return nil, fmt.Errorf("--index-dsn names a unix socket (%s), which --include-live cannot put in the file: "+
			"the generated SQL locates the index by host and port so it can be run from another machine, "+
			"and a socket path names only this one. Give --index-dsn a TCP address", cfg.Addr)
	}
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("--index-dsn has no usable host:port for the live leg (%q): %w", cfg.Addr, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("--index-dsn has an unusable port for the live leg (%q): %w", portStr, err)
	}
	// The DSN's own database, from the same parse: the generated ATTACH names
	// it, and a reader on another machine has no session to ask.
	if cfg.DBName == "" {
		return nil, fmt.Errorf("--index-dsn names no database, so the live leg has nothing to attach")
	}
	return &views.LiveIndex{Host: host, Port: int(port), Database: cfg.DBName, User: cfg.User}, nil
}
