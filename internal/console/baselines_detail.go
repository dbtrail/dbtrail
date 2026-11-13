package console

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/keyidx"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/views"
)

// This file serves the per-snapshot surfaces of the Snapshots page (#TBD):
//
//	GET /api/baselines/files    — one snapshot's tables, sizes and write span
//	GET /api/baselines/download — the whole snapshot as a tar.gz stream
//
// Both take ?at=<snapshot time> (the `time` field the listing returned) and
// resolve it to the snapshot DIRECTORY via reconstruct.SnapshotDirName — the
// same name the writers produced, so no other file can be reached: the
// parameter is parsed to a time.Time and re-rendered, never joined as a path.

// baselineTableSizeDTO is one table of a snapshot with its stored size.
type baselineTableSizeDTO struct {
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	SizeBytes int64  `json:"size_bytes"`
	// ProducedBy is how THIS table's rows got into THIS snapshot (#1545):
	// dump | fold | carried_forward | unknown. Per table, not per snapshot,
	// because carry-forward makes one snapshot a mix. Empty on an S3 source,
	// where it would cost one object read per table; the field being absent is
	// "not looked up", never "unknown".
	ProducedBy string `json:"produced_by,omitempty"`
	// From is the snapshot these rows came out of, for the two derived cases.
	From string `json:"from,omitempty"`
	// SourceReadAt is when these rows last came from a real read of the
	// source database (#1570), and FoldsSinceRead how many folds were applied
	// since: 0 for a table this backup read itself. Inherited through every
	// fold, and for a carried table read off the reused file, which IS the
	// older backup's bytes. Absent when the footer does not record it (a fold
	// written before #1570 over files that did not either), or, like
	// ProducedBy, when nothing was looked up.
	SourceReadAt   string `json:"source_read_at,omitempty"`
	FoldsSinceRead *int   `json:"folds_since_read,omitempty"`
	// Lock is how the database was locked when these rows were read (#1380):
	// consistent | unknown | torn. Inherited through every update, and for a
	// reused table read off the reused file. "unknown" is a file with no
	// record, or one whose footer could not be read. Absent, like
	// ProducedBy, when nothing was looked up (an S3 source).
	Lock string `json:"lock,omitempty"`
}

type baselineFilesResponse struct {
	Time   string                 `json:"time"`
	Tables []baselineTableSizeDTO `json:"tables"`
	// TotalBytes and Files cover EVERY stored file of the snapshot, markers
	// and manifest included — it is what a download of it weighs.
	TotalBytes int64 `json:"total_bytes"`
	Files      int   `json:"files"`
	// WroteFrom/WroteTo bound the storage timestamps of the snapshot's files:
	// an approximation of how long the run took, usable for snapshots whose
	// run this daemon never saw. The first file's timestamp is its COMPLETION
	// time, so the span underestimates the run by the first file's write.
	WroteFrom        string  `json:"wrote_from,omitempty"`
	WroteTo          string  `json:"wrote_to,omitempty"`
	WriteSpanSeconds float64 `json:"write_span_seconds"`
	// SourceReadAt is the OLDEST last read of the source among this backup's
	// tables that record one (#1570): how far back the real evidence under
	// this backup goes. SourceReadAgeSeconds is its distance from the backup's
	// own time, MaxFoldsSinceRead the most folds any of those tables has had
	// since its read (only when every dated table records its count), and
	// SourceReadMissing how many tables could not be
	// dated (the footer does not record it, or could not be read), so the
	// line never speaks for them. All absent when no table was looked up (an
	// S3 source).
	SourceReadAt         string  `json:"source_read_at,omitempty"`
	SourceReadAgeSeconds float64 `json:"source_read_age_seconds,omitempty"`
	MaxFoldsSinceRead    *int    `json:"max_folds_since_read,omitempty"`
	SourceReadMissing    int     `json:"source_read_missing,omitempty"`
	// SourceReadUncounted is how many dated tables record no count of folds
	// since their read. MaxFoldsSinceRead is absent whenever it is not zero:
	// a maximum that leaves tables out is not the most.
	SourceReadUncounted int `json:"source_read_uncounted,omitempty"`
	// Lock is the snapshot's word on how the database was locked when its
	// rows were read (#1380), the worst of its tables: consistent | unknown |
	// torn. LockTorn and LockUnknown count the tables behind the two that are
	// not consistent; a footer that could not be read counts as unknown. All
	// absent when no table was looked up (an S3 source).
	Lock        string `json:"lock,omitempty"`
	LockTorn    int    `json:"lock_torn,omitempty"`
	LockUnknown int    `json:"lock_unknown,omitempty"`
	// Incomplete marks a snapshot carrying an _INCOMPLETE marker without a
	// _SUCCESS one (a failed or unfinished run). The listing excludes such
	// snapshots, but the detail stays honest if one is addressed directly.
	Incomplete bool `json:"incomplete,omitempty"`
	// Run is the recorded console run that produced this snapshot, present
	// only when THIS daemon performed it: the exact duration. Snapshots made
	// elsewhere (CLI, another daemon) have no record; the write span above is
	// their approximation.
	Run *baselineRunDTO `json:"run,omitempty"`
	// ViewsSkipped is the views this snapshot left out (#1879), with their
	// names. Absent when nothing is recorded, which is never "no views".
	ViewsSkipped *viewsSkippedDTO `json:"views_skipped,omitempty"`
}

type baselineRunDTO struct {
	Kind    string  `json:"kind"`
	Seconds float64 `json:"seconds"`
	Tables  int     `json:"tables,omitempty"`
	Rows    int64   `json:"rows,omitempty"`
	// Why / WhyCode: for a scheduled full backup, why an update was not
	// possible when it ran (#1604). Persisted with the run, never recomputed.
	Why     string `json:"why,omitempty"`
	WhyCode string `json:"why_code,omitempty"`
	// UpdateWhy: for a scheduled update, why it ran where the slot would
	// otherwise have been skipped (#2674).
	UpdateWhy string `json:"update_why,omitempty"`
	// DiskCheck / DiskNote: the full read's free-space check (#1938).
	DiskCheck string `json:"disk_check,omitempty"`
	DiskNote  string `json:"disk_note,omitempty"`
	// TransportNote: the read reached the source without encryption (#1996).
	TransportNote string `json:"transport_note,omitempty"`
	// LeftOutTables / LeftOutTablesOmitted: the tables this full read left
	// out of the snapshot, and why (#2006).
	LeftOutTables        []RefusedTable `json:"left_out_tables,omitempty"`
	LeftOutTablesOmitted int            `json:"left_out_tables_omitted,omitempty"`
}

// baselineSnapshotFile is one stored file of a snapshot: its path relative to
// the baseline ROOT (so it starts with the snapshot directory name), always
// forward-slashed, plus size and the backend's modification time.
type baselineSnapshotFile struct {
	RelPath string
	Size    int64
	ModTime time.Time
}

// baselineObjectStore is the slice of the storage backend the S3 snapshot
// surfaces need. *storage.S3Backend satisfies it; tests substitute a fake via
// newBaselineObjectStore.
type baselineObjectStore interface {
	ListInfo(ctx context.Context, prefix string) ([]storage.ObjectInfo, error)
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}

// newBaselineObjectStore opens the object store behind an s3:// baseline
// source. A package variable so handler tests can substitute a fake without
// AWS; the real constructor resolves credentials and region from the
// environment (including the #697 IMDS fallback).
var newBaselineObjectStore = func(ctx context.Context, src string) (baselineObjectStore, error) {
	bucket, prefix, err := storage.ParseS3URL(src)
	if err != nil {
		return nil, err
	}
	return storage.NewS3BackendUnprobed(ctx, storage.S3Config{Bucket: bucket, Prefix: prefix})
}

// snapshotSource reads one baseline source's files, local directory or S3.
type snapshotSource struct {
	localRoot string              // set for a local directory source
	store     baselineObjectStore // set for an s3:// source
	srcURL    string              // the s3:// root as configured, for realPath
}

func openSnapshotSource(ctx context.Context, src string) (*snapshotSource, error) {
	if strings.HasPrefix(src, "s3://") {
		store, err := newBaselineObjectStore(ctx, src)
		if err != nil {
			return nil, err
		}
		return &snapshotSource{store: store, srcURL: strings.TrimSuffix(src, "/")}, nil
	}
	return &snapshotSource{localRoot: src}, nil
}

// realPath spells one enumerated file the way a reader of the SOURCE would
// address it — a local filesystem path or an s3:// URL. It is what the
// decimals resolver keys by (#1583); the tarball's views file respells the
// same tables relative afterwards.
func (ss *snapshotSource) realPath(relPath string) string {
	if ss.store != nil {
		return ss.srcURL + "/" + relPath
	}
	return filepath.Join(ss.localRoot, filepath.FromSlash(relPath))
}

// files enumerates every stored file of the snapshot directory dirName,
// markers included, sorted by RelPath. fs.ErrNotExist when the snapshot does
// not exist (locally: no directory; S3: an empty listing — S3 has no
// directories, so absent and empty are the same observation).
func (ss *snapshotSource) files(ctx context.Context, dirName string) ([]baselineSnapshotFile, error) {
	var out []baselineSnapshotFile
	if ss.store != nil {
		infos, err := ss.store.ListInfo(ctx, dirName+"/")
		if err != nil {
			return nil, err
		}
		for _, o := range infos {
			out = append(out, baselineSnapshotFile{RelPath: o.Key, Size: o.Size, ModTime: o.LastModified})
		}
	} else {
		root := filepath.Join(ss.localRoot, dirName)
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() {
				return walkErr
			}
			// A key index build's .tmp (#2496) is not a snapshot file.
			if strings.HasSuffix(d.Name(), keyidx.Suffix+".tmp") {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(ss.localRoot, p)
			if err != nil {
				return err
			}
			out = append(out, baselineSnapshotFile{
				RelPath: filepath.ToSlash(rel), Size: info.Size(), ModTime: info.ModTime()})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if len(out) == 0 {
		return nil, fs.ErrNotExist
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RelPath < out[j].RelPath })
	return out, nil
}

// open returns a reader for one file previously returned by files. relPath is
// trusted — it came from our own enumeration, never from the request.
func (ss *snapshotSource) open(ctx context.Context, relPath string) (io.ReadCloser, error) {
	if ss.store != nil {
		return ss.store.Get(ctx, relPath)
	}
	return os.Open(filepath.Join(ss.localRoot, filepath.FromSlash(relPath)))
}

// snapshotIncomplete mirrors baseline.SnapshotComplete over an enumerated file
// list (which serves S3 too): _SUCCESS wins, else an _INCOMPLETE marker means
// a failed or unfinished run, else a legacy snapshot is complete by default.
func snapshotIncomplete(files []baselineSnapshotFile) bool {
	hasSuccess, hasIncomplete := false, false
	for _, f := range files {
		switch path.Base(f.RelPath) {
		case baseline.SuccessMarker:
			hasSuccess = true
		case baseline.IncompleteMarker:
			hasIncomplete = true
		}
	}
	return hasIncomplete && !hasSuccess
}

// parseSnapshotAt parses the ?at= parameter: the listing's own `time` format
// first, RFC3339 as a fallback. Both are UTC, and the result is truncated to
// the second: Go's parser accepts a fractional second even when the layout
// has none, and every consumer of this instant collapses to seconds anyway —
// the snapshot directory name, the status and history stamps. Left in, a
// fraction is a way past every "already exists at exactly" guard: the
// snapshot at 10:00:00 is at-or-before 10:00:00.5, so the fold anchors on it,
// names its output 10-00-00Z, and overwrites it in place (#1541).
func parseSnapshotAt(raw string) (time.Time, bool) {
	if t, err := time.ParseInLocation(consoleTSFormat, raw, time.UTC); err == nil {
		return t.Truncate(time.Second), true
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC().Truncate(time.Second), true
	}
	return time.Time{}, false
}

// resolveSnapshotRequest runs the gates and lookups the two per-snapshot
// handlers share. A nil return means the response has already been written.
func (s *Server) resolveSnapshotRequest(w http.ResponseWriter, r *http.Request, gate string) (*snapshotSource, string, []baselineSnapshotFile) {
	b := s.resolveOr(w, r)
	if b == nil {
		return nil, "", nil
	}
	// Same invariant as the listing (#1075): baseline reads bypass RBAC
	// redaction, so a session carrying a data profile is refused.
	if sessionRestricted(r) {
		recordProfileGateDeny(r, gate)
		writeJSONError(w, http.StatusForbidden,
			"snapshots are unavailable while an access-control profile is active: baseline reads aren't redacted")
		return nil, "", nil
	}
	if b.baselineSrc == "" {
		writeJSONError(w, http.StatusNotFound, "no snapshot location is configured for this server")
		return nil, "", nil
	}
	ts, ok := parseSnapshotAt(r.URL.Query().Get("at"))
	if !ok {
		writeJSONError(w, http.StatusBadRequest,
			"at must be a snapshot time as the listing returned it (YYYY-MM-DD HH:MM:SS, UTC)")
		return nil, "", nil
	}
	dirName := reconstruct.SnapshotDirName(ts)
	// Every configured location, in the same order the listing consults them
	// (#1542). Before the listing merged them this could only ever be asked
	// about a snapshot the primary held, because no other row existed. Now that
	// an S3-only snapshot has a row, opening the primary alone would answer
	// "no snapshot found" for a row the same page just said is there — and the
	// download button, which is built inside the success path, would never
	// appear for exactly the snapshots #1542 exists to reveal.
	//
	// The same fallback bundle.findBaseline already performs, for the same
	// reason: local retention prunes while the durable copy remains.
	var firstErr error
	for _, src := range baselineSourcesOf(b) {
		// Bounded, for the reason the listing is: an s3:// source builds an
		// object store (which HEADs the bucket) and then lists it, inside the
		// process that is also capturing, and the server sets no WriteTimeout
		// on purpose. Before this loop the detail tier only ever opened the
		// primary, so on a dir+S3 server it never touched the bucket at all;
		// extending it without a deadline would have traded a 404 for a handler
		// pinned indefinitely.
		srcCtx, cancel := context.WithTimeout(r.Context(), baselineListTimeout)
		ss, err := openSnapshotSource(srcCtx, src)
		if err != nil {
			cancel()
			if firstErr == nil {
				firstErr = fmt.Errorf("open snapshot storage: %w", err)
			}
			continue
		}
		files, err := ss.files(srcCtx, dirName)
		cancel()
		if err == nil {
			return ss, dirName, files
		}
		// Not-found is not an error yet: a later location may hold it. Anything
		// else IS reported, but only after every location has been tried, so an
		// unreachable bucket cannot hide a snapshot sitting on local disk.
		if !errors.Is(err, fs.ErrNotExist) && firstErr == nil {
			firstErr = fmt.Errorf("list snapshot files: %w", err)
		}
	}
	if firstErr != nil {
		writeJSONError(w, http.StatusBadGateway, firstErr.Error())
		return nil, "", nil
	}
	writeJSONError(w, http.StatusNotFound, "no snapshot found at "+ts.Format(consoleTSFormat))
	return nil, "", nil
}

// handleBaselineFiles serves GET /api/baselines/files?at=…: one snapshot's
// tables with sizes, its total weight, and the span its files were written
// over.
//
// Deliberately NOT audited: like the listing, this is metadata (names, sizes,
// timestamps) — no row data leaves the store. The download below is audited.
func (s *Server) handleBaselineFiles(w http.ResponseWriter, r *http.Request) {
	src, dirName, files := s.resolveSnapshotRequest(w, r, "baseline-files")
	if files == nil {
		return
	}
	ts, _ := parseSnapshotAt(r.URL.Query().Get("at"))
	resp := baselineFilesResponse{
		Time:       ts.Format(consoleTSFormat),
		Tables:     []baselineTableSizeDTO{},
		Incomplete: snapshotIncomplete(files),
	}
	var oldest, newest time.Time
	// Each table's delta files (#1638): with deltas on the table file is
	// carried forward on every refresh, changed or not, and the run's own
	// footer sits on the chain's newest pair, so a table cannot be described
	// from its file alone.
	deltaNames := tableDeltaNames(files)
	reads := snapshotSourceReads{}
	locks := snapshotLocks{}
	for _, f := range files {
		resp.TotalBytes += f.Size
		resp.Files++
		if !f.ModTime.IsZero() {
			if oldest.IsZero() || f.ModTime.Before(oldest) {
				oldest = f.ModTime
			}
			if f.ModTime.After(newest) {
				newest = f.ModTime
			}
		}
		// <tsdir>/<schema>/<table>.parquet — anything else (markers, the
		// integrity manifest) weighs in the totals but is not a table.
		parts := strings.Split(f.RelPath, "/")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".parquet") {
			continue
		}
		row := baselineTableSizeDTO{
			Schema: parts[1], Table: strings.TrimSuffix(parts[2], ".parquet"), SizeBytes: f.Size}
		// Local only. Over S3 this is one object read per table, two with a
		// chain, which is the same latency the listing already declines to
		// spend on footers, and a row with no verdict reads as "not looked
		// up" rather than as unknown.
		if src != nil && src.localRoot != "" {
			dir := filepath.Join(src.localRoot, filepath.FromSlash(parts[0]), parts[1])
			d := describeTable(filepath.Join(dir, parts[2]), dir, deltaNames[parts[0]+"/"+parts[1]+"/"+row.Table], ts)
			row.ProducedBy, row.From = d.producedBy, d.from
			// Every table looked at counts, the unreadable ones too: the
			// snapshot's line must not speak for a table it could not date.
			reads.add(d.read)
			// A footer that could not be read is unknown on the table as it
			// is in the snapshot's count, so the page can mark the table the
			// count speaks of.
			locks.add(d.lock, d.lockRead)
			row.Lock = baseline.ReadUnknown.String()
			if d.lockRead {
				row.Lock = d.lock.String()
			}
			if d.read.Known() {
				row.SourceReadAt = d.read.At.UTC().Format(consoleTSFormat)
				if d.read.Folds >= 0 {
					n := d.read.Folds
					row.FoldsSinceRead = &n
				}
			}
		}
		resp.Tables = append(resp.Tables, row)
	}
	reads.fill(&resp, ts)
	resp.Lock, resp.LockTorn, resp.LockUnknown = locks.verdict(), locks.torn, locks.unknown
	sort.Slice(resp.Tables, func(i, j int) bool {
		if resp.Tables[i].Schema != resp.Tables[j].Schema {
			return resp.Tables[i].Schema < resp.Tables[j].Schema
		}
		return resp.Tables[i].Table < resp.Tables[j].Table
	})
	if !oldest.IsZero() {
		resp.WroteFrom = oldest.UTC().Format(consoleTSFormat)
		resp.WroteTo = newest.UTC().Format(consoleTSFormat)
		resp.WriteSpanSeconds = newest.Sub(oldest).Seconds()
	}
	if s.baselineHistory != nil {
		if rec := s.baselineHistory.FindBySnapshot(s.selectedServerID(r), ts.Format(time.RFC3339)); rec != nil {
			run := &baselineRunDTO{Kind: rec.Kind, Tables: rec.Tables, Rows: rec.Rows, Why: rec.Why, WhyCode: rec.WhyCode, UpdateWhy: rec.UpdateWhy,
				DiskCheck: rec.DiskCheck, DiskNote: rec.DiskNote, TransportNote: rec.TransportNote,
				LeftOutTables: rec.LeftOutTables, LeftOutTablesOmitted: rec.LeftOutTablesOmitted}
			if st, err1 := time.Parse(time.RFC3339, rec.StartedAt); err1 == nil {
				if fin, err2 := time.Parse(time.RFC3339, rec.FinishedAt); err2 == nil {
					run.Seconds = fin.Sub(st).Seconds()
				}
			}
			resp.Run = run
		}
	}
	// The snapshot's own record first; this daemon's count of the run for a
	// snapshot that has none.
	vctx, vcancel := context.WithTimeout(r.Context(), baselineListTimeout)
	resp.ViewsSkipped = snapshotViewsSkipped(vctx, src, dirName, files)
	vcancel()
	if resp.ViewsSkipped == nil {
		resp.ViewsSkipped = s.viewsSkippedFromRun(s.selectedServerID(r), ts)
	}
	writeJSON(w, http.StatusOK, resp)
}

// snapshotViewsRelative renders the tarball's own views.sql (#1583): the same
// snapshot-scoped file the producers publish beside _SUCCESS, respelled with
// "./" relative paths so it works from inside the unpacked folder wherever
// that folder lands. Empty when the snapshot holds no tables.
//
// The decimals go through the server memo keyed by the SOURCE root — the same
// key the /api/views.sql tier uses — so a download after a schema download
// (or the reverse) pays the footer reads once. Respelling happens after, per
// RespellBaselines' contract.
func (s *Server) snapshotViewsRelative(ctx context.Context, ss *snapshotSource, ts time.Time, files []baselineSnapshotFile) string {
	src := ss.srcURL
	if src == "" {
		src = ss.localRoot
	}
	// The download carries every file of the snapshot, a table's delta chain
	// (#1638, #1718) included, so the marks are read off the list already in
	// hand, one schema directory at a time.
	byDir := map[string][]string{}
	for _, f := range files {
		parts := strings.Split(f.RelPath, "/")
		if len(parts) == 3 {
			byDir[parts[0]+"/"+parts[1]] = append(byDir[parts[0]+"/"+parts[1]], parts[2])
		}
	}
	chains := map[string]*baseline.TableDeltaChain{}
	for dir, names := range byDir {
		c, err := baseline.MarkTableDeltaFiles(dir, names)
		if err != nil {
			// No views.sql rather than a wrong one: with half a pair there is
			// no state to describe, and a view over the file alone would show
			// the table as it was when its chain started. The files themselves
			// still download.
			slog.Warn("snapshot download: a table's delta files do not form whole pairs, so no views.sql is included",
				"snapshot", ts.UTC().Format(time.RFC3339), "error", err)
			return ""
		}
		for k, v := range c {
			chains[k] = v
		}
	}
	var tables []views.BaselineTable
	for _, f := range files {
		parts := strings.Split(f.RelPath, "/")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".parquet") {
			continue
		}
		c := chains[f.RelPath]
		tables = append(tables, views.BaselineTable{
			Schema:      parts[1],
			Table:       strings.TrimSuffix(parts[2], ".parquet"),
			Path:        ss.realPath(f.RelPath),
			Rel:         parts[1] + "/" + parts[2],
			Delta:       c != nil,
			DeltaLegacy: c != nil && c.Legacy,
		})
	}
	if len(tables) == 0 {
		return ""
	}
	in := views.SnapshotScopedInput(ts, tables)
	in.BaselineSource = strings.TrimSuffix(src, "/")
	s.resolveBaselineDecimals(ctx, &in)
	in.RespellBaselines(".")
	return views.Generate(in)
}

// handleBaselineDownload serves GET /api/baselines/download?at=…: the whole
// snapshot directory as one tar.gz stream, markers and manifest included, so
// what lands on the operator's disk is a complete, discoverable snapshot.
//
// One stored file is REPLACED rather than copied: the snapshot's own
// views.sql names the paths of wherever the snapshot lives (that is its job,
// #1583), and every one of those paths is wrong inside an unpacked tarball.
// The stream carries a freshly rendered relative copy under the same name.
//
// Mid-stream errors panic with http.ErrAbortHandler: the status is already
// written, and cutting the connection is what makes the CLIENT fail loudly.
// A plain return would end the chunked body cleanly: curl -O or wget would
// save the truncated archive as a success (the console's own fetch+blob
// rejects on a cut body either way), and the operator would learn it is
// garbage at extraction time, plausibly mid-incident.
// downloadRule is the rule the cleanup prunes the selected server's folder
// by: its count and the age, both 0 where this process prunes nothing. A
// var for the tests.
var downloadRule = func(s *Server, r *http.Request) (keep int, retain time.Duration) {
	return pruneRule(s, s.selectedServerID(r))
}

// pruneRule is downloadRule for the server with id, for a reader that has no
// request to read the selection from (the MCP reconstruct tool, `_snapshot`
// through the port). Read at each call: a count saved in the settings
// applies to the next reader. A var for the tests.
var pruneRule = func(s *Server, id string) (keep int, retain time.Duration) {
	if !s.localPruneLoop {
		return 0, 0
	}
	if lr := s.localRetentionOf(id); lr != nil {
		keep = lr.KeepNewest
	}
	retain = time.Duration(s.pruneRetainMinutes()) * time.Minute
	inRegistry, serverS3 := false, ""
	if s.cm.reg != nil {
		if e, ok := s.cm.reg.Get(id); ok {
			inRegistry, serverS3 = true, e.BaselineS3
		}
	}
	return downloadRuleFor(keep, retain, inRegistry, serverS3, s.cm.defaultBaselineS3)
}

// downloadRuleFor is the rule for one server. With a count, the count and
// the age. Without one, the age alone prunes only a folder whose snapshots
// go to S3: a server of the registry with a destination of its own, or the
// daemon's own server (not in the registry: its folder is --baseline-dir),
// pruned by age when the daemon has a bucket (--baseline-s3). A folder with
// neither a count nor a destination keeps every snapshot: no rule.
func downloadRuleFor(keep int, retain time.Duration, inRegistry bool, serverS3, bootS3 string) (int, time.Duration) {
	if keep > 0 {
		return keep, retain
	}
	dest := bootS3
	if inRegistry {
		dest = serverS3
	}
	if strings.TrimSpace(dest) == "" {
		return 0, 0
	}
	return 0, retain
}

// downloadCeilingPerCap is how many different snapshots the downloads of one
// server may pin in all, as a multiple of the cap on old ones: twice. The
// cap (the server's count N, or 3 where the folder is pruned by age alone)
// bounds the snapshots that are past what the cleanup keeps; a download of
// one it keeps anyway is past the count a few minutes later, so those are
// bounded too: N more, the number of snapshots the count keeps at any
// moment. In all, downloads never pin, and so never hold back, more than
// 2 x N snapshots, each for no longer than its own time limit
// (downloadPinLimit).
const downloadCeilingPerCap = 2

// downloadCap is how many snapshots past what the cleanup keeps may be held
// for downloads at once: the server's count, or the default count where the
// folder is pruned by age alone.
func downloadCap(keep int) int {
	if keep > 0 {
		return keep
	}
	return DefaultLocalKeepNewest
}

// pinForDownload pins the snapshot a download streams. keep is the count
// the cleanup prunes this server's folder to and retain the age it prunes
// by, both 0 where no cleanup removes anything from it (a `serve` console, a
// server that keeps every snapshot): there a pin holds nothing back and no
// download is refused. Elsewhere the cap is worked out on what the cleanup
// keeps NOW (baseline.PinDownload), so a download that started on the
// newest snapshot counts once that snapshot is past the count.
func pinForDownload(root, name string, keep int, retain, limit time.Duration) (release func(), ok bool) {
	return pinForRead(root, name, "a snapshot download", keep, retain, limit)
}

// pinForRead is pinForDownload for any reader whose snapshot a client
// chooses (#2691): a download, and a read of one table at a past moment.
// They share the one cap (baseline.PinCounted). Where nothing is pruned the
// pin is still taken as one that counts, so a count saved while it runs
// sees it.
func pinForRead(root, name, what string, keep int, retain, limit time.Duration) (release func(), ok bool) {
	if keep <= 0 && retain <= 0 {
		return baseline.PinCounted(root, name, what, limit, nil, math.MaxInt, math.MaxInt)
	}
	kept, err := baseline.KeptNow(root, keep, retain)
	if err != nil {
		// The folder cannot be listed: nothing is known to be kept, so
		// every pin counts toward the cap.
		kept = nil
	}
	max := downloadCap(keep)
	return baseline.PinCounted(root, name, what, limit, kept, max, max*downloadCeilingPerCap)
}

// pinBaselineRead is what a reader of one table at a past moment on the
// server with id pins its snapshot with (#2691): /api/reconstruct, the MCP
// reconstruct tool and `_snapshot` through the port, through
// reconstruct.FindPinned. The folder and the snapshot come from the path the
// lookup returned, never from the server's settings: the lookup may have
// answered from another location. The pin lasts what a download of the
// table's files would (downloadPinLimit), because a full-table `_snapshot`
// streams at the pace of its MySQL client, or as long as the reader's own
// deadline when that is later (readPinLimit). It is counted with the
// downloads: past the cap the read goes on without a pin, as it did before,
// and fails if the cleanup removes its snapshot meanwhile. A var for the
// tests.
var pinBaselineRead = func(s *Server, id string) reconstruct.PinFunc {
	return func(ctx context.Context, path, what string) (release func()) {
		root, name, ok := baseline.TableFileSnapshot(path)
		if !ok {
			return func() {}
		}
		keep, retain := pruneRule(s, id)
		release, ok = pinForRead(root, name, what, keep, retain, readPinLimit(ctx, baseline.TableReadBytes(path)))
		if !ok {
			warnUnpinnedRead(id, name, what)
		}
		return release
	}
}

// readPinLimit is the deadline of a read's pin: a download's of its bytes,
// or what is left of the reader's own deadline when that is later. A query
// through the port first reads the change log, which can take minutes on a
// small table, and its time limit (--flashback-query-timeout) is the server's,
// not the client's.
func readPinLimit(ctx context.Context, bytes int64) time.Duration {
	limit := downloadPinLimit(bytes)
	if d, ok := ctx.Deadline(); ok {
		limit = max(limit, time.Until(d))
	}
	return limit
}

// unpinnedReadWarnEvery bounds the warning of a read past the cap to one
// per server in this interval: a client repeating reads would otherwise
// fill the log, and one line is enough to explain a read that then fails
// on a removed snapshot.
const unpinnedReadWarnEvery = 10 * time.Minute

var unpinnedReadWarned sync.Map // server id -> time.Time of the last warning

// unpinnedReadNow is warnUnpinnedRead's clock, replaced in tests.
var unpinnedReadNow = time.Now

func warnUnpinnedRead(id, name, what string) {
	now := unpinnedReadNow()
	if last, ok := unpinnedReadWarned.Load(id); ok && now.Sub(last.(time.Time)) < unpinnedReadWarnEvery {
		slog.Debug("console: a read of a past moment goes without a pin", "server", id, "snapshot", name, "reader", what)
		return
	}
	unpinnedReadWarned.Store(id, now)
	slog.Warn("console: a read of a past moment goes without a pin: downloads and reads of this server's older snapshots hold their limit, so the cleanup may remove its snapshot while it runs and the read then fails; it can be run again (said once every 10 minutes per server)",
		"server", id, "snapshot", name, "reader", what)
}

// downloadsBusyMessage is the refusal of a download past the cap. It
// states both limits, so it is true whichever of the two refused.
func downloadsBusyMessage(max, ceiling int) string {
	return fmt.Sprintf("Downloads of this server's snapshots are at their limit: they may hold %d snapshots at once, "+
		"and %d of those that the cleanup would have removed. Try again shortly, when a download ends.", ceiling, max)
}

// downloadStallTimeout is how long one write of the archive may wait for the
// client before the download is dropped. A write hands the connection at
// most a few tens of kilobytes (gzip's output buffer), so a client that has
// not taken that much in a minute is not reading. A var for the tests.
var downloadStallTimeout = time.Minute

// downloadMinBytesPerSecond sizes the pin of a download: the snapshot is
// kept for the time its bytes take at this rate, plus one stall timeout.
// 1 MiB/s is a tenth of a 100 Mbit link: a 10 GiB snapshot is kept for
// under three hours, a 100 MiB one for under three minutes. A client slower
// than that keeps its connection, and loses the protection.
const downloadMinBytesPerSecond = 1 << 20

func downloadPinLimit(totalBytes int64) time.Duration {
	return downloadStallTimeout + time.Duration(totalBytes/downloadMinBytesPerSecond)*time.Second
}

// stallWriter gives every write of a streamed response its own deadline.
type stallWriter struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

func (s *stallWriter) Write(b []byte) (int, error) {
	// Not supported by the writer (a test recorder): no deadline, as before.
	_ = s.rc.SetWriteDeadline(time.Now().Add(downloadStallTimeout))
	return s.w.Write(b)
}

// done clears the deadline, so it does not outlive this response on a
// connection that is used again.
func (s *stallWriter) done() { _ = s.rc.SetWriteDeadline(time.Time{}) }

func (s *Server) handleBaselineDownload(w http.ResponseWriter, r *http.Request) {
	ss, dirName, files := s.resolveSnapshotRequest(w, r, "baseline-download")
	if files == nil {
		return
	}
	if ss.localRoot != "" {
		// The files are opened one by one while the archive streams, so the
		// cleanup keeps THIS snapshot while it does, and goes on removing
		// the others past the count (#2678). For a bounded time, sized by
		// what is streamed (downloadPinLimit): how long the client takes is
		// the client's choice, and it must not decide how long a snapshot
		// stays on the disk. Past it the stream goes on unprotected; if the
		// cleanup then removes the snapshot, the next file fails to open and
		// the connection is cut, as for any file unreadable mid-stream.
		var total int64
		for _, f := range files {
			total += f.Size
		}
		keep, retain := downloadRule(s, r)
		release, ok := pinForDownload(ss.localRoot, dirName, keep, retain, downloadPinLimit(total))
		if !ok {
			w.Header().Set("Retry-After", "60")
			writeJSONError(w, http.StatusTooManyRequests, downloadsBusyMessage(downloadCap(keep), downloadCap(keep)*downloadCeilingPerCap))
			return
		}
		defer release()
	}
	if snapshotIncomplete(files) {
		writeJSONError(w, http.StatusConflict,
			"this snapshot is marked incomplete (a failed or unfinished run); refusing to download it")
		return
	}
	// Rendered BEFORE the first byte: the footer reads behind the decimal
	// casts can be slow (or hang on a bad store), and a stall belongs ahead
	// of the 200, where the client still sees an honest failure, not
	// mid-stream where curl saves a truncated archive. Bounded for the reason
	// resolveSnapshotRequest bounds its listings: this runs in the process
	// that is also capturing, the server sets no WriteTimeout, and DuckDB's
	// per-file httpfs fallback is O(tables) network reads — a hung store must
	// cost the reader their casts, never pin the handler.
	ts, _ := parseSnapshotAt(r.URL.Query().Get("at"))
	vctx, vcancel := context.WithTimeout(r.Context(), baselineListTimeout)
	viewsSQL := s.snapshotViewsRelative(vctx, ss, ts, files)
	vcancel()
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="dbtrail-backup-`+dirName+`.tar.gz"`)

	// Row data leaves the store from the FIRST byte, so the audit record must
	// not depend on the stream finishing: an aborted read is exactly the read
	// an auditor most wants to see (see recordConsoleAccess's own contract),
	// and gating on success would let a client fetch all but the gzip trailer
	// of every backup unaudited. Emission is unconditional once the handler
	// commits to streaming (every refusal returned above): a zero-byte abort
	// still records the attempt rather than betting on deflate buffering the
	// tar headers. Deferred so the abort panics land here too. The files
	// count is what was HANDED OVER, not the snapshot's inventory — an
	// auditor must never read "files: 40, bytes: 4" as forty delivered.
	var sent int64
	var sentFiles int
	completed := false
	defer func() {
		detail := map[string]string{
			"snapshot": dirName,
			"files":    strconv.Itoa(sentFiles),
			"bytes":    strconv.FormatInt(sent, 10),
		}
		if !completed {
			detail["aborted"] = "true"
		}
		recordConsoleAccess(r, "baseline.download", "", "", detail)
	}()

	abort := func(msg, file string, err error) {
		// A canceled request context is the client hanging up: expected, and
		// logging it as a storage fault would send an operator chasing S3
		// errors that were browser cancels.
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			slog.Info("snapshot download canceled by the client", "snapshot", dirName, "file", file, "bytes", sent)
		} else {
			slog.Warn(msg, "snapshot", dirName, "file", file, "error", err)
		}
		panic(http.ErrAbortHandler)
	}

	// A client that stops reading is dropped (#2678): the server has no
	// write timeout, on purpose, so without this a connection opened and
	// never read holds the handler, and the snapshot it pins, for as long
	// as the client likes. The deadline moves forward with every write that
	// goes through, so a slow client that keeps reading is never cut.
	stall := &stallWriter{w: w, rc: http.NewResponseController(w)}
	defer stall.done()
	gz := gzip.NewWriter(stall)
	tw := tar.NewWriter(gz)
	for _, f := range files {
		// The stored views file never rides along (#1583): its paths name
		// wherever the snapshot lives, which is the one place an unpacked
		// tarball is not. Its replacement is appended after the loop — always
		// skipped, even if rendering the replacement produced nothing, because
		// a file whose every path is wrong is worse than no file. A crashed
		// publish's staging leftover carries the same wrong-paths content and
		// is skipped by its prefix.
		if f.RelPath == dirName+"/"+views.SnapshotFileName ||
			strings.HasPrefix(path.Base(f.RelPath), views.SnapshotFileTempPrefix) {
			continue
		}
		hdr := &tar.Header{Name: f.RelPath, Mode: 0o644, Size: f.Size, ModTime: f.ModTime}
		if err := tw.WriteHeader(hdr); err != nil {
			abort("snapshot download aborted: tar header write failed", f.RelPath, err)
		}
		rc, err := ss.open(r.Context(), f.RelPath)
		if err != nil {
			abort("snapshot download aborted: file unreadable mid-stream", f.RelPath, err)
		}
		n, err := io.Copy(tw, rc)
		rc.Close()
		sent += n
		if err == nil && n == f.Size {
			sentFiles++
		}
		if err != nil {
			abort("snapshot download aborted mid-file", f.RelPath, err)
		}
		if n != f.Size {
			// A file that shrank between the listing and the copy: without
			// this check the mismatch only surfaces on the NEXT header write,
			// blaming the wrong file.
			abort("snapshot download aborted: file shorter than listed", f.RelPath,
				fmt.Errorf("read %d bytes, listing said %d", n, f.Size))
		}
	}
	if viewsSQL != "" {
		hdr := &tar.Header{
			Name: dirName + "/" + views.SnapshotFileName, Mode: 0o644,
			Size: int64(len(viewsSQL)), ModTime: time.Now().UTC(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			abort("snapshot download aborted: tar header write failed", hdr.Name, err)
		}
		n, err := io.Copy(tw, strings.NewReader(viewsSQL))
		sent += n
		if err != nil {
			abort("snapshot download aborted mid-file", hdr.Name, err)
		}
		sentFiles++
	}
	if err := tw.Close(); err != nil {
		abort("snapshot download: tar finalize failed", "", err)
	}
	if err := gz.Close(); err != nil {
		abort("snapshot download: gzip finalize failed", "", err)
	}
	completed = true
}

// tableDeltaNames groups a snapshot's delta file names by table, keyed
// "<tsdir>/<schema>/<table>", with the parser the chain reader filters by, so
// each table is handed exactly the names it would have kept out of its whole
// schema directory. Handing every table the whole directory made one listing
// parse tables × files names.
func tableDeltaNames(files []baselineSnapshotFile) map[string][]string {
	out := map[string][]string{}
	for _, f := range files {
		parts := strings.Split(f.RelPath, "/")
		if len(parts) != 3 {
			continue
		}
		if stem, _, _, ok := baseline.ParseTableDeltaName(parts[2]); ok {
			k := parts[0] + "/" + parts[1] + "/" + stem
			out[k] = append(out[k], parts[2])
		}
	}
	return out
}

// tableDescription is what describeTable found out about one table of a
// snapshot. The zero value is "could not find out", which the page shows as
// no verdict: a different answer from "the file records nothing".
type tableDescription struct {
	producedBy, from string
	read             baseline.SourceRead
	// lock is how the database was locked when the table's rows were read
	// (#1380), from the table file's own footer: the file under a chain of
	// table deltas is the one the rows not changed since still come from.
	// lockRead is false when that footer could not be read.
	lock     baseline.ReadConsistency
	lockRead bool
}

// describeTable reads one table's footers and derives how its rows reached
// this snapshot (#1545) and when they last came from a read of the source
// (#1570). dir is the table's schema directory and names its delta files,
// for the chain of table deltas beside it (#1638).
//
// With a chain, the table file alone describes nothing about THIS snapshot:
// it is carried forward on every refresh, changed or not, so on its own it
// reads "reused unchanged" for a table this very run changed. The newest
// pair is the run's footer when the run wrote one; the table file describes
// the table only when it is at least as new (the empty pair a full backup or
// a compaction starts a chain with shares the file's writer instant; a full
// backup's carries no keys of its own).
//
// Best-effort, and quiet about it: a footer that will not open, or a chain
// that does not form whole pairs, leaves the row with NO verdict rather than
// "unknown". The two are different answers, one is "we did not find out",
// the other is "the file carries no signal", and a listing that turned an
// unreadable file into a confident verdict would be the same class of
// mistake the audit reader was fixed for (ee#115).
func describeTable(path, dir string, names []string, snapshotAt time.Time) tableDescription {
	md, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		slog.Warn("console: could not read a snapshot table's footer for provenance",
			"path", path, "error", err)
		// NOT ProducedByUnknown: see above. An empty verdict renders as a dash.
		return tableDescription{}
	}
	lock := baseline.ReadConsistencyOf(md)
	chain, err := baseline.TableDeltaChainIn(dir, names, strings.TrimSuffix(filepath.Base(path), ".parquet"))
	if err != nil {
		slog.Warn("console: a snapshot table's delta files do not form a chain, so how it was made is not shown",
			"path", path, "error", err)
		return tableDescription{lock: lock, lockRead: true}
	}
	describing, last := md, (*baseline.DumpMetadata)(nil)
	if chain != nil {
		lm, err := baseline.ReadParquetMetadata(chain.LastFileUpserts())
		if err != nil {
			slog.Warn("console: could not read the newest delta of a snapshot table for provenance",
				"path", chain.LastFileUpserts(), "error", err)
			return tableDescription{lock: lock, lockRead: true}
		}
		last = &lm
		if lm.SnapshotTimestamp.IsZero() {
			// Every writer stamps a pair's instant, so a pair without one is
			// damaged, and which file is newer cannot be told. Describing the
			// table from its file alone would call it reused unchanged, the
			// very answer this function exists to stop giving: no verdict.
			// When the rows were last read is still the file's to say.
			slog.Warn("console: the newest delta of a snapshot table records no writer instant, so how the table was made is not shown",
				"path", chain.LastFileUpserts())
			return tableDescription{read: baseline.ChainSourceRead(md, last), lock: lock, lockRead: true}
		}
		if lm.SnapshotTimestamp.After(md.SnapshotTimestamp) {
			describing = lm
		}
	}
	p := baseline.ProvenanceOf(snapshotAt, describing)
	d := tableDescription{producedBy: p.ProducedBy, read: baseline.ChainSourceRead(md, last), lock: lock, lockRead: true}
	if !p.From.IsZero() {
		d.from = p.From.UTC().Format(consoleTSFormat)
	}
	return d
}

// snapshotSourceReads folds the per-table source reads of one snapshot into
// its line on the page: the oldest read, the most folds since one, how many
// tables could not be dated, and how many were dated with no count of folds.
//
// The last one is its own counter because a maximum over the tables that DO
// record a count says nothing about the ones that do not: after an upgrade a
// table whose chain predates the keys keeps an unknown count until its next
// full backup, and "updated once since" beside it would be the
// fresher-than-true answer this feature exists to rule out.
type snapshotSourceReads struct {
	oldest    time.Time
	maxFolds  int
	anyFolds  bool
	missing   int
	uncounted int
}

func (r *snapshotSourceReads) add(read baseline.SourceRead) {
	if !read.Known() {
		r.missing++
		return
	}
	if r.oldest.IsZero() || read.At.Before(r.oldest) {
		r.oldest = read.At
	}
	if read.Folds < 0 {
		r.uncounted++
		return
	}
	if !r.anyFolds || read.Folds > r.maxFolds {
		r.maxFolds, r.anyFolds = read.Folds, true
	}
}

func (r *snapshotSourceReads) fill(resp *baselineFilesResponse, snapshotAt time.Time) {
	resp.SourceReadMissing = r.missing
	if r.oldest.IsZero() {
		return
	}
	resp.SourceReadUncounted = r.uncounted
	resp.SourceReadAt = r.oldest.UTC().Format(consoleTSFormat)
	if age := snapshotAt.Sub(r.oldest); age > 0 {
		resp.SourceReadAgeSeconds = age.Seconds()
	}
	if r.anyFolds && r.uncounted == 0 {
		n := r.maxFolds
		resp.MaxFoldsSinceRead = &n
	}
}

// How a snapshot was locked when the database was read (#1380).
//
// Every table file records it in its footer (baseline.MetaKeyLockMode) and a
// snapshot updated from the recorded changes inherits it, so the answer for a
// snapshot is the worst of its tables: one table read with no locks makes the
// snapshot torn. Three rules, the same on the row and in the detail:
//
//   - No record is "unknown", never "consistent": every snapshot written
//     before the record existed has none.
//   - A table that was not looked at is not spoken for. Over S3 a footer is
//     one request per table, which the listing does not spend, so a snapshot
//     with a table only there has no answer, unless a table that WAS looked
//     at is torn: that is true of the snapshot whatever the others say.
//   - A footer that cannot be read counts as unknown.

// snapshotLocks folds the per-table answers of one snapshot.
type snapshotLocks struct {
	looked    int // tables whose footer was asked for
	torn      int
	unknown   int // no record, or a footer that could not be read
	notLooked int // tables nobody asked about (S3)
}

func (l *snapshotLocks) add(c baseline.ReadConsistency, read bool) {
	l.looked++
	switch {
	case !read:
		l.unknown++
	case c == baseline.ReadTorn:
		l.torn++
	case c != baseline.ReadConsistent:
		l.unknown++
	}
}

// verdict is the snapshot's word: torn | unknown | consistent, or "" when it
// cannot be said (no table looked at, or some not looked at and none torn).
func (l snapshotLocks) verdict() string {
	switch {
	case l.torn > 0:
		return baseline.ReadTorn.String()
	case l.looked == 0 || l.notLooked > 0:
		return ""
	case l.unknown > 0:
		return baseline.ReadUnknown.String()
	}
	return baseline.ReadConsistent.String()
}

// lockMemo remembers the lock record of local table files, so the listing
// does not open every footer of every snapshot on every load. A published
// table file is never modified, and the entry is keyed on the file's size and
// modification time as well as its path, so a different file at the same path
// is read again.
type lockMemo struct {
	mu sync.Mutex
	m  map[string]lockMemoEntry
}

type lockMemoEntry struct {
	size int64
	mod  time.Time
	lock baseline.ReadConsistency
}

// lockMemoCap bounds the memo. Past it the memo starts over: the entries of
// pruned snapshots are never asked for again and would otherwise stay.
const lockMemoCap = 20000

var snapshotLockMemo = &lockMemo{m: map[string]lockMemoEntry{}}

// of answers for one local table file. read is false when the file or its
// footer could not be read, which is logged and never remembered.
func (m *lockMemo) of(path string) (lock baseline.ReadConsistency, read bool) {
	fi, err := os.Stat(path)
	if err != nil {
		slog.Warn("console: a snapshot table file cannot be read, so how it was locked is not known",
			"path", path, "error", err)
		return baseline.ReadUnknown, false
	}
	m.mu.Lock()
	e, ok := m.m[path]
	m.mu.Unlock()
	if ok && e.size == fi.Size() && e.mod.Equal(fi.ModTime()) {
		return e.lock, true
	}
	md, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		slog.Warn("console: a snapshot table's footer cannot be read, so how it was locked is not known",
			"path", path, "error", err)
		return baseline.ReadUnknown, false
	}
	lock = baseline.ReadConsistencyOf(md)
	m.mu.Lock()
	if len(m.m) >= lockMemoCap {
		m.m = map[string]lockMemoEntry{}
	}
	m.m[path] = lockMemoEntry{size: fi.Size(), mod: fi.ModTime(), lock: lock}
	m.mu.Unlock()
	return lock, true
}
