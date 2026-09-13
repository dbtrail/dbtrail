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
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/views"
)

// This file serves the per-snapshot surfaces of the Backups page (#TBD):
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
	// Incomplete marks a snapshot carrying an _INCOMPLETE marker without a
	// _SUCCESS one (a failed or unfinished run). The listing excludes such
	// snapshots, but the detail stays honest if one is addressed directly.
	Incomplete bool `json:"incomplete,omitempty"`
	// Run is the recorded console run that produced this snapshot, present
	// only when THIS daemon performed it: the exact duration. Snapshots made
	// elsewhere (CLI, another daemon) have no record; the write span above is
	// their approximation.
	Run *baselineRunDTO `json:"run,omitempty"`
}

type baselineRunDTO struct {
	Kind    string  `json:"kind"`
	Seconds float64 `json:"seconds"`
	Tables  int     `json:"tables,omitempty"`
	Rows    int64   `json:"rows,omitempty"`
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
	return storage.NewS3Backend(ctx, storage.S3Config{Bucket: bucket, Prefix: prefix})
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
			"backups are unavailable while an access-control profile is active: baseline reads aren't redacted")
		return nil, "", nil
	}
	if b.baselineSrc == "" {
		writeJSONError(w, http.StatusNotFound, "no backup location is configured for this server")
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
	// "no backup found" for a row the same page just said is there — and the
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
				firstErr = fmt.Errorf("open backup storage: %w", err)
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
			firstErr = fmt.Errorf("list backup files: %w", err)
		}
	}
	if firstErr != nil {
		writeJSONError(w, http.StatusBadGateway, firstErr.Error())
		return nil, "", nil
	}
	writeJSONError(w, http.StatusNotFound, "no backup found at "+ts.Format(consoleTSFormat))
	return nil, "", nil
}

// handleBaselineFiles serves GET /api/baselines/files?at=…: one snapshot's
// tables with sizes, its total weight, and the span its files were written
// over.
//
// Deliberately NOT audited: like the listing, this is metadata (names, sizes,
// timestamps) — no row data leaves the store. The download below is audited.
func (s *Server) handleBaselineFiles(w http.ResponseWriter, r *http.Request) {
	src, _, files := s.resolveSnapshotRequest(w, r, "baseline-files")
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
		// Local only. Over S3 this is one object read per table, which is the
		// same latency the listing already declines to spend on footers, and a
		// row with no verdict reads as "not looked up" rather than as unknown.
		if src != nil && src.localRoot != "" {
			row.ProducedBy, row.From = tableProvenance(filepath.Join(src.localRoot, filepath.FromSlash(f.RelPath)), ts)
		}
		resp.Tables = append(resp.Tables, row)
	}
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
			run := &baselineRunDTO{Kind: rec.Kind, Tables: rec.Tables, Rows: rec.Rows}
			if st, err1 := time.Parse(time.RFC3339, rec.StartedAt); err1 == nil {
				if fin, err2 := time.Parse(time.RFC3339, rec.FinishedAt); err2 == nil {
					run.Seconds = fin.Sub(st).Seconds()
				}
			}
			resp.Run = run
		}
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
	var tables []views.BaselineTable
	for _, f := range files {
		parts := strings.Split(f.RelPath, "/")
		if len(parts) != 3 || !strings.HasSuffix(parts[2], ".parquet") {
			continue
		}
		tables = append(tables, views.BaselineTable{
			Schema: parts[1],
			Table:  strings.TrimSuffix(parts[2], ".parquet"),
			Path:   ss.realPath(f.RelPath),
			Rel:    parts[1] + "/" + parts[2],
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
func (s *Server) handleBaselineDownload(w http.ResponseWriter, r *http.Request) {
	ss, dirName, files := s.resolveSnapshotRequest(w, r, "baseline-download")
	if files == nil {
		return
	}
	if snapshotIncomplete(files) {
		writeJSONError(w, http.StatusConflict,
			"this backup is marked incomplete (a failed or unfinished run); refusing to download it")
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
			slog.Info("backup download canceled by the client", "snapshot", dirName, "file", file, "bytes", sent)
		} else {
			slog.Warn(msg, "snapshot", dirName, "file", file, "error", err)
		}
		panic(http.ErrAbortHandler)
	}

	gz := gzip.NewWriter(w)
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
			abort("backup download aborted: tar header write failed", f.RelPath, err)
		}
		rc, err := ss.open(r.Context(), f.RelPath)
		if err != nil {
			abort("backup download aborted: file unreadable mid-stream", f.RelPath, err)
		}
		n, err := io.Copy(tw, rc)
		rc.Close()
		sent += n
		if err == nil && n == f.Size {
			sentFiles++
		}
		if err != nil {
			abort("backup download aborted mid-file", f.RelPath, err)
		}
		if n != f.Size {
			// A file that shrank between the listing and the copy: without
			// this check the mismatch only surfaces on the NEXT header write,
			// blaming the wrong file.
			abort("backup download aborted: file shorter than listed", f.RelPath,
				fmt.Errorf("read %d bytes, listing said %d", n, f.Size))
		}
	}
	if viewsSQL != "" {
		hdr := &tar.Header{
			Name: dirName + "/" + views.SnapshotFileName, Mode: 0o644,
			Size: int64(len(viewsSQL)), ModTime: time.Now().UTC(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			abort("backup download aborted: tar header write failed", hdr.Name, err)
		}
		n, err := io.Copy(tw, strings.NewReader(viewsSQL))
		sent += n
		if err != nil {
			abort("backup download aborted mid-file", hdr.Name, err)
		}
		sentFiles++
	}
	if err := tw.Close(); err != nil {
		abort("backup download: tar finalize failed", "", err)
	}
	if err := gz.Close(); err != nil {
		abort("backup download: gzip finalize failed", "", err)
	}
	completed = true
}

// tableProvenance reads one table's footer and derives how its rows reached
// this snapshot (#1545).
//
// Best-effort, and quiet about it: a footer that will not open leaves the row
// with NO verdict rather than "unknown". The two are different answers — one is
// "we did not find out", the other is "the file carries no signal" — and a
// listing that turned an unreadable file into a confident verdict would be the
// same class of mistake the audit reader was fixed for (ee#115).
func tableProvenance(path string, snapshotAt time.Time) (string, string) {
	md, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		slog.Warn("console: could not read a backup table's footer for provenance",
			"path", path, "error", err)
		// NOT ProducedByUnknown. "we could not find out" and "the file records
		// nothing" are different answers, and collapsing them is the ee#115
		// class this comment cites: the reader is handed a verdict nobody
		// checked. An empty verdict renders as a dash.
		return "", ""
	}
	p := baseline.ProvenanceOf(snapshotAt, md)
	from := ""
	if !p.From.IsZero() {
		from = p.From.UTC().Format(consoleTSFormat)
	}
	return p.ProducedBy, from
}
