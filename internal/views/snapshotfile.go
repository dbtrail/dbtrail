package views

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// The snapshot's own views file (#1583).
//
// A views.sql generated ON REQUEST describes data it does not sit beside, at a
// different moment from the data, and both of the ways the followed shapes
// fail come from that gap: an S3 root's follow-newest file resolves through a
// session variable that dies with the session, and the pointer file has to
// preflight for tables that left the snapshot after it was written. A file
// published WITH the snapshot — beside its _SUCCESS marker, by the same
// producer, at the same moment — has neither problem: it is pinned to its own
// prefix and it always names exactly the files sitting next to it.
//
// The mechanism is deliberately the existing generator with a different root
// spelling, wired through two hooks in internal/baseline (marker.go publishes
// the local copy inside WriteSuccessMarker; upload.go respells it for S3),
// because that package cannot import this one — the dependency already points
// the other way for the pointer and the markers.

// SnapshotFileName aliases the name internal/baseline owns, so neither
// package spells a second literal — same rule as DecimalColumn.
const SnapshotFileName = baseline.SnapshotViewsName

// snapshotViewsTimeout bounds the publish-time hook: the only real cost is
// one DuckDB session reading the tables' footers for their DECIMAL columns.
// WriteSuccessMarker carries no context, so the hook makes its own.
const snapshotViewsTimeout = 60 * time.Second

// producerVersion stamps the published file's header. Pushed in by each main
// package (cli.SetBuildVersion chains here; consoleapp.Main calls directly)
// because the hooks below run with no caller context to carry it. Empty
// renders as "(unknown version)"; never load-bearing.
var producerVersion string

// SetProducerVersion records the running binary's version for the
// snapshot-published views file.
func SetProducerVersion(v string) { producerVersion = v }

func init() {
	// Both hooks are panic-isolated. The generator's cost center is DuckDB
	// (cgo), the codebase's known panic source, and these run INSIDE the
	// publish path: unrecovered, a deterministic panic makes a COMPLETE
	// snapshot permanently unpublishable — WriteSuccessMarker dies before
	// _SUCCESS on every retry, or an upload aborts mid-walk with _INCOMPLETE
	// standing. Unlike the job-slot recovers the v0.70.0 notes warn about,
	// recover-warn-continue is safe here: the hook is synchronous and its
	// caller proceeds to publish; the artifact is the only loss.
	baseline.SetSnapshotViewsWriter(func(snapshotDir string) {
		defer recoverSnapshotViews("publish", snapshotDir)
		ctx, cancel := context.WithTimeout(context.Background(), snapshotViewsTimeout)
		defer cancel()
		if err := WriteSnapshotViews(ctx, snapshotDir); err != nil {
			// Warn, not Error: the snapshot is complete and recovery is
			// unaffected. Unlike the pointer's failure (silent staleness at
			// query time), a missing views.sql is VISIBLY missing, and
			// `bintrail views` rebuilds it.
			slog.Warn("could not publish the snapshot's views.sql (the snapshot is complete; "+
				"regenerate the file with `bintrail views`)", "dir", snapshotDir, "error", err)
		}
	})
	baseline.SetSnapshotViewsRespeller(func(ctx context.Context, snapshotDir, root string) (content string, ok bool, err error) {
		defer func() {
			if r := recover(); r != nil {
				recoverLogSnapshotViews("respell", snapshotDir, r)
				content, ok, err = "", false, fmt.Errorf("views generator panicked: %v", r)
			}
		}()
		return GenerateSnapshotViews(ctx, snapshotDir, root)
	})
}

// recoverSnapshotViews is the writer hook's deferred recover; split from the
// logging so the respeller's value-returning recover shares the message.
func recoverSnapshotViews(what, dir string) {
	if r := recover(); r != nil {
		recoverLogSnapshotViews(what, dir, r)
	}
}

func recoverLogSnapshotViews(what, dir string, r any) {
	slog.Error("snapshot views.sql "+what+" panicked; the snapshot itself is unaffected "+
		"(regenerate the file with `bintrail views`)",
		"dir", dir, "panic", r, "stack", string(debug.Stack()))
}

// WriteSnapshotViews publishes SnapshotFileName into snapshotDir, spelling
// every path absolute so the file works from any working directory on the
// machine that holds the snapshot. A directory that does not parse as a
// snapshot, or holds no <schema>/<table>.parquet layout, is silently not a
// candidate — `reconstruct --output-format mydumper` completes through the
// same marker path with a dump directory this file has no business in, the
// exact shape PublishCurrentPointer already declines.
func WriteSnapshotViews(ctx context.Context, snapshotDir string) error {
	sweepSnapshotViewsTemp(snapshotDir)
	abs, err := filepath.Abs(snapshotDir)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", snapshotDir, err)
	}
	content, ok, err := GenerateSnapshotViews(ctx, snapshotDir, filepath.ToSlash(abs))
	if err != nil || !ok {
		return err
	}
	// Atomic publish, same discipline as every store this project writes: a
	// crash mid-write must not leave half a script for someone to paste into
	// DuckDB. The temp file lives in the snapshot directory so the rename
	// cannot cross filesystems.
	tmp, err := os.CreateTemp(snapshotDir, snapshotViewsTempPrefix+"*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(snapshotDir, SnapshotFileName))
}

// GenerateSnapshotViews renders the pinned, snapshot-scoped views file for one
// COMPLETE local snapshot directory, spelling every table path under root —
// the absolute local directory for the copy that lives beside the data, an
// s3://bucket/prefix/<timestamp> URL for the copy the upload publishes.
// ok=false means the directory is not a snapshot this file can describe (its
// name does not parse, or it holds no tables); that is a decline, not an
// error.
func GenerateSnapshotViews(ctx context.Context, snapshotDir, root string) (string, bool, error) {
	ts, ok := snapshotdir.ParseTime(filepath.Base(snapshotDir))
	if !ok {
		return "", false, nil
	}
	tables, err := snapshotTables(snapshotDir)
	if err != nil {
		return "", false, err
	}
	if len(tables) == 0 {
		return "", false, nil
	}
	in := SnapshotScopedInput(ts, tables)
	// Decimals from the LOCAL files, before the respell: the map is keyed by
	// the path as passed, and the local files are the ones actually here.
	// Best-effort for the same reason the CLI's resolver is: types make the
	// file better, they are not what it is for.
	if decimals, err := baseline.DecimalColumnsFor(ctx, in.BaselinePaths()); err != nil {
		slog.Warn("snapshot views: could not read column types from the Parquet footers; "+
			"the state views will not cast decimal columns", "dir", snapshotDir, "error", err)
	} else {
		in.ApplyDecimals(decimals)
	}
	in.RespellBaselines(root)
	return Generate(in), true, nil
}

// SnapshotScopedInput is the Input shape every producer of an in-snapshot
// views file shares (#1583): pinned, scoped to one snapshot, no archive or
// live legs. Baselines arrive with their REAL paths (that is what the
// decimals resolvers key by); RespellBaselines is the second half.
func SnapshotScopedInput(ts time.Time, tables []BaselineTable) Input {
	return Input{
		GeneratedAt:      time.Now().UTC(),
		Version:          producerVersion,
		BaselineSnapshot: ts,
		Follow:           FollowNone,
		SnapshotScoped:   true,
		Baselines:        tables,
	}
}

// RespellBaselines rewrites every table path as root + "/" + Rel, and makes
// root the header's baseline source. ONE derivation for every producer of a
// respelled file — the local absolute copy, the S3 copy, the tarball's "./"
// relative copy — because two spellings of the same rewrite is exactly how
// the #1558 class starts. Call it AFTER the decimals are applied: the maps
// are keyed by the path as it was when the resolver ran.
func (in *Input) RespellBaselines(root string) {
	base := strings.TrimSuffix(root, "/")
	in.BaselineSource = base
	for i := range in.Baselines {
		in.Baselines[i].Path = base + "/" + in.Baselines[i].Rel
	}
}

// sweepSnapshotViewsTemp removes staging leftovers from a publish that died
// between CreateTemp and the rename. Without it the junk file — full of this
// machine's absolute paths — rides every later upload and tarball as snapshot
// data (the upload walk skips it by prefix as a belt; this is the cleanup).
// Best-effort and unlogged, like the pointer's own staging sweep.
func sweepSnapshotViewsTemp(snapshotDir string) {
	matches, err := filepath.Glob(filepath.Join(snapshotDir, snapshotViewsTempPrefix+"*"))
	if err != nil {
		return
	}
	for _, m := range matches {
		_ = os.Remove(m)
	}
}

// SnapshotFileTempPrefix names the atomic-publish staging files. Exported for
// the console's tar stream, which must not ship a crashed publish's leftover
// as snapshot data; internal/baseline cannot import this package (the arrow
// points the other way), so its upload walk carries the same literal, pinned
// against drift by TestSnapshotViewsTempPrefixMatchesTheUploadSkip.
const SnapshotFileTempPrefix = "." + SnapshotFileName + ".tmp"

const snapshotViewsTempPrefix = SnapshotFileTempPrefix

// snapshotTables walks a snapshot directory's <schema>/<table>.parquet layout.
// Path is the LOCAL path (the caller respells it); Rel is the tail below the
// snapshot directory, forward-slashed, the same shape the followed producers
// fill. Sorted so the rendered file is deterministic for a given snapshot.
func snapshotTables(snapshotDir string) ([]BaselineTable, error) {
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return nil, err
	}
	var out []BaselineTable
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(snapshotDir, e.Name()))
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(files))
		for _, f := range files {
			if !f.IsDir() {
				names = append(names, f.Name())
			}
		}
		// The listing is already in hand, so the chain (#1638, #1718) is read
		// off it. Half a pair is refused for the reason MarkTableDeltas gives.
		chains, err := baseline.MarkTableDeltaFiles(filepath.Join(snapshotDir, e.Name()), names)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".parquet") {
				continue
			}
			path := filepath.Join(snapshotDir, e.Name(), f.Name())
			c := chains[path]
			out = append(out, BaselineTable{
				Schema:      e.Name(),
				Table:       strings.TrimSuffix(f.Name(), ".parquet"),
				Path:        path,
				Rel:         e.Name() + "/" + f.Name(),
				Delta:       c != nil,
				DeltaLegacy: c != nil && c.Legacy,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Table < out[j].Table
	})
	return out, nil
}
