package reconstruct

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// The table lookup on an s3:// source (#1740). It used to open a DuckDB
// session and glob the whole prefix three times per call: the markers
// (prefix/*/_*), the table (prefix/*/<schema>/<table>.parquet) and every
// table file (prefix/*/*/*.parquet) for the staleness check. Each glob is a
// ListObjectsV2 walk with no delimiter, a request per 1,000 objects under the
// prefix whatever the table, so a Time-travel row lookup on a prefix of 556
// snapshots and 11,062 objects cost 33 requests, in sequence.
//
// Now it walks the inventory the listing keeps (#1679, #1847): the snapshot
// directories, newest first from `at` back, reading each one's contents
// until one holds the table. One directory read says all three things the
// globs said about that snapshot (is it complete, does it hold the table,
// does it hold any table at all), so the common lookup, a table that is in
// the newest snapshot, is the directory listing and one directory read, and
// both are kept: the next lookup on the source asks S3 nothing inside
// s3DirsTTL.
//
// What it answers is what the globs answered:
//
//   - a snapshot with _INCOMPLETE and no _SUCCESS is never used and never
//     counts as the newest one; a snapshot with no marker is complete (#467);
//   - the pick is the newest complete snapshot at or before `at` that holds
//     the table, never one after it;
//   - the StaleWarning names the newest complete snapshot at or before `at`
//     that holds a table file, when that is newer than the pick (#466).
//
// What changed is the failure side. The staleness glob was advisory: when
// it failed, the baseline already found was returned with no warning. There
// is no advisory read left. Every directory read here is of a snapshot NEWER
// than the pick, or of one read in the same round as it (a round after the
// first reads several at once). The pick is only the pick if the newer ones
// do not hold the table, so a read that fails, fails the lookup: an older
// snapshot is never chosen because a newer one could not be read. A failed
// read of a directory older than the pick fails the lookup too, which costs
// a retry and never a wrong answer.

// findWindow is how many directories the first round reads, and the factor
// each later round grows by. The first round is one directory because the
// table is nearly always in the newest snapshot; a table that is in none is
// found out in a handful of rounds, each read in parallel.
const (
	findWindow       = 1
	findWindowGrowth = 4
)

// errS3DirectoryVanished marks a snapshot directory the listing named and
// whose own listing then came back empty: removed since, or a transient
// empty answer. The lookup lists the directories again and retries once.
var errS3DirectoryVanished = errors.New("it was listed and came back empty")

func findBaselineS3(ctx context.Context, s3URL, schema, table string, at time.Time) (string, time.Time, StaleWarning, error) {
	path, snap, stale, err := findBaselineS3Once(ctx, s3URL, schema, table, at)
	if errors.Is(err, errS3DirectoryVanished) {
		// A directory pruned between the listing (kept up to s3DirsTTL) and
		// its read is gone from a fresh listing, and the lookup goes on
		// without it. One that is still listed and still empty is not an
		// answer, and the second error stands.
		InvalidateS3Inventory(s3URL)
		path, snap, stale, err = findBaselineS3Once(ctx, s3URL, schema, table, at)
	}
	if err != nil && !errors.Is(err, ErrNoBaseline) {
		// The error names what could not be read (the prefix, or one
		// snapshot folder) and what to do. It reaches the console, the MCP
		// tool and the shim's client as written, so it names no command and
		// no flag.
		advice := "Check that the store answers and that these credentials can list that location, then try again"
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// The caller gave up, or its time ran out: nothing says the
			// store or the credentials are at fault.
			advice = "The lookup was stopped before it finished. Try again"
		}
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("find the baseline of %s.%s in %q: %w; no older snapshot was used in its place. %s", schema, table, s3URL, err, advice)
	}
	return path, snap, stale, err
}

func findBaselineS3Once(ctx context.Context, s3URL, schema, table string, at time.Time) (string, time.Time, StaleWarning, error) {
	x, err := openS3SnapshotIndex(ctx, s3URL)
	if err != nil {
		return "", time.Time{}, StaleWarning{}, fmt.Errorf("could not list the snapshot folders under %q: %w", strings.TrimSuffix(s3URL, "/"), err)
	}
	var eligible []s3SnapshotDir // newest first, like x.dirs
	for _, d := range x.dirs {
		if !d.at.After(at) {
			eligible = append(eligible, d)
		}
	}
	// A name with a slash in it puts the file one level deeper than the
	// table files a directory read keeps, so it is asked for by its own key.
	deep := strings.Contains(schema, "/") || strings.Contains(table, "/")

	var newest time.Time // newest complete snapshot holding a table file
	for start, n := 0, findWindow; start < len(eligible); n *= findWindowGrowth {
		end := min(start+n, len(eligible))
		reads, err := x.readDirs(ctx, eligible[start:end])
		if err != nil {
			return "", time.Time{}, StaleWarning{}, err
		}
		for _, r := range reads {
			if !r.complete {
				continue // partial snapshot (#467)
			}
			if newest.IsZero() && len(r.files) > 0 {
				newest = r.dir.at
			}
			path, ok, err := x.tableIn(ctx, r, schema, table, deep)
			if err != nil {
				return "", time.Time{}, StaleWarning{}, err
			}
			if ok {
				return path, r.dir.at, staleFallback(schema, table, r.dir.at, newest), nil
			}
		}
		start = end
	}
	return "", time.Time{}, StaleWarning{}, fmt.Errorf("%w: %s.%s at or before %s in %q",
		ErrNoBaseline, schema, table, at.UTC().Format(time.RFC3339), s3URL)
}

// tableIn returns the path of the table's file in one complete snapshot.
func (x *s3SnapshotIndex) tableIn(ctx context.Context, r s3DirRead, schema, table string, deep bool) (string, bool, error) {
	if !deep {
		for _, f := range r.files {
			if f.Schema == schema && f.Table == table {
				return f.Path, true, nil
			}
		}
		return "", false, nil
	}
	key := r.dir.name + "/" + schema + "/" + table + ".parquet"
	infos, err := x.lister.ListInfoFrom(ctx, key, "")
	if err != nil {
		return "", false, fmt.Errorf("could not read the snapshot folder %s/%s: list S3 baseline table file: %w", x.prefix, r.dir.name, err)
	}
	for _, o := range infos {
		if o.Key == key {
			return x.prefix + "/" + key, true, nil
		}
	}
	return "", false, nil
}

// s3DirRead is one snapshot directory as the lookup sees it.
type s3DirRead struct {
	dir      s3SnapshotDir
	complete bool // false: _INCOMPLETE with no _SUCCESS (#467)
	files    []BaselineFile
}

// readDirs returns the contents of these directories, in the order given,
// reading the ones the inventory does not hold, in parallel. It is files
// without the flattening: the lookup needs to know WHICH directory is
// partial and which holds nothing, and a flat list of files says neither.
//
// Any read that fails, fails the call, and so does a directory that came
// back empty (errS3DirectoryVanished): the caller must not take "could not
// be read" for "does not hold the table".
func (x *s3SnapshotIndex) readDirs(ctx context.Context, dirs []s3SnapshotDir) ([]s3DirRead, error) {
	out := make([]s3DirRead, len(dirs))
	var missing []int
	x.inv.mu.Lock()
	for i, d := range dirs {
		out[i].dir = d
		if c, ok := x.inv.contents[d.name]; ok {
			out[i].complete, out[i].files = true, c.files
			continue
		}
		missing = append(missing, i)
	}
	x.inv.mu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}

	asked := make([]s3SnapshotDir, len(missing))
	read := make([]s3DirContents, len(missing))
	complete := make([]bool, len(missing))
	signed := make([]dirSignature, len(missing))
	for j, i := range missing {
		asked[j] = dirs[i]
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s3DirReadConcurrency)
	for j, d := range asked {
		g.Go(func() error {
			c, sig, ok, err := x.readDir(gctx, d)
			if err != nil {
				return fmt.Errorf("could not read the snapshot folder %s/%s: %w", x.prefix, d.name, err)
			}
			read[j], complete[j], signed[j] = c, ok, sig
			return nil
		})
	}
	err := g.Wait()
	// Kept on the error path too, as files does: a complete directory that
	// was read is immutable, and the retry has less to do.
	x.keep(asked, read, complete, signed)
	if err != nil {
		return nil, err
	}
	for j, i := range missing {
		// readDir reports a partial snapshot and an empty answer the same
		// way but for the signature: only a listing that returned something
		// was read for one.
		if !complete[j] && !signed[j].read {
			return nil, fmt.Errorf("could not read the snapshot folder %s/%s: %w", x.prefix, asked[j].name, errS3DirectoryVanished)
		}
		out[i].complete, out[i].files = complete[j], read[j].files
	}
	return out, nil
}
