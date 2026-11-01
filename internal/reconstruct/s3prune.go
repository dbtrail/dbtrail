package reconstruct

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// PruneMarker is written into an S3 snapshot folder before the S3 prune
// removes anything from it (#2596), and is the last object removed. A
// prune that stops midway leaves it, and the next run finishes that folder
// whatever its age: the age rule chose it when the mark was set.
const PruneMarker = "_PRUNING"

// PruneS3Options is one destination's S3 retention run.
type PruneS3Options struct {
	S3URL string
	// Retain is the age past which a snapshot may be removed. Zero is
	// refused: it is never "remove everything".
	Retain time.Duration
	Now    time.Time
	// MaxPerRun bounds the snapshots removed in one run (0 = 100), and
	// MaxObjects the objects in them (0 = 5,000; each is one request), so one
	// destination with a long backlog does not hold the cleanup of the
	// others for long. The rest go in the next runs, oldest first. A
	// snapshot is never split, and one larger than the whole budget goes
	// alone.
	MaxPerRun  int
	MaxObjects int
	// Proceed, when set, is asked before each snapshot is removed; false
	// stops the run there, as when a restore starts reading the destination.
	Proceed func() bool
}

// PruneS3Result is what one run did.
type PruneS3Result struct {
	// Pruned names the snapshot folders removed whole, oldest first.
	Pruned []string
	// Resumed counts the folders among Pruned a stopped run had begun.
	Resumed int
	// KeptNewest counts old snapshots kept because they hold the newest
	// complete copy of a table.
	KeptNewest int
	// KeptIncomplete counts old snapshots kept because they are incomplete:
	// an upload in progress, or one that failed. Never ours to remove.
	KeptIncomplete int
	// Deferred counts the snapshots past the age left for the next run,
	// and Stopped says why: "bound" (the run's snapshot or object budget),
	// "busy" (Proceed said a reader started) or "cancelled".
	Deferred int
	Stopped  string
	// Failed names the snapshot the run stopped on, with the error returned.
	Failed string
}

// s3PruneStore is what the S3 prune needs of the bucket: storage.S3Backend,
// or a fake in tests. Delete never carries a version: on a versioned bucket
// it leaves a delete marker, and the old versions stay until a bucket rule
// expires them.
type s3PruneStore interface {
	ListDirs(ctx context.Context, prefix string) ([]string, error)
	ListInfoFrom(ctx context.Context, prefix, startAfter string) ([]storage.ObjectInfo, error)
	Put(ctx context.Context, key string, r io.Reader) error
	Delete(ctx context.Context, key string) error
}

var newS3PruneStore = func(ctx context.Context, s3URL string) (s3PruneStore, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return nil, err
	}
	return storage.NewS3BackendUnprobed(ctx, storage.S3Config{Bucket: bucket, Prefix: prefix})
}

// PruneS3 removes the snapshots at an S3 destination that are older than
// opts.Retain (#2596), with the rules of the local prune:
//
//   - The newest complete snapshot holding each table is kept, however old,
//     so every table always keeps a copy in the bucket.
//   - An incomplete snapshot (an upload in progress, or one that failed) is
//     never touched, and covers no table.
//   - Only folders whose name is a snapshot time are read or touched:
//     archives and other servers' snapshots under the same prefix are not.
//   - Any doubt removes nothing: a listing that fails, or one snapshot
//     folder that cannot be read (it may be the newest copy of a table).
//
// A snapshot is removed so that no moment of the removal reads as complete:
// PruneMarker and _INCOMPLETE are written first, _SUCCESS is the first
// object removed, then the data, then _INCOMPLETE, then PruneMarker.
func PruneS3(ctx context.Context, opts PruneS3Options) (PruneS3Result, error) {
	store, err := newS3PruneStore(ctx, opts.S3URL)
	if err != nil {
		return PruneS3Result{}, fmt.Errorf("S3 snapshot prune: open %s: %w", opts.S3URL, err)
	}
	return pruneS3With(ctx, store, opts)
}

type s3PruneDir struct {
	name       string
	at         time.Time
	keys       []string
	tables     []string // schema + "/" + table: a name may hold a dot, never a slash
	writers    string   // who signed it, as one key; "" for an unsigned snapshot
	success    bool
	incomplete bool
	pruning    bool
}

// complete is the reader's rule (readDir): _INCOMPLETE with no _SUCCESS is
// not complete. A folder a stopped prune marked is complete while _SUCCESS
// is still there, since nothing has been removed from it yet.
func (d s3PruneDir) complete() bool {
	return !(d.incomplete && !d.success)
}

func pruneS3With(ctx context.Context, store s3PruneStore, opts PruneS3Options) (PruneS3Result, error) {
	var res PruneS3Result
	if opts.Retain <= 0 {
		return res, errors.New("S3 snapshot prune: no age to remove snapshots at")
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	limit := opts.MaxPerRun
	if limit <= 0 {
		limit = 100
	}
	budget := opts.MaxObjects
	if budget <= 0 {
		budget = 5000
	}
	cut := opts.Now.Add(-opts.Retain)
	names, err := store.ListDirs(ctx, "")
	if err != nil {
		return res, fmt.Errorf("S3 snapshot prune: list %s: %w", opts.S3URL, err)
	}
	var dirs []s3PruneDir
	for _, n := range names {
		if at, ok := parseDirTimestamp(n); ok {
			dirs = append(dirs, s3PruneDir{name: n, at: at})
		}
	}
	// Newest first: the keepers are found walking down.
	slices.SortFunc(dirs, func(a, b s3PruneDir) int { return b.at.Compare(a.at) })
	// When no folder is past the age, none is read. A folder a stopped run
	// marked was past it when marked; one the age was since raised over is
	// left as it is (readers skip it as incomplete) until time brings it past
	// the age again, or a run that reads every folder finishes it first.
	if len(dirs) == 0 || !dirs[len(dirs)-1].at.Before(cut) {
		return res, nil
	}
	for i := range dirs {
		infos, err := store.ListInfoFrom(ctx, dirs[i].name+"/", "")
		if err != nil {
			return res, fmt.Errorf("S3 snapshot prune: read snapshot %s at %s: %w", dirs[i].name, opts.S3URL, err)
		}
		var plain []string
		for _, o := range infos {
			parts := strings.Split(o.Key, "/")
			if parts[0] != dirs[i].name {
				continue
			}
			dirs[i].keys = append(dirs[i].keys, o.Key)
			if len(parts) == 2 {
				plain = append(plain, parts[1])
			}
			switch {
			case len(parts) == 2 && parts[1] == baseline.SuccessMarker:
				dirs[i].success = true
			case len(parts) == 2 && parts[1] == baseline.IncompleteMarker:
				dirs[i].incomplete = true
			case len(parts) == 2 && parts[1] == PruneMarker:
				dirs[i].pruning = true
			case len(parts) == 3 && strings.HasSuffix(parts[2], ".parquet"):
				dirs[i].tables = append(dirs[i].tables, parts[1]+"/"+strings.TrimSuffix(parts[2], ".parquet"))
			}
		}
		// The listing of directories said it holds objects, so a read with
		// none is a contradiction (a store whose listings lag): a doubt.
		if len(dirs[i].keys) == 0 {
			return res, fmt.Errorf("S3 snapshot prune: snapshot %s at %s listed as a folder and read as empty; nothing is removed this time", dirs[i].name, opts.S3URL)
		}
		// Coverage is per writer (#1762): two installations sharing a prefix
		// can hold tables of the same name, and one's newer copy is not a
		// copy of the other's. A name that looks like a signature and cannot
		// be read is a writer of its own, which covers nobody else.
		writers, unreadable := baseline.WritersFromNames(plain)
		dirs[i].writers = strings.Join(writers, ",")
		if len(unreadable) > 0 {
			dirs[i].writers += "|?" + dirs[i].name
		}
	}

	covered := map[string]bool{}
	var todo, unmark []s3PruneDir
	for _, d := range dirs {
		// Dated after now: readers look at or before now, so it is not the
		// present and covers nothing; and it is not removed.
		if d.at.After(opts.Now) {
			continue
		}
		if d.pruning && !d.success {
			// Removal begun: nothing of it reads as complete; finished.
			todo = append(todo, d)
			continue
		}
		if d.pruning {
			// Marked by a stopped run and still listing _SUCCESS. On a store
			// whose listings lag that can be a folder already going, so it
			// covers nothing in this run and is not removed. With no
			// _INCOMPLETE listed the run stopped before writing it, so
			// nothing was removed and the mark comes off; the next run
			// weighs it as any other. With _INCOMPLETE the mark stays: if
			// _SUCCESS is in fact gone, the next consistent listing finishes
			// it, where taking the mark off would leave a half-removed
			// folder every run reads as a failed upload.
			if !d.incomplete {
				unmark = append(unmark, d)
			}
			continue
		}
		if !d.complete() {
			if d.at.Before(cut) {
				res.KeptIncomplete++
			}
			continue
		}
		keeper := false
		for _, t := range d.tables {
			if k := d.writers + "\x00" + t; !covered[k] {
				covered[k], keeper = true, true
			}
		}
		if !d.at.Before(cut) || keeper {
			if keeper && d.at.Before(cut) {
				res.KeptNewest++
			}
			continue
		}
		todo = append(todo, d)
	}
	for _, d := range unmark {
		if err := store.Delete(ctx, d.name+"/"+PruneMarker); err != nil {
			return res, fmt.Errorf("S3 snapshot prune: take the mark off snapshot %s at %s: %w", d.name, opts.S3URL, err)
		}
	}
	// Oldest first: a run cut short by its bound, or by a failure, leaves
	// the bucket's history shortened from the far end only.
	slices.SortFunc(todo, func(a, b s3PruneDir) int { return a.at.Compare(b.at) })
	spent := 0
	for i, d := range todo {
		stop := ""
		switch {
		case ctx.Err() != nil:
			stop = "cancelled"
		case i >= limit || (i > 0 && spent+len(d.keys) > budget):
			stop = "bound"
		case opts.Proceed != nil && !opts.Proceed():
			stop = "busy"
		}
		if stop != "" {
			res.Deferred, res.Stopped = len(todo)-i, stop
			break
		}
		spent += len(d.keys)
		// Dropped before as well as after: a reader of this process that
		// holds the folder as complete must not answer from memory while it
		// goes.
		InvalidateS3Inventory(strings.TrimSuffix(opts.S3URL, "/") + "/" + d.name)
		err := removeS3Snapshot(ctx, store, d)
		// The inventory may hold this folder's contents as a complete
		// snapshot; dropped whether or not the removal finished, since a
		// half-removed one must not be answered from memory.
		InvalidateS3Inventory(strings.TrimSuffix(opts.S3URL, "/") + "/" + d.name)
		if err != nil {
			res.Failed = d.name
			return res, fmt.Errorf("S3 snapshot prune: remove snapshot %s at %s: %w", d.name, opts.S3URL, err)
		}
		res.Pruned = append(res.Pruned, d.name)
		if d.pruning {
			res.Resumed++
		}
	}
	InvalidateS3Inventory(opts.S3URL)
	return res, nil
}

// removeS3Snapshot removes one snapshot folder in the order PruneS3 states.
func removeS3Snapshot(ctx context.Context, store s3PruneStore, d s3PruneDir) error {
	prune := d.name + "/" + PruneMarker
	incomplete := d.name + "/" + baseline.IncompleteMarker
	success := d.name + "/" + baseline.SuccessMarker
	if !d.pruning {
		if err := store.Put(ctx, prune, bytes.NewReader(nil)); err != nil {
			return err
		}
	}
	if d.success || !d.incomplete {
		if err := store.Put(ctx, incomplete, bytes.NewReader(nil)); err != nil {
			return err
		}
	}
	if d.success {
		if err := store.Delete(ctx, success); err != nil {
			return err
		}
	}
	for _, k := range d.keys {
		if k == prune || k == incomplete || k == success {
			continue
		}
		if err := store.Delete(ctx, k); err != nil {
			return err
		}
	}
	if err := store.Delete(ctx, incomplete); err != nil {
		return err
	}
	return store.Delete(ctx, prune)
}
