package icebergexport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// The export compacts its own tables (#2719). Every delta run adds one data
// file and one equality-delete file per touched table, and some readers pay
// for each delete file on every read and cannot compact the table themselves:
// DuckDB has no rewrite, ClickHouse refuses to compact a table that holds
// `overwrite` snapshots, and S3 Tables' automatic compaction did not run on a
// small table. A compaction rewrites the table's live rows into new data files
// and drops every delete file, in ONE commit:
//
//   - The rows are read back through iceberg-go's scanner, with the deletes
//     applied, and written unchanged. Nothing about the source is consulted,
//     so a compaction cannot change what the table holds; it only changes how
//     many files hold it. That is also why this is not "reading Iceberg back
//     into the recovery path": the export reads its own output to maintain it.
//   - EVERY data file is rewritten, never a subset. An equality delete applies
//     only to data files with a lower sequence number, and the new files carry
//     the commit's own, higher one, so once no old data file survives every
//     delete file is dead and removing all of them is safe. A partial rewrite
//     would need to work out which deletes still apply to the files it kept.
//   - The commit is a `replace` snapshot (iceberg-go's RewriteFiles), the
//     operation that tells readers and other engines the data did not change.
//   - It changes no table property, so the cursor stays where the delta left
//     it.
//   - It goes through the same guard as every other commit: if anything else
//     committed to the table after the compaction read it, the commit is
//     refused and nothing is applied. A compaction that dies before the commit
//     leaves only unreferenced files; the table is as the delta left it.

// Snapshot summary properties of a compaction commit.
const (
	// summaryCompaction is a random id per compaction commit. It is how the
	// guard tells this commit from another writer's after a catalog error.
	summaryCompaction           = "bintrail.export.compaction"
	summaryCompactedDataFiles   = "bintrail.export.compacted_data_files"
	summaryCompactedDeleteFiles = "bintrail.export.compacted_delete_files"
	summaryCompactedRows        = "bintrail.export.compacted_rows"
)

// compactScanConcurrency is how many data files a compaction reads at once.
// iceberg-go's default is GOMAXPROCS, and each file read in flight holds its
// decoded rows; two keeps the memory of a large table's compaction close to
// what the rest of the export uses.
const compactScanConcurrency = 2

// Compaction is what a table's compaction did. Err is nil when it committed;
// otherwise the table is as the delta left it and the next run tries again.
type Compaction struct {
	DataFilesBefore   int
	DeleteFilesBefore int // all of them removed when the compaction committed
	DataFilesAfter    int
	Rows              int64  // rows written, the table's live rows
	SnapshotID        *int64 // the replace snapshot; nil unless it committed
	Err               error
}

// Compaction statuses, as Status reports them.
const (
	CompactionApplied    = "applied"
	CompactionNotApplied = "not_applied"
	// CompactionUnknown: the catalog answered the commit with an error that
	// could not be read back, so it may or may not have been applied. The
	// rows are the same either way.
	CompactionUnknown = "unknown"
)

// Status says whether the compaction is in the table.
func (c *Compaction) Status() string {
	switch {
	case c.Err == nil:
		return CompactionApplied
	case errors.Is(c.Err, errCommitUnknown):
		return CompactionUnknown
	default:
		return CompactionNotApplied
	}
}

// note is the outcome's detail suffix for this compaction.
func (c *Compaction) note() string {
	switch {
	case c.Err == nil:
		return fmt.Sprintf("; compacted %s and %s into %s (%d rows)",
			files(c.DataFilesBefore, "data"), files(c.DeleteFilesBefore, "delete"), files(c.DataFilesAfter, "data"), c.Rows)
	case errors.Is(c.Err, ErrConcurrentCommit):
		return "; compaction not applied: another writer committed to the table while it was being compacted; the rows are unchanged and the next run tries again"
	case errors.Is(c.Err, errCommitUnknown):
		return "; the catalog could not say whether the compaction was applied; the rows are the same either way, and the next run compacts again if the delete files are still there"
	default:
		return "; compaction failed, the table is as this run's delta left it and the next run tries again: " + c.Err.Error()
	}
}

func files(n int, kind string) string {
	if n == 1 {
		return "1 " + kind + " file"
	}
	return fmt.Sprintf("%d %s files", n, kind)
}

// liveFiles lists the data files and the delete files of the table's current
// snapshot, from its manifests (entries marked deleted are skipped).
func liveFiles(ctx context.Context, tbl *table.Table) (data, deletes []iceberg.DataFile, err error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil, nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, nil, err
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, nil, fmt.Errorf("read the manifest list: %w", err)
	}
	seen := map[string]bool{}
	for _, m := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		for e, err := range m.Entries(fs, true) {
			if err != nil {
				return nil, nil, fmt.Errorf("read manifest %s: %w", m.FilePath(), err)
			}
			df := e.DataFile()
			if seen[df.FilePath()] {
				continue
			}
			if df.ContentType() == iceberg.EntryContentData && df.Count() == 0 {
				// The scan plan leaves empty data files out, so they are
				// not rewritten; they hold no row a delete could apply to.
				continue
			}
			seen[df.FilePath()] = true
			if df.ContentType() == iceberg.EntryContentData {
				data = append(data, df)
			} else {
				deletes = append(deletes, df)
			}
		}
	}
	return data, deletes, nil
}

// compactTable compacts tbl when its current snapshot holds at least
// threshold delete files. It returns (tbl, nil) when there is nothing to do:
// a threshold of 0, a table without a snapshot, or fewer delete files than
// the threshold. Otherwise the Compaction says what happened, and the table
// returned is the committed one, or tbl unchanged when Err is set.
//
// beforeCommit, when set, runs after the new files are written and before the
// commit: the seam the tests use to interrupt a compaction there, or to land
// another writer's commit under it. Production passes nil.
func compactTable(ctx context.Context, tbl *table.Table, threshold int, beforeCommit func() error) (*table.Table, *Compaction) {
	if threshold <= 0 || tbl.CurrentSnapshot() == nil {
		return tbl, nil
	}
	data, deletes, err := liveFiles(ctx, tbl)
	if err != nil {
		return tbl, &Compaction{Err: fmt.Errorf("list the table's files: %w", err)}
	}
	if len(deletes) < threshold {
		return tbl, nil
	}
	c := &Compaction{DataFilesBefore: len(data), DeleteFilesBefore: len(deletes)}
	fail := func(err error) (*table.Table, *Compaction) {
		c.Err = err
		c.DataFilesAfter, c.Rows = 0, 0
		return tbl, c
	}

	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return fail(fmt.Errorf("plan the scan: %w", err))
	}
	// Removing every delete file is safe only because every data file is
	// rewritten. The scan plan and the manifests are two readings of the same
	// snapshot; if they ever disagree on the data files, the removal is no
	// longer proven safe, so nothing is done.
	if err := sameDataFiles(tasks, data); err != nil {
		return fail(err)
	}

	var oldFiles, newFiles []iceberg.DataFile
	if len(tasks) > 0 {
		var size int64
		for _, t := range tasks {
			size += t.File.FileSizeBytes()
		}
		gr, err := table.ExecuteCompactionGroup(ctx, tbl,
			table.CompactionTaskGroup{Tasks: tasks, TotalSizeBytes: size},
			table.WithCompactionScanConcurrency(compactScanConcurrency))
		if err != nil {
			return fail(fmt.Errorf("rewrite the live rows: %w", err))
		}
		oldFiles, newFiles = gr.OldDataFiles, gr.NewDataFiles
	}
	for _, f := range newFiles {
		c.Rows += f.Count()
	}
	c.DataFilesAfter = len(newFiles)

	id, err := compactionID()
	if err != nil {
		return discard(err)
	}
	tx := tbl.NewTransaction()
	rw := tx.NewRewrite(iceberg.Properties{
		summaryCompaction:           id,
		summaryCompactedDataFiles:   strconv.Itoa(len(oldFiles)),
		summaryCompactedDeleteFiles: strconv.Itoa(len(deletes)),
		summaryCompactedRows:        strconv.FormatInt(c.Rows, 10),
	})
	// A failure before the commit reached the catalog leaves the new files
	// unreferenced for good; they are a whole copy of the table, so they are
	// removed instead of piling up run after run. Once the request was sent,
	// they stay whatever the answer: a commit the catalog applies late would
	// otherwise point at files that no longer exist.
	discard := func(err error) (*table.Table, *Compaction) {
		removeFiles(ctx, tbl, newFiles)
		return fail(err)
	}
	// The delete files go in as they are listed in the manifests; the
	// position deletes ExecuteCompactionGroup reports as safe are among
	// them, and passing them twice would be refused as a duplicate.
	rw.Apply(oldFiles, newFiles, deletes)
	if err := rw.Commit(ctx); err != nil {
		return discard(fmt.Errorf("stage the compaction: %w", err))
	}
	if beforeCommit != nil {
		if err := beforeCommit(); err != nil {
			return discard(err)
		}
	}
	committed, err := tx.Commit(withCompactionBase(ctx, tbl, id))
	switch {
	case err == nil:
	case errors.Is(err, errNotSent):
		return discard(fmt.Errorf("commit the compaction: %w", err))
	case errors.Is(err, ErrConcurrentCommit):
		// The reload after a catalog error showed another writer's state
		// and not this commit. The request went out, so the files stay.
		return fail(fmt.Errorf("commit the compaction: %w", err))
	case errors.Is(err, errCommitUnknown):
		return fail(fmt.Errorf("commit the compaction: %w", err))
	default:
		// The catalog answered with an error the guard could not read
		// back (a timeout whose reload failed too): the commit may have
		// been applied, so it is reported as unknown and its files kept.
		return fail(fmt.Errorf("commit the compaction: %w: %v", errCommitUnknown, err))
	}
	c.SnapshotID = compactionSnapshot(committed.Metadata(), id)
	return committed, c
}

// compactionSnapshot finds the snapshot a compaction committed, by the id in
// its summary, walking back from the current one: another writer may have
// committed on top of it before the table was read back.
func compactionSnapshot(meta table.Metadata, id string) *int64 {
	for s := meta.CurrentSnapshot(); s != nil; {
		if s.Summary != nil && s.Summary.Properties[summaryCompaction] == id {
			v := s.SnapshotID
			return &v
		}
		if s.ParentSnapshotID == nil {
			return nil
		}
		s = meta.SnapshotByID(*s.ParentSnapshotID)
	}
	return nil
}

// removeFiles deletes files a compaction wrote and did not commit, best
// effort: one that cannot be removed is only logged, since nothing refers to
// it and the table is correct either way.
func removeFiles(ctx context.Context, tbl *table.Table, files []iceberg.DataFile) {
	if len(files) == 0 {
		return
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		slog.Warn("iceberg export: could not remove the files of a compaction that was not applied", "files", len(files), "err", err.Error())
		return
	}
	for _, f := range files {
		if err := fs.Remove(f.FilePath()); err != nil {
			slog.Warn("iceberg export: could not remove a file of a compaction that was not applied; nothing refers to it", "file", f.FilePath(), "err", err.Error())
		}
	}
}

// sameDataFiles checks that the scan plan names exactly the data files the
// manifests list.
func sameDataFiles(tasks []table.FileScanTask, data []iceberg.DataFile) error {
	want := make(map[string]bool, len(data))
	for _, df := range data {
		want[df.FilePath()] = true
	}
	got := make(map[string]bool, len(tasks))
	for _, t := range tasks {
		got[t.File.FilePath()] = true
	}
	if len(got) != len(tasks) || len(got) != len(want) {
		return fmt.Errorf("the scan plan names %d data files (%d tasks) and the manifests list %d; not compacting", len(got), len(tasks), len(want))
	}
	for p := range got {
		if !want[p] {
			return fmt.Errorf("the scan plan names data file %s, which the manifests do not list; not compacting", p)
		}
	}
	return nil
}

func compactionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("make a compaction id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// compact runs a table's compaction after its delta, reports a committed one
// to OnCommit, and logs one that was not applied.
func (d *deps) compact(ctx context.Context, schema, name string, tbl *table.Table, cur cursor) *Compaction {
	committed, c := compactTable(ctx, tbl, d.cfg.CompactAfter, nil)
	if c == nil {
		return nil
	}
	if c.Err != nil {
		slog.Warn("iceberg export: compaction not applied", "schema", schema, "table", name, "err", c.Err.Error())
		return c
	}
	slog.Info("iceberg export: compaction committed", "schema", schema, "table", name,
		"data_files_before", c.DataFilesBefore, "delete_files_before", c.DeleteFilesBefore,
		"data_files_after", c.DataFilesAfter, "rows", c.Rows)
	d.committed(Commit{Schema: schema, Table: name, Kind: CommitCompact, Rows: c.Rows, SnapshotID: c.SnapshotID,
		Cursor: cur.String(), Location: committed.Location()})
	return c
}
