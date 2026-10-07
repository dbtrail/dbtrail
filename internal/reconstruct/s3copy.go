package reconstruct

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// Copying an unchanged table inside S3 (#2212).
//
// A server whose snapshots live only in S3 folds from the bucket and builds
// the new snapshot in a staging folder that is uploaded and deleted. Without
// this, every table of such an update was downloaded, rewritten and uploaded
// again, changed or not, which is as much traffic as a full read. This is the
// S3 analog of carryForward's hard link: a table with no events in the
// window is not written at all; the run lists its previous object(s) in the
// table's report (TableReport.S3Copies), and the upload copies them inside
// S3 into the new snapshot's prefix, between its _INCOMPLETE marker and its
// _SUCCESS, so a copy that fails leaves nothing published.
//
// # The same decision as the local carry, at the same places
//
// The two arms sit exactly where carryForward is called (step 5b, and the
// table-delta publish), behind every refusal the fold makes first, for the
// reasons carryForward gives: a TRUNCATE or DROP emits no row events and is
// refused by CheckDestructiveDDL before the change map exists; a known
// capture gap disqualifies the table (carryForwardEligible says why). With
// table deltas the chain's own end rules apply too (chainCompactReason): a
// quiet table must not ride its chain past the day cap or the index's floor.
//
// What the copy carries is what the local carry carries: the file as it was,
// footer included, so the table keeps its old binlog anchor in a newer
// snapshot. Readers find snapshots by their folder's name, never by that
// footer, which is what makes this correct (carryForward explains it).
//
// # Integrity without reading the bytes
//
// The local carry validates its source against the source's manifest before
// linking it. Here that would mean downloading the object, which is the cost
// this exists to avoid. Instead the copy takes the digest the SOURCE's
// manifest records into the new manifest (baselineintegrity.WriteManifestWith):
// a copy inside S3 is the same bytes, so if the source rotted, the copy's
// read fails on that digest exactly as the source's would. Nothing is
// certified afresh. A file the source manifest does not vouch for (no
// manifest, no entry, unreadable) is not copied: the table is rewritten,
// which hashes it, and from then on its copies carry a digest.
//
// # Only within one bucket
//
// The copy is sent with the destination bucket's client, which S3 lets read
// another bucket only when the same credentials can. A previous snapshot in
// another bucket than the destination (the S3 location was changed) is
// rewritten instead, and the log says why, rather than trading a working
// rewrite for a copy that may be refused at upload time and fail the run.

// s3FileDigest and listS3TableDelta are indirected for tests: both read the
// bucket.
var (
	s3FileDigest     = baselineintegrity.S3ManifestDigest
	listS3TableDelta = baseline.ListTableDelta
)

// s3CopyEligible reports whether a table may be published by a copy inside
// S3, before any per-file check. why is set only when the answer is a
// surprise an operator should read (another bucket): the other refusals are
// the ordinary reasons a table is folded.
func s3CopyEligible(dest, format, srcPath string, changes int, capGap *CaptureGap) (ok bool, why string) {
	if dest == "" || format != OutputFormatParquet || !strings.HasPrefix(srcPath, "s3://") || changes != 0 || capGap != nil {
		return false, ""
	}
	destBucket, _, err := storage.ParseS3URL(dest)
	if err != nil {
		return false, "the destination " + dest + " does not parse (" + err.Error() + ")"
	}
	srcBucket, _, err := storage.ParseS3URL(srcPath)
	if err != nil {
		return false, "the previous file " + srcPath + " does not parse (" + err.Error() + ")"
	}
	if srcBucket != destBucket {
		return false, fmt.Sprintf("the previous snapshot is in bucket %s and this one goes to bucket %s; a table is copied inside S3 only within one bucket",
			srcBucket, destBucket)
	}
	return true, ""
}

// planS3Copies lists srcs as copies into schema's folder of the new snapshot,
// each with the digest its own snapshot's manifest records. why says which
// file cannot be copied, and then nothing is.
func planS3Copies(ctx context.Context, schema string, srcs []string) (copies []baseline.RemoteCopy, why string) {
	for _, src := range srcs {
		crc, ok, err := s3FileDigest(ctx, src)
		switch {
		case err != nil:
			return nil, "the integrity manifest of the previous snapshot could not be read (" + err.Error() + ")"
		case !ok:
			return nil, "the integrity manifest of the previous snapshot does not list " + src
		}
		copies = append(copies, baseline.RemoteCopy{Rel: schema + "/" + src[strings.LastIndexByte(src, '/')+1:], Src: src, CRC32C: crc})
	}
	return copies, ""
}

// s3CopyUnchanged is step 5b's arm for an s3:// previous snapshot: it reports
// whether the table was published by a copy inside S3, with rep filled in.
// false means fold as usual; it never fails the table, since folding is
// always a correct answer.
func s3CopyUnchanged(ctx context.Context, cfg FullTableConfig, schema, table, baselinePath string, changes int, capGap *CaptureGap, rep *TableReport) bool {
	if !cfg.CarryForwardUnchanged {
		return false
	}
	ok, why := s3CopyEligible(cfg.S3CopyUnchangedTo, cfg.OutputFormat, baselinePath, changes, capGap)
	if !ok {
		logS3CopyDeclined(schema, table, why)
		return false
	}
	copies, why := planS3Copies(ctx, schema, []string{baselinePath})
	if copies == nil {
		logS3CopyDeclined(schema, table, why)
		return false
	}
	rep.S3Copies = copies
	// A reuse, never a disk saving: the bucket stores a full copy per
	// snapshot, so CarriedByLink stays false (#1578).
	rep.CarriedForward, rep.CarriedByLink = true, false
	rep.Files = []string{filepath.Join(schema, table+".parquet")}
	slog.Info("table copied inside S3 unchanged: no events in the window, so its previous file is copied into the new snapshot when it is uploaded, without being downloaded",
		"schema", schema, "table", table, "src", baselinePath,
		"fetch_ms", rep.FetchDuration.Milliseconds(), "fold_ms", rep.FoldDuration.Milliseconds())
	return true
}

// s3CopyUnchangedChain is the table-delta publish's arm for an s3://
// previous snapshot: the base and its chain are copied when the window held
// nothing and the chain may go on (chainCompactReason). Only the chain an S3
// update writes is copied, one pair numbered 0; any other shape (a longer
// chain uploaded from a local fold, a range or legacy pair, none at all) is
// rewritten once into that shape, which keeps the size rule, which needs the
// pairs' sizes, out of the question.
func s3CopyUnchangedChain(ctx context.Context, p tableDeltaPublish, hasAnchor bool, reserved string, rep *TableReport) bool {
	changes := len(p.fold.Changes)
	if p.fold.Spill != nil {
		changes++ // a window that went to disk is not empty
	}
	ok, why := s3CopyEligible(p.cfg.S3CopyUnchangedTo, p.cfg.OutputFormat, p.basePath, changes, p.capGap)
	if !ok {
		logS3CopyDeclined(p.schema, p.table, why)
		return false
	}
	chain, err := listS3TableDelta(ctx, p.basePath)
	switch {
	case err != nil:
		logS3CopyDeclined(p.schema, p.table, "the changes beside the previous file could not be listed ("+err.Error()+")")
		return false
	case chain == nil:
		logS3CopyDeclined(p.schema, p.table, "the previous file has no changes beside it, and this run starts them")
		return false
	case chain.Legacy || len(chain.Files) != 1 || chain.Files[0].Seq != 0 || chain.Files[0].Range():
		logS3CopyDeclined(p.schema, p.table, "the changes beside the previous file are not the one pair an S3 update writes; rewriting once")
		return false
	}
	start, err := chainStartAt(ctx, chain.LastFileUpserts())
	if err != nil {
		logS3CopyDeclined(p.schema, p.table, "where the changes beside the previous file start could not be read ("+err.Error()+")")
		return false
	}
	prev := &tableDelta{Chain: chain, Meta: baseline.DumpMetadata{DeltaSeq: 0, DeltaChainStart: start}}
	if reason := chainCompactReason(prev, 0, p.capGap, p.cfg.At, hasAnchor, reserved, p.cfg.ChainStartFloor, time.Time{}); reason != "" {
		logS3CopyDeclined(p.schema, p.table, reason)
		return false
	}
	f := chain.Files[0]
	copies, why := planS3Copies(ctx, p.schema, []string{p.basePath, f.Posdel, f.Upserts})
	if copies == nil {
		logS3CopyDeclined(p.schema, p.table, why)
		return false
	}
	rep.S3Copies, rep.S3CopyChainStart = copies, start
	rep.TableDelta, rep.DeltaPairWritten, rep.DeltaSeq = true, false, 0
	rep.DeltaChainFiles, rep.DeltaChainCopied = 1, 1
	rep.Files = []string{filepath.Join(p.schema, p.table+".parquet")}
	slog.Info("table copied inside S3 unchanged with the changes beside it: no events in the window, so its previous file and pair are copied into the new snapshot when it is uploaded, without being downloaded",
		"schema", p.schema, "table", p.table, "src", p.basePath, "chain_start", start.UTC().Format(time.RFC3339),
		"fetch_ms", rep.FetchDuration.Milliseconds(), "fold_ms", rep.FoldDuration.Milliseconds())
	return true
}

// logS3CopyDeclined says why an unchanged table is rewritten rather than
// copied, when there is something to say. Info: it is why this table's bytes
// cross the network on this run, which is the cost the copy avoids.
func logS3CopyDeclined(schema, table, why string) {
	if why == "" {
		return
	}
	slog.Info("table not copied inside S3, so it is downloaded and rewritten: "+why, "schema", schema, "table", table)
}

// carriedDigests is the manifest's view of a run's copies: each copied
// file's snapshot-relative path and the digest it carries.
func carriedDigests(reports []*TableReport) map[string]string {
	var out map[string]string
	for _, r := range reports {
		if r == nil {
			continue
		}
		for _, c := range r.S3Copies {
			if out == nil {
				out = map[string]string{}
			}
			out[c.Rel] = c.CRC32C
		}
	}
	return out
}

// S3Copies gathers every file a run's reports list as copied inside S3, for
// the upload (baseline.UploadWithCopies).
func S3Copies(reports []*TableReport) []baseline.RemoteCopy {
	var out []baseline.RemoteCopy
	for _, r := range reports {
		if r != nil {
			out = append(out, r.S3Copies...)
		}
	}
	return out
}

// S3CopyChainStarts is, per table copied inside S3, where its readers start
// their fetch: its chain's start (TableReport.S3CopyChainStart), or zero for
// a table copied without a chain, whose readers start at the snapshot
// itself. For SnapshotReadsFromWith.
func S3CopyChainStarts(reports []*TableReport) []time.Time {
	var out []time.Time
	for _, r := range reports {
		if r != nil && len(r.S3Copies) > 0 {
			out = append(out, r.S3CopyChainStart)
		}
	}
	return out
}
