package baseline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"

	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// Upload walks outputDir and uploads every file to the S3 URL, preserving the
// relative directory structure under the prefix. region is optional — if empty,
// the AWS SDK resolves it from AWS_REGION or ~/.aws/config. When retry is true,
// files that already exist in S3 are skipped (checked via HeadObject). Returns
// the number of files uploaded.
//
// The upload mirrors the local Run marker contract (#467) so a mid-upload death
// leaves a snapshot that S3 discovery treats as INCOMPLETE, not complete:
//
//  1. _INCOMPLETE FIRST, per snapshot dir (a zero-byte object — the local
//     _INCOMPLETE marker was already removed once Run succeeded, so there is no
//     local file to walk).
//  2. every data file.
//  3. _SUCCESS LAST. "_SUCCESS" can sort before sibling schema dirs depending
//     on the database name's first byte ('_' is 0x5F — before lowercase letters
//     but after digits and uppercase), so a single-pass lexical WalkDir could
//     publish it before all data is up. We defer it UNCONDITIONALLY, which keeps
//     the S3 snapshot un-marked-complete until its data is fully present.
//  4. best-effort _INCOMPLETE delete. The readers only take a snapshot for
//     incomplete when _INCOMPLETE is present AND _SUCCESS is absent, so
//     a leftover _INCOMPLETE next to a published _SUCCESS is harmless — a failed
//     delete never demotes a completed snapshot.
//
// Extracted from cmd/bintrail (#613) so the bintrail-console daemon can run the
// dump→convert→upload pipeline in-process, without a docker socket.
func Upload(ctx context.Context, outputDir, s3URL, region string, retry bool) (int, error) {
	return UploadWithCopies(ctx, outputDir, s3URL, region, retry, nil)
}

// UploadWithCopies is Upload for ONE snapshot directory some of whose files
// are not on disk (#2212): each copy is made inside S3, from its source
// object to the key the file would have had on disk, between the
// _INCOMPLETE marker and _SUCCESS like every data file. A copy that fails
// fails the upload, so _SUCCESS is never written over a snapshot missing a
// table. The copies are requested through the destination bucket's client,
// so they run in its region and with its credentials.
//
// With copies, outputDir must be the snapshot directory itself; the list is
// checked (validateRemoteCopies) before anything is sent.
func UploadWithCopies(ctx context.Context, outputDir, s3URL, region string, retry bool, copies []RemoteCopy) (int, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return 0, fmt.Errorf("invalid upload URL: %w", err)
	}

	client, err := storage.NewS3ClientForBucket(ctx, bucket, region)
	if err != nil {
		return 0, err
	}

	return uploadWithOpsCopies(ctx, outputDir, prefix, retry, newS3UploadOps(client, bucket), copies)
}

// newS3UploadOps routes the upload's S3 operations through an injectable seam
// so the ordering invariant can be unit-tested with a recording mock (#524
// review), and so a test can check every operation is wired.
func newS3UploadOps(client *s3.Client, bucket string) s3UploadOps {
	ops := s3UploadOps{
		putEmpty: func(ctx context.Context, key string) error { return storage.PutEmptyObject(ctx, client, bucket, key) },
		uploadFile: func(ctx context.Context, path, key string) error {
			return storage.UploadFile(ctx, client, path, bucket, key)
		},
		objectExists: func(ctx context.Context, key string) (bool, error) {
			return storage.S3ObjectExists(ctx, client, bucket, key)
		},
		deleteObject: func(ctx context.Context, key string) error { return storage.DeleteObject(ctx, client, bucket, key) },
		copyObject: func(ctx context.Context, src, key string) error {
			srcBucket, srcKey, err := storage.ParseS3URL(src)
			if err != nil {
				return err
			}
			return storage.CopyObject(ctx, client, srcBucket, srcKey, bucket, key)
		},
		putObject: func(ctx context.Context, key string, body []byte) error {
			return storage.PutSmallObject(ctx, client, bucket, key, body)
		},
		getObject: func(ctx context.Context, key string) ([]byte, bool, error) {
			return storage.GetSmallObject(ctx, client, bucket, key, 4096)
		},
	}
	ops.objectURL = func(key string) string { return "s3://" + bucket + "/" + key }
	return ops
}

// s3UploadOps abstracts the four S3 operations the baseline upload performs, so
// the crash-safe ordering invariant (_INCOMPLETE first → data files → _SUCCESS
// last → best-effort _INCOMPLETE delete) can be pinned by a recording mock
// without a live client (#524 review).
type s3UploadOps struct {
	putEmpty     func(ctx context.Context, key string) error
	uploadFile   func(ctx context.Context, path, key string) error
	objectExists func(ctx context.Context, key string) (bool, error)
	deleteObject func(ctx context.Context, key string) error
	// objectURL spells a key as the s3:// URL a READER of the destination
	// will use — the root the respelled views file (#1583) names its tables
	// under. Optional in the same sense the respeller hook is: nil means the
	// views file is skipped, never copied wrong.
	objectURL func(key string) string
	// putObject and getObject read and write the root's newest-snapshot
	// pointer (#2052). getObject reports found=false for a missing key and an
	// error for anything else. nil skips the pointer (tests of other steps).
	putObject func(ctx context.Context, key string, body []byte) error
	getObject func(ctx context.Context, key string) (body []byte, found bool, err error)
	// copyObject copies the s3:// object src to key inside S3 (#2212). Only
	// an upload with copies calls it, and one without it is refused.
	copyObject func(ctx context.Context, src, key string) error
}

// snapshotViewsRespeller regenerates a snapshot's views file against a
// different root spelling, for the upload below: the local file names the
// producing machine's absolute paths, and shipping those bytes to S3 would
// publish a file whose every path is wrong for every reader of the bucket. A
// hook for the same reason marker.go's writer is one — the generator lives in
// internal/views, which imports this package. ok=false means the directory
// holds nothing the generator can describe.
//
// extra lists the snapshot's files that are not on disk (#2212): their tables
// belong in the file all the same.
var snapshotViewsRespeller func(ctx context.Context, snapshotDir, root string, extra []RemoteCopy) (content string, ok bool, err error)

// SetSnapshotViewsRespeller arms the upload-time respell. Nil disarms (tests).
func SetSnapshotViewsRespeller(f func(ctx context.Context, snapshotDir, root string, extra []RemoteCopy) (string, bool, error)) {
	snapshotViewsRespeller = f
}

// SnapshotViewsRespellerArmed is the wiring probe, sibling of
// SnapshotViewsWriterArmed: arming rides the import graph, and a binary that
// silently stopped linking the generator would upload snapshots whose views
// file is skipped, with only a per-upload warning to say so.
func SnapshotViewsRespellerArmed() bool { return snapshotViewsRespeller != nil }

// uploadWithOps performs the crash-safe upload ordering against ops. See the
// Upload doc for the four-step contract it guarantees.
func uploadWithOps(ctx context.Context, outputDir, prefix string, retry bool, ops s3UploadOps) (int, error) {
	return uploadWithOpsCopies(ctx, outputDir, prefix, retry, ops, nil)
}

// uploadWithOpsCopies is uploadWithOps with the snapshot's files that are
// not on disk (UploadWithCopies).
func uploadWithOpsCopies(ctx context.Context, outputDir, prefix string, retry bool, ops s3UploadOps, copies []RemoteCopy) (int, error) {
	// skipExisting reports whether --retry finds key already in the bucket.
	skipExisting := func(ctx context.Context, key string) (bool, error) {
		if !retry {
			return false, nil
		}
		exists, err := ops.objectExists(ctx, key)
		if err != nil {
			return false, err
		}
		if exists {
			slog.Info("skipping existing S3 object (--retry)", "key", key)
		}
		return exists, nil
	}
	upload := func(ctx context.Context, path string) error {
		key, err := storage.BuildS3Key(outputDir, path, prefix)
		if err != nil {
			return err
		}
		if skip, err := skipExisting(ctx, key); err != nil || skip {
			return err
		}
		if err := ops.uploadFile(ctx, path, key); err != nil {
			return err
		}
		slog.Debug("uploaded", "file", path, "key", key)
		return nil
	}

	// Snapshot dirs to upload, identified by a local _SUCCESS marker — only
	// completed snapshots reach here post-Run. Each one's _INCOMPLETE marker is
	// keyed off the snapshot dir, NOT off a walked file (Run already removed it).
	snapDirs, err := snapshotDirsWithSuccess(outputDir)
	if err != nil {
		return 0, err
	}
	// Steps 1 and 4 are the only readers of snapDirs; the walk that uploads the
	// data and the deferred _SUCCESS are not gated on it. So an empty snapDirs
	// does not upload nothing — it uploads EVERYTHING except the crash-safety
	// bracket, and a remote snapshot carrying neither marker is complete by
	// default (#467). An upload interrupted at three tables of twelve would
	// then be discoverable and readable and wrong.
	//
	// Every caller reaches here right after a successful Run or fold, so a
	// completed snapshot is always present and this costs them nothing. It is
	// the assertion they were already relying on, now stated.
	if len(snapDirs) == 0 {
		return 0, fmt.Errorf("refusing to upload %q: no completed snapshot was found in it or under it, so the "+
			"%s marker cannot be written and an interrupted upload would read as a complete backup",
			outputDir, IncompleteMarker)
	}
	if len(copies) > 0 {
		// Copies are keyed below ONE snapshot directory, so the upload must
		// be of exactly that directory: under a root holding several, a copy
		// would land in the wrong prefix, or in none.
		if len(snapDirs) != 1 || filepath.Clean(snapDirs[0]) != filepath.Clean(outputDir) {
			return 0, fmt.Errorf("refusing to upload %q with %d files copied inside S3: copies need the upload to be of one snapshot directory", outputDir, len(copies))
		}
		if ops.copyObject == nil {
			return 0, fmt.Errorf("refusing to upload %q: %d of its files are to be copied inside S3 and this upload path cannot copy", outputDir, len(copies))
		}
		if err := validateRemoteCopies(outputDir, copies); err != nil {
			return 0, err
		}
	}
	incompleteKey := func(snapDir string) (string, error) {
		return storage.BuildS3Key(outputDir, filepath.Join(snapDir, IncompleteMarker), prefix)
	}

	// 1. Publish _INCOMPLETE FIRST so an interrupted upload reads as incomplete.
	for _, snapDir := range snapDirs {
		key, err := incompleteKey(snapDir)
		if err != nil {
			return 0, err
		}
		if err := ops.putEmpty(ctx, key); err != nil {
			return 0, err
		}
	}

	// 2 & 3. Upload data files; defer the _SUCCESS marker(s) to the very end.
	// The walk only COLLECTS the data files; they are sent afterwards, up to
	// uploadConcurrency at a time (#2181). A snapshot update writes about
	// three small files per table, and sent one by one at a round trip each
	// they were most of an update's time on an S3 destination. Large files
	// go after them, one at a time (uploadParallelMaxSize). The bracket is
	// untouched: every data file has FINISHED before any _SUCCESS is sent.
	var count int
	var successMarkers []string
	var jobs, alone []func(context.Context) (uploaded bool, err error)
	err = filepath.WalkDir(outputDir, func(path string, d fs.DirEntry, walkErr error) error {
		// The daemon's own work beside the snapshots (isDaemonWorkDir) is
		// not snapshot data: an upload of the whole root left it in the
		// bucket, where nothing reads it and no retention removes it.
		if walkErr == nil && d.IsDir() && isDaemonWorkDir(outputDir, path) {
			return filepath.SkipDir
		}
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		// The baselines root's `current` pointer (see pointer.go) is a symlink
		// to a directory. WalkDir does not follow symlinks, so it arrives here
		// with IsDir() false and would be handed to the file uploader, which
		// opens the path, follows it, and fails with "is a directory" — taking
		// the whole upload down. It is a local convenience that means nothing
		// in S3, so skip it by name.
		if isPointerArtifact(outputDir, path, d) || isPointerLock(outputDir, path) || isPruneArtifact(outputDir, path) {
			return nil
		}
		// Every OTHER non-regular entry is RESOLVED, not skipped. An operator
		// who symlinks one large table's Parquet onto another volume had it
		// uploaded correctly before the pointer existed, and must keep having
		// it: skipping silently would publish _SUCCESS over a snapshot missing
		// a table, and the loss would surface mid-recovery. A link to a
		// directory stays a refusal, as it always was, but now says so.
		if !d.Type().IsRegular() {
			info, serr := os.Stat(path) // follows the link
			if serr != nil {
				return fmt.Errorf("cannot resolve %s in the snapshot: %w "+
					"(refusing to publish this snapshot as complete while a file it holds is unreadable)", path, serr)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("%s in the snapshot resolves to a %s, not a file; "+
					"refusing to publish the snapshot as complete while it cannot be uploaded whole", path, info.Mode().Type())
			}
		}
		if d.Name() == SuccessMarker {
			successMarkers = append(successMarkers, path) // defer to the end
			return nil
		}
		// The snapshot's own views file (#1583) is never COPIED: its bodies
		// spell the producing machine's absolute paths, and publishing those
		// bytes would hand every reader of the bucket a file whose every path
		// is wrong. It is REGENERATED against the destination's own s3://
		// spelling — same generator, different root — and skipped with a
		// warning when the regenerator is not linked, because no file beats a
		// wrong one. Skips are warnings, not errors: the snapshot's DATA is
		// what _SUCCESS vouches for, and `bintrail views` can always produce
		// the file later.
		//
		// Gated by NAME plus a snapshot-shaped PARENT, not by the snapDirs
		// membership: that set holds only _SUCCESS-marked directories, and a
		// crash between the views publish and the _SUCCESS write leaves a
		// snapshot outside it whose views.sql would fall through to the plain
		// copy — the exact wrong-paths artifact this branch exists to stop.
		// Name-shaped fails closed; a views.sql at the baselines ROOT (an
		// operator's own `bintrail views --output`) has a non-timestamp parent
		// and still uploads verbatim, which is theirs to spell.
		if base := filepath.Base(filepath.Dir(path)); d.Name() == SnapshotViewsName {
			if _, isSnap := snapshotdir.ParseTime(base); isSnap {
				if len(copies) > 0 && filepath.Dir(path) == filepath.Clean(outputDir) {
					return nil // regenerated below, with the copied tables
				}
				// One at a time: the views generator reads the snapshot
				// through DuckDB, and nothing is gained by running several
				// (a sweep can carry one per snapshot) beside the data.
				alone = append(alone, func(ctx context.Context) (bool, error) {
					return uploadRespelledViews(ctx, path, outputDir, prefix, retry, ops, nil)
				})
				return nil
			}
		}
		// A crashed publish's staging leftover is not snapshot data; the
		// writer sweeps them on its next run, and the walk must not ship one
		// meanwhile (its content is the same wrong-paths artifact as above).
		if strings.HasPrefix(d.Name(), snapshotViewsTempPrefix) {
			return nil
		}
		job := func(ctx context.Context) (bool, error) {
			return true, upload(ctx, path)
		}
		if info, serr := os.Stat(path); serr != nil || info.Size() >= uploadParallelMaxSize {
			alone = append(alone, job)
		} else {
			jobs = append(jobs, job)
		}
		return nil
	})
	if err != nil {
		return count, err
	}
	// The copies are data files like any other (#2212): in the same pool,
	// inside the same bracket. The views file is regenerated whether or not
	// the fold left one on disk: with every table copied it left none, and
	// the copied tables are in the snapshot all the same.
	for _, c := range copies {
		key, err := storage.BuildS3Key(outputDir, filepath.Join(outputDir, filepath.FromSlash(c.Rel)), prefix)
		if err != nil {
			return count, err
		}
		jobs = append(jobs, func(ctx context.Context) (bool, error) {
			if skip, err := skipExisting(ctx, key); err != nil || skip {
				return false, err
			}
			// Once sent, a copy is the bucket's to finish: cutting the request
			// does not stop it. So none starts after another file failed, and
			// one that started is waited for, on a context the failure does not
			// cancel (bounded, so a hung request cannot hold the run forever):
			// the caller's cleanup of a failed upload must find every object
			// that will ever land, or a late copy would sit in a folder with
			// no marker, which discovery reads as complete (#467).
			if err := ctx.Err(); err != nil {
				return false, err
			}
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), copyTimeout)
			defer cancel()
			if err := ops.copyObject(cctx, c.Src, key); err != nil {
				if cctx.Err() != nil && errors.Is(err, context.DeadlineExceeded) {
					return false, &CopyMayStillLandError{Key: key, Err: err}
				}
				return false, fmt.Errorf("copy %s into the new snapshot inside S3: %w", c.Rel, err)
			}
			slog.Debug("copied inside S3", "src", c.Src, "key", key)
			return true, nil
		})
	}
	if len(copies) > 0 {
		viewsPath := filepath.Join(outputDir, SnapshotViewsName)
		alone = append(alone, func(ctx context.Context) (bool, error) {
			return uploadRespelledViews(ctx, viewsPath, outputDir, prefix, retry, ops, copies)
		})
	}
	sent, err := runUploadJobs(ctx, jobs, uploadConcurrency)
	count += sent
	if err != nil {
		return count, err
	}
	sent, err = runUploadJobs(ctx, alone, 1)
	count += sent
	if err != nil {
		return count, err
	}
	for _, path := range successMarkers {
		if err := upload(ctx, path); err != nil {
			return count, err
		}
		count++
	}

	// 4. Best-effort _INCOMPLETE cleanup — harmless to leave (see the func doc).
	for _, snapDir := range snapDirs {
		key, err := incompleteKey(snapDir)
		if err != nil {
			slog.Warn("could not build _INCOMPLETE marker key for cleanup", "snapshot", snapDir, "error", err)
			continue
		}
		if err := ops.deleteObject(ctx, key); err != nil {
			slog.Warn("could not remove S3 _INCOMPLETE marker after upload (harmless; _SUCCESS decides completeness)",
				"key", key, "error", err)
		}
	}

	// 5. Point the root at the newest snapshot just published (#2052). Last,
	// so it never names a snapshot whose _SUCCESS is not there yet.
	if err := publishNewestPointers(ctx, outputDir, prefix, snapDirs, ops); err != nil {
		return count, err
	}
	return count, nil
}

// uploadConcurrency is how many of a snapshot's data files are sent at once.
// A copy inside S3 (#2212) runs in the same pool; one over 5 GiB is a
// multipart copy, whose abort on failure runs on a context the cancellation
// does not reach (storage.CopyObject).
// Each is one PUT: the files sent this way are under uploadParallelMaxSize,
// below storage.UploadFileSinglePutMax (a test pins that), so a failure that
// cancels the others never leaves a half-done multipart upload behind. Files
// are read in place from disk, not buffered, so this bounds open files and
// connections rather than memory.
const uploadConcurrency = 8

// copyTimeout bounds one copy inside S3 once started (#2212), parts
// included. S3 copies at well over 100 MiB/s, so an hour covers objects of
// hundreds of GiB; past it the request is cut, the run fails, and a copy the
// bucket still finishes lands after the cleanup, which is what the bound
// trades for never hanging.
//
// The cost of waiting: a started copy of a very large object runs to its end
// (up to this bound) even after another file failed and the run is lost.
// That is time and S3 requests spent on a doomed run, never a correctness
// problem; the alternative is an object landing after the cleanup.
//
// A variable for tests.
var copyTimeout = time.Hour

// CopyMayStillLandError is a copy inside S3 that ran out of time
// (copyTimeout): the request was cut, and the bucket may still finish the
// copy afterwards. A caller cleaning up the failed upload must keep the
// snapshot folder's _INCOMPLETE marker, or that late object would sit in a
// folder with no marker, which discovery reads as complete (#467).
type CopyMayStillLandError struct {
	Key string // the destination key that may still appear
	Err error
}

func (e *CopyMayStillLandError) Error() string {
	return fmt.Sprintf("the copy inside S3 to %s did not finish in %s, and the bucket may still complete it: %v", e.Key, copyTimeout, e.Err)
}

func (e *CopyMayStillLandError) Unwrap() error { return e.Err }

// uploadParallelMaxSize is the size from which a file is sent on its own,
// after the small ones. A large file is not waiting on a round trip but on
// bandwidth, which the SDK already splits into concurrent parts; sending
// several at once would only take more of the network from the capture
// running in the same process, for no gain.
const uploadParallelMaxSize = 16 << 20

// runUploadJobs runs the upload jobs up to limit at a time and
// returns how many put an object in the bucket. The first failure cancels the
// rest, and jobs not yet started are not started: the snapshot stays marked
// incomplete either way, so sending more of it after a failure buys nothing.
func runUploadJobs(ctx context.Context, jobs []func(context.Context) (bool, error), limit int) (int, error) {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)
	var sent atomic.Int64
	for _, job := range jobs {
		if gctx.Err() != nil {
			break
		}
		g.Go(func() (err error) {
			// errgroup does not recover: a panic here would end the whole
			// process, and the process uploading is also the one capturing.
			// It becomes this upload's error instead, which withholds
			// _SUCCESS like any other failure.
			defer func() {
				if r := recover(); r != nil {
					slog.Error("snapshot upload panicked; the snapshot is left marked incomplete",
						"panic", r, "stack", string(debug.Stack()))
					err = fmt.Errorf("snapshot upload panicked: %v", r)
				}
			}()
			if err := gctx.Err(); err != nil {
				return err
			}
			uploaded, err := job(gctx)
			if err != nil {
				return err
			}
			if uploaded {
				sent.Add(1)
			}
			return nil
		})
	}
	err := g.Wait()
	if err == nil {
		// A cancellation of the caller's context that landed after the last
		// job started still means the upload did not finish as asked.
		err = ctx.Err()
	}
	if err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		// The bare cancellation names no file and no phase; a job's own
		// failure is already wrapped with its file and key.
		err = fmt.Errorf("snapshot upload stopped before every data file was sent: %w", err)
	}
	return int(sent.Load()), err
}

// snapshotViewsTempPrefix mirrors the views writer's staging-file prefix so
// the upload walk can skip leftovers of a crashed publish. A literal rather
// than an alias, for the same import-direction reason as SnapshotViewsName —
// pinned against drift by TestSnapshotViewsTempPrefixMatchesTheUploadSkip in
// the views package, which reads it through SnapshotViewsStagingPrefix.
const snapshotViewsTempPrefix = "." + SnapshotViewsName + ".tmp"

// SnapshotViewsStagingPrefix exposes the prefix the upload walk skips, for
// the views package's drift pin. Read-only; the walk is the consumer.
func SnapshotViewsStagingPrefix() string { return snapshotViewsTempPrefix }

// uploadRespelledViews publishes one snapshot's views file to S3 by
// REGENERATING it against the destination's own spelling. localPath is the
// snapshot's local views.sql; the key it lands under is the same one a plain
// copy would have taken, so retry's exists-check and the layout are unchanged.
// uploaded reports whether an object actually landed, so the caller's count
// stays a count of objects in the bucket, never of intentions.
//
// Every refusal here is a skip with a warning, never an error: the file is a
// convenience beside the data, and failing the upload over it would hold the
// _SUCCESS marker hostage to an artifact `bintrail views` can rebuild.
func uploadRespelledViews(ctx context.Context, localPath, outputDir, prefix string, retry bool, ops s3UploadOps, extra []RemoteCopy) (uploaded bool, err error) {
	key, err := storage.BuildS3Key(outputDir, localPath, prefix)
	if err != nil {
		return false, err
	}
	if retry {
		exists, err := ops.objectExists(ctx, key)
		if err != nil {
			return false, err
		}
		if exists {
			slog.Info("skipping existing S3 object (--retry)", "key", key)
			return false, nil
		}
	}
	// Two unarmed shapes, named apart: one is an import-graph fact about this
	// binary, the other a caller that built its ops without objectURL. An
	// operator sent to audit the wrong one chases nothing.
	if snapshotViewsRespeller == nil {
		slog.Warn("skipping the snapshot's views file: no generator is linked into this binary "+
			"to respell it for S3, and the local copy names this machine's paths "+
			"(regenerate with `bintrail views` against the bucket)", "key", key)
		return false, nil
	}
	if ops.objectURL == nil {
		slog.Warn("skipping the snapshot's views file: this upload path carries no destination "+
			"URL spelling for the respell (regenerate with `bintrail views` against the bucket)", "key", key)
		return false, nil
	}
	dirKey := ""
	if i := strings.LastIndex(key, "/"); i >= 0 {
		dirKey = key[:i]
	}
	content, ok, genErr := snapshotViewsRespeller(ctx, filepath.Dir(localPath), ops.objectURL(dirKey), extra)
	switch {
	case genErr != nil:
		slog.Warn("skipping the snapshot's views file: could not regenerate it for S3 "+
			"(regenerate with `bintrail views` against the bucket)", "key", key, "error", genErr,
			// With copies there is no local file to fall back on (#2212).
			"tables_copied_in_s3", len(extra))
		return false, nil
	case !ok:
		// A decline, not a failure: the directory holds nothing the generator
		// describes. Reaching here with a real views.sql beside real tables
		// would be a generator bug, so it stays visible, but without a nil
		// error dressed as a cause.
		slog.Warn("skipping the snapshot's views file: the directory holds no describable snapshot "+
			"(regenerate with `bintrail views` against the bucket)", "key", key)
		return false, nil
	}
	tmp, err := os.CreateTemp("", "bintrail-views-s3-*.sql")
	if err != nil {
		slog.Warn("skipping the snapshot's views file: no temp file for the respelled copy", "key", key, "error", err)
		return false, nil
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		slog.Warn("skipping the snapshot's views file: could not stage the respelled copy", "key", key, "error", err)
		return false, nil
	}
	if err := tmp.Close(); err != nil {
		slog.Warn("skipping the snapshot's views file: could not stage the respelled copy", "key", key, "error", err)
		return false, nil
	}
	// From here the failure is the TRANSPORT, same as any data file: let it
	// fail the upload, or a flaky bucket would down-grade to a silent skip.
	if err := ops.uploadFile(ctx, tmp.Name(), key); err != nil {
		return false, err
	}
	slog.Debug("uploaded respelled views file", "key", key)
	return true, nil
}

// snapshotDirsWithSuccess returns the completed snapshot directories under
// outputDir. The baseline layout is <output>/<timestamp>/..., so only one
// level of children is scanned.
//
// outputDir may also BE a single snapshot directory, which is how the
// scheduled refresh uploads the one snapshot it just folded (#1539) instead of
// re-walking every snapshot the server ever wrote. Without this branch that
// call found no snapshot, so steps 1 and 4 silently did nothing: the data and
// _SUCCESS still landed in the right keys, and only the crash-safety marker
// went missing — an interrupted upload would then read as COMPLETE, because a
// snapshot with neither marker is complete-by-default (#467).
func snapshotDirsWithSuccess(outputDir string) ([]string, error) {
	switch _, err := os.Stat(filepath.Join(outputDir, SuccessMarker)); {
	case err == nil:
		return []string{outputDir}, nil
	case !errors.Is(err, fs.ErrNotExist):
		// Anything but "it is not there" is a real IO answer, and swallowing it
		// would send a single-snapshot upload down the children scan, which
		// finds no snapshot and reports none — the markerless upload the
		// caller's guard exists to refuse.
		return nil, fmt.Errorf("look for the %s marker in %q: %w", SuccessMarker, outputDir, err)
	}
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return nil, fmt.Errorf("read output directory %q: %w", outputDir, err)
	}
	var dirs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		snapDir := filepath.Join(outputDir, e.Name())
		if _, err := os.Stat(filepath.Join(snapDir, SuccessMarker)); err == nil {
			dirs = append(dirs, snapDir)
		}
	}
	return dirs, nil
}

// isPointerArtifact reports whether path is the baselines root's `current`
// pointer or a staging link left by an interrupted publish (see pointer.go).
// Both are symlinks directly under the root, both are local conveniences that
// mean nothing in S3, and neither is part of any snapshot.
//
// The staging half matters as much as the pointer: a crash between the symlink
// and the rename leaves a dangling `.current.tmp.<pid>.<nanos>`, and treating
// that as a broken snapshot file would make every later upload refuse until an
// operator found and deleted a hidden link they never created.
func isPointerArtifact(root, path string, d fs.DirEntry) bool {
	if d.Type()&fs.ModeSymlink == 0 || filepath.Dir(path) != filepath.Clean(root) {
		return false
	}
	name := d.Name()
	return name == CurrentLinkName || strings.HasPrefix(name, currentLinkTmp)
}

// isPointerLock reports whether path is the pointer's flock file. Unlike the
// pointer and its staging links it is a REGULAR file directly under the root,
// so the symlink test above cannot see it and it would otherwise be uploaded as
// snapshot data. It is a local mutex; it means nothing in S3.
func isPointerLock(root, path string) bool {
	return filepath.Dir(path) == filepath.Clean(root) && filepath.Base(path) == pointerLockName
}
