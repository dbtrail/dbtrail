package baseline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

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
//  4. best-effort _INCOMPLETE delete. s3IncompleteSnapshots only flags a
//     snapshot incomplete when _INCOMPLETE is present AND _SUCCESS is absent, so
//     a leftover _INCOMPLETE next to a published _SUCCESS is harmless — a failed
//     delete never demotes a completed snapshot.
//
// Extracted from cmd/bintrail (#613) so the bintrail-console daemon can run the
// dump→convert→upload pipeline in-process, without a docker socket.
func Upload(ctx context.Context, outputDir, s3URL, region string, retry bool) (int, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return 0, fmt.Errorf("invalid upload URL: %w", err)
	}

	client, err := storage.NewS3ClientForBucket(ctx, bucket, region)
	if err != nil {
		return 0, err
	}

	// Route the four S3 operations through an injectable seam so the ordering
	// invariant can be unit-tested with a recording mock (#524 review).
	ops := s3UploadOps{
		putEmpty: func(ctx context.Context, key string) error { return storage.PutEmptyObject(ctx, client, bucket, key) },
		uploadFile: func(ctx context.Context, path, key string) error {
			return storage.UploadFile(ctx, client, path, bucket, key)
		},
		objectExists: func(ctx context.Context, key string) (bool, error) {
			return storage.S3ObjectExists(ctx, client, bucket, key)
		},
		deleteObject: func(ctx context.Context, key string) error { return storage.DeleteObject(ctx, client, bucket, key) },
	}
	ops.objectURL = func(key string) string { return "s3://" + bucket + "/" + key }
	return uploadWithOps(ctx, outputDir, prefix, retry, ops)
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
}

// snapshotViewsRespeller regenerates a snapshot's views file against a
// different root spelling, for the upload below: the local file names the
// producing machine's absolute paths, and shipping those bytes to S3 would
// publish a file whose every path is wrong for every reader of the bucket. A
// hook for the same reason marker.go's writer is one — the generator lives in
// internal/views, which imports this package. ok=false means the directory
// holds nothing the generator can describe.
var snapshotViewsRespeller func(ctx context.Context, snapshotDir, root string) (content string, ok bool, err error)

// SetSnapshotViewsRespeller arms the upload-time respell. Nil disarms (tests).
func SetSnapshotViewsRespeller(f func(ctx context.Context, snapshotDir, root string) (string, bool, error)) {
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
	upload := func(path string) error {
		key, err := storage.BuildS3Key(outputDir, path, prefix)
		if err != nil {
			return err
		}
		if retry {
			exists, err := ops.objectExists(ctx, key)
			if err != nil {
				return err
			}
			if exists {
				slog.Info("skipping existing S3 object (--retry)", "key", key)
				return nil
			}
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
	var count int
	var successMarkers []string
	err = filepath.WalkDir(outputDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		// The baselines root's `current` pointer (see pointer.go) is a symlink
		// to a directory. WalkDir does not follow symlinks, so it arrives here
		// with IsDir() false and would be handed to the file uploader, which
		// opens the path, follows it, and fails with "is a directory" — taking
		// the whole upload down. It is a local convenience that means nothing
		// in S3, so skip it by name.
		if isPointerArtifact(outputDir, path, d) || isPointerLock(outputDir, path) {
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
		// operator's own `bintrail views --out`) has a non-timestamp parent
		// and still uploads verbatim, which is theirs to spell.
		if base := filepath.Base(filepath.Dir(path)); d.Name() == SnapshotViewsName {
			if _, isSnap := snapshotdir.ParseTime(base); isSnap {
				uploaded, err := uploadRespelledViews(ctx, path, outputDir, prefix, retry, ops)
				if err != nil {
					return err
				}
				if uploaded {
					count++
				}
				return nil
			}
		}
		// A crashed publish's staging leftover is not snapshot data; the
		// writer sweeps them on its next run, and the walk must not ship one
		// meanwhile (its content is the same wrong-paths artifact as above).
		if strings.HasPrefix(d.Name(), snapshotViewsTempPrefix) {
			return nil
		}
		if err := upload(path); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return count, err
	}
	for _, path := range successMarkers {
		if err := upload(path); err != nil {
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
	return count, nil
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
func uploadRespelledViews(ctx context.Context, localPath, outputDir, prefix string, retry bool, ops s3UploadOps) (uploaded bool, err error) {
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
	content, ok, genErr := snapshotViewsRespeller(ctx, filepath.Dir(localPath), ops.objectURL(dirKey))
	switch {
	case genErr != nil:
		slog.Warn("skipping the snapshot's views file: could not regenerate it for S3 "+
			"(regenerate with `bintrail views` against the bucket)", "key", key, "error", genErr)
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
