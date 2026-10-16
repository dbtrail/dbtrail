// Package copymirror keeps a LOCAL mirror of the snapshots a server publishes
// to S3, so SQL on the copy can read them (#2302).
//
// Why a mirror and not S3 itself: the worker that runs a statement
// (internal/sqlsandbox) has no network, no credentials and a memory ceiling
// sized from DuckDB's own limit, and all three would have to give for it to
// read s3:// paths. A snapshot changes once per backup or update; statements
// arrive all the time. So the files are brought to local disk once per
// snapshot, by the parent, and the worker reads them as it reads any local
// copy.
//
// What a round does (Sync):
//
//   - lists the snapshot directories under the source root and picks the
//     newest COMPLETE one, by the readers' own rule: _INCOMPLETE without
//     _SUCCESS is not complete, anything else is (baseline.SnapshotComplete).
//     If the mirror already holds that one, or a newer one, the round brings
//     nothing;
//   - reads the snapshot's _MANIFEST, which records a CRC-32C for each table
//     file and each delta file;
//   - builds the snapshot in a directory whose name does not parse as a
//     snapshot (".<name>.downloading"), so no reader can find it half-written.
//     That matters because a directory with NO marker reads as complete
//     (#467): writing _SUCCESS last would not be enough on its own;
//   - for each file the manifest records, in this order: keeps the file an
//     interrupted round left there, links the file the newest mirrored
//     snapshot holds at the same path, or downloads it. The first two only
//     after hashing the bytes on disk and finding the manifest's digest, so
//     nothing is taken on its size or its name. Most tables do not change
//     between two snapshots, which is why most of a snapshot is bytes the
//     mirror already has;
//   - downloads every other file of the snapshot (the manifest does not cover
//     them, so nothing on disk can be shown to be them) and checks each
//     against its listed size;
//   - renames the directory to the snapshot's name and completes it through
//     baseline.WriteSuccessMarker, the one door every producer uses, which
//     moves the `current` pointer and, in a binary that links the views
//     generator, writes the local views.sql;
//   - removes the mirrored snapshots past Config.Keep, renaming each aside
//     before deleting it. Only a round that did not fail removes anything,
//     and what the source lists never decides it: a snapshot deleted from
//     the source leaves the mirror when newer ones have pushed it past Keep.
//
// Not taken from the source: a snapshot's views.sql, which the upload rewrote
// to name s3:// paths, and its markers, which are written here. The source
// root's _NEWEST pointer is not read.
//
// A snapshot with no manifest this build can use is mirrored whole, every
// file downloaded and checked by size only, in every round that brings it.
//
// The mirror root belongs to this package and to ONE source, named in
// SourceFile at the root. Sync refuses a directory that already holds
// snapshots and is not a mirror (ErrNotAMirror), and a mirror of another
// source (ErrOtherSource): it never mixes two roots' snapshots, and it never
// removes what it cannot show to be its own.
package copymirror

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// SourceFile is the file at a mirror's root that names the source it follows.
// Dot-prefixed and not a directory, so no reader of a snapshot root takes it
// for a snapshot.
const SourceFile = ".mirror-source"

const (
	downloadingSuffix = ".downloading"
	removingSuffix    = ".removing"
	// partSuffix marks a file still being written inside a downloading
	// directory.
	partSuffix = ".mirror-part"
	lockName   = ".mirror.lock"

	// maxManifestBytes bounds the manifest, which is read whole into memory
	// before anything else of the snapshot. It holds a line per file; this
	// is room for hundreds of thousands.
	maxManifestBytes = 64 << 20

	// DefaultKeep is how many mirrored snapshots stay: the newest, which
	// statements read, and the one before it, which a statement that started
	// just before the newest landed may still be reading.
	DefaultKeep = 2
	// DefaultStallTimeout is how long a request to the source may go without
	// answering, or a download without a byte arriving, before it is given
	// up. A connection that hangs must not hold the round, and with it every
	// later round, for good.
	DefaultStallTimeout = 2 * time.Minute
	// DefaultHeadroom is the free space a download must leave on the disk.
	// The mirror may share it with the index or the working folder, and a
	// read cache must never be what fills it.
	DefaultHeadroom = 1 << 30
)

// Source is the snapshot root in S3, keys relative to it. *storage.S3Backend
// built over the root's prefix is one.
type Source interface {
	// ListDirs names the directories directly under prefix.
	ListDirs(ctx context.Context, prefix string) ([]string, error)
	// ListInfo lists every object under prefix with its size.
	ListInfo(ctx context.Context, prefix string) ([]storage.ObjectInfo, error)
	// Get opens one object.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}

// Config is one mirror: a source root and the local directory that follows it.
type Config struct {
	// Dir is the mirror root, an absolute path. Created if missing, mode
	// 0700. It must be this mirror's alone.
	Dir string
	// Source is the snapshot root in S3.
	Source Source
	// SourceName names the source in SourceFile and in messages: its s3://
	// URL, never a credential. Required. One trailing slash is ignored, as
	// the storage layer ignores it.
	SourceName string
	// Keep is how many mirrored snapshots stay on disk. Zero or less is
	// DefaultKeep.
	Keep int
	// Headroom is the free space a download must leave. Zero is
	// DefaultHeadroom; negative is none.
	Headroom int64

	// StallTimeout is how long the source may stay silent. Zero is
	// DefaultStallTimeout.
	StallTimeout time.Duration
	// OnDownload, when set, is called once the round knows it is bringing a
	// snapshot and that it fits, before the first file: a download can take
	// long, and the round reports nothing else until it ends.
	OnDownload func(snapshot string, files int, bytes int64)

	// FreeBytes reports the free space on the disk holding dir. Nil asks the
	// operating system (on Linux and macOS; elsewhere it cannot be known).
	// ok=false means it cannot be known, and the check is skipped.
	FreeBytes func(dir string) (free uint64, ok bool)
	// Gone reports whether err from Source.Get means the object is not
	// there. Nil is baselineintegrity.S3ObjectAbsent.
	Gone func(error) bool
	// Now is the clock. Nil is time.Now.
	Now func() time.Time
}

// Result is what a round did. Every field is set on a failed round too, as
// far as the round got.
type Result struct {
	// Busy: another round holds this mirror, in this process or another.
	// This one did nothing but read Newest. (The lock is flock; a platform
	// without it never reports Busy.)
	Busy bool
	// SourceNewest is the newest complete snapshot the source listed, when
	// the round established one: the snapshot it set out to bring, or the
	// mirror's own newest when the source's newest is that one. "" otherwise:
	// the source lists none, its newest is older than the mirror's, it was
	// not asked (Busy), or the listing failed.
	SourceNewest string
	// Newest is the newest snapshot in the mirror when the round ended: what
	// a statement reads. "" when the mirror holds none. Older than
	// SourceNewest after a round that could not bring it; newer when the
	// source lost its newest.
	Newest string
	// Downloaded names the snapshot this round brought in, "" when it
	// brought none: the mirror was up to date, the round failed, or Busy.
	Downloaded string
	// Removed names the snapshots this round took out of the mirror. One
	// whose files could not all be deleted is named too, and has a note.
	Removed []string
	// How the files of Downloaded (or of the snapshot a failed round was
	// bringing) got there: over the network, by a link to the snapshot
	// before, or left by an interrupted round. Bytes are file sizes.
	FilesDownloaded, FilesLinked, FilesResumed int
	BytesDownloaded, BytesLinked, BytesResumed int64
	// Notes say what a round that did not fail still wants read: a manifest
	// it could not use, a leftover it could not delete.
	Notes []string
}

// ErrNoSpace: what the round has to download does not fit on the mirror's
// disk with the headroom left over. The first check runs before any file of
// the snapshot is fetched.
var ErrNoSpace = errors.New("not enough free disk for the local mirror of the snapshot")

// NoSpaceError carries the numbers behind ErrNoSpace.
type NoSpaceError struct {
	Dir                  string
	Snapshot             string
	Need, Free, Headroom int64
}

// Error says what does not fit, with the sizes first and in units a person
// reads: the sentence is shown on a page as it is, and the path is the part
// that can be cut.
func (e *NoSpaceError) Error() string {
	return fmt.Sprintf("%v: snapshot %s needs %s downloaded, the disk has %s free, and %s must stay free (mirror folder %s)",
		ErrNoSpace, e.Snapshot, sizeText(e.Need), sizeText(e.Free), sizeText(e.Headroom), e.Dir)
}

// sizeText writes a byte count in binary units, one decimal past bytes.
func sizeText(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 3; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %s", float64(n)/float64(div), []string{"KiB", "MiB", "GiB", "TiB"}[exp])
}

func (e *NoSpaceError) Unwrap() error { return ErrNoSpace }

// ErrSourceChanged: the snapshot is not whole in the source. An object its
// listing named is gone, or its manifest records a file the listing does not
// name. The next round lists again.
var ErrSourceChanged = errors.New("the snapshot is not whole in S3")

// ErrSourceSilent: a request to the source got no answer, or a download no
// byte, within the stall timeout. It does not wrap the cancellation that
// ended the request: nothing asked this round to stop, and a caller that
// tells a stopping daemon by context.Canceled must not take this for one.
var ErrSourceSilent = errors.New("the source went silent")

// ErrCorrupt: what the source returned is not what the snapshot's manifest
// or its listing describe. Nothing of it is kept under a name a later round
// would trust.
var ErrCorrupt = errors.New("a mirrored file does not match the snapshot it came from")

// ErrUnsafeName: the source lists a key that cannot be written below the
// snapshot directory as it is spelled. The snapshot is not mirrored.
var ErrUnsafeName = errors.New("the snapshot holds a file name the mirror will not write")

// ErrNotAMirror: the directory holds snapshots and no SourceFile, so it is
// some producer's snapshot root. Nothing in it was touched but the lock file.
var ErrNotAMirror = errors.New("the directory holds snapshots and is not a mirror")

// ErrOtherSource: the directory is the mirror of another source. Nothing in
// it was touched but the lock file; a mirror follows one source, and its
// directory is deleted by whoever changes that.
var ErrOtherSource = errors.New("the directory is the mirror of another source")

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// linkFile is os.Link; a test replaces it with a file system that cannot.
var linkFile = os.Link

// Sync runs one round. It never waits for another round on the same mirror
// (Result.Busy). A snapshot becomes visible only whole, and a round that
// fails removes none.
func Sync(ctx context.Context, cfg Config) (Result, error) {
	var res Result
	if cfg.Dir == "" || cfg.Source == nil || sourceName(cfg) == "" {
		return res, errors.New("copymirror: a mirror needs a directory, a source and the source's name")
	}
	if !filepath.IsAbs(cfg.Dir) {
		return res, fmt.Errorf("copymirror: the mirror directory %q must be an absolute path", cfg.Dir)
	}
	if cfg.Keep <= 0 {
		cfg.Keep = DefaultKeep
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return res, fmt.Errorf("create the mirror directory %s: %w", cfg.Dir, err)
	}
	unlock, busy, err := lockMirror(cfg.Dir)
	if err != nil {
		return res, err
	}
	m := &mirror{cfg: cfg, res: &res}
	if busy {
		// The other round is doing this one's work; what is readable
		// meanwhile is what is on disk.
		res.Busy = true
		held, err := m.held()
		res.Newest = newestOf(held)
		return res, err
	}
	defer unlock()

	err = m.round(ctx)
	held, heldErr := m.held()
	res.Newest = newestOf(held)
	return res, errors.Join(err, heldErr)
}

func sourceName(cfg Config) string { return strings.TrimSuffix(cfg.SourceName, "/") }

type mirror struct {
	cfg Config
	res *Result
}

func (m *mirror) note(format string, args ...any) {
	m.res.Notes = append(m.res.Notes, fmt.Sprintf(format, args...))
}

func (m *mirror) now() time.Time {
	if m.cfg.Now != nil {
		return m.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

func (m *mirror) stall() time.Duration {
	if m.cfg.StallTimeout > 0 {
		return m.cfg.StallTimeout
	}
	return DefaultStallTimeout
}

// get opens one object under a watch: the request, and then each read of
// the body, must make progress within the stall timeout or the returned
// context is cancelled, which ends both. The caller calls stop when done.
func (m *mirror) get(ctx context.Context, key string) (body io.ReadCloser, stop func(), err error) {
	gctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(m.stall(), cancel)
	stop = func() { timer.Stop(); cancel() }
	rc, err := m.cfg.Source.Get(gctx, key)
	if err != nil {
		// Asked before stop, which cancels the same context: afterwards
		// every failure would look like the watch's own.
		silent := gctx.Err() != nil && ctx.Err() == nil
		stop()
		if silent {
			err = fmt.Errorf("%w: no answer in %s (%v)", ErrSourceSilent, m.stall(), err)
		}
		return nil, func() {}, err
	}
	return &watchedBody{rc: rc, timer: timer, stall: m.stall(), gctx: gctx, ctx: ctx}, stop, nil
}

// watchedBody pushes the stall timer back on every read that brings bytes.
type watchedBody struct {
	rc        io.ReadCloser
	timer     *time.Timer
	stall     time.Duration
	gctx, ctx context.Context
}

func (w *watchedBody) Read(p []byte) (int, error) {
	n, err := w.rc.Read(p)
	if n > 0 {
		w.timer.Reset(w.stall)
	}
	if err != nil && err != io.EOF && w.gctx.Err() != nil && w.ctx.Err() == nil {
		err = fmt.Errorf("%w: no bytes for %s (%v)", ErrSourceSilent, w.stall, err)
	}
	return n, err
}

func (w *watchedBody) Close() error { return w.rc.Close() }

func (m *mirror) gone(err error) bool {
	if m.cfg.Gone != nil {
		return m.cfg.Gone(err)
	}
	return baselineintegrity.S3ObjectAbsent(err)
}

func (m *mirror) round(ctx context.Context) error {
	if err := m.claim(); err != nil {
		return err
	}
	if err := m.finishRenamed(); err != nil {
		return err
	}
	held, err := m.held()
	if err != nil {
		return err
	}
	target, objs, err := m.pick(ctx, held)
	if err != nil {
		return err
	}
	if err := m.sweep(target); err != nil {
		return err
	}
	if target != "" {
		if err := m.download(ctx, target, objs, held); err != nil {
			return err // a round that fails removes nothing
		}
	}
	return m.retain()
}

// snapshot is a directory in the mirror root whose name parses as a snapshot.
type snapshot struct {
	name string
	at   time.Time
}

// entries reads the mirror root. A failure is an error, never "nothing
// there": a round that could not see its snapshots would bring them all
// again and report none.
func (m *mirror) entries() ([]fs.DirEntry, error) {
	entries, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("read the mirror directory %s: %w", m.cfg.Dir, err)
	}
	return entries, nil
}

// snapshots lists the snapshot directories in the root, newest first, each
// with whether it carries _SUCCESS.
func (m *mirror) snapshots() (done, undone []snapshot, err error) {
	entries, err := m.entries()
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		at, ok := snapshotdir.ParseTime(e.Name())
		if !ok || !e.IsDir() {
			continue
		}
		s := snapshot{e.Name(), at}
		switch _, err := os.Stat(filepath.Join(m.cfg.Dir, e.Name(), baseline.SuccessMarker)); {
		case err == nil:
			done = append(done, s)
		case errors.Is(err, fs.ErrNotExist):
			undone = append(undone, s)
		default:
			return nil, nil, fmt.Errorf("look for the marker of mirrored snapshot %s: %w", e.Name(), err)
		}
	}
	sort.Slice(done, func(i, j int) bool { return done[i].at.After(done[j].at) })
	return done, undone, nil
}

// held lists the mirror's complete snapshots, newest first.
func (m *mirror) held() ([]snapshot, error) {
	done, _, err := m.snapshots()
	return done, err
}

func newestOf(s []snapshot) string {
	if len(s) == 0 {
		return ""
	}
	return s[0].name
}

// claim establishes that the directory is this source's mirror: it carries
// this source's name, or it holds no snapshot and takes the name now. Every
// later step treats each snapshot directory in the root as the mirror's own,
// and that is only true past this check.
func (m *mirror) claim() error {
	p := filepath.Join(m.cfg.Dir, SourceFile)
	want := sourceName(m.cfg)
	b, err := os.ReadFile(p)
	switch {
	case err == nil && len(bytes.TrimSpace(b)) == 0:
		// A name that was never written (older builds wrote the file in
		// place, and a full disk leaves it empty). The same as no file: the
		// snapshot check below still decides whether this can be a mirror.
	case err == nil:
		if got := strings.TrimSuffix(string(b), "\n"); got != want {
			return fmt.Errorf("%w: %s follows %s, and this round is for %s", ErrOtherSource, m.cfg.Dir, got, want)
		}
		return nil
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("read %s: %w", p, err)
	}
	done, undone, err := m.snapshots()
	if err != nil {
		return err
	}
	if n := len(done) + len(undone); n > 0 {
		return fmt.Errorf("%w: %s holds %d and no %s", ErrNotAMirror, m.cfg.Dir, n, SourceFile)
	}
	// Whole or not at all: a name half written would read as another
	// source's, and nothing would ever write it again.
	tmp := p + partSuffix
	if err := os.WriteFile(tmp, []byte(want+"\n"), 0o600); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write %s: %w", p, err)
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write %s: %w", p, err)
	}
	return nil
}

// finishRenamed completes a snapshot a round renamed into place and did not
// get to mark. Only a whole snapshot is ever renamed, so the marker is all
// that is missing; until it lands the directory carries _INCOMPLETE and no
// reader takes it. A failure here fails the round, every round: the name is
// taken, so nothing newer of that name can land, and saying so each time is
// the only signal there is.
func (m *mirror) finishRenamed() error {
	_, undone, err := m.snapshots()
	if err != nil {
		return err
	}
	for _, s := range undone {
		dir := filepath.Join(m.cfg.Dir, s.name)
		if err := baseline.WriteSuccessMarker(dir); err != nil {
			return fmt.Errorf("mirrored snapshot %s is downloaded but cannot be marked complete (fix what refuses the write, or delete %s): %w",
				s.name, dir, err)
		}
	}
	return nil
}

// pick lists the source and returns the snapshot to bring, with its objects
// (keys relative to the snapshot directory): the source's newest complete
// snapshot, when it is newer than everything the mirror holds. target is ""
// when there is nothing to bring.
//
// Only newer: a mirror that brought an older one would remove it again as
// past Keep, in every round, for as long as the source stayed behind it.
func (m *mirror) pick(ctx context.Context, held []snapshot) (target string, objs []storage.ObjectInfo, err error) {
	lctx, cancel := context.WithTimeout(ctx, m.stall())
	dirs, err := m.cfg.Source.ListDirs(lctx, "")
	cancel()
	if err != nil {
		return "", nil, fmt.Errorf("list the snapshots in %s: %w", sourceName(m.cfg), err)
	}
	now := m.now()
	// What the mirror's newest is measured by: its newest snapshot that is
	// not dated after the clock. One brought while the clock ran ahead must
	// not keep every real one out until its date comes.
	var have *snapshot
	for i := range held {
		if !held[i].at.After(now) {
			have = &held[i]
			break
		}
	}
	var snaps []snapshot
	for _, d := range dirs {
		at, ok := snapshotdir.ParseTime(d)
		if !ok || !plainName(d) {
			// Not a snapshot, or a name that parses as a time and is not
			// spelled as a directory of one is: nothing the source lists
			// becomes a path here on the parser's word alone.
			continue
		}
		if at.After(now) {
			// Dated in the future: baseline.PublishCurrentPointer never
			// gives it the pointer, so the mirror does not bring it.
			continue
		}
		snaps = append(snaps, snapshot{d, at})
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].at.After(snaps[j].at) })
	for _, s := range snaps {
		if have != nil && !s.at.After(have.at) {
			// The mirror has this one or a newer one, so there is nothing
			// to bring. It is known complete only when it is the mirror's.
			if s.name == have.name {
				m.res.SourceNewest = s.name
			}
			return "", nil, nil
		}
		// A listing is a request per thousand objects: several silences.
		lctx, cancel := context.WithTimeout(ctx, 10*m.stall())
		infos, err := m.cfg.Source.ListInfo(lctx, s.name+"/")
		cancel()
		if err != nil {
			return "", nil, fmt.Errorf("list snapshot %s in %s: %w", s.name, sourceName(m.cfg), err)
		}
		var success, incomplete bool
		rel := make([]storage.ObjectInfo, 0, len(infos))
		for _, o := range infos {
			r := strings.TrimPrefix(o.Key, s.name+"/")
			if r == "" || r == o.Key || (strings.HasSuffix(r, "/") && o.Size == 0) {
				// The directory's own placeholder, something not below it,
				// or the empty "folder" object a console makes: none is a file.
				continue
			}
			switch r {
			case baseline.SuccessMarker:
				success = true
			case baseline.IncompleteMarker:
				incomplete = true
			}
			rel = append(rel, storage.ObjectInfo{Key: r, Size: o.Size, LastModified: o.LastModified})
		}
		if len(rel) == 0 || (incomplete && !success) {
			continue // gone since the listing, or still being uploaded
		}
		m.res.SourceNewest = s.name
		return s.name, rel, nil
	}
	return "", nil, nil
}

// plainName reports whether a snapshot directory's name is made only of what
// a time is written with once its colons are replaced: digits, letters, and
// - + . (snapshotdir.Name writes none of the last two; the parser takes them).
func plainName(name string) bool {
	if name == "" || name[0] == '.' {
		return false
	}
	for _, c := range name {
		switch {
		case c >= '0' && c <= '9', c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c == '-', c == '+', c == '.':
		default:
			return false
		}
	}
	return true
}

// skipped reports the files of a snapshot the mirror does not take from the
// source: the markers, which it writes itself, and views.sql, which names
// s3:// paths there.
func skipped(rel string) bool {
	switch rel {
	case baseline.SuccessMarker, baseline.IncompleteMarker, baseline.SnapshotViewsName:
		return true
	}
	return false
}

// safeRel reports whether rel can be written below a directory exactly as it
// is spelled: forward-slashed segments, none empty, none a dot name that
// walks, nothing a file system reads as a separator or a terminator.
func safeRel(rel string) bool {
	if rel == "" || strings.ContainsAny(rel, "\\\x00") || strings.HasSuffix(rel, partSuffix) {
		return false
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return path.Clean(rel) == rel
}

type fileAction int

const (
	actDownload fileAction = iota
	actLink
	actResumed
)

type plannedFile struct {
	rel    string
	size   int64
	action fileAction
	from   string // actLink: the mirrored file to link
	digest string // the manifest's CRC-32C, "" when it records none for this file
}

// manifest fetches and reads the snapshot's manifest. raw is its bytes, nil
// when the snapshot has none; digests is nil when there is none this build
// can use, which is not a fault of the snapshot: the read paths serve such a
// snapshot unverified (baselineintegrity.ValidateLocalFile), and so does the
// mirror, with every file downloaded.
func (m *mirror) manifest(ctx context.Context, name string, size int64) (raw []byte, digests map[string]string, err error) {
	if size < 0 {
		return nil, nil, nil
	}
	if size > maxManifestBytes {
		return nil, nil, fmt.Errorf("%w: the manifest of snapshot %s is %d bytes, more than a manifest can be (%d)",
			ErrCorrupt, name, size, maxManifestBytes)
	}
	rel := baselineintegrity.ManifestName
	body, stop, err := m.get(ctx, name+"/"+rel)
	if err != nil {
		return nil, nil, m.getError(err, name, rel)
	}
	defer stop()
	defer body.Close()
	raw, err = io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return nil, nil, fmt.Errorf("download %s of snapshot %s: %w", rel, name, err)
	}
	if int64(len(raw)) != size {
		return nil, nil, fmt.Errorf("%w: %s of snapshot %s is not the %d bytes its listing says", ErrCorrupt, rel, name, size)
	}
	var man baselineintegrity.Manifest
	switch err := json.Unmarshal(raw, &man); {
	case err != nil:
		m.note("the manifest of snapshot %s cannot be read, so its files are downloaded whole and checked by size only: %v", name, err)
	case man.Version != 1 || man.Algo != "crc32c":
		// The one kind there is (baselineintegrity writes version 1, crc32c).
		m.note("the manifest of snapshot %s is of a kind this build does not know (version %d, %q), so its files are downloaded whole and checked by size only",
			name, man.Version, man.Algo)
	default:
		digests = man.Files
	}
	return raw, digests, nil
}

func (m *mirror) getError(err error, name, rel string) error {
	if m.gone(err) {
		return fmt.Errorf("%w: %s of snapshot %s is no longer in %s", ErrSourceChanged, rel, name, sourceName(m.cfg))
	}
	return fmt.Errorf("download %s of snapshot %s: %w", rel, name, err)
}

// hasBytes reports whether path is a regular file of size bytes whose
// CRC-32C is digest. It reads the whole file: that is the point.
func hasBytes(path string, size int64, digest string) bool {
	fi, err := os.Lstat(path)
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != size {
		return false
	}
	got, err := baselineintegrity.CRC32CFile(path)
	return err == nil && got == digest
}

// plan decides, per file, where its bytes come from. A file the manifest
// records is kept from an interrupted round, or linked from the newest
// mirrored snapshot, when the bytes on disk hash to the manifest's digest;
// anything else is downloaded.
//
// With verify false nothing is hashed: a file on disk of the right size is
// taken at the word of the manifests (this snapshot's, and for a link the
// mirrored snapshot's own). That is the plan's lower bound on what must be
// downloaded, cheap enough to run before the real one: a snapshot that does
// not fit even so is refused without reading gigabytes to find out, in every
// round for as long as the disk stays full.
func (m *mirror) plan(name string, objs []storage.ObjectInfo, digests map[string]string, held []snapshot, verify bool) ([]plannedFile, error) {
	tmp := m.tmpDir(name)
	var files []plannedFile
	listed := map[string]bool{}
	for _, o := range objs {
		if skipped(o.Key) || o.Key == baselineintegrity.ManifestName {
			continue
		}
		if !safeRel(o.Key) {
			return nil, fmt.Errorf("%w: %q in snapshot %s of %s", ErrUnsafeName, o.Key, name, sourceName(m.cfg))
		}
		listed[o.Key] = true
		files = append(files, plannedFile{rel: o.Key, size: o.Size, digest: digests[o.Key]})
	}
	// A file the manifest records and the listing does not name: the
	// snapshot is losing objects (an expiry rule, a removal under way) with
	// its _SUCCESS still there. Not whole, so not mirrored.
	for rel := range digests {
		if !listed[rel] {
			return nil, fmt.Errorf("%w: its manifest records %s, which %s no longer lists under snapshot %s",
				ErrSourceChanged, rel, sourceName(m.cfg), name)
		}
	}
	prior := ""
	var priorDigests map[string]string
	if len(held) > 0 {
		prior = filepath.Join(m.cfg.Dir, held[0].name)
		if !verify {
			priorDigests = localDigests(prior)
		}
	}
	for i := range files {
		f := &files[i]
		if f.digest == "" {
			continue // nothing can show a file on disk to be this one
		}
		left := filepath.Join(tmp, filepath.FromSlash(f.rel))
		if (verify && hasBytes(left, f.size, f.digest)) || (!verify && hasSize(left, f.size)) {
			f.action = actResumed
			continue
		}
		if prior == "" {
			continue
		}
		from := filepath.Join(prior, filepath.FromSlash(f.rel))
		if (verify && hasBytes(from, f.size, f.digest)) || (!verify && priorDigests[f.rel] == f.digest && hasSize(from, f.size)) {
			f.action, f.from = actLink, from
		}
	}
	return files, nil
}

// localDigests reads the digests a mirrored snapshot's own manifest records,
// nil when it has none this build can use.
func localDigests(dir string) map[string]string {
	man, ok, err := baselineintegrity.LoadManifest(dir)
	if err != nil || !ok || man == nil || man.Version != 1 || man.Algo != "crc32c" {
		return nil
	}
	return man.Files
}

func hasSize(path string, size int64) bool {
	fi, err := os.Lstat(path)
	return err == nil && fi.Mode().IsRegular() && fi.Size() == size
}

func downloadBytes(files []plannedFile) (n int64) {
	for _, f := range files {
		if f.action == actDownload {
			n += f.size
		}
	}
	return n
}

// room refuses need bytes that would not leave the headroom free.
func (m *mirror) room(name string, need int64) error {
	headroom := m.cfg.Headroom
	switch {
	case headroom == 0:
		headroom = DefaultHeadroom
	case headroom < 0:
		headroom = 0
	}
	ask := m.cfg.FreeBytes
	if ask == nil {
		ask = diskFree
	}
	free, ok := ask(m.cfg.Dir)
	if !ok {
		return nil
	}
	if free > uint64(1)<<62 {
		free = uint64(1) << 62
	}
	if need+headroom > int64(free) {
		return &NoSpaceError{Dir: m.cfg.Dir, Snapshot: name, Need: need, Free: int64(free), Headroom: headroom}
	}
	return nil
}

func (m *mirror) tmpDir(name string) string {
	return filepath.Join(m.cfg.Dir, "."+name+downloadingSuffix)
}

// download brings one snapshot into the mirror: manifest, plan, disk check,
// files, rename, marker.
func (m *mirror) download(ctx context.Context, name string, objs []storage.ObjectInfo, held []snapshot) error {
	var manifestSize int64 = -1
	for _, o := range objs {
		if o.Key == baselineintegrity.ManifestName {
			manifestSize = o.Size
		}
	}
	raw, digests, err := m.manifest(ctx, name, manifestSize)
	if err != nil {
		return err
	}
	// The cheap plan first: see plan.
	least, err := m.plan(name, objs, digests, held, false)
	if err != nil {
		return err
	}
	if err := m.room(name, downloadBytes(least)+int64(len(raw))); err != nil {
		return err
	}
	files, err := m.plan(name, objs, digests, held, true)
	if err != nil {
		return err
	}
	// pending is what the plan still has to download. It is what a file
	// that was to be linked, and has to be downloaded after all, competes
	// with for the free space.
	pending := downloadBytes(files)
	if err := m.room(name, pending+int64(len(raw))); err != nil {
		return err
	}
	if m.cfg.OnDownload != nil {
		m.cfg.OnDownload(name, len(files), pending)
	}

	tmp := m.tmpDir(name)
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	// In the directory from its first moment, so that if it were ever found
	// under a snapshot's name before it is whole, it reads as incomplete.
	if err := baseline.WriteIncompleteMarker(tmp); err != nil {
		return err
	}
	want := map[string]bool{baseline.IncompleteMarker: true}
	if raw != nil {
		if err := os.WriteFile(filepath.Join(tmp, baselineintegrity.ManifestName), raw, 0o600); err != nil {
			return fmt.Errorf("write the manifest of snapshot %s: %w", name, err)
		}
		want[baselineintegrity.ManifestName] = true
		m.res.FilesDownloaded++
		m.res.BytesDownloaded += int64(len(raw))
	}
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		want[f.rel] = true
		dst := filepath.Join(tmp, filepath.FromSlash(f.rel))
		switch f.action {
		case actResumed:
			m.res.FilesResumed++
			m.res.BytesResumed += f.size
			continue
		case actLink:
			if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
				return fmt.Errorf("create %s: %w", filepath.Dir(dst), err)
			}
			if err := replaceWithLink(f.from, dst); err == nil {
				m.res.FilesLinked++
				m.res.BytesLinked += f.size
				continue
			}
			// A file system that does not link: the bytes are still in the
			// source, and now they take space the plan did not count, on
			// top of everything the plan still has to download.
			if err := m.room(name, pending+f.size); err != nil {
				return err
			}
			pending += f.size
		}
		if err := m.fetch(ctx, name, f); err != nil {
			return err
		}
		pending -= f.size
		m.res.FilesDownloaded++
		m.res.BytesDownloaded += f.size
	}
	// Exactly the snapshot's files, each of its listed size: nothing an
	// interrupted round left that this one did not take, nothing missing.
	if err := m.settle(tmp, name, files, want); err != nil {
		return err
	}
	final := filepath.Join(m.cfg.Dir, name)
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("publish snapshot %s into the mirror: %w", name, err)
	}
	// Under its final name, so a views file written here names the paths
	// statements will read, and the pointer moves to a directory that exists.
	if err := baseline.WriteSuccessMarker(final); err != nil {
		return fmt.Errorf("mark mirrored snapshot %s complete: %w", name, err)
	}
	m.res.Downloaded = name
	return nil
}

// replaceWithLink links from at dst, over whatever an interrupted round left
// there.
func replaceWithLink(from, dst string) error {
	if err := os.Remove(dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return linkFile(from, dst)
}

// settle removes from the downloading directory every file that is not one of
// the snapshot's, and confirms each of the snapshot's is there at its size.
func (m *mirror) settle(tmp, name string, files []plannedFile, want map[string]bool) error {
	err := filepath.WalkDir(tmp, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(tmp, p)
		if err != nil {
			return err
		}
		if !want[filepath.ToSlash(rel)] {
			return os.Remove(p)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("check the download of snapshot %s: %w", name, err)
	}
	for _, f := range files {
		fi, err := os.Lstat(filepath.Join(tmp, filepath.FromSlash(f.rel)))
		if err != nil || !fi.Mode().IsRegular() || fi.Size() != f.size {
			return fmt.Errorf("%w: %s of snapshot %s is not in place after the download", ErrCorrupt, f.rel, name)
		}
	}
	return nil
}

// fetch downloads one file into the downloading directory: to a part name,
// hashed and counted as it is written, then renamed to its own name only if
// it is the size the listing gave and, when the manifest records it, the
// bytes the manifest records.
func (m *mirror) fetch(ctx context.Context, name string, f plannedFile) (err error) {
	dst := filepath.Join(m.tmpDir(name), filepath.FromSlash(f.rel))
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(dst), err)
	}
	body, stop, err := m.get(ctx, name+"/"+f.rel)
	if err != nil {
		return m.getError(err, name, f.rel)
	}
	defer stop()
	defer body.Close()
	part := dst + partSuffix
	out, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", part, err)
	}
	defer func() {
		if err != nil {
			out.Close()
			os.Remove(part)
		}
	}()
	h := crc32.New(crc32cTable)
	// One byte past the listed size at most: a body longer than its listing
	// is refused below, and must not be what fills the disk on the way there
	// (the disk check counted the listed sizes).
	n, err := io.Copy(io.MultiWriter(out, h), io.LimitReader(body, f.size+1))
	if err != nil {
		return fmt.Errorf("download %s of snapshot %s (after %d of %d bytes): %w", f.rel, name, n, f.size, err)
	}
	if n != f.size {
		return fmt.Errorf("%w: %s of snapshot %s is not the %d bytes its listing says", ErrCorrupt, f.rel, name, f.size)
	}
	if got := fmt.Sprintf("%08x", h.Sum32()); f.digest != "" && got != f.digest {
		return fmt.Errorf("%w: %s of snapshot %s has CRC-32C %s and the snapshot's manifest records %s",
			ErrCorrupt, f.rel, name, got, f.digest)
	}
	if err = out.Sync(); err != nil {
		return fmt.Errorf("write %s: %w", part, err)
	}
	if err = out.Close(); err != nil {
		return fmt.Errorf("write %s: %w", part, err)
	}
	if err = os.Rename(part, dst); err != nil {
		return fmt.Errorf("write %s: %w", dst, err)
	}
	return nil
}

// sweep removes what interrupted rounds left behind: a directory being
// removed, and a download of any snapshot but the one this round is after.
func (m *mirror) sweep(target string) error {
	entries, err := m.entries()
	if err != nil {
		return err
	}
	for _, e := range entries {
		n := e.Name()
		if !e.IsDir() || !strings.HasPrefix(n, ".") {
			continue
		}
		switch {
		case strings.HasSuffix(n, removingSuffix):
		case strings.HasSuffix(n, downloadingSuffix) && (target == "" || n != "."+target+downloadingSuffix):
		default:
			continue
		}
		if err := os.RemoveAll(filepath.Join(m.cfg.Dir, n)); err != nil {
			m.note("could not remove the leftover %s: %v", n, err)
		}
	}
	return nil
}

// retain removes the mirrored snapshots past Keep. It never consults the
// source: what the source lists decides what is brought in, not what is
// thrown away.
func (m *mirror) retain() error {
	held, err := m.held()
	if err != nil {
		return err
	}
	for i, s := range held {
		if i < m.cfg.Keep {
			continue
		}
		dir := filepath.Join(m.cfg.Dir, s.name)
		aside := filepath.Join(m.cfg.Dir, "."+s.name+removingSuffix)
		// Renamed first: the name stops parsing as a snapshot at once, so
		// no reader finds a directory with some of its files gone.
		if err := os.Rename(dir, aside); err != nil {
			m.note("could not remove mirrored snapshot %s: %v", s.name, err)
			continue
		}
		m.res.Removed = append(m.res.Removed, s.name)
		if err := os.RemoveAll(aside); err != nil {
			m.note("mirrored snapshot %s is out of the mirror but its files could not be deleted: %v", s.name, err)
		}
	}
	return nil
}
