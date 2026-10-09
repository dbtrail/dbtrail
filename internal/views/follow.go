package views

import (
	"context"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// ApplyFollow decides how the state views will reach a snapshot published after
// this file is generated, and records it on in.
//
// One function for both producers on purpose. The CLI and the console download
// serve the same file to the same reader, and they used to carry a byte-identical
// copy of this decision each; a mode added to one of them and not the other is a
// file whose header describes a following its own views do not do.
//
// pin is the operator asking for today's rows to stay today's, and it is the only
// decision made here. Every other reason to refuse belongs to the mechanism:
// baseline.RewriteToPointer owns the pointer's conditions, and the S3 arm below
// owns its own.
func ApplyFollow(in *Input, root string, pin bool) {
	if pin || len(in.Baselines) == 0 {
		return
	}
	// A backslash in the root is not followable, and it is measured rather than
	// assumed: DuckDB's glob treats `\` as a path SEPARATOR, not an escape, so
	// a pattern under `/data/back\up/baselines` is globbed as
	// `/data/back/up/baselines` and matches nothing. read_parquet reads the same
	// literal path perfectly well, which is why the view bodies would work while
	// the dropped-table check refused every table of a healthy snapshot — the
	// worst outcome this file has.
	//
	// There is no escape to reach for: as-is, doubled, and a single-character
	// class were all tried against the pinned engine and all match nothing. So
	// the honest answer is not to follow. The file is pinned, its header says
	// so, and no check is promised that is not there.
	//
	// On Linux, where this ships, a backslash is an ordinary legal filename
	// character. On Windows it is the separator, so following would be broken
	// for every root; refusing is right in both.
	if strings.Contains(root, `\`) {
		return
	}
	paths := make([]string, len(in.Baselines))
	for i, t := range in.Baselines {
		paths[i] = t.Path
	}
	if rewritten, rels, ok := baseline.RewriteToPointer(root, paths); ok {
		// Rel comes back from the rewrite rather than being cut off the paths a
		// second time. This branch used to set Follow UNCONDITIONALLY while
		// setting Rel only when a local re-derivation happened to agree, and the
		// two disagreed for every root that is not already filepath.Clean-shaped
		// ("./baselines", "/data/bl//", "/data/./bl"): RewriteToPointer resolves
		// with filepath.Rel, which CLEANS both sides, and the local helper cut a
		// raw byte prefix. The result was a file that followed, promised the
		// dropped-table check in its own header, and carried no check at all
		// (#1558).
		for i := range in.Baselines {
			in.Baselines[i].Path = rewritten[i]
			in.Baselines[i].Rel = rels[i]
		}
		in.Follow = FollowPointer
		return
	}
	// A local root that refused the rewrite stops here rather than falling
	// through: RewriteToPointer refuses when the pointer names a DIFFERENT
	// snapshot than the one discovered, and answering that by following
	// something else is how a file comes to serve rows its header does not
	// describe. FollowNewest is the S3 answer to having no pointer at all.
	if !isS3(root) {
		return
	}
	// A root whose text holds a pattern character is not followed either
	// (#2246). The newest snapshot is found with a glob over the root, where
	// the root's own text is a pattern: under "s3://b/da?a/" it also matches
	// the snapshots of a sibling "s3://b/data/", the newest of the two wins,
	// and every view then reads that one's files, with no error. Pinned, each
	// path is named through a class (fileGlob), so the file reads its own.
	if strings.ContainsAny(strings.TrimPrefix(root, "s3://"), "*?[{") {
		return
	}
	rels := snapshotRels(root, paths)
	if rels == nil {
		// All or nothing. A file where some state views follow and others
		// are pinned reads exactly like one where they all do, and the two
		// halves would drift apart at the first refresh.
		return
	}
	for i := range in.Baselines {
		in.Baselines[i].Rel = rels[i]
	}
	in.Follow = FollowNewest
}

// snapshotRels cuts every path down to its position inside a snapshot
// directory, or returns nil if any one of them will not cut.
//
// All or nothing on purpose: the callers use these to describe the whole file,
// and a half-filled slice would describe half of it while looking complete.
func snapshotRels(root string, paths []string) []string {
	out := make([]string, len(paths))
	for i, path := range paths {
		rel, ok := snapshotRel(root, path)
		if !ok {
			return nil
		}
		out[i] = rel
	}
	return out
}

// snapshotRel cuts a table's path down to its position inside a snapshot
// directory: "s3://bucket/baselines/2026-08-31T06-00-45Z/shop/orders.parquet"
// under root "s3://bucket/baselines/" gives "shop/orders.parquet".
//
// String operations, never filepath: filepath.Clean collapses the "//" in a
// scheme, so every path helper in the standard library turns "s3://bucket/x"
// into "s3:/bucket/x". Rel happens to survive that because it mangles both
// sides identically, which is a coincidence and not a contract.
func snapshotRel(root, path string) (string, bool) {
	base := strings.TrimSuffix(root, "/") + "/"
	rel, ok := strings.CutPrefix(path, base)
	if !ok {
		return "", false
	}
	_, rest, found := strings.Cut(rel, "/")
	if !found || rest == "" {
		return "", false
	}
	return rest, true
}

// readNewestPointer is baseline.ReadNewestPointer, swapped out by tests.
var readNewestPointer = baseline.ReadNewestPointer

// newestPointerTimeout bounds the one GET: a file generated without the
// pointer is slower to open, never wrong, so a stalled store must not hold up
// generation.
const newestPointerTimeout = 10 * time.Second

// UseNewestPointer lets a marker-followed file resolve its snapshot through
// <root>/_NEWEST (#2052) instead of listing the whole root, when the pointer
// names exactly the snapshot just discovered. Any other answer (no pointer,
// an older or newer name, a read that failed) leaves the listing in place:
// the pointer is a shortcut, and a producer that cannot confirm it is current
// must not hand the reader a file that follows a stale one.
//
// Called by every producer right after ApplyFollow. It is separate only
// because it reads the network and ApplyFollow does not.
func UseNewestPointer(ctx context.Context, in *Input) {
	if in.Follow != FollowNewest {
		return
	}
	// The pointer arm joins the RAW root into a path the preflight globs; the
	// listing arm's value is a glob result and never has this problem.
	if strings.ContainsAny(strings.TrimPrefix(in.BaselineSource, "s3://"), "*?[{") {
		return
	}
	// The uploader derives the pointer's key with path.Dir, which CLEANS, and
	// the file joins the root as typed. They agree only on a clean root: under
	// "s3://b/bl//" the pointer lives at bl/_NEWEST while the file would read
	// bl//_NEWEST and fail on open. Same class as #1558.
	if _, prefix, err := storage.ParseS3URL(in.BaselineSource); err != nil ||
		(strings.TrimSuffix(prefix, "/") != "" && strings.TrimSuffix(prefix, "/") != path.Clean(strings.TrimSuffix(prefix, "/"))) ||
		strings.HasPrefix(prefix, "/") {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, newestPointerTimeout)
	defer cancel()
	name, found, err := readNewestPointer(ctx, in.BaselineSource)
	if err != nil {
		slog.Warn("views: could not read the baselines root's newest-snapshot pointer; "+
			"the file will list the root to find the newest snapshot instead, which is slower to open",
			"root", in.BaselineSource, "error", err)
		return
	}
	if found && name == snapshotdir.Name(in.BaselineSnapshot) {
		in.NewestPointer = name
	}
}
