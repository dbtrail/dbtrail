package console

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// Restore Coverage answers "how far back can this be restored", and it used to
// derive that from bundle.baselineSrc alone (#1571). On a server with a local
// directory AND an S3 destination that is a verdict about a subset: a table
// whose only surviving anchor is in the bucket was not graded, not named
// broken, and not counted -- it was simply absent, and the panel reported a
// clean verdict over an inventory missing a table.
//
// This is the third instance of the shape; #1542 fixed the listing and #1541
// the restore. The helpers those added are what the fix reuses.
func TestCoverageAPI_gradesEveryBackupLocation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215") // the floor
	tsDir := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }

	primary, fallback := t.TempDir(), t.TempDir()
	// Healthy and well inside the floor, so the verdict is "ok" either way and
	// the test cannot pass merely because everything went unknown.
	writeBaselineFixture(t, primary, tsDir(time.Hour), "shop", "orders.parquet")
	// This table exists ONLY in the second location, and its newest anchor
	// predates the floor. Ungraded it vanishes; graded it is broken.
	writeBaselineFixture(t, fallback, tsDir(150*time.Hour), "shop", "archived.parquet")

	srv := newBaselineServerWithFallback(t, primary, fallback)
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if got.FullTableStatus != "ok" {
		t.Fatalf("status = %q, want ok: both locations answered", got.FullTableStatus)
	}
	found := false
	for _, b := range got.BrokenTables {
		if b == "shop.archived" {
			found = true
		}
	}
	if !found {
		t.Errorf("broken_tables = %v, want shop.archived named. Its only anchor lives in the "+
			"second backup location, so reading one location drops the table from the verdict "+
			"entirely: not covered, not broken, just absent from a panel that claims to say "+
			"what is restorable", got.BrokenTables)
	}
}

// A location that does not answer makes the verdict UNKNOWN, even when the
// other one did. A partial listing can only understate coverage, and an
// understated window names healthy tables broken -- the cry-wolf failure
// status.DeltaFloor already refuses when archives cannot be attributed.
// Neither "ok" over a subset nor "broken" from a subset: unknown.
func TestCoverageAPI_partialListingIsUnknownNotAShorterWindow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")
	tsDir := func(age time.Duration) string { return now.Add(-age).Format("2006-01-02T15-04-05Z") }

	primary := t.TempDir()
	writeBaselineFixture(t, primary, tsDir(time.Hour), "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, primary, "/definitely/not/a/directory/1571")
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if got.FullTableStatus != "unknown" {
		t.Errorf("status = %q with one location unreadable, want unknown. The readable half is a "+
			"SUBSET, so grading it states a window narrower than the one that exists and can name "+
			"healthy tables broken", got.FullTableStatus)
	}
	if got.FullTableFrom != "" {
		t.Errorf("full_table_from = %q, want empty: an unknown verdict must claim no anchor", got.FullTableFrom)
	}
	if len(got.BrokenTables) != 0 {
		t.Errorf("broken_tables = %v, want none: a partial view must not accuse", got.BrokenTables)
	}
	// The delta half is computed from the index, not from the backup
	// locations, so it stays a real answer.
	if got.DeltaFrom == "" {
		t.Error("the live floor is independent of the backup listing and must still be reported")
	}
}

// The listing behind this card reads every backup location (#1571); the
// Restore button beside it folds from ONE of them (#1541): the local
// directory on a server with no S3 destination of its own. A table whose only
// usable anchor is in a daemon-wide bucket must therefore NOT advance
// full_table_from -- the card would print a start the button then refuses
// with "no backup exists at or before <t>" -- and must NOT join
// broken_tables, which drives an alarm a backup that exists off site does
// not deserve.
//
// Driven through gradeFullTable rather than the endpoint because an s3://
// location cannot be listed from a unit test, so a handler-level test can only
// ever produce local anchors: exactly the shape this split is not about.
func TestGradeFullTable_dirRestore_anS3OnlyAnchorDoesNotWidenTheRestoreWindow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}
	// OLDER than the S3-only anchor on purpose: the window is the LATEST of the
	// per-table starts, so counting the unreachable anchor would move `from`
	// forward to it. With the local table newer, both readings coincide and
	// the assertion below could not tell them apart.
	localOrders := now.Add(-9 * time.Hour)

	got := gradeFullTable([]reconstruct.BaselineFile{
		// A healthy local table, which is what full_table_from may name.
		{Schema: "shop", Table: "orders", SnapshotTime: localOrders, Path: "/backups/2026/shop/orders.parquet"},
		// The SAME table, also in the bucket and OLDER than its local copy --
		// the #616 local-retention shape. It is what makes `from`
		// discriminate: reading earliestUsable instead of the reachable one
		// would start the window here, at an instant with no local snapshot
		// at or before it, which is exactly the refusal quoted above.
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-40 * time.Hour), Path: "s3://bucket/prefix/2026/shop/orders.parquet"},
		// NO local copy at all, so findBaseline gets ErrNoBaseline from the
		// local root and the #766 fallback really does serve this from S3.
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://bucket/prefix/2026/shop/carts.parquet"},
		// A second S3-only table, so the sort order of unreachable_tables is
		// asserted rather than hidden behind a single element.
		{Schema: "shop", Table: "audit", SnapshotTime: now.Add(-3 * time.Hour), Path: "s3://bucket/prefix/2026/shop/audit.parquet"},
	}, restoreReach{kind: "dir"}, floor, now)

	if got.from.Equal(now.Add(-2 * time.Hour)) {
		t.Errorf("full_table_from advanced to the S3-only anchor. This server's Restore folds " +
			"from its local directory and never opens the bucket, so this start is one the " +
			"button refuses -- a green panel over a restore that cannot run")
	}
	if !got.from.Equal(localOrders) {
		t.Errorf("from = %s, want %s: the window is the latest LOCAL earliest-usable anchor", got.from, localOrders)
	}
	if len(got.broken) != 0 {
		t.Errorf("broken = %v, want none: shop.carts has a current backup, it is just off site. "+
			"broken_tables drives an alarm and says 'take a fresh backup', which is wrong advice here", got.broken)
	}
	if !slices.Equal(got.unreachable, []string{"shop.audit", "shop.carts"}) {
		t.Errorf("unreachable = %v, want [shop.audit shop.carts] in that order: silence here is the "+
			"failure -- the operator sees a clean card and never learns the tables are "+
			"unrestorable from this console -- and an unsorted list reorders between requests", got.unreachable)
	}
	if len(got.unevaluable) != 0 {
		t.Errorf("unevaluable = %v: an unreachable anchor is a known answer, not an unreadable one", got.unevaluable)
	}
}

// The other half of #1541, and the whole reason the card has to say which
// location it graded: on a server whose backups go to S3, Restore folds from
// the BUCKET (BaselineFoldSource, the scheduled update's rule). The S3-only
// tables above are now exactly what the button reaches, so they define the
// window and nothing is unreachable, while the same files graded under "dir"
// give the verdict above. Both are asserted from ONE listing so a grader that
// ignores the reach cannot pass by satisfying either test alone.
func TestGradeFullTable_s3Restore_theBucketDefinesTheWindow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}
	files := []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-9 * time.Hour), Path: "/backups/2026/shop/orders.parquet"},
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-40 * time.Hour), Path: "s3://bucket/prefix/2026/shop/orders.parquet"},
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://bucket/prefix/2026/shop/carts.parquet"},
		{Schema: "shop", Table: "audit", SnapshotTime: now.Add(-3 * time.Hour), Path: "s3://bucket/prefix/2026/shop/audit.parquet"},
	}

	dir := gradeFullTable(files, restoreReach{kind: "dir"}, floor, now)
	s3 := gradeFullTable(files, restoreReach{kind: "s3"}, floor, now)

	if len(dir.unreachable) != 2 || len(s3.unreachable) != 0 {
		t.Fatalf("unreachable dir=%v s3=%v: the same S3-only tables are unreachable from a local "+
			"restore and exactly what an S3 restore folds from", dir.unreachable, s3.unreachable)
	}
	// Restore lists the bucket, where shop.orders' only copy is the 40h one:
	// the local 9h copy was never uploaded, and the bucket is what the fold
	// opens. The window is the latest per-table start: carts at -2h.
	if !s3.from.Equal(now.Add(-2 * time.Hour)) {
		t.Errorf("s3 from = %s, want %s: the latest earliest-usable anchor IN THE BUCKET", s3.from, now.Add(-2*time.Hour))
	}
	if len(s3.broken) != 0 || len(s3.unevaluable) != 0 {
		t.Errorf("s3 broken=%v unevaluable=%v, want none: every table has a usable copy in the bucket", s3.broken, s3.unevaluable)
	}
}

// Under "s3" the unreachable case is the MIRROR of the one above: a snapshot
// that exists only on this host. The daemon-wide refresh loop writes those,
// and so does an upload that failed. Restore lists the bucket and never sees
// it, so the card must not count it and must name it, and must not call it
// broken: Time-travel reads it (local first, #766).
func TestGradeFullTable_s3Restore_aLocalOnlySnapshotIsUnreachableNotBroken(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}

	got := gradeFullTable([]reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://bucket/prefix/2026/shop/orders.parquet"},
		// Fresh, on this host only.
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-time.Hour), Path: "/backups/2026/shop/carts.parquet"},
	}, restoreReach{kind: "s3"}, floor, now)

	if !slices.Equal(got.unreachable, []string{"shop.carts"}) {
		t.Errorf("unreachable = %v, want [shop.carts]: its only copy is where an S3 restore does not look", got.unreachable)
	}
	if len(got.broken) != 0 {
		t.Errorf("broken = %v, want none: the backup exists and Time-travel reads it", got.broken)
	}
	if !got.from.Equal(now.Add(-2 * time.Hour)) {
		t.Errorf("from = %s, want the bucket's own anchor, not the local-only one", got.from)
	}
}

// A file present in BOTH locations keeps its LOCAL path in the merge (the
// footer read wants it), so under "s3" the path alone would call every
// uploaded-and-kept snapshot unreachable -- the routine shape of a healthy
// S3-backed server. The merge's InS3 set is what says the bucket has it.
func TestGradeFullTable_s3Restore_aFileInBothLocationsIsReachableByItsLocalPath(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}
	f := reconstruct.BaselineFile{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-2 * time.Hour), Path: "/backups/2026/shop/orders.parquet"}

	without := gradeFullTable([]reconstruct.BaselineFile{f}, restoreReach{kind: "s3"}, floor, now)
	if len(without.unreachable) != 1 {
		t.Fatalf("unreachable = %v: with no evidence the bucket has it, a local path is local-only", without.unreachable)
	}
	with := gradeFullTable([]reconstruct.BaselineFile{f}, restoreReach{kind: "s3", inS3: map[baselineFileKey]bool{keyOf(f): true}}, floor, now)
	if len(with.unreachable) != 0 || !with.from.Equal(f.SnapshotTime) {
		t.Errorf("unreachable=%v from=%s: the bucket has this file, so an S3 restore reaches it", with.unreachable, with.from)
	}
}

// The merge is what feeds InS3, so the wiring is pinned here rather than
// trusted: a file listed by an s3 location is marked whichever path won.
func TestListBaselinesMerged_marksEveryFileTheBucketListed(t *testing.T) {
	ts := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	both := reconstruct.BaselineFile{Schema: "shop", Table: "orders", SnapshotTime: ts}
	lister := func(_ context.Context, src string) ([]reconstruct.BaselineFile, error) {
		f := both
		f.Path = src + "/2026-06-10T12-00-00Z/shop/orders.parquet"
		if baselineKindOf(src) == "dir" {
			f2 := reconstruct.BaselineFile{Schema: "shop", Table: "carts", SnapshotTime: ts, Path: src + "/2026-06-10T12-00-00Z/shop/carts.parquet"}
			return []reconstruct.BaselineFile{f, f2}, nil
		}
		return []reconstruct.BaselineFile{f}, nil
	}
	got := listBaselinesMerged(context.Background(), []string{"/backups", "s3://bucket/prefix"}, lister)

	if !got.InS3[keyOf(both)] {
		t.Error("shop.orders was listed by the bucket and is not marked InS3; under an S3 restore the card would call it unreachable")
	}
	if got.InS3[baselineFileKey{unixNano: ts.UnixNano(), schema: "shop", table: "carts"}] {
		t.Error("shop.carts exists only locally and is marked InS3")
	}
	if len(got.Files) != 2 || baselineKindOf(got.Files[1].Path) != "dir" {
		t.Errorf("files = %+v, want two, the shared one keeping its local path", got.Files)
	}
}

// A STALE LOCAL COPY SHADOWS THE FRESH OFFSITE ONE, and that is broken, not
// unreachable, on a server whose Restore reads the local directory.
// bundle.findBaseline falls back to the bucket only on ErrNoBaseline (#766); a
// table with any local snapshot at-or-before the instant gets a nil error, so
// the fallback never fires and time travel resolves the stale copy. No console
// surface reaches the fresh S3 sibling.
//
// Calling this unreachable would trade the red "take a fresh backup" this card
// gave before #1571 for a warning that PROMISES a working time travel -- the
// operator would read the promise and not take the backup.
func TestGradeFullTable_aStaleLocalCopyShadowsTheOffsiteOneAndStaysBroken(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}
	files := []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), Path: "/backups/new/shop/orders.parquet"},
		// Below the floor, and it is what findBaseline will resolve.
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-150 * time.Hour), Path: "/backups/old/shop/carts.parquet"},
		// Fresh, in the bucket, and unreachable because of the line above.
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://bucket/prefix/2026/shop/carts.parquet"},
	}

	got := gradeFullTable(files, restoreReach{kind: "dir"}, floor, now)
	if len(got.unreachable) != 0 {
		t.Errorf("unreachable = %v, want none: the stale LOCAL copy is what findBaseline resolves, so "+
			"the fresh S3 one is unreachable from every console surface and the card would be "+
			"promising a time travel that silently serves the stale data", got.unreachable)
	}
	if len(got.broken) != 1 || got.broken[0] != "shop.carts" {
		t.Errorf("broken = %v, want [shop.carts]: this is the verdict the card gave before it read "+
			"both locations, and reading the second one must not silence it", got.broken)
	}

	// The same shape on an S3-backed server is RESTORABLE: Restore folds the
	// fresh copy from the bucket, so "take a fresh backup" would be wrong
	// advice about a restore that works. (Time-travel still prefers the
	// stale local copy; that divergence is the fold's, documented on
	// BaselineFoldSource, not this card's to hide a working restore behind.)
	s3 := gradeFullTable(files, restoreReach{kind: "s3", inS3: map[baselineFileKey]bool{keyOf(files[0]): true}}, floor, now)
	if len(s3.broken) != 0 || len(s3.unreachable) != 0 || !s3.from.Equal(now.Add(-time.Hour)) {
		t.Errorf("s3 broken=%v unreachable=%v from=%s: both tables have a usable copy in the bucket", s3.broken, s3.unreachable, s3.from)
	}
}

// The complement, so the classifier is not simply "s3 means unreachable": a
// table whose usable anchor IS local keeps defining the window even when a
// newer copy also sits in the bucket. Without this, returning unreachable for
// every table with any S3 file would pass the test above.
func TestGradeFullTable_aLocalAnchorStillCountsWhenS3AlsoHasIt(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}
	local := now.Add(-3 * time.Hour)

	got := gradeFullTable([]reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: local, Path: "/backups/2026/shop/orders.parquet"},
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), Path: "s3://bucket/p/2026/shop/orders.parquet"},
	}, restoreReach{kind: "dir"}, floor, now)

	if !got.from.Equal(local) {
		t.Errorf("from = %s, want %s: the earliest usable LOCAL anchor is what this Restore can reach", got.from, local)
	}
	if len(got.unreachable) != 0 {
		t.Errorf("unreachable = %v, want none: this table is restorable from the local directory", got.unreachable)
	}
}

// An S3-only server has no local directory to fold into, so Restore refuses
// outright and every table would be enumerated as unreachable; the card would
// lose the number it exists to print. That is a configuration fact, stated
// once. A warn line naming the whole schema is the kind of line operators
// learn to skip.
func TestCoverageAPI_anS3OnlyServerStatesItOnceInsteadOfNamingEveryTable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")

	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	srv := newBaselineServerWithFallback(t, "s3://bucket/prefix", "")
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if len(got.UnreachableTables) != 0 {
		t.Errorf("unreachable_tables = %v, want none: with no local directory at all, every table is "+
			"unreachable and the enumeration says nothing a single sentence does not", got.UnreachableTables)
	}
	if !got.RestoreNeedsLocal {
		t.Error("restore_needs_local is false: the operator gets no explanation for a card with " +
			"no restore window, on a server where Restore refuses outright for want of a local folder")
	}
	if got.RestoreReads != "" {
		t.Errorf("restore_reads = %q, want empty: Restore refuses this server, so the card must not "+
			"claim to grade against the bucket next to that refusal", got.RestoreReads)
	}
}

func TestGradeFullTable_anUnattributableFloorStillNamesTheTable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	files := []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), Path: "/backups/new/shop/orders.parquet"},
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-150 * time.Hour), Path: "/backups/old/shop/carts.parquet"},
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://bucket/prefix/2026/shop/carts.parquet"},
	}

	attributable := gradeFullTable(files, restoreReach{kind: "dir"}, status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}, now)
	if !slices.Equal(attributable.broken, []string{"shop.carts"}) {
		t.Fatalf("broken = %v, want [shop.carts] on an attributable floor", attributable.broken)
	}

	got := gradeFullTable(files, restoreReach{kind: "dir"}, status.DeltaFloor{Hour: now.Add(-100 * time.Hour), BelowIsUnknown: true}, now)
	if len(got.broken) != 0 {
		t.Errorf("broken = %v, want none: an unattributable floor must not accuse (#1219)", got.broken)
	}
	if !slices.Equal(got.unevaluable, []string{"shop.carts"}) {
		t.Errorf("unevaluable = %v, want [shop.carts]. The card says 'could not be checked' and the "+
			"operator gets no table name, on the verdict that is ROUTINE for a multi-source index -- "+
			"the shadowing this branch exists to catch would be invisible there", got.unevaluable)
	}
	if len(got.unreachable) != 0 {
		t.Errorf("unreachable = %v, want none: the stale local copy still shadows the bucket, whatever "+
			"the floor can attribute", got.unreachable)
	}
}

// With no location of its own to restore into, every usable table is
// unreachable by construction, and the list would say nothing the single
// restore_needs_local sentence does not. A warn line enumerating every table
// is the line operators learn to skip.
func TestGradeFullTable_withNoLocationOfItsOwnTheUnreachableListIsSuppressed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}
	files := []reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), Path: "s3://b/p/2026/shop/orders.parquet"},
		{Schema: "shop", Table: "carts", SnapshotTime: now.Add(-2 * time.Hour), Path: "s3://b/p/2026/shop/carts.parquet"},
	}

	if got := gradeFullTable(files, restoreReach{}, floor, now); len(got.unreachable) != 0 || !got.from.IsZero() {
		t.Errorf("unreachable = %v from = %s, want none and no window: Restore refuses this server, so "+
			"a start would sit next to a button that cannot run, and the enumeration is the whole schema", got.unreachable, got.from)
	}

	// The complement, so the suppression is not simply "unreachable is never
	// reported".
	if got := gradeFullTable(files, restoreReach{kind: "dir"}, floor, now); len(got.unreachable) != 2 {
		t.Errorf("unreachable = %v, want both tables: a server restoring from a local directory needs "+
			"to know which tables its Restore cannot fold", got.unreachable)
	}
}

// restoreReadsFrom is the card's copy of BaselineFoldSource's precedence,
// behind handleBaselineRestore's refusal; the three are pinned against each
// other so the card cannot grade against a location the button does not open.
// An S3-only server is the case that matters: the fold source WOULD be the
// bucket, but the button refuses for want of a directory to write into
// (rebuildPossible says the same), so the card must read from nothing.
func TestRestoreReadsFrom_agreesWithBaselineFoldSource(t *testing.T) {
	for _, tc := range []struct{ dir, s3, want string }{
		{"/b", "", "dir"}, {"/b", "s3://k/p/", "s3"}, {"", "s3://k/p/", ""}, {"", "", ""},
	} {
		got := restoreReadsFrom(tc.dir, tc.s3)
		if got != tc.want {
			t.Errorf("restoreReadsFrom(%q,%q) = %q, want %q", tc.dir, tc.s3, got, tc.want)
		}
		e := ServerEntry{DSN: "d", BaselineDir: tc.dir, BaselineS3: tc.s3}
		refused := rebuildPossible(e) != nil
		if refused != (got == "") {
			t.Errorf("rebuildPossible refuses=%v but the card reads %q: the two must agree on WHETHER a restore runs", refused, got)
		}
		if src := BaselineFoldSource(e); !refused && baselineKindOf(src) != got {
			t.Errorf("BaselineFoldSource picks %q (%s) but the card grades against %q", src, baselineKindOf(src), got)
		}
	}
}

// A dir-backed server must report restore_needs_local FALSE. Nothing asserted
// the negative, and omitempty makes a spurious true invisible: hardcoding the
// field survived the entire console suite, which would have told every
// operator on the product that their backups go to S3 only.
func TestCoverageAPI_aDirBackedServerDoesNotAskForALocalFolder(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")
	tsDir := now.Add(-time.Hour).Format("2006-01-02T15-04-05Z")

	primary := t.TempDir()
	writeBaselineFixture(t, primary, tsDir, "shop", "orders.parquet")

	srv := newBaselineServerWithFallback(t, primary, "")
	srv.cm.boot.db = coverageMockDB(t, part, latest, nil)
	srv.cm.boot.dbName = "binlog_index"
	got := coverageGet(t, srv)

	if got.RestoreNeedsLocal {
		t.Error("restore_needs_local is true on a server whose backups go to a local directory. " +
			"The card would tell the operator their backups are S3-only and drop the unreachable list")
	}
	if got.RestoreReads != "inherited" {
		t.Errorf("restore_reads = %q, want inherited: the boot server names no location of its own, so "+
			"the card graded the daemon-wide directory, and Restore is refused for it (#1602)", got.RestoreReads)
	}
	if got.FullTableFrom == "" {
		t.Error("a dir-backed server with a healthy backup must still report its restore window")
	}
}

// A registry server that names a location of its own is graded against it
// and the card says the kind: this is the only shape whose Restore button
// runs, and withBaselineDefaults hands the bundle exactly those locations.
func TestCoverageAPI_aServerWithItsOwnDirReportsDir(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")

	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{}})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	e, err := reg.Add(ServerEntry{Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx",
		SourceDSN: "src:pw@tcp(127.0.0.1:3306)/", BaselineDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	srv.cm.bundles[e.ID] = &bundle{db: coverageMockDB(t, part, latest, nil), dbName: "binlog_index",
		baselineSrc: dir, baselineConfigured: true}

	rec, body := doServersReqHeader(t, srv, "GET", "/api/coverage", "", e.ID)
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got coverageResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.RestoreReads != "dir" {
		t.Errorf("restore_reads = %q, want dir: this server names its own directory, so its Restore runs and folds from it", got.RestoreReads)
	}
}

// A registry server that names NO backup location of its own inherits the
// daemon-wide one through its bundle. Restore refuses it (#1602), but the card
// grading it as reading nothing would render the "no usable baseline exists
// yet" shape — no window, no unreachable list, no restore_reads — over a
// backup that is there and that time-travel reads. It grades from the bundle's
// sources instead, as the boot entry does, and keeps the window it reported
// before #1541.
func TestCoverageAPI_anEntryInheritingTheDaemonDirKeepsItsWindow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	latest := now.Add(-30 * time.Second)
	part := now.Add(-100 * time.Hour).Format("p_2006010215")
	tsDir := now.Add(-time.Hour).Format("2006-01-02T15-04-05Z")

	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.Add(ServerEntry{Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx", SourceDSN: "src:pw@tcp(127.0.0.1:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	daemonDir := t.TempDir()
	writeBaselineFixture(t, daemonDir, tsDir, "shop", "orders.parquet")
	srv.cm.bundles[e.ID] = &bundle{db: coverageMockDB(t, part, latest, nil), dbName: "binlog_index",
		baselineSrc: daemonDir, baselineConfigured: true}

	rec, body := doServersReqHeader(t, srv, "GET", "/api/coverage", "", e.ID)
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got coverageResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.RestoreReads != "inherited" || got.FullTableFrom == "" {
		t.Errorf("restore_reads = %q full_table_from = %q, want inherited and a window: the inherited "+
			"directory holds a healthy backup, so reading nothing would erase it from the card, and "+
			"calling it dir would claim a Restore button the server does not get (#1602)", got.RestoreReads, got.FullTableFrom)
	}
}

// The s3 twin of the stale-local shadow: the bucket holds only a STALE copy
// (uploaded, then pruned locally) while a fresh copy sits on this host alone
// (a restore whose upload failed, or the daemon-wide refresh). Restore reads
// the bucket, anchors on the stale copy and refuses the whole run, so this is
// broken — "take a fresh backup" is the remedy, and a full backup sends the
// directory up. Calling it unreachable would say "backed up only on this
// host", which is false: an older copy IS in S3, and it is the one the
// button would use.
func TestGradeFullTable_s3Restore_aStaleBucketCopyIsBrokenNotUnreachable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	floor := status.DeltaFloor{Hour: now.Add(-100 * time.Hour)}

	got := gradeFullTable([]reconstruct.BaselineFile{
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-200 * time.Hour), Path: "s3://bucket/prefix/2026/shop/orders.parquet"},
		{Schema: "shop", Table: "orders", SnapshotTime: now.Add(-time.Hour), Path: "/backups/2026/shop/orders.parquet"},
	}, restoreReach{kind: "s3"}, floor, now)

	if !slices.Equal(got.broken, []string{"shop.orders"}) || len(got.unreachable) != 0 {
		t.Errorf("broken=%v unreachable=%v, want [shop.orders] broken: the bucket copy the fold anchors on "+
			"predates coverage, and the fresh local copy was never sent there", got.broken, got.unreachable)
	}
}
