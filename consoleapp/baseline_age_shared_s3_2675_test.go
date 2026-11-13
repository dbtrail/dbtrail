package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #2675 point 5: pruning by age alone with a shared S3 prefix. The prune
// asks the bucket for <prefix>/<name>/_SUCCESS, and a snapshot name has no
// server in it, so two servers on one prefix whose snapshots share a name
// confirm each other's. #2658 closed it for the count; these pin it for the
// age.

// sameNamedSnapshots writes the same two complete snapshots, by name, into
// every dir: two and four days old, so both are past a one-day age, and the
// older one is not the newest of its table (the newer one is), so only the
// bucket's answer keeps it.
func sameNamedSnapshots(t *testing.T, dirs ...string) (older, newer string) {
	t.Helper()
	now := time.Now().UTC()
	names := []string{
		strings.ReplaceAll(now.Add(-96*time.Hour).Truncate(time.Second).Format(time.RFC3339), ":", "-"),
		strings.ReplaceAll(now.Add(-48*time.Hour).Truncate(time.Second).Format(time.RFC3339), ":", "-"),
	}
	for _, d := range dirs {
		for _, n := range names {
			writeSnapshotFiles(t, filepath.Join(d, n), baseline.SuccessMarker)
		}
	}
	return names[0], names[1]
}

// bucketOf is a fake bucket shared by every prune of a cycle: the
// _SUCCESS keys uploaded, by prefix (trailing slashes aside) and name.
type bucketOf struct {
	mu   sync.Mutex
	keys map[string]bool
}

func (b *bucketOf) put(prefix, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.keys == nil {
		b.keys = map[string]bool{}
	}
	b.keys[strings.TrimRight(strings.TrimSpace(prefix), "/")+"/"+name+"/_SUCCESS"] = true
}

// prune is the cycle's pruneFn: PruneLocal with the bucket asked for the
// exact key PruneLocal asks S3 for.
func (b *bucketOf) prune(ctx context.Context, opts baseline.PruneOptions) (baseline.PruneResult, error) {
	return baseline.PruneLocalAsking(ctx, opts, func(_ context.Context, name string) (bool, error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.keys[strings.TrimRight(strings.TrimSpace(opts.S3URL), "/")+"/"+name+"/_SUCCESS"], nil
	})
}

// Two servers, one prefix, the same snapshot names, and only server a's
// upload of the older one reached the bucket. Before the fix the age prune
// of b's folder asked for <prefix>/<older>/_SUCCESS, found a's, and removed
// b's only copy.
func TestPruneCycle_byAgeASharedPrefixDoesNotConfirmAnotherServersSnapshot(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	older, newer := sameNamedSnapshots(t, a, b)
	entries := []console.ServerEntry{
		{ID: "a", Name: "a", BaselineDir: a, BaselineS3: "s3://bucket/shared/"},
		{ID: "b", Name: "b", BaselineDir: b, BaselineS3: "s3://bucket/shared"},
	}
	var bucket bucketOf
	bucket.put("s3://bucket/shared/", older)

	runBaselinePruneCycle(context.Background(), prunePlan(entries, "", "", 24*time.Hour), 24*time.Hour, bucket.prune)

	for _, d := range []string{a, b} {
		if got := snapshotsIn(t, d); !slices.Equal(got, []string{older, newer}) {
			t.Errorf("%s holds %v after the cycle, want both snapshots kept: the bucket cannot say whose %s it has", d, got, older)
		}
	}
}

// The same two folders on prefixes of their own: the age prune still runs,
// so the fix above is the shared prefix and not a prune that stopped.
func TestPruneCycle_byAgeAPrefixOfItsOwnIsStillPruned(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	older, newer := sameNamedSnapshots(t, a, b)
	entries := []console.ServerEntry{
		{ID: "a", Name: "a", BaselineDir: a, BaselineS3: "s3://bucket/a/"},
		{ID: "b", Name: "b", BaselineDir: b, BaselineS3: "s3://bucket/b/"},
	}
	var bucket bucketOf
	bucket.put("s3://bucket/a/", older)

	runBaselinePruneCycle(context.Background(), prunePlan(entries, "", "", 24*time.Hour), 24*time.Hour, bucket.prune)

	if got := snapshotsIn(t, a); !slices.Equal(got, []string{newer}) {
		t.Errorf("a holds %v, want only %s: its older snapshot is in its own prefix", got, newer)
	}
	if got := snapshotsIn(t, b); !slices.Equal(got, []string{older, newer}) {
		t.Errorf("b holds %v, want both: its own prefix does not have %s", got, older)
	}
}

// Who shares a prefix, as the age plan sees it. Each case names the folders
// the plan prunes by age.
func TestPrunePlan_byAgeWhoSharesAPrefix(t *testing.T) {
	root := t.TempDir()
	dir := func(n string) string {
		p := filepath.Join(root, n)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	a, b, g := dir("a"), dir("b"), dir("global")
	link := filepath.Join(root, "a-link")
	if err := os.Symlink(a, link); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name                string
		entries             []console.ServerEntry
		globalDir, globalS3 string
		want                []string
	}{
		{"trailing slash and spaces are one prefix",
			[]console.ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: " s3://bk/p/ "}, {ID: "b", BaselineDir: b, BaselineS3: "s3://bk/p"}},
			"", "", nil},
		{"a nested prefix is its own",
			[]console.ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: "s3://bk/p/"}, {ID: "b", BaselineDir: b, BaselineS3: "s3://bk/p/sub/"}},
			"", "", []string{a, b}},
		{"a prefix in another case is another prefix (S3 keys are case-sensitive)",
			[]console.ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: "s3://bk/p/"}, {ID: "b", BaselineDir: b, BaselineS3: "s3://bk/P/"}},
			"", "", []string{a, b}},
		{"a server with only S3 on the prefix shares it",
			[]console.ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: "s3://bk/p/"}, {ID: "b", BaselineS3: "s3://bk/p/"}},
			"", "", nil},
		{"the daemon's own prefix with another folder: neither the server nor the daemon's folder",
			[]console.ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: "s3://bk/p/"}},
			g, "s3://bk/p", nil},
		{"the daemon's own prefix with no folder of its own shares it",
			[]console.ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: "s3://bk/p/"}},
			"", "s3://bk/p/", nil},
		{"the startup migration's pair (the daemon's folder AND prefix) is one set of snapshots",
			[]console.ServerEntry{{ID: "a", BaselineDir: g, BaselineS3: "s3://bk/p/"}, {ID: "b", BaselineDir: g, BaselineS3: "s3://bk/p/"}},
			g, "s3://bk/p/", []string{g}},
		{"one folder named through a link is one folder",
			[]console.ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: "s3://bk/p/"}, {ID: "b", BaselineDir: link, BaselineS3: "s3://bk/p/"}},
			"", "", []string{a, link}},
		{"a server with no S3 shares nothing",
			[]console.ServerEntry{{ID: "a", BaselineDir: a, BaselineS3: "s3://bk/p/"}, {ID: "b", BaselineDir: b}},
			"", "", []string{a}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got []string
			for _, tgt := range prunePlan(c.entries, c.globalDir, c.globalS3, 24*time.Hour) {
				if tgt.keepNewest != 0 {
					t.Fatalf("a count target in a plan with no count: %+v", tgt)
				}
				got = append(got, tgt.dir)
			}
			slices.Sort(got)
			want := slices.Clone(c.want)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("pruned by age: %v, want %v", got, want)
			}
		})
	}
}

// With no age, nothing is left out and nothing is said: the plan is the
// count's alone.
func TestWarnAgeSharedS3_saysNothingWithoutAnAge(t *testing.T) {
	entries := []console.ServerEntry{
		{ID: "a", BaselineDir: "/srv/a", BaselineS3: "s3://bk/p/"},
		{ID: "b", BaselineDir: "/srv/b", BaselineS3: "s3://bk/p/"},
	}
	logs := captureSlogFor(t)
	warnAgeSharedS3(entries, "", "", 0)
	if strings.Contains(logs.String(), "S3 prefix too") {
		t.Errorf("with no age the shared prefix is warned about: %s", logs.String())
	}
	warnAgeSharedS3(entries, "", "", 24*time.Hour)
	out := logs.String()
	t.Logf("log: %s", out)
	if strings.Count(out, ageSharedS3Message) != 2 || !strings.Contains(out, "/srv/a") || !strings.Contains(out, "/srv/b") {
		t.Errorf("each folder left out is not named once: %s", out)
	}
}
