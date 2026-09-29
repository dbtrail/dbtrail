package console

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Reads consult a server's previous snapshot locations after its current
// ones, and the newest snapshot wins (#1684). These drive the time travel
// lookup (bundle.findBaseline, which the MCP tool and the cascade provider
// share) with the locations answered by a stub.

var (
	t0      = time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	leftAt  = time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	readsAt = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
)

// snapPath is the path FindBaseline returns for a table of the snapshot of
// loc taken at snap.
func snapPath(loc string, snap time.Time) string {
	return strings.TrimRight(loc, "/") + "/" + reconstruct.SnapshotDirName(snap) + "/db/t.parquet"
}

// fakeLocation is one location as the stub answers it: its snapshots (each
// with its signers), or an error for every lookup.
type fakeLocation struct {
	snaps   map[time.Time][]string // snapshot time -> signers (nil = unsigned)
	err     error
	unknown bool // who signed is not known here
}

type fakeLocations struct {
	locs  map[string]fakeLocation
	asked map[string]time.Time // source -> the at it was asked with
}

func stubLocations(t *testing.T, locs map[string]fakeLocation, own string) *fakeLocations {
	t.Helper()
	f := &fakeLocations{locs: locs, asked: map[string]time.Time{}}
	prevFind, prevSigners, prevOwn := findBaselineAt, previousSnapshotSigners, bundleOwnWriter
	findBaselineAt = func(_ context.Context, source, schema, table string, at time.Time) (string, time.Time, reconstruct.StaleWarning, error) {
		f.asked[source] = at
		l, ok := f.locs[source]
		if !ok {
			t.Fatalf("a location nobody configured was read: %s", source)
		}
		if l.err != nil {
			return "", time.Time{}, reconstruct.StaleWarning{}, l.err
		}
		var best time.Time
		for s := range l.snaps {
			if !s.After(at) && s.After(best) {
				best = s
			}
		}
		if best.IsZero() {
			return "", time.Time{}, reconstruct.StaleWarning{}, fmt.Errorf("%w for %s.%s", reconstruct.ErrNoBaseline, schema, table)
		}
		return snapPath(source, best), best, reconstruct.StaleWarning{}, nil
	}
	previousSnapshotSigners = func(source string, at time.Time) ([]string, bool, error) {
		l := f.locs[source]
		if l.unknown {
			return nil, false, nil
		}
		w, ok := l.snaps[at]
		if !ok {
			return nil, false, nil
		}
		return w, true, nil
	}
	bundleOwnWriter = func(context.Context, *bundle) string { return own }
	t.Cleanup(func() { findBaselineAt, previousSnapshotSigners, bundleOwnWriter = prevFind, prevSigners, prevOwn })
	return f
}

func prevLoc(loc string, left time.Time) PreviousLocation {
	return PreviousLocation{Location: loc, LeftAt: left.UTC().Format(time.RFC3339)}
}

// The newest snapshot wins, whichever location holds it; a previous location
// is asked only up to the moment the server left it.
func TestFindBaselineAcrossLocations_newestWins(t *testing.T) {
	f := stubLocations(t, map[string]fakeLocation{
		"/cur":  {snaps: map[time.Time][]string{}},
		"/prev": {snaps: map[time.Time][]string{t0: nil, t0.Add(24 * time.Hour): nil}},
	}, "")
	b := &bundle{baselineSrc: "/cur", previous: []PreviousLocation{prevLoc("/prev", leftAt)}}
	path, snap, stale, err := b.findBaseline(context.Background(), "db", "t", readsAt)
	if err != nil {
		t.Fatalf("a snapshot in the previous location was not found: %v", err)
	}
	if want := snapPath("/prev", t0.Add(24*time.Hour)); path != want || !snap.Equal(t0.Add(24*time.Hour)) {
		t.Fatalf("got %s at %v, want %s", path, snap, want)
	}
	if stale.Stale() {
		t.Fatalf("a clean answer carries a warning: %q", stale.Message)
	}
	if got := f.asked["/prev"]; !got.Equal(leftAt) {
		t.Fatalf("the previous location was asked at %v, want the moment it was left (%v)", got, leftAt)
	}
	// A current snapshot newer than every previous one wins.
	f.locs["/cur"] = fakeLocation{snaps: map[time.Time][]string{readsAt.Add(-time.Hour): nil}}
	path, _, _, err = b.findBaseline(context.Background(), "db", "t", readsAt)
	if err != nil || path != snapPath("/cur", readsAt.Add(-time.Hour)) {
		t.Fatalf("got %s %v, want the current location's newer snapshot", path, err)
	}
	// Asked about a moment before the move, the previous location is asked
	// at that moment, not at the move.
	b.findBaseline(context.Background(), "db", "t", t0.Add(time.Hour))
	if got := f.asked["/prev"]; !got.Equal(t0.Add(time.Hour)) {
		t.Fatalf("asked at %v, want the requested moment", got)
	}
}

// Edge case 3: a previous location that cannot be read. The others still
// answer, and the answer says which one could not be read.
func TestFindBaselineAcrossLocations_unreachablePrevious(t *testing.T) {
	stubLocations(t, map[string]fakeLocation{
		"/cur":            {snaps: map[time.Time][]string{readsAt.Add(-time.Hour): nil}},
		"s3://gone/snaps": {err: errors.New("AccessDenied")},
		"/prev":           {snaps: map[time.Time][]string{t0: nil}},
	}, "")
	b := &bundle{baselineSrc: "/cur", previous: []PreviousLocation{prevLoc("s3://gone/snaps", leftAt), prevLoc("/prev", leftAt)}}
	path, _, stale, err := b.findBaseline(context.Background(), "db", "t", readsAt)
	if err != nil || path != snapPath("/cur", readsAt.Add(-time.Hour)) {
		t.Fatalf("got %s %v, want the current answer", path, err)
	}
	if !strings.Contains(stale.Message, "s3://gone/snaps") || !strings.Contains(stale.Message, "AccessDenied") {
		t.Fatalf("the answer does not name the location it could not read: %q", stale.Message)
	}
	// Nothing current: the answer comes from the readable previous one, and
	// still names the unreadable one.
	b.baselineSrc = "/empty"
	stubLocations(t, map[string]fakeLocation{
		"/empty":          {snaps: map[time.Time][]string{}},
		"s3://gone/snaps": {err: errors.New("AccessDenied")},
		"/prev":           {snaps: map[time.Time][]string{t0: nil}},
	}, "")
	path, _, stale, err = b.findBaseline(context.Background(), "db", "t", readsAt)
	if err != nil || path != snapPath("/prev", t0) || !strings.Contains(stale.Message, "s3://gone/snaps") {
		t.Fatalf("got %s %v %q", path, err, stale.Message)
	}
	// Nothing anywhere: the refusal names the unreadable location instead of
	// a plain "no snapshot", which would read as a shorter history.
	stubLocations(t, map[string]fakeLocation{
		"/empty":          {snaps: map[time.Time][]string{}},
		"s3://gone/snaps": {err: errors.New("AccessDenied")},
		"/prev":           {snaps: map[time.Time][]string{}},
	}, "")
	_, _, _, err = b.findBaseline(context.Background(), "db", "t", readsAt)
	if err == nil || errors.Is(err, reconstruct.ErrNoBaseline) || !strings.Contains(err.Error(), "s3://gone/snaps") {
		t.Fatalf("err = %v, want a refusal that names the unreadable location and is not a plain no-snapshot", err)
	}
	// With nothing unreadable, nothing anywhere stays the plain answer.
	b.previous = []PreviousLocation{prevLoc("/prev", leftAt)}
	_, _, _, err = b.findBaseline(context.Background(), "db", "t", readsAt)
	if !errors.Is(err, reconstruct.ErrNoBaseline) {
		t.Fatalf("err = %v, want ErrNoBaseline", err)
	}
	// A move time that cannot be read is said, never read as "no bound".
	b.previous = []PreviousLocation{{Location: "/prev", LeftAt: "yesterday"}}
	_, _, _, err = b.findBaseline(context.Background(), "db", "t", readsAt)
	if err == nil || errors.Is(err, reconstruct.ErrNoBaseline) || !strings.Contains(err.Error(), "/prev") {
		t.Fatalf("err = %v, want a refusal naming /prev", err)
	}
}

// Edge case 4: the same snapshot time in two locations. The current one
// wins, whatever the order.
func TestFindBaselineAcrossLocations_sameTimeTwice(t *testing.T) {
	stubLocations(t, map[string]fakeLocation{
		"/cur":  {snaps: map[time.Time][]string{t0: nil}},
		"/prev": {snaps: map[time.Time][]string{t0: nil}},
		"/old":  {snaps: map[time.Time][]string{t0: nil}},
	}, "")
	b := &bundle{baselineSrc: "/cur", previous: []PreviousLocation{prevLoc("/prev", leftAt), prevLoc("/old", leftAt)}}
	path, _, _, err := b.findBaseline(context.Background(), "db", "t", readsAt)
	if err != nil || path != snapPath("/cur", t0) {
		t.Fatalf("got %s %v, want the current copy", path, err)
	}
	// Between two previous ones, the most recently left.
	b.baselineSrc = "/none"
	stubLocations(t, map[string]fakeLocation{
		"/none": {snaps: map[time.Time][]string{}},
		"/prev": {snaps: map[time.Time][]string{t0: nil}},
		"/old":  {snaps: map[time.Time][]string{t0: nil}},
	}, "")
	path, _, _, err = b.findBaseline(context.Background(), "db", "t", readsAt)
	if err != nil || path != snapPath("/prev", t0) {
		t.Fatalf("got %s %v, want the most recently left copy", path, err)
	}
}

// Edge case 5: a previous location that another server took over as its
// current one. Everything written there after this server left is the other
// server's, signed or not, and is never served; what this server wrote
// before it left still is.
func TestFindBaselineAcrossLocations_placeTakenOver(t *testing.T) {
	f := stubLocations(t, map[string]fakeLocation{
		"/cur":    {snaps: map[time.Time][]string{}},
		"/shared": {snaps: map[time.Time][]string{t0: nil, leftAt.Add(time.Hour): nil, readsAt.Add(-time.Hour): {"other-writer"}}},
	}, "")
	b := &bundle{baselineSrc: "/cur", previous: []PreviousLocation{prevLoc("/shared", leftAt)}}
	path, _, _, err := b.findBaseline(context.Background(), "db", "t", readsAt)
	if err != nil || path != snapPath("/shared", t0) {
		t.Fatalf("got %s %v, want this server's own snapshot from before the move", path, err)
	}
	if !f.asked["/shared"].Equal(leftAt) {
		t.Fatalf("asked at %v", f.asked["/shared"])
	}
}

// The signature: a snapshot of a previous location signed by ANOTHER writer
// is never served as this server's, even from before the move. Unsigned
// ones keep today's behaviour; this server's own are served; a signed one is
// not served when this server's own identity is unknown, or when who signed
// cannot be told here.
func TestFindBaselineAcrossLocations_signature(t *testing.T) {
	cases := []struct {
		name    string
		signers []string
		unknown bool
		own     string
		served  bool
		says    string
	}{
		{"unsigned", nil, false, "me", true, ""},
		{"unsigned, own unknown", nil, false, "", true, ""},
		{"own", []string{"me"}, false, "me", true, ""},
		{"another writer", []string{"other"}, false, "me", false, "other"},
		{"own and another", []string{"me", "other"}, false, "me", false, "other"},
		{"signed, own unknown", []string{"me"}, false, "", false, "names no writer"},
		{"not known here", nil, true, "me", false, "not known"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubLocations(t, map[string]fakeLocation{
				"/cur":  {snaps: map[time.Time][]string{t0.Add(-48 * time.Hour): nil}},
				"/prev": {snaps: map[time.Time][]string{t0: tc.signers}, unknown: tc.unknown},
			}, tc.own)
			b := &bundle{baselineSrc: "/cur", previous: []PreviousLocation{prevLoc("/prev", leftAt)}}
			path, _, stale, err := b.findBaseline(context.Background(), "db", "t", readsAt)
			if err != nil {
				t.Fatal(err)
			}
			if tc.served {
				if path != snapPath("/prev", t0) {
					t.Fatalf("got %s, want the previous location's snapshot", path)
				}
				return
			}
			if path != snapPath("/cur", t0.Add(-48*time.Hour)) {
				t.Fatalf("got %s: another writer's snapshot was served as this server's", path)
			}
			if !strings.Contains(stale.Message, "/prev") || !strings.Contains(stale.Message, tc.says) {
				t.Fatalf("the answer does not say why the newer snapshot was not used: %q", stale.Message)
			}
		})
	}
}

func TestPreviousSnapshotRefusal(t *testing.T) {
	stubLocations(t, map[string]fakeLocation{
		"/p": {snaps: map[time.Time][]string{t0: {"other"}, t0.Add(time.Hour): nil, t0.Add(2 * time.Hour): {"me"}}},
	}, "")
	own := func() string { return "me" }
	if err := PreviousSnapshotRefusal("/p", t0, own); err == nil || !strings.Contains(err.Error(), "other") {
		t.Fatalf("another writer's snapshot: %v", err)
	}
	if err := PreviousSnapshotRefusal("/p", t0.Add(time.Hour), own); err != nil {
		t.Fatalf("unsigned: %v", err)
	}
	if err := PreviousSnapshotRefusal("/p", t0.Add(2*time.Hour), own); err != nil {
		t.Fatalf("own: %v", err)
	}
	if err := PreviousSnapshotRefusal("/p", t0.Add(3*time.Hour), own); err == nil {
		t.Fatal("a snapshot whose signature is not known here was allowed")
	}
	previousSnapshotSigners = func(string, time.Time) ([]string, bool, error) { return nil, false, errors.New("permission denied") }
	if err := PreviousSnapshotRefusal("/p", t0, own); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("an unreadable signature: %v", err)
	}
}

// The snapshot list reads the previous locations too, under the same two
// guards, and says what it left out and which location it could not read.
func TestListBaselineLocations_previous(t *testing.T) {
	both := t0.Add(48 * time.Hour)
	foreign := t0.Add(time.Hour)
	after := leftAt.Add(time.Hour)
	lister := fakeLister(map[string][]reconstruct.BaselineFile{
		"/cur": {{SnapshotTime: both, Schema: "db", Table: "t", Path: snapPath("/cur", both)}},
		"/prev": {
			{SnapshotTime: after, Schema: "db", Table: "t", Path: snapPath("/prev", after)},
			{SnapshotTime: both, Schema: "db", Table: "t", Path: snapPath("/prev", both)},
			{SnapshotTime: foreign, Schema: "db", Table: "t", Path: snapPath("/prev", foreign)},
			{SnapshotTime: foreign, Schema: "db", Table: "u", Path: strings.Replace(snapPath("/prev", foreign), "t.parquet", "u.parquet", 1)},
			{SnapshotTime: t0, Schema: "db", Table: "t", Path: snapPath("/prev", t0)},
		},
	}, map[string]error{"s3://gone/snaps": errors.New("AccessDenied")})
	refuse := func(loc string, snap time.Time) error {
		if loc == "/prev" && snap.Equal(foreign) {
			return errors.New("written by another writer (other)")
		}
		return nil
	}
	got := listBaselineLocations(context.Background(), []listSource{
		{Source: "/cur"},
		{Source: "/prev", Previous: &PreviousLocation{Location: "/prev", LeftAt: leftAt.Format(time.RFC3339)}},
		{Source: "s3://gone/snaps", Previous: &PreviousLocation{Location: "s3://gone/snaps", LeftAt: leftAt.Format(time.RFC3339)}},
	}, lister, refuse)

	var paths []string
	for _, f := range got.Files {
		paths = append(paths, f.Path)
	}
	want := []string{snapPath("/cur", both), snapPath("/prev", t0)}
	if strings.Join(paths, ",") != strings.Join(want, ",") {
		t.Fatalf("files = %v, want %v (the current copy of a shared time, nothing after the move, nothing another writer signed)", paths, want)
	}
	if k := got.Kinds[both.UnixNano()]; strings.Join(k, ",") != "dir,previous" {
		t.Fatalf("kinds of the shared time = %v", k)
	}
	if k := got.Kinds[t0.UnixNano()]; strings.Join(k, ",") != "previous" {
		t.Fatalf("kinds of the previous-only snapshot = %v", k)
	}
	if got.Listed != 2 || len(got.Sources) != 3 {
		t.Fatalf("listed %d of %d sources", got.Listed, len(got.Sources))
	}
	prev := got.Sources[1]
	if !prev.Previous || prev.LeftAt != leftAt.Format(time.RFC3339) || prev.Count != 2 || prev.Hidden != 2 {
		t.Fatalf("previous source = %+v, want previous, left_at, 2 kept and 2 snapshots hidden", prev)
	}
	if !strings.Contains(prev.HiddenWhy, "after this server left") || !strings.Contains(prev.HiddenWhy, "another writer") {
		t.Fatalf("hidden_why does not say both reasons: %q", prev.HiddenWhy)
	}
	gone := got.Sources[2]
	if !gone.Previous || !strings.Contains(gone.Error, "AccessDenied") {
		t.Fatalf("the unreadable previous location is not named: %+v", gone)
	}
	if got.Sources[0].Previous {
		t.Fatal("the current location is marked previous")
	}
	// A move time that cannot be read: the location is not read, and says so.
	got = listBaselineLocations(context.Background(), []listSource{
		{Source: "/cur"},
		{Source: "/prev", Previous: &PreviousLocation{Location: "/prev", LeftAt: "garbage"}},
	}, lister, refuse)
	if len(got.Files) != 1 || got.Listed != 1 || !strings.Contains(got.Sources[1].Error, "cannot be read") {
		t.Fatalf("an unparseable move time: files %d listed %d source %+v", len(got.Files), got.Listed, got.Sources[1])
	}
}
