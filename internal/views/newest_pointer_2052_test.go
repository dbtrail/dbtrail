package views

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// newestPointerFixture is newestFixture reading through <root>/_NEWEST (#2052): the
// pointer is written to disk with the trailing newline the publisher writes,
// and the Input carries the name the producer confirmed.
func newestPointerFixture(t *testing.T, root, content, confirmed string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, baseline.NewestPointerName), []byte(content), 0o644); err != nil {
		t.Fatalf("write pointer: %v", err)
	}
	return Generate(Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   root,
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Follow:           FollowNewest,
		NewestPointer:    confirmed,
		Baselines: []BaselineTable{{
			Schema: "shop", Table: "orders",
			Path: filepath.Join(root, "2026-04-30T03-00-00Z", "shop", "orders.parquet"),
			Rel:  "shop/orders.parquet",
		}},
	})
}

// The pointer names the OLDER of two marked snapshots. A file that still
// listed the root would pick the newer one, so reading "old" is what proves
// the view went through the pointer and not the glob.
func TestPointerStateView_readsWhatThePointerNames(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "old")
	writeSnapshot(t, root, "2026-05-30T03-00-00Z", true, "new")

	sqlText := newestPointerFixture(t, root, "2026-04-30T03-00-00Z\n", "2026-04-30T03-00-00Z")
	if strings.Contains(sqlText, "*/"+baseline.SuccessMarker) {
		t.Errorf("pointer-mode file still lists the root for %s markers", baseline.SuccessMarker)
	}
	db := execViews(t, sqlText)
	var status string
	if err := db.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "old" {
		t.Errorf("state view read %q; want \"old\", the snapshot _NEWEST names", status)
	}
}

// A pointer that no longer holds a snapshot name must refuse, naming the
// pointer, rather than resolve to NULL: the dropped-table preflight fails open
// on a NULL directory because it trusts this statement to have raised already.
func TestPointerStateView_refusesAPointerThatIsNotASnapshotName(t *testing.T) {
	for name, content := range map[string]string{
		"empty":        "",
		"garbage":      "hello\n",
		"colon form":   "2026-04-30T03:00:00Z\n",
		"path in it":   "../2026-04-30T03-00-00Z\n",
		"two names":    "2026-04-30T03-00-00Z\n2026-05-30T03-00-00Z\n",
		"only newline": "\n",
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "old")
			_, err := loadViews(t, newestPointerFixture(t, root, content, "2026-04-30T03-00-00Z"))
			if err == nil {
				t.Fatalf("file loaded over a pointer holding %q; want a refusal", content)
			}
			if !strings.Contains(err.Error(), baseline.NewestPointerName) {
				t.Errorf("refusal does not name the pointer: %v", err)
			}
		})
	}
}

// Whitespace a hand edit or another writer might leave around the name is
// not a reason to refuse.
func TestPointerStateView_toleratesSurroundingWhitespace(t *testing.T) {
	root := t.TempDir()
	writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "old")
	db := execViews(t, newestPointerFixture(t, root, " \t2026-04-30T03-00-00Z\r\n", "2026-04-30T03-00-00Z"))
	var status string
	if err := db.QueryRow(`SELECT "status" FROM shop.orders`).Scan(&status); err != nil {
		t.Fatalf("query state view: %v", err)
	}
	if status != "old" {
		t.Errorf("state view read %q; want \"old\"", status)
	}
}

func TestUseNewestPointer(t *testing.T) {
	const discovered = "2026-04-30T03-00-00Z"
	base := func() Input {
		in := goldenInput()
		ApplyFollow(&in, in.BaselineSource, false)
		if in.Follow != FollowNewest {
			t.Fatalf("fixture does not follow by marker: %v", in.Follow)
		}
		return in
	}
	cases := []struct {
		name     string
		mutate   func(*Input)
		ptr      string
		found    bool
		err      error
		want     string
		wantRead bool
	}{
		{name: "names the discovered snapshot", ptr: discovered, found: true, want: discovered, wantRead: true},
		{name: "older than discovered", ptr: "2026-03-30T03-00-00Z", found: true, wantRead: true},
		{name: "newer than discovered", ptr: "2026-05-30T03-00-00Z", found: true, wantRead: true},
		{name: "colon form of the same instant", ptr: "2026-04-30T03:00:00Z", found: true, wantRead: true},
		{name: "absent", wantRead: true},
		{name: "unreadable", err: errors.New("AccessDenied"), wantRead: true},
		{name: "pinned file", mutate: func(in *Input) { in.Follow = FollowNone }, ptr: discovered, found: true},
		{name: "glob character in the root", mutate: func(in *Input) { in.BaselineSource = "s3://my-bucket/base[1]/" },
			ptr: discovered, found: true},
		{name: "brace in the root", mutate: func(in *Input) { in.BaselineSource = "s3://my-bucket/a{b,c}/" },
			ptr: discovered, found: true},
		{name: "doubled slash in the root", mutate: func(in *Input) { in.BaselineSource = "s3://my-bucket/baselines//" },
			ptr: discovered, found: true},
		{name: "dot segment in the root", mutate: func(in *Input) { in.BaselineSource = "s3://my-bucket/x/../baselines/" },
			ptr: discovered, found: true},
		{name: "bucket root", mutate: func(in *Input) { in.BaselineSource = "s3://my-bucket" },
			ptr: discovered, found: true, want: discovered, wantRead: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := base()
			if tc.mutate != nil {
				tc.mutate(&in)
			}
			read := false
			orig := readNewestPointer
			readNewestPointer = func(_ context.Context, root string) (string, bool, error) {
				read = true
				if root != in.BaselineSource {
					t.Errorf("read pointer under %q; want %q", root, in.BaselineSource)
				}
				return tc.ptr, tc.found, tc.err
			}
			t.Cleanup(func() { readNewestPointer = orig })

			UseNewestPointer(context.Background(), &in)
			if in.NewestPointer != tc.want {
				t.Errorf("NewestPointer = %q; want %q", in.NewestPointer, tc.want)
			}
			if read != tc.wantRead {
				t.Errorf("pointer read = %v; want %v", read, tc.wantRead)
			}
		})
	}
}

// TestGenerate_newestPointerGolden pins the pointer-followed render as bytes,
// beside views.newest.golden.sql, which stays the fallback for a root with no
// pointer. Regenerate with `go test ./internal/views -update`.
func TestGenerate_newestPointerGolden(t *testing.T) {
	in := newestInput()
	in.NewestPointer = "2026-04-30T03-00-00Z"
	got := Generate(in)
	golden := filepath.Join("testdata", "views.newest.pointer.golden.sql")
	if *update {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Log("golden updated")
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create it): %v", err)
	}
	if got != string(want) {
		t.Errorf("generated SQL differs from %s.\n--- got ---\n%s\n--- want ---\n%s", golden, got, want)
	}
}
