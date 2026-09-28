package baseline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

var viewsReadAt = time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)

func TestNewViewsSkipped(t *testing.T) {
	many := make([]string, ViewsSkippedKept+7)
	for i := range many {
		many[i] = fmt.Sprintf("shop.v%04d", i)
	}
	for _, tt := range []struct {
		name      string
		in        []string
		wantCount int
		wantNames []string
	}{
		{"none", nil, 0, nil},
		{"one", []string{"shop.big_orders"}, 1, []string{"shop.big_orders"}},
		{"sorted", []string{"shop.b", "shop.a"}, 2, []string{"shop.a", "shop.b"}},
		{"the same name twice is one view", []string{"shop.a", "shop.a"}, 1, []string{"shop.a"}},
		{"case is kept, two spellings are two views", []string{"shop.A", "shop.a"}, 2, []string{"shop.A", "shop.a"}},
		{"an empty name is counted and not listed", []string{"", "  ", "shop.a"}, 3, []string{"shop.a"}},
		{"line breaks fold to one line", []string{"shop.a\nb\t c"}, 1, []string{"shop.a b c"}},
		{"bytes that are not text", []string{"shop.\xff\xfe"}, 1, []string{"shop.?"}},
		{"markup is kept as written", []string{"shop.<img src=x onerror=alert(1)>"}, 1, []string{"shop.<img src=x onerror=alert(1)>"}},
		{"past the cap", many, len(many), many[:ViewsSkippedKept]},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := NewViewsSkipped(tt.in, viewsReadAt)
			if rec.Count != tt.wantCount || !slices.Equal(rec.Views, tt.wantNames) {
				t.Fatalf("count %d names %q, want %d %q", rec.Count, rec.Views, tt.wantCount, tt.wantNames)
			}
			if rec.Carried {
				t.Error("a full read is not carried")
			}
			if tt.wantCount > 0 && rec.ReadAt != "2026-09-27T10:00:00Z" {
				t.Errorf("read_at = %q", rec.ReadAt)
			}
		})
	}
	long := NewViewsSkipped([]string{"shop." + strings.Repeat("é", 500)}, viewsReadAt)
	if n := len([]rune(long.Views[0])); n != viewNameCap || !strings.HasSuffix(long.Views[0], "...") {
		t.Errorf("a long name is %d characters, want %d ending in ...", n, viewNameCap)
	}
}

func TestParseViewsSkipped(t *testing.T) {
	for _, tt := range []struct {
		name, body string
		wantOK     bool
		wantErr    bool
		want       ViewsSkipped
	}{
		{"as written", `{"version":1,"read_at":"2026-09-27T10:00:00Z","count":2,"views":["a.v","a.w"]}`, true, false,
			ViewsSkipped{Version: 1, ReadAt: "2026-09-27T10:00:00Z", Count: 2, Views: []string{"a.v", "a.w"}}},
		{"carried", `{"version":1,"read_at":"2026-09-27T10:00:00Z","carried":true,"count":1,"views":["a.v"]}`, true, false,
			ViewsSkipped{Version: 1, ReadAt: "2026-09-27T10:00:00Z", Carried: true, Count: 1, Views: []string{"a.v"}}},
		{"a key this version does not know", `{"version":1,"read_at":"2026-09-27T10:00:00Z","count":1,"views":["a.v"],"later":{"x":1}}`, true, false,
			ViewsSkipped{Version: 1, ReadAt: "2026-09-27T10:00:00Z", Count: 1, Views: []string{"a.v"}}},
		{"a count above the names kept", `{"version":1,"read_at":"2026-09-27T10:00:00Z","count":300,"views":["a.v"]}`, true, false,
			ViewsSkipped{Version: 1, ReadAt: "2026-09-27T10:00:00Z", Count: 300, Views: []string{"a.v"}}},
		{"a count below the names is the names", `{"version":1,"read_at":"2026-09-27T10:00:00Z","count":1,"views":["a.v","a.w"]}`, true, false,
			ViewsSkipped{Version: 1, ReadAt: "2026-09-27T10:00:00Z", Count: 2, Views: []string{"a.v", "a.w"}}},
		{"a date that cannot be read is no date", `{"version":1,"read_at":"yesterday","count":1,"views":["a.v"]}`, true, false,
			ViewsSkipped{Version: 1, Count: 1, Views: []string{"a.v"}}},
		{"names are folded to one line", `{"version":1,"read_at":"2026-09-27T10:00:00Z","count":1,"views":["a.v\nb"]}`, true, false,
			ViewsSkipped{Version: 1, ReadAt: "2026-09-27T10:00:00Z", Count: 1, Views: []string{"a.v b"}}},
		{"zero", `{"version":1,"read_at":"2026-09-27T10:00:00Z","count":0,"views":[]}`, false, false, ViewsSkipped{}},
		{"negative", `{"version":1,"count":-4}`, false, false, ViewsSkipped{}},
		{"empty file", ``, false, true, ViewsSkipped{}},
		{"not JSON", `1 view`, false, true, ViewsSkipped{}},
		{"another shape", `["a.v"]`, false, true, ViewsSkipped{}},
		{"a count that is not a number", `{"version":1,"count":"3","views":["a.v"]}`, false, true, ViewsSkipped{}},
		{"no version", `{"count":1,"views":["a.v"]}`, false, true, ViewsSkipped{}},
		{"a later version", `{"version":2,"count":1,"views":["a.v"]}`, false, true, ViewsSkipped{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := ParseViewsSkipped([]byte(tt.body))
			if ok != tt.wantOK || (err != nil) != tt.wantErr {
				t.Fatalf("ok %v err %v, want ok %v err %v", ok, err, tt.wantOK, tt.wantErr)
			}
			if got.Count != tt.want.Count || got.ReadAt != tt.want.ReadAt || got.Carried != tt.want.Carried ||
				got.Version != tt.want.Version || !slices.Equal(got.Views, tt.want.Views) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestViewsSkipped_writeAndRead(t *testing.T) {
	dir := t.TempDir()
	if _, ok, err := ReadViewsSkipped(dir); ok || err != nil {
		t.Fatalf("a snapshot with no record: ok %v err %v, want neither", ok, err)
	}
	if err := WriteViewsSkipped(dir, NewViewsSkipped([]string{"shop.big_orders"}, viewsReadAt)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadViewsSkipped(dir)
	if !ok || err != nil || got.Count != 1 || !slices.Equal(got.Views, []string{"shop.big_orders"}) {
		t.Fatalf("read back %+v ok %v err %v", got, ok, err)
	}
	// What is on disk, for a reader that is not this program.
	raw, err := os.ReadFile(filepath.Join(dir, ViewsSkippedName))
	if err != nil {
		t.Fatal(err)
	}
	var disk map[string]any
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatalf("the record is not JSON: %v\n%s", err, raw)
	}
	if disk["version"] != float64(1) || disk["count"] != float64(1) || disk["read_at"] != "2026-09-27T10:00:00Z" {
		t.Errorf("on disk: %s", raw)
	}
	if _, has := disk["carried"]; has {
		t.Errorf("a full read writes no carried key: %s", raw)
	}

	// No views: the record of an earlier attempt goes.
	if err := WriteViewsSkipped(dir, NewViewsSkipped(nil, viewsReadAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ViewsSkippedName)); !os.IsNotExist(err) {
		t.Fatalf("a run with no views left a record behind (stat err %v)", err)
	}
	// And removing what is not there is not an error.
	if err := WriteViewsSkipped(dir, ViewsSkipped{}); err != nil {
		t.Fatal(err)
	}
}

func TestReadViewsSkipped_aFileThatCannotBeRead(t *testing.T) {
	for name, body := range map[string][]byte{
		"not JSON":     []byte("<html>"),
		"over the cap": []byte(`{"version":1,"count":1,"views":["` + strings.Repeat("a", maxViewsSkippedBytes) + `"]}`),
	} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, ViewsSkippedName), body, 0o644); err != nil {
			t.Fatal(err)
		}
		got, ok, err := ReadViewsSkipped(dir)
		if ok || err == nil || got.Count != 0 {
			t.Errorf("%s: got %+v ok %v err %v, want an error and no count", name, got, ok, err)
		}
	}
}

// Recording never costs the snapshot.
func TestRecordViewsSkipped_neverFails(t *testing.T) {
	RecordViewsSkipped(filepath.Join(t.TempDir(), "gone"), NewViewsSkipped([]string{"a.v"}, viewsReadAt))
	RecordViewsSkipped(filepath.Join(t.TempDir(), "gone"), NewViewsSkipped(nil, viewsReadAt))
}

func snapshotOf(t *testing.T, out string) string {
	t.Helper()
	snaps, err := filepath.Glob(filepath.Join(out, "20*"))
	if err != nil || len(snaps) != 1 {
		t.Fatalf("snapshot directories = %v (err %v), want exactly one", snaps, err)
	}
	return snaps[0]
}

// The closing case of the issue: two tables and one view.
func TestRun_recordsTheViewsItSkipped_1879(t *testing.T) {
	out := t.TempDir()
	if _, err := Run(context.Background(), Config{InputDir: fixtureViews, OutputDir: out}); err != nil {
		t.Fatal(err)
	}
	snap := snapshotOf(t, out)
	rec, ok, err := ReadViewsSkipped(snap)
	if !ok || err != nil {
		t.Fatalf("no record in the snapshot: ok %v err %v", ok, err)
	}
	if rec.Count != 1 || !slices.Equal(rec.Views, []string{"shop.big_orders"}) || rec.Carried {
		t.Errorf("record = %+v, want 1 view, shop.big_orders, not carried", rec)
	}
	// The record says when the source was read: the snapshot's own time.
	if want := strings.ReplaceAll(rec.ReadAt, ":", "-"); want != filepath.Base(snap) {
		t.Errorf("read_at %q is not the time of the snapshot %s", rec.ReadAt, filepath.Base(snap))
	}
	if _, err := os.Stat(filepath.Join(snap, SuccessMarker)); err != nil {
		t.Errorf("the snapshot is not complete: %v", err)
	}
}

// A schema that holds views only, beside one that holds tables: the view is
// named although the snapshot has no folder for its schema.
func TestRun_aSchemaOfViewsOnly_1879(t *testing.T) {
	out := t.TempDir()
	if _, err := Run(context.Background(), Config{InputDir: fixtureViewsEdge, OutputDir: out}); err != nil {
		t.Fatal(err)
	}
	snap := snapshotOf(t, out)
	rec, ok, err := ReadViewsSkipped(snap)
	if !ok || err != nil {
		t.Fatalf("no record: ok %v err %v", ok, err)
	}
	if !slices.Contains(rec.Views, "onlyviews.v1") {
		t.Errorf("views = %q, want onlyviews.v1 among them", rec.Views)
	}
	if _, err := os.Stat(filepath.Join(snap, "onlyviews")); !os.IsNotExist(err) {
		t.Errorf("the snapshot holds a folder for a schema with no table (stat err %v)", err)
	}
	if rec.Count != len(rec.Views) {
		t.Errorf("count %d, names %d", rec.Count, len(rec.Views))
	}
}

func TestRun_noViewsWritesNoRecord_1879(t *testing.T) {
	at := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	t.Run("a source with no views", func(t *testing.T) {
		dir, out := t.TempDir(), t.TempDir()
		writeFile(t, dir, "d.t-schema.sql", realTableSchema)
		writeFile(t, dir, "d.t.00000.sql", realData)
		if _, err := Run(context.Background(), Config{InputDir: dir, OutputDir: out, Timestamp: at}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(snapshotOf(t, out), ViewsSkippedName)); !os.IsNotExist(err) {
			t.Errorf("a record was written for a source with no views (stat err %v)", err)
		}
	})
	t.Run("a run that names its tables, over an earlier attempt's record", func(t *testing.T) {
		out := t.TempDir()
		if _, err := Run(context.Background(), Config{InputDir: fixtureViews, OutputDir: out, Timestamp: at}); err != nil {
			t.Fatal(err)
		}
		snap := snapshotOf(t, out)
		if _, ok, _ := ReadViewsSkipped(snap); !ok {
			t.Fatal("the first run left no record")
		}
		if _, err := Run(context.Background(), Config{InputDir: fixtureViews, OutputDir: out, Timestamp: at,
			Tables: []string{"shop.orders"}, Retry: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(snap, ViewsSkippedName)); !os.IsNotExist(err) {
			t.Errorf("the record of the earlier attempt is still there (stat err %v)", err)
		}
	})
}

// The record reaches S3 with the snapshot, before the marker that makes the
// snapshot readable there.
func TestUpload_carriesTheViewsRecord_1879(t *testing.T) {
	out := t.TempDir()
	if _, err := Run(context.Background(), Config{InputDir: fixtureViews, OutputDir: out}); err != nil {
		t.Fatal(err)
	}
	snap := snapshotOf(t, out)
	var calls []string
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, k string) error { calls = append(calls, "put "+k); return nil },
		uploadFile:   func(_ context.Context, _, k string) error { calls = append(calls, "upload "+k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, k string) error { calls = append(calls, "delete "+k); return nil },
	}
	if _, err := uploadWithOps(context.Background(), out, "p", false, ops); err != nil {
		t.Fatal(err)
	}
	rec := slices.Index(calls, "upload p/"+filepath.Base(snap)+"/"+ViewsSkippedName)
	done := slices.Index(calls, "upload p/"+filepath.Base(snap)+"/"+SuccessMarker)
	if rec < 0 || done < 0 || rec > done {
		t.Fatalf("record at %d, %s at %d, in:\n%s", rec, SuccessMarker, done, strings.Join(calls, "\n"))
	}
}

// The integrity manifest of a snapshot that holds the record lists the same
// files as before: the record is not a table file, and a manifest that named
// it would make a program from before the record refuse nothing and check
// nothing new.
func TestRun_theManifestDoesNotListTheRecord_1879(t *testing.T) {
	out := t.TempDir()
	if _, err := Run(context.Background(), Config{InputDir: fixtureViews, OutputDir: out}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(snapshotOf(t, out), "_MANIFEST"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Files map[string]string `json:"files"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	var names []string
	for n := range m.Files {
		names = append(names, n)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"shop/customers.parquet", "shop/orders.parquet"}) {
		t.Errorf("manifest lists %q", names)
	}
}
