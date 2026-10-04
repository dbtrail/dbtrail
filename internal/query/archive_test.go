package query

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// TestResolveArchiveSourcesRouting pins the per-row routing decisions (#383):
// a local base is preferred only when it actually HOLDS parquet data; an
// existing-but-fileless local tree (post-cleanup: files pruned after S3
// upload, tree left behind) falls back to the S3 copy instead of shadowing
// it; and a registered source is NEVER omitted — when nothing usable
// remains, the unusable local base is returned anyway so the fetch (not
// silence) reports the problem under strict mode (#377).
func TestResolveArchiveSourcesRouting(t *testing.T) {
	dir := t.TempDir()

	// Base with real data — local wins.
	dataBase := filepath.Join(dir, "bintrail_id=with-data")
	if err := os.MkdirAll(filepath.Join(dataBase, "event_date=2026-06-05", "event_hour=10"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataBase, "event_date=2026-06-05", "event_hour=10", "events.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Base that exists but holds no parquet files — the shadow case.
	emptyBase := filepath.Join(dir, "bintrail_id=pruned")
	if err := os.MkdirAll(filepath.Join(emptyBase, "event_date=2026-06-05"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Same fileless shape, but with no S3 columns to fall back to.
	orphanBase := filepath.Join(dir, "bintrail_id=orphan")
	if err := os.MkdirAll(orphanBase, 0o755); err != nil {
		t.Fatal(err)
	}

	// Local path entirely gone (stat fails).
	goneBase := filepath.Join(dir, "bintrail_id=gone") // never created

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	cols := []string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}
	mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows(cols).
		// (1) data present locally + S3 registered → local wins.
		AddRow("with-data", filepath.Join(dataBase, "events.parquet"), "bkt", "events/bintrail_id=with-data/f.parquet").
		// (2) local tree exists but fileless + S3 registered → S3.
		AddRow("pruned", filepath.Join(emptyBase, "events.parquet"), "bkt", "events/bintrail_id=pruned/f.parquet").
		// (3) fileless local, NO S3 → keep the local base (never omit).
		AddRow("orphan", filepath.Join(orphanBase, "events.parquet"), nil, nil).
		// (4) local gone entirely + S3 → S3 (pre-#383 behavior preserved).
		AddRow("gone", filepath.Join(goneBase, "events.parquet"), "bkt", "events/bintrail_id=gone/f.parquet").
		// (5) local gone entirely, NO S3 → keep the local base (NEW: was
		// silently omitted, leaving the planner-claimed coverage with
		// nothing to fail on).
		AddRow("gone-orphan", filepath.Join(dir, "bintrail_id=gone-orphan", "events.parquet"), nil, nil))
	// (1) has data locally AND an S3 copy, so its files are checked one by
	// one (#2078): the one registered file is on disk, local stays.
	mock.ExpectQuery(`SELECT local_path, s3_bucket, s3_key FROM archive_state WHERE bintrail_id = \?`).WithArgs("with-data").
		WillReturnRows(sqlmock.NewRows([]string{"local_path", "s3_bucket", "s3_key"}).
			AddRow(filepath.Join(dataBase, "event_date=2026-06-05", "event_hour=10", "events.parquet"), "bkt", "events/bintrail_id=with-data/f.parquet"))

	got, rerr := ResolveArchiveSources(context.Background(), db)
	if rerr != nil {
		t.Fatalf("unexpected resolver error: %v", rerr)
	}
	want := []string{
		dataBase,
		"s3://bkt/events/bintrail_id=pruned",
		orphanBase,
		"s3://bkt/events/bintrail_id=gone",
		filepath.Join(dir, "bintrail_id=gone-orphan"),
	}
	if len(got) != len(want) {
		t.Fatalf("got %d sources %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sources[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestLocalBaseHasParquet(t *testing.T) {
	dir := t.TempDir()

	// Empty dir → false, no root error.
	if found, rootErr := localBaseHasParquet(dir); found || rootErr != nil {
		t.Errorf("empty dir: found=%v rootErr=%v, want false/nil", found, rootErr)
	}
	// Non-parquet files only → false.
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if found, _ := localBaseHasParquet(dir); found {
		t.Error("dir with only non-parquet files: want false")
	}
	// Parquet directly under base (test-fixture layout) → true.
	if err := os.WriteFile(filepath.Join(dir, "events.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if found, rootErr := localBaseHasParquet(dir); !found || rootErr != nil {
		t.Errorf("parquet directly under base: found=%v rootErr=%v, want true/nil", found, rootErr)
	}

	// Parquet nested in the rotate layout → true.
	nested := t.TempDir()
	sub := filepath.Join(nested, "event_date=2026-06-05", "event_hour=10")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "events.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if found, _ := localBaseHasParquet(nested); !found {
		t.Error("nested parquet: want true")
	}

	// Nonexistent base → false with a root error (callers distinguish
	// "unreadable" from "legitimately pruned" — #383 review).
	if found, rootErr := localBaseHasParquet(filepath.Join(dir, "nope")); found || rootErr == nil {
		t.Errorf("nonexistent base: found=%v rootErr=%v, want false/non-nil", found, rootErr)
	}

	// Unreadable base (no permission bits) → false with a root error.
	// Skipped for root, who bypasses permissions.
	if os.Getuid() != 0 {
		locked := t.TempDir()
		lockedBase := filepath.Join(locked, "bintrail_id=locked")
		if err := os.MkdirAll(lockedBase, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(lockedBase, "events.parquet"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(lockedBase, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(lockedBase, 0o755) })
		found, rootErr := localBaseHasParquet(lockedBase)
		if found || rootErr == nil {
			t.Errorf("unreadable base: found=%v rootErr=%v, want false/non-nil", found, rootErr)
		}
	}
}

func TestExtractBasePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "local path",
			path: "/data/archives/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet",
			want: "/data/archives/bintrail_id=abc-123",
		},
		{
			name: "s3 key",
			path: "prefix/bintrail_id=abc-123/event_date=2026-01-10/event_hour=14/events.parquet",
			want: "prefix/bintrail_id=abc-123",
		},
		{
			name: "no prefix",
			path: "bintrail_id=abc-123/event_date=2026-01-10/events.parquet",
			want: "bintrail_id=abc-123",
		},
		{
			name: "no trailing slash",
			path: "bintrail_id=abc-123",
			want: "bintrail_id=abc-123",
		},
		{
			name: "no bintrail_id marker",
			path: "/data/archives/event_date=2026-01-10/events.parquet",
			want: "",
		},
		{
			name: "empty path",
			path: "",
			want: "",
		},
		{
			name: "deep nesting",
			path: "a/b/c/bintrail_id=97adaf56-fe9e-4c1b-9794-b042f7faf197/event_date=2026-03-05/event_hour=18/events.parquet",
			want: "a/b/c/bintrail_id=97adaf56-fe9e-4c1b-9794-b042f7faf197",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractBasePath(tt.path)
			if got != tt.want {
				t.Errorf("extractBasePath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

// TestResolveArchiveSourcesErrors pins the #383 error-transport contract:
// MySQL error 1146 (archive_state doesn't exist — legitimate on
// pre-archive indexes) stays a silent empty result, while any OTHER
// registry-read failure is returned instead of silently shrinking the
// source list out from under the planner's coverage claims.
func TestResolveArchiveSourcesErrors(t *testing.T) {
	t.Run("1146 table-not-found stays swallowed (back-compat)", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("FROM archive_state").WillReturnError(
			&mysql.MySQLError{Number: 1146, Message: "Table 'idx.archive_state' doesn't exist"})
		got, rerr := ResolveArchiveSources(context.Background(), db)
		if rerr != nil || got != nil {
			t.Fatalf("pre-archive index must resolve to (nil, nil), got (%v, %v)", got, rerr)
		}
	})

	t.Run("any other query error propagates", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		forced := &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"}
		mock.ExpectQuery("FROM archive_state").WillReturnError(forced)
		_, rerr := ResolveArchiveSources(context.Background(), db)
		if !errors.Is(rerr, forced) {
			t.Fatalf("expected wrapped registry error, got %v", rerr)
		}
	})

	t.Run("nil db stays (nil, nil)", func(t *testing.T) {
		got, rerr := ResolveArchiveSources(context.Background(), nil)
		if got != nil || rerr != nil {
			t.Fatalf("nil db must resolve to (nil, nil), got (%v, %v)", got, rerr)
		}
	})
}

// TestResolveArchiveSourcesLocalMustBeComplete pins #2078: a local base that
// holds SOME of a source's archived files must not shadow the S3 copy that
// holds all of them. Every reader globs the base it is handed, and the
// planner counts hours from archive_state, so a partial local base read as
// "the archive" drops the missing hours with no error anywhere.
func TestResolveArchiveSourcesLocalMustBeComplete(t *testing.T) {
	const s3Root = "s3://bkt/arch/bintrail_id=src"
	key := func(hour string) string {
		return "arch/bintrail_id=src/event_date=2026-05-01/event_hour=" + hour + "/events.parquet"
	}
	type row struct {
		hour    string
		onDisk  bool // the local file exists
		noLocal bool // local_path is NULL in the registry
		noS3    bool // the row has no S3 columns
		asDir   bool // a directory sits where the file should be
		moved   bool // the file exists, under ANOTHER local base
		oddKey  bool // the S3 key is under another prefix than the source's
	}
	cases := []struct {
		name    string
		rows    []row
		detail  error // the per-file query fails
		want    string
		wantErr bool
		// neither copy is whole: the one outcome that still reads short,
		// and the log line is the only thing that tells it from "local".
		wantWarn bool
	}{
		{name: "every registered file is on disk: local", rows: []row{{hour: "03", onDisk: true}, {hour: "04", onDisk: true}}, want: "local"},
		{name: "an older hour was uploaded and pruned, a newer one is still on disk: S3",
			rows: []row{{hour: "03"}, {hour: "04", onDisk: true}}, want: s3Root},
		{name: "the newest hour is the missing one: S3", rows: []row{{hour: "03", onDisk: true}, {hour: "04"}}, want: s3Root},
		{name: "a row registered in S3 only (no local path) beside a local file: S3",
			rows: []row{{hour: "03", noLocal: true}, {hour: "04", onDisk: true}}, want: s3Root},
		{name: "a directory where the file should be is not the file: S3",
			rows: []row{{hour: "03", asDir: true}, {hour: "04", onDisk: true}}, want: s3Root},
		{name: "neither copy is complete (an hour archived before S3 was configured): local, as before",
			rows: []row{{hour: "02", onDisk: true, noS3: true}, {hour: "03"}, {hour: "04", onDisk: true}}, want: "local", wantWarn: true},
		{name: "every file exists, one under another local base the reader will not glob: S3",
			rows: []row{{hour: "03", moved: true}, {hour: "04", onDisk: true}}, want: s3Root},
		{name: "local is missing a file and one S3 key is outside the source's prefix: neither",
			rows: []row{{hour: "03", oddKey: true}, {hour: "04", onDisk: true}}, want: "local", wantWarn: true},
		{name: "the per-file read fails: an error, never a guess",
			rows: []row{{hour: "03"}, {hour: "04", onDisk: true}}, detail: errors.New("boom"), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			base := filepath.Join(root, "bintrail_id=src")
			var logged bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
			t.Cleanup(func() { slog.SetDefault(prev) })
			detail := sqlmock.NewRows([]string{"local_path", "s3_bucket", "s3_key"})
			sample := filepath.Join(base, "event_date=2026-05-01", "event_hour=03", "events.parquet")
			for _, r := range tc.rows {
				p := filepath.Join(base, "event_date=2026-05-01", "event_hour="+r.hour, "events.parquet")
				if r.moved {
					p = filepath.Join(root, "old", "bintrail_id=src", "event_date=2026-05-01", "event_hour="+r.hour, "events.parquet")
				}
				switch {
				case r.asDir:
					if err := os.MkdirAll(p, 0o755); err != nil {
						t.Fatal(err)
					}
				case r.onDisk, r.moved:
					if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				var local, bucket, k any = p, "bkt", key(r.hour)
				if r.noLocal {
					local = nil
				}
				if r.noS3 {
					bucket, k = nil, nil
				}
				if r.oddKey {
					k = "elsewhere/bintrail_id=src/event_date=2026-05-01/event_hour=" + r.hour + "/events.parquet"
				}
				detail.AddRow(local, bucket, k)
			}
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery(`MIN\(local_path\)`).WillReturnRows(
				sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
					AddRow("src", sample, "bkt", key("03")))
			q := mock.ExpectQuery(`SELECT local_path, s3_bucket, s3_key FROM archive_state WHERE bintrail_id = \?`).WithArgs("src")
			if tc.detail != nil {
				q.WillReturnError(tc.detail)
			} else {
				q.WillReturnRows(detail)
			}

			got, rerr := ResolveArchiveSources(context.Background(), db)
			if tc.wantErr {
				if !errors.Is(rerr, tc.detail) {
					t.Fatalf("got (%v, %v), want the read error", got, rerr)
				}
				return
			}
			if rerr != nil {
				t.Fatal(rerr)
			}
			want := tc.want
			if want == "local" {
				want = base
			}
			if len(got) != 1 || got[0] != want {
				t.Errorf("sources = %v, want [%s]", got, want)
			}
			if warned := strings.Contains(logged.String(), "neither the local archive base nor the S3 copy"); warned != tc.wantWarn {
				t.Errorf("warned that neither copy is whole = %v, want %v; log:\n%s", warned, tc.wantWarn, logged.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet expectations: %v", err)
			}
		})
	}
}
