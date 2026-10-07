package baseline

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestNormalizeWriter(t *testing.T) {
	cases := []struct {
		name, in, want string
		ok             bool
	}{
		{"a uuid", "3e11fa47-71ca-11e1-9e33-c80aa9429562", "3e11fa47-71ca-11e1-9e33-c80aa9429562", true},
		{"upper case is the same writer", "3E11FA47-71CA-11E1-9E33-C80AA9429562", "3e11fa47-71ca-11e1-9e33-c80aa9429562", true},
		{"spaces around it", "  abc-1 \n", "abc-1", true},
		{"empty", "", "", false},
		{"only spaces", " \t\n", "", false},
		{"a space inside", "abc 1", "", false},
		{"two lines", "abc\ndef", "", false},
		{"a path separator", "abc/def", "", false},
		{"a backslash", `abc\def`, "", false},
		{"a parent folder", "..", "", false},
		{"only punctuation", "-._", "", false},
		{"not ascii", "señor", "", false},
		{"too long", strings.Repeat("a", 129), "", false},
		{"the longest allowed", strings.Repeat("a", 128), strings.Repeat("a", 128), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := NormalizeWriter(tc.in)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("NormalizeWriter(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestWritersFromNames(t *testing.T) {
	cases := []struct {
		name       string
		names      []string
		writers    []string
		unreadable []string
	}{
		{"an unsigned snapshot names nobody", []string{"_SUCCESS", "_MANIFEST", "views.sql"}, nil, nil},
		{"one signature", []string{"_SUCCESS", "_WRITER.abc-1"}, []string{"abc-1"}, nil},
		{"upper and lower case of one id are one writer", []string{"_WRITER.ABC-1", "_WRITER.abc-1"}, []string{"abc-1"}, nil},
		{"two writers published the same timestamp", []string{"_WRITER.b", "_WRITER.a"}, []string{"a", "b"}, nil},
		{"an empty signature", []string{"_WRITER."}, nil, []string{"_WRITER."}},
		{"a signature with spaces", []string{"_WRITER. abc "}, nil, []string{"_WRITER. abc "}},
		{"a signature with a space inside", []string{"_WRITER.a b"}, nil, []string{"_WRITER.a b"}},
		{"an unreadable one beside a good one", []string{"_WRITER.a b", "_WRITER.abc"}, []string{"abc"}, []string{"_WRITER.a b"}},
		{"a name that only resembles the marker", []string{"_WRITER", "_WRITERS.abc", "x_WRITER.abc"}, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, u := WritersFromNames(tc.names)
			if !slices.Equal(w, tc.writers) || !slices.Equal(u, tc.unreadable) {
				t.Fatalf("WritersFromNames(%q) = %q, %q; want %q, %q", tc.names, w, u, tc.writers, tc.unreadable)
			}
		})
	}
}

func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

func TestWriteWriterMarker(t *testing.T) {
	t.Run("signs with the normalized id and reads back", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteWriterMarker(dir, " ABC-1 "); err != nil {
			t.Fatal(err)
		}
		if got := names(t, dir); !slices.Equal(got, []string{"_WRITER.abc-1"}) {
			t.Fatalf("directory holds %q", got)
		}
		w, u, err := ReadSnapshotWriters(dir)
		if err != nil || !slices.Equal(w, []string{"abc-1"}) || len(u) != 0 {
			t.Fatalf("read back %q, %q, %v", w, u, err)
		}
	})
	t.Run("an empty writer signs nothing and is not an error", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteWriterMarker(dir, "  "); err != nil {
			t.Fatal(err)
		}
		if got := names(t, dir); len(got) != 0 {
			t.Fatalf("directory holds %q", got)
		}
	})
	t.Run("a retried directory is signed by the writer that completes it", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteWriterMarker(dir, "first"); err != nil {
			t.Fatal(err)
		}
		if err := WriteWriterMarker(dir, "second"); err != nil {
			t.Fatal(err)
		}
		if got := names(t, dir); !slices.Equal(got, []string{"_WRITER.second"}) {
			t.Fatalf("directory holds %q", got)
		}
	})
	t.Run("an unsigned run over a signed directory leaves no signature", func(t *testing.T) {
		dir := t.TempDir()
		if err := WriteWriterMarker(dir, "first"); err != nil {
			t.Fatal(err)
		}
		if err := WriteWriterMarker(dir, ""); err != nil {
			t.Fatal(err)
		}
		if got := names(t, dir); len(got) != 0 {
			t.Fatalf("directory holds %q", got)
		}
	})
	t.Run("an identity that cannot be a file name is refused and nothing is written", func(t *testing.T) {
		for _, bad := range []string{"a/b", "../x", "a b", "a\nb"} {
			dir := t.TempDir()
			if err := WriteWriterMarker(dir, bad); err == nil {
				t.Fatalf("%q signed a snapshot", bad)
			}
			if got := names(t, dir); len(got) != 0 {
				t.Fatalf("%q left %q", bad, got)
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "x")); err == nil {
				t.Fatalf("%q wrote outside the snapshot", bad)
			}
		}
	})
	t.Run("a directory that is not there is an error", func(t *testing.T) {
		if err := WriteWriterMarker(filepath.Join(t.TempDir(), "gone"), "abc"); err == nil {
			t.Fatal("no error")
		}
	})
	t.Run("the other files of the snapshot are left alone", func(t *testing.T) {
		dir := t.TempDir()
		for _, n := range []string{"_MANIFEST", "_INCOMPLETE", "views.sql"} {
			if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Mkdir(filepath.Join(dir, "_WRITER.folder"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := WriteWriterMarker(dir, "abc"); err != nil {
			t.Fatal(err)
		}
		if got := names(t, dir); !slices.Equal(got, []string{"_INCOMPLETE", "_MANIFEST", "_WRITER.abc", "_WRITER.folder", "views.sql"}) {
			t.Fatalf("directory holds %q", got)
		}
	})
}

// SignSnapshot never fails a snapshot: a signature that cannot be written
// is a warning, and the producer goes on to the manifest and the marker.
func TestSignSnapshotNeverFails(t *testing.T) {
	SignSnapshot(filepath.Join(t.TempDir(), "gone"), "abc")
	SignSnapshot(t.TempDir(), "a/b")
}

// runSigned converts a one-table dump with cfg's writer and returns the
// snapshot directory it published.
func runSigned(t *testing.T, writer string, deltas bool) string {
	t.Helper()
	inputDir, outputDir := t.TempDir(), t.TempDir()
	for name, body := range map[string]string{
		"metadata":               sampleMetadata,
		"shop.orders-schema.sql": sampleSchema,
		"shop.orders.00000.sql":  "INSERT INTO `orders` VALUES(1,10,'9.99','n','2025-01-01 00:00:00','2025-01-15');\n",
	} {
		if err := os.WriteFile(filepath.Join(inputDir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Run(context.Background(), Config{InputDir: inputDir, OutputDir: outputDir, Compression: "none",
		RowGroupSize: 100, WriterID: writer, TableDeltas: deltas}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var snap string
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			snap = filepath.Join(outputDir, e.Name())
		}
	}
	if snap == "" {
		t.Fatal("no snapshot directory was published")
	}
	if _, err := os.Stat(filepath.Join(snap, SuccessMarker)); err != nil {
		t.Fatalf("the snapshot is not complete: %v", err)
	}
	return snap
}

// The conversion from a dump signs what it publishes, with table deltas
// written beside the tables or not.
func TestRun_signsTheSnapshot(t *testing.T) {
	const id = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	for _, deltas := range []bool{false, true} {
		snap := runSigned(t, strings.ToUpper(id), deltas)
		w, bad, err := ReadSnapshotWriters(snap)
		if err != nil || !slices.Equal(w, []string{id}) || len(bad) != 0 {
			t.Fatalf("deltas=%v: the snapshot is signed by %q (unreadable %q, err %v), want %s", deltas, w, bad, err, id)
		}
	}
}

// Without an identity the snapshot is published as before, unsigned.
func TestRun_withoutAWriterPublishesUnsigned(t *testing.T) {
	snap := runSigned(t, "", false)
	w, bad, err := ReadSnapshotWriters(snap)
	if err != nil || len(w) != 0 || len(bad) != 0 {
		t.Fatalf("writers %q, unreadable %q, err %v", w, bad, err)
	}
}

// An identity that cannot sign does not cost the snapshot.
func TestRun_aBadWriterStillPublishes(t *testing.T) {
	snap := runSigned(t, "not/an id", false)
	if w, _, _ := ReadSnapshotWriters(snap); len(w) != 0 {
		t.Fatalf("signed by %q", w)
	}
}

// The signature reaches S3 with the snapshot, before the marker that makes
// the snapshot readable there.
func TestUpload_carriesTheSignature(t *testing.T) {
	snap := runSigned(t, "abc-1", false)
	var calls []string
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, k string) error { calls = append(calls, "put "+k); return nil },
		uploadFile:   func(_ context.Context, _, k string) error { calls = append(calls, "upload "+k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, k string) error { calls = append(calls, "delete "+k); return nil },
	}
	if _, err := uploadWithOps(context.Background(), filepath.Dir(snap), "p", false, lockedOps(ops)); err != nil {
		t.Fatal(err)
	}
	sig := slices.Index(calls, "upload p/"+filepath.Base(snap)+"/_WRITER.abc-1")
	done := slices.Index(calls, "upload p/"+filepath.Base(snap)+"/"+SuccessMarker)
	if sig < 0 || done < 0 || sig > done {
		t.Fatalf("signature at %d, %s at %d, in:\n%s", sig, SuccessMarker, done, strings.Join(calls, "\n"))
	}
}
