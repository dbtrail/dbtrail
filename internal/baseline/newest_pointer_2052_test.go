package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeBucket is an in-memory S3 for the upload: every call is recorded in
// order, objects keep their bytes, and an error can be planted per op+key.
type fakeBucket struct {
	calls   []string
	objects map[string][]byte
	fail    map[string]error // "put <key>" / "get <key>"
}

func newFakeBucket() *fakeBucket {
	return &fakeBucket{objects: map[string][]byte{}, fail: map[string]error{}}
}

func (b *fakeBucket) ops() s3UploadOps {
	return s3UploadOps{
		putEmpty: func(_ context.Context, k string) error {
			b.calls = append(b.calls, "put "+k)
			b.objects[k] = nil
			return nil
		},
		uploadFile: func(_ context.Context, _, k string) error {
			b.calls = append(b.calls, "upload "+k)
			b.objects[k] = []byte("x")
			return nil
		},
		objectExists: func(_ context.Context, k string) (bool, error) { _, ok := b.objects[k]; return ok, nil },
		deleteObject: func(_ context.Context, k string) error {
			b.calls = append(b.calls, "delete "+k)
			delete(b.objects, k)
			return nil
		},
		putObject: func(_ context.Context, k string, body []byte) error {
			if err := b.fail["put "+k]; err != nil {
				return err
			}
			b.calls = append(b.calls, "putobject "+k)
			b.objects[k] = append([]byte(nil), body...)
			return nil
		},
		getObject: func(_ context.Context, k string) ([]byte, bool, error) {
			if err := b.fail["get "+k]; err != nil {
				return nil, false, err
			}
			v, ok := b.objects[k]
			return v, ok, nil
		},
	}
}

func mkUploadSnapshot(t *testing.T, root, name string) string {
	t.Helper()
	snap := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{filepath.Join(snap, "shop", "orders.parquet"), filepath.Join(snap, SuccessMarker)} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return snap
}

const ptrKey = "p/" + NewestPointerName

// #2052: after a snapshot is published, the root names it in one small
// object, so a reader following the newest snapshot reads one object
// instead of listing every snapshot the root holds. Written last: after
// every _SUCCESS, so it never names a snapshot a reader cannot use yet.
func TestNewestPointer_writtenLastNamingTheSnapshot_2052(t *testing.T) {
	root := t.TempDir()
	mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
	b := newFakeBucket()
	if _, err := uploadWithOps(context.Background(), root, "p", false, b.ops()); err != nil {
		t.Fatal(err)
	}
	if got := string(b.objects[ptrKey]); got != "2026-10-04T12-00-03Z\n" {
		t.Fatalf("pointer = %q, want the snapshot name and a newline", got)
	}
	ptrAt, lastSuccess := -1, -1
	for i, c := range b.calls {
		if c == "putobject "+ptrKey {
			ptrAt = i
		}
		if strings.HasSuffix(c, "/"+SuccessMarker) {
			lastSuccess = i
		}
	}
	if ptrAt < lastSuccess {
		t.Errorf("pointer written at call %d, before the last _SUCCESS at %d: %v", ptrAt, lastSuccess, b.calls)
	}
}

// The console uploads ONE snapshot directory to <root>/<name>: the pointer
// still goes to the root, next to the snapshot, never inside it.
func TestNewestPointer_singleSnapshotDirUpload_2052(t *testing.T) {
	root := t.TempDir()
	snap := mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
	b := newFakeBucket()
	if _, err := uploadWithOps(context.Background(), snap, "p/2026-10-04T12-00-03Z", false, b.ops()); err != nil {
		t.Fatal(err)
	}
	if got := string(b.objects[ptrKey]); got != "2026-10-04T12-00-03Z\n" {
		t.Errorf("pointer at %s = %q; objects: %v", ptrKey, got, objKeys(b.objects))
	}
}

// Never backward: a sweep that uploads an older snapshot the destination
// lacks leaves the pointer on the newer one; an equal name is not rewritten
// (every PUT is a version on a versioned bucket); a newer one moves it.
func TestNewestPointer_neverMovesBackward_2052(t *testing.T) {
	for _, c := range []struct {
		name, existing, upload, want string
		wrote                        bool
	}{
		{"older upload", "2026-10-04T12-00-03Z\n", "2026-10-03T00-00-00Z", "2026-10-04T12-00-03Z\n", false},
		{"same snapshot", "2026-10-04T12-00-03Z\n", "2026-10-04T12-00-03Z", "2026-10-04T12-00-03Z\n", false},
		{"newer upload", "2026-10-03T00-00-00Z\n", "2026-10-04T12-00-03Z", "2026-10-04T12-00-03Z\n", true},
		// Unreadable content cannot name anything newer: replaced.
		{"garbage pointer", "not a snapshot", "2026-10-04T12-00-03Z", "2026-10-04T12-00-03Z\n", true},
		// Whitespace and a missing newline are still the same name.
		{"same name, no newline", "  2026-10-04T12-00-03Z ", "2026-10-04T12-00-03Z", "  2026-10-04T12-00-03Z ", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			mkUploadSnapshot(t, root, c.upload)
			b := newFakeBucket()
			b.objects[ptrKey] = []byte(c.existing)
			if _, err := uploadWithOps(context.Background(), root, "p", false, b.ops()); err != nil {
				t.Fatal(err)
			}
			if got := string(b.objects[ptrKey]); got != c.want {
				t.Errorf("pointer = %q, want %q", got, c.want)
			}
			wrote := false
			for _, call := range b.calls {
				wrote = wrote || call == "putobject "+ptrKey
			}
			if wrote != c.wrote {
				t.Errorf("pointer written = %v, want %v", wrote, c.wrote)
			}
		})
	}
}

// Two snapshots in one pass: the pointer names the newer, whatever the order
// they were walked in.
func TestNewestPointer_namesTheNewestOfThePass_2052(t *testing.T) {
	root := t.TempDir()
	mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
	mkUploadSnapshot(t, root, "2026-10-03T00-00-00Z")
	b := newFakeBucket()
	if _, err := uploadWithOps(context.Background(), root, "p", false, b.ops()); err != nil {
		t.Fatal(err)
	}
	if got := string(b.objects[ptrKey]); got != "2026-10-04T12-00-03Z\n" {
		t.Errorf("pointer = %q", got)
	}
}

// A pointer that cannot be read or written fails the upload with
// ErrNewestPointer and says the snapshot itself is published, so a caller
// can tell "the backup is there, the shortcut is stale" from a lost upload.
// A read failure must not be taken as "absent": writing blind could move the
// pointer backward.
func TestNewestPointer_failuresAreLoudAndTyped_2052(t *testing.T) {
	for _, op := range []string{"get", "put"} {
		t.Run(op, func(t *testing.T) {
			root := t.TempDir()
			mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
			b := newFakeBucket()
			b.fail[op+" "+ptrKey] = errors.New("AccessDenied")
			_, err := uploadWithOps(context.Background(), root, "p", false, b.ops())
			if !errors.Is(err, ErrNewestPointer) {
				t.Fatalf("err = %v, want ErrNewestPointer", err)
			}
			if !strings.Contains(err.Error(), "AccessDenied") || !strings.Contains(err.Error(), "2026-10-04T12-00-03Z") {
				t.Errorf("err %q does not carry the cause and the snapshot", err)
			}
			if _, ok := b.objects["p/2026-10-04T12-00-03Z/"+SuccessMarker]; !ok {
				t.Error("the snapshot's _SUCCESS is missing: the pointer step ran before the publish")
			}
			if op == "get" {
				for _, c := range b.calls {
					if c == "putobject "+ptrKey {
						t.Error("pointer written although the existing one could not be read")
					}
				}
			}
		})
	}
}

// A destination whose snapshot directory is not snapshot-shaped (an operator
// uploading to a prefix of their own) gets no pointer: there is no root to
// put it in that a reader would look at.
func TestNewestPointer_notSnapshotShapedGetsNone_2052(t *testing.T) {
	root := t.TempDir()
	snap := mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
	b := newFakeBucket()
	if _, err := uploadWithOps(context.Background(), snap, "p/mybackup", false, b.ops()); err != nil {
		t.Fatal(err)
	}
	for k := range b.objects {
		if strings.HasSuffix(k, NewestPointerName) {
			t.Errorf("pointer written at %s for a non-snapshot directory", k)
		}
	}
}

func objKeys(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Upload's real operations include the pointer's two: a nil there would
// silently skip the pointer in production (publishNewestPointers treats nil
// as "a test of another step").
func TestNewS3UploadOps_wiresThePointerOps_2052(t *testing.T) {
	ops := newS3UploadOps(s3.New(s3.Options{Region: "us-west-2"}), "b")
	if ops.putObject == nil || ops.getObject == nil {
		t.Fatal("Upload's ops do not carry putObject/getObject: the pointer would never be written")
	}
	if ops.putEmpty == nil || ops.uploadFile == nil || ops.objectExists == nil || ops.deleteObject == nil || ops.objectURL == nil {
		t.Fatal("an upload op is missing")
	}
}

// The newest of a pass is chosen by name, not by the order the directories
// arrive in (ReadDir happens to sort them ascending today).
func TestNewestPointer_newestRegardlessOfOrder_2052(t *testing.T) {
	root := t.TempDir()
	newer := mkUploadSnapshot(t, root, "2026-10-04T12-00-03Z")
	older := mkUploadSnapshot(t, root, "2026-10-03T00-00-00Z")
	b := newFakeBucket()
	if err := publishNewestPointers(context.Background(), root, "p", []string{newer, older}, b.ops()); err != nil {
		t.Fatal(err)
	}
	if got := string(b.objects[ptrKey]); got != "2026-10-04T12-00-03Z\n" {
		t.Errorf("pointer = %q, want the newer snapshot", got)
	}
}
