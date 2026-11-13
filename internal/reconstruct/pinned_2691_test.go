package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// pinRecorder is a PinFunc that records what it pinned and what it released.
type pinRecorder struct {
	pinned, released []string
	what             []string
}

func (p *pinRecorder) pin(_ context.Context, path, what string) func() {
	p.pinned = append(p.pinned, path)
	p.what = append(p.what, what)
	return func() { p.released = append(p.released, path) }
}

func tableFile(t *testing.T, root, snap string) string {
	t.Helper()
	p := filepath.Join(root, snap, "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// FindPinned (#2691): the snapshot found is pinned until the reader
// releases it; one removed between the lookup and the pin is looked up
// again, once.
func TestFindPinned(t *testing.T) {
	at := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	root := t.TempDir()
	present := tableFile(t, root, "2026-06-01T00-00-00Z")
	gone := filepath.Join(root, "2026-05-31T00-00-00Z", "shop", "orders.parquet")
	lookups := func(paths ...string) (FindFunc, *int) {
		n := 0
		return func(context.Context, string, string, time.Time) (string, time.Time, StaleWarning, error) {
			p := paths[min(n, len(paths)-1)]
			n++
			return p, at, StaleWarning{}, nil
		}, &n
	}

	t.Run("the snapshot found is pinned and released by the reader", func(t *testing.T) {
		find, n := lookups(present)
		rec := &pinRecorder{}
		path, _, _, release, err := FindPinned(context.Background(), find, rec.pin, "shop", "orders", at, "a time-travel read")
		if err != nil || path != present || *n != 1 {
			t.Fatalf("path %q, err %v, lookups %d; want %q, nil, 1", path, err, *n, present)
		}
		if len(rec.pinned) != 1 || rec.pinned[0] != present || rec.what[0] != "a time-travel read" || len(rec.released) != 0 {
			t.Fatalf("pinned %v (%v), released %v; want the one snapshot, held", rec.pinned, rec.what, rec.released)
		}
		release()
		if len(rec.released) != 1 {
			t.Fatalf("release did not release the pin: %v", rec.released)
		}
	})

	t.Run("removed before the pin: looked up again, the first pin released", func(t *testing.T) {
		find, n := lookups(gone, present)
		rec := &pinRecorder{}
		path, _, _, release, err := FindPinned(context.Background(), find, rec.pin, "shop", "orders", at, "r")
		defer release()
		if err != nil || path != present || *n != 2 {
			t.Fatalf("path %q, err %v, lookups %d; want the snapshot there now after 2 lookups", path, err, *n)
		}
		if len(rec.released) != 1 || rec.released[0] != gone {
			t.Fatalf("released %v, want only the pin on the snapshot that was gone", rec.released)
		}
	})

	t.Run("still gone at the second lookup: once, not a loop", func(t *testing.T) {
		find, n := lookups(gone)
		rec := &pinRecorder{}
		path, _, _, release, err := FindPinned(context.Background(), find, rec.pin, "shop", "orders", at, "r")
		defer release()
		if err != nil || path != gone || *n != 2 {
			t.Fatalf("path %q, err %v, lookups %d; want the path handed to the read after 2 lookups", path, err, *n)
		}
		if len(rec.pinned) != 2 || len(rec.released) != 1 {
			t.Fatalf("pinned %v, released %v; want two pins, the first released and the second left to the reader", rec.pinned, rec.released)
		}
	})

	t.Run("in S3: pinned through the PinFunc, never looked up again", func(t *testing.T) {
		find, n := lookups("s3://bkt/b/2026-05-31T00-00-00Z/shop/orders.parquet")
		rec := &pinRecorder{}
		_, _, _, release, err := FindPinned(context.Background(), find, rec.pin, "shop", "orders", at, "r")
		defer release()
		if err != nil || *n != 1 {
			t.Fatalf("err %v, lookups %d; want 1", err, *n)
		}
	})

	t.Run("no PinFunc: the lookup alone", func(t *testing.T) {
		find, n := lookups(gone, present)
		path, _, _, release, err := FindPinned(context.Background(), find, nil, "shop", "orders", at, "r")
		release()
		if err != nil || path != gone || *n != 1 {
			t.Fatalf("path %q, err %v, lookups %d; want the lookup's answer as it was, once", path, err, *n)
		}
	})

	t.Run("a lookup that fails pins nothing", func(t *testing.T) {
		rec := &pinRecorder{}
		find := func(context.Context, string, string, time.Time) (string, time.Time, StaleWarning, error) {
			return "", time.Time{}, StaleWarning{}, ErrNoBaseline
		}
		_, _, _, release, err := FindPinned(context.Background(), find, rec.pin, "shop", "orders", at, "r")
		release()
		if !errors.Is(err, ErrNoBaseline) || len(rec.pinned) != 0 {
			t.Fatalf("err %v, pinned %v; want ErrNoBaseline and nothing pinned", err, rec.pinned)
		}
	})
}
