package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// #2212: an S3-only server's update publishes its unchanged tables by copying
// the previous snapshot's objects inside S3. The local snapshot directory
// holds only what changed; the copies travel beside it as a list, and the
// upload performs them INSIDE its crash-safety bracket. The edge cases,
// written before the code:
//   - mixed: local files are uploaded, listed files are copied, and nothing
//     listed is ever uploaded; every copy finishes before _SUCCESS;
//   - the final key set holds every table, local and copied, and the pair
//     files of a copied chain;
//   - a copy that fails: an error, no _SUCCESS, _INCOMPLETE left standing;
//   - copies given for a directory that is not ONE snapshot: refused before
//     anything is sent (the keys would land in the wrong prefix);
//   - a listed path that escapes the snapshot, is absolute, is not a table
//     file, is listed twice or is ALSO on disk: refused before anything is sent;
//   - --retry: a copy whose key exists is skipped, like an upload;
//   - ops with no copy operation: refused, never a silent skip;
//   - the views file is regenerated with the copied tables in it, even when
//     every table was copied and the fold wrote no local views file.

const copyStamp = "2026-10-07T12-00-00Z"

func copySnapshot(t *testing.T, local ...string) string {
	t.Helper()
	snap := filepath.Join(t.TempDir(), copyStamp)
	for _, rel := range append(local, SuccessMarker) {
		p := filepath.Join(snap, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return snap
}

type copyRecorder struct {
	calls  []string
	failOn string
}

func (r *copyRecorder) ops() s3UploadOps {
	return lockedOps(s3UploadOps{
		putEmpty:     func(_ context.Context, k string) error { r.calls = append(r.calls, "put "+k); return nil },
		uploadFile:   func(_ context.Context, _, k string) error { r.calls = append(r.calls, "upload "+k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, k string) error { r.calls = append(r.calls, "delete "+k); return nil },
		copyObject: func(_ context.Context, src, k string) error {
			r.calls = append(r.calls, "copy "+k+" <- "+src)
			if r.failOn != "" && strings.HasSuffix(k, r.failOn) {
				return errors.New("AccessDenied")
			}
			return nil
		},
	})
}

func (r *copyRecorder) index(pred func(string) bool) (first, last int) {
	first, last = -1, -1
	for i, c := range r.calls {
		if pred(c) {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	return first, last
}

var prevSnap = "s3://bkt/srv/2026-10-07T11-00-00Z/"

func mixedCopies() []RemoteCopy {
	return []RemoteCopy{
		{Rel: "shop/customers.parquet", Src: prevSnap + "shop/customers.parquet"},
		{Rel: "shop/customers.000000.posdel", Src: prevSnap + "shop/customers.000000.posdel"},
		{Rel: "shop/customers.000000.upserts", Src: prevSnap + "shop/customers.000000.upserts"},
		{Rel: "crm/leads.parquet", Src: prevSnap + "crm/leads.parquet"},
	}
}

func TestUploadCopies_mixedCopiesInsideTheBracketAndUploadsNothingCopied(t *testing.T) {
	snap := copySnapshot(t, "shop/orders.parquet", "shop/orders.000000.posdel", "shop/orders.000000.upserts")
	r := &copyRecorder{}
	prefix := "srv/" + copyStamp
	n, err := uploadWithOpsCopies(context.Background(), snap, prefix, false, r.ops(), mixedCopies())
	if err != nil {
		t.Fatal(err)
	}
	if n != 3+4+1 {
		t.Errorf("count = %d, want 3 uploads + 4 copies + _SUCCESS", n)
	}
	putInc, _ := r.index(func(c string) bool { return strings.HasPrefix(c, "put ") && strings.Contains(c, IncompleteMarker) })
	firstCopy, lastCopy := r.index(func(c string) bool { return strings.HasPrefix(c, "copy ") })
	success, _ := r.index(func(c string) bool { return strings.HasPrefix(c, "upload ") && strings.HasSuffix(c, SuccessMarker) })
	if putInc < 0 || firstCopy < 0 || success < 0 || putInc > firstCopy || lastCopy > success {
		t.Fatalf("order wrong (put_incomplete=%d copies=%d..%d success=%d): %v", putInc, firstCopy, lastCopy, success, r.calls)
	}
	// Every table of the snapshot ends up under the new prefix, by one
	// route or the other, and nothing listed as a copy is ever uploaded.
	got := map[string]string{}
	for _, c := range r.calls {
		verb, rest, _ := strings.Cut(c, " ")
		key, _, _ := strings.Cut(rest, " <- ")
		if verb == "upload" || verb == "copy" {
			if prev, dup := got[key]; dup {
				t.Errorf("%s reached the bucket twice (%s, then %s)", key, prev, verb)
			}
			got[key] = verb
		}
	}
	want := map[string]string{
		prefix + "/shop/orders.parquet":           "upload",
		prefix + "/shop/orders.000000.posdel":     "upload",
		prefix + "/shop/orders.000000.upserts":    "upload",
		prefix + "/shop/customers.parquet":        "copy",
		prefix + "/shop/customers.000000.posdel":  "copy",
		prefix + "/shop/customers.000000.upserts": "copy",
		prefix + "/crm/leads.parquet":             "copy",
		prefix + "/" + SuccessMarker:              "upload",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %q, want %q (calls: %v)", k, got[k], v, r.calls)
		}
	}
	if len(got) != len(want) {
		t.Errorf("keys = %v, want exactly %v", got, want)
	}
	if !slices.Contains(r.calls, "copy "+prefix+"/crm/leads.parquet <- "+prevSnap+"crm/leads.parquet") {
		t.Errorf("the copy does not name its source: %v", r.calls)
	}
}

func TestUploadCopies_aFailedCopyWritesNoSuccess(t *testing.T) {
	snap := copySnapshot(t, "shop/orders.parquet")
	r := &copyRecorder{failOn: "crm/leads.parquet"}
	_, err := uploadWithOpsCopies(context.Background(), snap, "srv/"+copyStamp, false, r.ops(), mixedCopies())
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") || !strings.Contains(err.Error(), "crm/leads.parquet") {
		t.Fatalf("err = %v, want the copy failure naming the file", err)
	}
	for _, c := range r.calls {
		if strings.HasSuffix(c, SuccessMarker) || strings.HasPrefix(c, "delete ") {
			t.Fatalf("a failed copy still published or unbracketed the snapshot: %v", r.calls)
		}
	}
}

func TestUploadCopies_refusedBeforeAnythingIsSent(t *testing.T) {
	root := filepath.Dir(copySnapshot(t, "shop/orders.parquet"))
	cases := []struct {
		name   string
		dir    string
		copies []RemoteCopy
		ops    func(*copyRecorder) s3UploadOps
	}{
		{"a root holding a snapshot, not the snapshot", root, mixedCopies(), nil},
		{"escapes the snapshot", "", []RemoteCopy{{Rel: "../x/t.parquet", Src: prevSnap + "x/t.parquet"}}, nil},
		{"absolute", "", []RemoteCopy{{Rel: "/shop/t.parquet", Src: prevSnap + "shop/t.parquet"}}, nil},
		{"not a table file", "", []RemoteCopy{{Rel: "shop/notes.txt", Src: prevSnap + "shop/notes.txt"}}, nil},
		{"the marker", "", []RemoteCopy{{Rel: SuccessMarker + ".parquet", Src: prevSnap + "x"}}, nil},
		{"no schema folder", "", []RemoteCopy{{Rel: "t.parquet", Src: prevSnap + "t.parquet"}}, nil},
		{"source not in S3", "", []RemoteCopy{{Rel: "shop/t.parquet", Src: "/tmp/shop/t.parquet"}}, nil},
		{"listed twice", "", []RemoteCopy{{Rel: "shop/t.parquet", Src: prevSnap + "shop/t.parquet"}, {Rel: "shop/t.parquet", Src: prevSnap + "shop/t.parquet"}}, nil},
		{"also on disk", "", []RemoteCopy{{Rel: "shop/orders.parquet", Src: prevSnap + "shop/orders.parquet"}}, nil},
		{"no copy operation", "", mixedCopies(), func(r *copyRecorder) s3UploadOps {
			o := r.ops()
			o.copyObject = nil
			return o
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := tc.dir
			if dir == "" {
				dir = copySnapshot(t, "shop/orders.parquet")
			}
			r := &copyRecorder{}
			ops := r.ops()
			if tc.ops != nil {
				ops = tc.ops(r)
			}
			_, err := uploadWithOpsCopies(context.Background(), dir, "srv", false, ops, tc.copies)
			if err == nil {
				t.Fatal("accepted")
			}
			if len(r.calls) != 0 {
				t.Fatalf("refused only after sending %v", r.calls)
			}
			t.Log(err)
		})
	}
}

func TestUploadCopies_retrySkipsACopyThatIsThere(t *testing.T) {
	snap := copySnapshot(t, "shop/orders.parquet")
	r := &copyRecorder{}
	ops := r.ops()
	ops.objectExists = func(_ context.Context, k string) (bool, error) { return strings.HasSuffix(k, "crm/leads.parquet"), nil }
	if _, err := uploadWithOpsCopies(context.Background(), snap, "srv", true, ops, mixedCopies()); err != nil {
		t.Fatal(err)
	}
	for _, c := range r.calls {
		if strings.HasPrefix(c, "copy ") && strings.Contains(c, "crm/leads.parquet <-") {
			t.Fatalf("retry copied an object that was there: %v", r.calls)
		}
	}
}

func TestUploadCopies_viewsAreRegeneratedWithTheCopiedTables(t *testing.T) {
	var gotExtra []RemoteCopy
	calls := 0
	SetSnapshotViewsRespeller(func(_ context.Context, snapshotDir, root string, extra []RemoteCopy) (string, bool, error) {
		calls++
		gotExtra = extra
		return "-- views", true, nil
	})
	t.Cleanup(func() { SetSnapshotViewsRespeller(nil) })

	// Every table copied: the fold had nothing to write locally, so no
	// local views file exists to trigger the respell.
	snap := copySnapshot(t)
	r := &copyRecorder{}
	ops := r.ops()
	ops.objectURL = func(k string) string { return "s3://bkt/" + k }
	if _, err := uploadWithOpsCopies(context.Background(), snap, "srv/"+copyStamp, false, ops, mixedCopies()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(gotExtra) != 4 {
		t.Fatalf("respell calls=%d extra=%v, want one call carrying the four copied files", calls, gotExtra)
	}
	if !slices.Contains(r.calls, "upload srv/"+copyStamp+"/"+SnapshotViewsName) {
		t.Fatalf("no views file published: %v", r.calls)
	}

	// A local views file beside copies is regenerated ONCE, not twice.
	calls = 0
	snap = copySnapshot(t, "shop/orders.parquet", SnapshotViewsName)
	r = &copyRecorder{}
	ops = r.ops()
	ops.objectURL = func(k string) string { return "s3://bkt/" + k }
	if _, err := uploadWithOpsCopies(context.Background(), snap, "srv/"+copyStamp, false, ops, mixedCopies()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("respell ran %d times", calls)
	}
}
