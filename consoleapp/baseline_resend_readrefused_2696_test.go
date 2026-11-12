package consoleapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// A snapshot S3 could not be asked about is sent again at most once an hour
// (#2696), even when the send succeeds: a destination that takes uploads and
// refuses reads would otherwise get the same snapshots at every cleanup, a
// new version each time under versioning or Object Lock.
func TestResendOfNotAskedWaitsAnHour(t *testing.T) {
	d := t.TempDir()
	names := []string{"a", "b", "c", "d"}
	for _, n := range names {
		snapDir(t, d, n, true)
	}
	clock := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	prevNow, prevSend := resendNow, resendSnapshotFn
	t.Cleanup(func() { resendNow, resendSnapshotFn = prevNow, prevSend })
	resendNow = func() time.Time { return clock }
	sends := 0
	resendSnapshotFn = func(context.Context, string, string, string) error { sends++; return nil }
	prune := func(context.Context, baseline.PruneOptions) (baseline.PruneResult, error) {
		return baseline.PruneResult{KeptNotDurable: 4, Denied: 4, NotAtDestination: names, NotAsked: names}, nil
	}
	run := func() { runBaselinePruneCycle(context.Background(), []baselinePruneTarget{{dir: d, s3: "s3://bkt/p/"}}, 4*time.Hour, prune) }
	var total []int
	for i := 0; i < 5; i++ {
		run()
		total = append(total, sends)
		clock = clock.Add(5 * time.Minute)
	}
	if !slices.Equal(total, []int{3, 3, 3, 3, 3}) {
		t.Fatalf("sends after each cycle %v, want 3 and then none within the hour", total)
	}
	rec, ok, err := baseline.ReadNotInDestination(d)
	if err != nil || !ok || rec.NextSend == nil || !rec.WaitsForAnswer || !rec.NextSend.Equal(time.Date(2026, 11, 12, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("record during the wait %+v ok=%v err=%v: want next_send at 11:00 for a send S3 could not confirm", rec, ok, err)
	}
	if got := baseline.ResentUnconfirmed(d); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("noted as sent %v, want a, b, c", got)
	}
	clock = time.Date(2026, 11, 12, 11, 1, 0, 0, time.UTC)
	run()
	if sends != 6 {
		t.Fatalf("after the hour %d sends, want 6", sends)
	}
}

// A snapshot S3 took and then refused to read is not sent again, and stays
// on the record as such; a snapshot S3 answered it lacks is sent as before,
// with no wait when the send succeeds.
func TestResendSkipsReadRefusedAndSendsTheMissing(t *testing.T) {
	d := t.TempDir()
	for _, n := range []string{"a", "b"} {
		snapDir(t, d, n, true)
	}
	prevSend := resendSnapshotFn
	t.Cleanup(func() { resendSnapshotFn = prevSend })
	var tried []string
	resendSnapshotFn = func(_ context.Context, _, _, name string) error { tried = append(tried, name); return nil }
	prune := func(context.Context, baseline.PruneOptions) (baseline.PruneResult, error) {
		return baseline.PruneResult{KeptNotDurable: 2, Denied: 1, NotAtDestination: []string{"a", "b"}, NotAsked: []string{"b"}, ReadRefused: []string{"b"}}, nil
	}
	for i := 0; i < 2; i++ {
		runBaselinePruneCycle(context.Background(), []baselinePruneTarget{{dir: d, s3: "s3://bkt/p/"}}, 4*time.Hour, prune)
	}
	if !slices.Equal(tried, []string{"a", "a"}) {
		t.Fatalf("tried %v, want a at each cleanup and never b", tried)
	}
	rec, _, _ := baseline.ReadNotInDestination(d)
	if !slices.Equal(rec.ReadRefused, []string{"b"}) || rec.NextSend != nil {
		t.Fatalf("record %+v: want b as read-refused and no wait", rec)
	}
}

// End to end: sending again through the real upload, against an S3 that
// answers 403 for an object it does not have (no s3:ListBucket). The
// retry's "is it there already" is refused for every file, and each is sent.
func TestResendLackingThroughTheRealUploadWithout403Failures(t *testing.T) {
	var mu sync.Mutex
	stored := map[string]bool{}
	var heads403 int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			if stored[r.URL.Path] {
				w.WriteHeader(http.StatusOK)
				return
			}
			heads403++
			w.WriteHeader(http.StatusForbidden)
		case http.MethodPut:
			stored[r.URL.Path] = true
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			delete(stored, r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	t.Cleanup(srv.Close)
	for k, v := range map[string]string{
		"AWS_ACCESS_KEY_ID": "testdummykey", "AWS_SECRET_ACCESS_KEY": "testdummysecret", "AWS_SESSION_TOKEN": "",
		"AWS_REGION": "", "AWS_DEFAULT_REGION": "", "AWS_PROFILE": "", "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_CONFIG_FILE": filepath.Join(t.TempDir(), "none"), "AWS_SHARED_CREDENTIALS_FILE": filepath.Join(t.TempDir(), "none"),
		"AWS_ENDPOINT_URL": "", "AWS_ENDPOINT_URL_S3": "", storage.EnvS3Endpoint: "", storage.EnvS3PathStyle: "",
	} {
		t.Setenv(k, v)
	}
	st, err := storage.NewBucketStore(srv.URL, "", "")
	if err != nil {
		t.Fatal(err)
	}
	storage.SetBucketStores(map[string]storage.BucketStore{"bkt": st})
	t.Cleanup(func() { storage.SetBucketStores(nil) })
	prevUp, prevCut := uploadSnapshot, s3PruneCutFn
	t.Cleanup(func() { uploadSnapshot, s3PruneCutFn = prevUp, prevCut })
	uploadSnapshot, s3PruneCutFn = uploadAndInvalidate, nil

	d := t.TempDir()
	name := "2026-11-10T03-00-00Z"
	if err := os.MkdirAll(filepath.Join(d, name, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{filepath.Join(d, name, "shop", "orders.parquet"), filepath.Join(d, name, baseline.SuccessMarker)} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sup := &baselineSupervisor{ctx: context.Background()}
	if err := sup.resendLacking(context.Background(), d, "s3://bkt/p/", name); err != nil {
		t.Fatalf("the resend failed on the refusals: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if heads403 == 0 || !stored["/bkt/p/"+name+"/shop/orders.parquet"] || !stored["/bkt/p/"+name+"/"+baseline.SuccessMarker] {
		var keys []string
		for k := range stored {
			keys = append(keys, k)
		}
		t.Fatalf("refused HEADs %d, stored %s: want the data and _SUCCESS sent", heads403, strings.Join(keys, ", "))
	}
}
