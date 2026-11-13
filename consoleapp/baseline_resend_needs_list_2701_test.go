package consoleapp

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2701 point 2: with "Delete from S3 after" set and no s3:ListBucket, S3
// refuses (403) the question whether its own cleanup is removing a
// snapshot's folder. The re-send keeps refusing to send into such a folder
// (a folder an earlier S3 cleanup marked is finished by a later one whatever
// its age, so a copy sent there would go after the local copy went on its
// strength), and the record the page reads names the permission, through
// the wait that follows, until a round fails for something else.
func TestResend_aRefusedS3CleanupQuestionNamesListBucket(t *testing.T) {
	prevCut, prevPresent, prevUp, prevSend, prevNow := s3PruneCutFn, s3ObjectPresent, uploadSnapshot, resendSnapshotFn, resendNow
	t.Cleanup(func() {
		s3PruneCutFn, s3ObjectPresent, uploadSnapshot, resendSnapshotFn, resendNow = prevCut, prevPresent, prevUp, prevSend, prevNow
	})
	clock := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	resendNow = func() time.Time { return clock }
	s3PruneCutFn = func(_ string, now time.Time) (time.Time, bool) { return now.Add(-30 * 24 * time.Hour), true }
	denied := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
		Err:      &smithy.GenericAPIError{Code: "Forbidden", Message: "Forbidden"},
	}
	var present func(context.Context, string) (bool, error)
	present = func(context.Context, string) (bool, error) { return false, denied }
	s3ObjectPresent = func(ctx context.Context, u string) (bool, error) { return present(ctx, u) }
	sent := 0
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) { sent++; return 1, nil }
	root := t.TempDir()
	name := reconstruct.SnapshotDirName(time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
	writeSnapshotFiles(t, filepath.Join(root, name), baseline.SuccessMarker)
	s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	resendSnapshotFn = s.resendLacking

	if err := s.resendLacking(context.Background(), root, "s3://bkt/p/", name); !errors.Is(err, errResendNeedsList) {
		t.Fatalf("err = %v, want errResendNeedsList", err)
	} else {
		t.Logf("error: %v", err)
	}
	prune := func(context.Context, baseline.PruneOptions) (baseline.PruneResult, error) {
		return baseline.PruneResult{KeptNotDurable: 1, NotAtDestination: []string{name}}, nil
	}
	run := func() {
		runBaselinePruneCycle(context.Background(), []baselinePruneTarget{{dir: root, s3: "s3://bkt/p/"}}, 0, prune)
	}
	read := func() baseline.NotInDestination {
		t.Helper()
		rec, ok, err := baseline.ReadNotInDestination(root)
		if err != nil || !ok {
			t.Fatalf("record: ok=%v err=%v", ok, err)
		}
		return rec
	}
	run()
	if rec := read(); !rec.NeedsListBucket || rec.NextSend == nil || sent != 0 {
		t.Fatalf("after the refused round: %+v, sent %d: want the permission named, an hourly retry, nothing sent", rec, sent)
	}
	// During the wait the record keeps saying why.
	clock = clock.Add(5 * time.Minute)
	run()
	if rec := read(); !rec.NeedsListBucket {
		t.Fatalf("the reason is lost during the wait: %+v", rec)
	}
	// The next round fails for another reason: the record stops naming it.
	present = func(context.Context, string) (bool, error) { return false, errors.New("dial tcp: i/o timeout") }
	clock = clock.Add(2 * time.Hour)
	run()
	if rec := read(); rec.NeedsListBucket {
		t.Fatalf("a round that failed for another reason still names s3:ListBucket: %+v", rec)
	}
	if !strings.Contains(errResendNeedsList.Error(), "s3:ListBucket") {
		t.Fatal("the error does not name the permission")
	}
}
