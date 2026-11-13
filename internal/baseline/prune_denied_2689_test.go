package baseline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// A refusal (403) on ONE snapshot is that snapshot not confirmed, not the
// bucket going silent (#2689). Credentials without s3:ListBucket make a HEAD
// of a key that does not exist answer 403 instead of 404, so a snapshot whose
// upload failed is refused at every cycle. Counted as a silent bucket, it
// stopped every removal of the count for good while the folder grew by one
// snapshot per interval, and it was never listed as missing, so it was never
// sent again.

// deniedErr is what storage.S3ObjectExists returns for a HEAD that S3
// refused: a HEAD carries no body, so the code is the status text.
func deniedErr(name string) error {
	return fmt.Errorf("head s3://b/p/%s/_SUCCESS: %w", name, &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
		Err:      &smithy.GenericAPIError{Code: "Forbidden", Message: "Forbidden"},
	})
}

// noListBucket is a bucket read with credentials that may not list it: a
// snapshot it holds answers yes, any other is refused. It records what it
// was asked.
type noListBucket struct {
	has   map[string]bool
	asked []string
	// fail, when set for a name, is returned instead (a network error).
	fail map[string]error
	// refuseAll refuses every question: credentials that may not read the
	// bucket at all, or that S3 does not accept (a HEAD cannot tell which).
	refuseAll bool
}

func (b *noListBucket) probe(_ context.Context, name string) (bool, error) {
	b.asked = append(b.asked, name)
	if err := b.fail[name]; err != nil {
		return false, err
	}
	if b.refuseAll || !b.has[name] {
		return false, deniedErr(name)
	}
	return true, nil
}

func bucketWith(names ...string) *noListBucket {
	b := &noListBucket{has: map[string]bool{}, fail: map[string]error{}}
	for _, n := range names {
		b.has[n] = true
	}
	return b
}

func failureReason(t *testing.T, root string) string {
	t.Helper()
	rec, ok, err := ReadLastPruneFailure(root)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return ""
	}
	return rec.Reason
}

// One refused among confirmations: the confirmed ones go by the count, the
// refused one stays, is listed to be sent again, and is on record as one S3
// could not be asked about, never as one S3 answered it lacks.
func TestDenied_oneRefusedAmongConfirmations(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := bucketWith(s[1], s[2], s[3], s[4]) // s[0]'s upload failed
	res := s3Count(t, root, 1, b.probe)
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, s[1:4]) {
		t.Fatalf("pruned %v, want the three confirmed past the count %v", res.Pruned, s[1:4])
	}
	wantOnDisk(t, root, s[0], s[4])
	if res.DestinationSilent || res.DestinationDenied || res.ProbeErrors != 0 || res.KeptUnasked != 0 {
		t.Fatalf("one refused snapshot stopped the count: %+v", res)
	}
	if !slices.Equal(res.NotAtDestination, []string{s[0]}) || !slices.Equal(res.NotAsked, []string{s[0]}) || res.Denied != 1 {
		t.Fatalf("missing %v, not asked %v, denied %d: want %s listed to send and as not asked", res.NotAtDestination, res.NotAsked, res.Denied, s[0])
	}
	rec, ok, err := ReadNotInDestination(root)
	if err != nil || !ok || rec.Count != 1 || !slices.Equal(rec.Names, []string{s[0]}) || !slices.Equal(rec.NotAsked, []string{s[0]}) {
		t.Fatalf("the record the page reads: %+v ok=%v err=%v, want %s as not asked", rec, ok, err, s[0])
	}
	reason := failureReason(t, root)
	t.Logf("failure record, as the page shows it:\n%s", reason)
	want := "1 snapshot could not be checked at the S3 destination: S3 refused the question (access denied), so it is kept here until S3 confirms it has it. S3 answers that way about a copy it does not have when the credentials lack s3:ListBucket, and about every copy when they lack s3:GetObject."
	if reason != want {
		t.Fatalf("failure record %q, want %q", reason, want)
	}
}

// The bound: every question refused, the newest snapshot's included. These
// credentials may not read the bucket at all (or S3 does not accept them:
// a HEAD cannot tell expired or invalid keys from a missing object), so
// nothing can be confirmed. That is the general stop, as for a bucket that
// does not answer, and NOT "every snapshot is missing, send them all".
func TestDenied_everyQuestionRefusedIsTheGeneralStop(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := bucketWith(s...)
	b.refuseAll = true
	res := s3Count(t, root, 1, b.probe)
	if len(res.Pruned) != 0 {
		t.Fatalf("pruned %v while S3 refused every question", res.Pruned)
	}
	wantOnDisk(t, root, s...)
	if !res.DestinationSilent || !res.DestinationDenied || res.ProbeErrors != 4 || len(res.NotAtDestination) != 0 || len(res.NotAsked) != 0 || res.Denied != 0 {
		t.Fatalf("want the general stop, 4 unchecked, nothing listed to send: %+v", res)
	}
	// The newest snapshot was asked, as the check that S3 answers at all.
	if !slices.Contains(b.asked, s[4]) {
		t.Fatalf("asked %v: the newest snapshot was not asked as the check", b.asked)
	}
	if _, ok, _ := ReadNotInDestination(root); ok {
		t.Fatal("snapshots reported as not in S3 by a bucket that refused every question")
	}
	reason := failureReason(t, root)
	t.Logf("failure record, as the page shows it:\n%s", reason)
	for _, want := range []string{
		"The S3 destination could not be asked, so no snapshot that was sent to S3 was removed from this folder in this cleanup. DBTrail asks again at the next one.",
		"S3 refused every question about these snapshots (access denied), so DBTrail cannot tell which ones it has. Check that the credentials are valid and that they have s3:GetObject on this S3 prefix.",
	} {
		if !strings.Contains(reason, want) {
			t.Fatalf("failure record %q does not say %q", reason, want)
		}
	}
	if strings.Contains(reason, "s3:ListBucket") {
		t.Fatalf("the general stop blames a single snapshot's permission: %q", reason)
	}
}

// The shape of the bug once the count has caught up: the one snapshot past
// the count is the one whose upload failed, so it is the only question, and
// it is refused. The newest snapshot, which S3 has, is asked as the check:
// S3 answers, so the refusal is about that one snapshot, which is listed to
// send. Without the check, a lone refusal would read as "every question
// refused" and the snapshot would never be sent again.
func TestDenied_aLoneRefusalIsCheckedAgainstTheNewest(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := bucketWith(s[4]) // the older ones were never sent
	res := s3Count(t, root, 4, b.probe)
	if len(res.Pruned) != 0 || res.DestinationSilent || res.DestinationDenied || res.ProbeErrors != 0 {
		t.Fatalf("want nothing removed and no stop: %+v", res)
	}
	if !slices.Equal(res.NotAtDestination, []string{s[0]}) || !slices.Equal(res.NotAsked, []string{s[0]}) {
		t.Fatalf("missing %v, not asked %v: want %s listed to send", res.NotAtDestination, res.NotAsked, s[0])
	}
	if !slices.Equal(b.asked, []string{s[0], s[4]}) {
		t.Fatalf("asked %v, want %s and then the newest %s as the check", b.asked, s[0], s[4])
	}
}

// The check goes at most three snapshots back, newest first, and an error
// that is not a refusal ends it as the general stop.
func TestDenied_theCheckIsBoundedAndAnErrorStopsIt(t *testing.T) {
	root := t.TempDir()
	var s []string
	for _, m := range []int{60, 50, 40, 30, 20, 10} {
		s = append(s, minutes(t, root, m)...)
	}
	// Only the oldest is past a count of 5. None of the newer ones are in S3.
	b := bucketWith(s[1])
	res := s3Count(t, root, 5, b.probe)
	if !res.DestinationSilent || !res.DestinationDenied || res.ProbeErrors != 1 || len(res.NotAtDestination) != 0 {
		t.Fatalf("no check answered: want the general stop: %+v", res)
	}
	if want := []string{s[0], s[5], s[4], s[3]}; !slices.Equal(b.asked, want) {
		t.Fatalf("asked %v, want %v (three checks, newest first)", b.asked, want)
	}
	// The second check answers with a network error: the stop, and no
	// further check.
	b = bucketWith()
	b.fail[s[4]] = errors.New("dial tcp: i/o timeout")
	res = s3Count(t, root, 5, b.probe)
	if !res.DestinationSilent || res.DestinationDenied || res.ProbeErrors != 1 {
		t.Fatalf("a check that failed is the general stop, not a refusal of every question: %+v", res)
	}
	if want := []string{s[0], s[5], s[4]}; !slices.Equal(b.asked, want) {
		t.Fatalf("asked %v, want %v", b.asked, want)
	}
	// A check that answers "not here" (404) is an answer: the refusal is the
	// one snapshot's.
	asked := 0
	res = s3Count(t, root, 5, func(_ context.Context, name string) (bool, error) {
		asked++
		if name == s[0] {
			return false, deniedErr(name)
		}
		return false, nil
	})
	if res.DestinationSilent || !slices.Equal(res.NotAsked, []string{s[0]}) || asked != 2 {
		t.Fatalf("a 404 check did not count as an answer (asked %d): %+v", asked, res)
	}
}

// A refusal and a network error in the same cycle: the bucket did not answer
// everything, so it is the general stop, and the refused snapshot is
// counted with the unchecked ones, not listed as missing.
func TestDenied_aRefusalBesideANetworkErrorIsTheGeneralStop(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := bucketWith(s[2], s[3], s[4])
	b.fail[s[1]] = errors.New("dial tcp: i/o timeout")
	res := s3Count(t, root, 1, b.probe) // s[0] refused, s[1] errors, s[2] and s[3] confirmed
	if len(res.Pruned) != 0 {
		t.Fatalf("pruned %v with a network error in the cycle", res.Pruned)
	}
	wantOnDisk(t, root, s...)
	if !res.DestinationSilent || res.DestinationDenied || res.ProbeErrors != 2 || res.KeptUnasked != 2 || len(res.NotAtDestination) != 0 || len(res.NotAsked) != 0 {
		t.Fatalf("want the general stop with 2 unchecked and 2 held back, nothing listed: %+v", res)
	}
	if reason := failureReason(t, root); strings.Contains(reason, "refused") {
		t.Fatalf("a network error reads as a refusal: %q", reason)
	}
}

// A clean "not here" (404, credentials that may list the bucket) behaves as
// before: listed as missing, and NOT as one S3 could not be asked about.
func TestDenied_aMissingSnapshotIsStillMissing(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	res := s3Count(t, root, 1, answers(nil, s[1], s[2], s[3]))
	if !slices.Equal(res.NotAtDestination, []string{s[0]}) || len(res.NotAsked) != 0 || res.Denied != 0 {
		t.Fatalf("missing %v, not asked %v: want %s missing, and answered", res.NotAtDestination, res.NotAsked, s[0])
	}
	rec, ok, _ := ReadNotInDestination(root)
	if !ok || rec.Count != 1 || len(rec.NotAsked) != 0 {
		t.Fatalf("record %+v: a snapshot S3 answered it lacks is on record as not asked", rec)
	}
	if reason := failureReason(t, root); reason != "" {
		t.Fatalf("a clean answer recorded a failure: %q", reason)
	}
}

// The refused snapshot is sent again, and once S3 has it, the count removes
// it: the folder heals.
func TestDenied_theRefusedSnapshotIsRemovedOnceSent(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := bucketWith(s[1], s[2], s[3], s[4])
	res := s3Count(t, root, 1, b.probe)
	if !slices.Equal(res.NotAtDestination, []string{s[0]}) {
		t.Fatalf("not listed to send: %v", res.NotAtDestination)
	}
	b.has[s[0]] = true // what the resend does
	res = s3Count(t, root, 1, b.probe)
	if !slices.Equal(res.Pruned, []string{s[0]}) || len(res.NotAtDestination) != 0 {
		t.Fatalf("pruned %v, missing %v: want %s gone once S3 has it", res.Pruned, res.NotAtDestination, s[0])
	}
	wantOnDisk(t, root, s[4])
	if _, ok, _ := ReadNotInDestination(root); ok {
		t.Fatal("the record still lists a snapshot S3 now has")
	}
	if reason := failureReason(t, root); reason != "" {
		t.Fatalf("a healed folder still records a failure: %q", reason)
	}
}

// The restore's result keeps its own rule (#2681): a failed question about
// it, a refusal included, lists it to send and stops nothing; it is not an
// ordinary refusal (no failure line), and it is not the check either.
func TestDenied_theRestoreResultIsUnaffected(t *testing.T) {
	root := t.TempDir()
	s := minutes(t, root, 25, 20, 15, 10, 5)
	restored := snapName(7 * time.Minute)
	makeSnapshot(t, root, restored, true, "shop/orders")
	if err := MarkRestored(root, restored); err != nil {
		t.Fatal(err)
	}
	b := bucketWith(s...)
	res := s3Count(t, root, 1, b.probe)
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, s[:4]) || res.DestinationSilent || res.Denied != 0 {
		t.Fatalf("pruned %v: want the four past the count, no stop, no ordinary refusal: %+v", res.Pruned, res)
	}
	if res.RestoredUnasked != restored || !slices.Equal(res.NotAsked, []string{restored}) || !slices.Equal(res.NotAtDestination, []string{restored}) {
		t.Fatalf("the restore's result: %+v", res)
	}
	if reason := failureReason(t, root); reason != "" {
		t.Fatalf("a refused question about the restore's result recorded a failure: %q", reason)
	}
	// With a refused ordinary snapshot as well, the restore's own refusal is
	// not taken as the check that S3 answers: the newest is.
	root2 := t.TempDir()
	s2 := minutes(t, root2, 25, 5)
	makeSnapshot(t, root2, restored, true, "shop/orders")
	if err := MarkRestored(root2, restored); err != nil {
		t.Fatal(err)
	}
	b2 := bucketWith() // nothing is in S3: every question refused
	res = s3Count(t, root2, 1, b2.probe)
	if !res.DestinationSilent || !res.DestinationDenied || len(res.NotAsked) != 1 || res.NotAsked[0] != restored {
		t.Fatalf("want the general stop for the ordinary one, the restore's result still on its own rule: %+v", res)
	}
	if want := []string{s2[0], s2[1], restored}; !slices.Equal(b2.asked, want) || len(res.Pruned) != 0 {
		t.Fatalf("asked %v, pruned %v: want %v (the newest as the check, then the restore's result), nothing removed", b2.asked, res.Pruned, want)
	}
}

// With no S3 destination nobody is asked anything: the count alone.
func TestDenied_noDestinationIsUnaffected(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := bucketWith()
	b.refuseAll = true
	res, err := pruneWithProbe(context.Background(), PruneOptions{LocalDir: root, KeepNewest: 1, Now: kn}, b.probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.asked) != 0 || len(res.Pruned) != 4 || len(res.NotAsked) != 0 {
		t.Fatalf("asked %v, pruned %v: want the count alone", b.asked, res.Pruned)
	}
	wantOnDisk(t, root, s[4])
}

// The age mode uses the same rule: a refused snapshot is listed to send and
// is not counted as unchecked; every question refused is unchecked, as an
// error always was there.
func TestDenied_theAgeModeToo(t *testing.T) {
	ageOpts := func(root string) PruneOptions {
		return PruneOptions{LocalDir: root, S3URL: "s3://b/p", Retain: days(15), Now: kn}
	}
	root := t.TempDir()
	s := five(t, root)
	b := bucketWith(s[1], s[2], s[3], s[4])
	res, err := pruneWithProbe(context.Background(), ageOpts(root), b.probe)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, s[1:4]) || res.ProbeErrors != 0 || !slices.Equal(res.NotAsked, []string{s[0]}) || !slices.Equal(res.NotAtDestination, []string{s[0]}) {
		t.Fatalf("pruned %v: %+v", res.Pruned, res)
	}
	root2 := t.TempDir()
	five(t, root2)
	b2 := bucketWith()
	b2.refuseAll = true
	res, err = pruneWithProbe(context.Background(), ageOpts(root2), b2.probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Pruned) != 0 || res.ProbeErrors != 4 || !res.DestinationDenied || res.DestinationSilent || len(res.NotAtDestination) != 0 {
		t.Fatalf("every question refused in the age mode: %+v", res)
	}
	reason := failureReason(t, root2)
	t.Logf("failure record (age mode, every question refused):\n%s", reason)
	if !strings.Contains(reason, "4 snapshots could not be checked at the S3 destination, so they were kept.") ||
		!strings.Contains(reason, "S3 refused every question about these snapshots (access denied)") {
		t.Fatalf("failure record %q", reason)
	}
}

// A record an older build wrote names the restore's result in "unasked": it
// is read as not asked.
func TestDenied_anOlderRecordIsReadAsNotAsked(t *testing.T) {
	root := t.TempDir()
	b, _ := json.Marshal(map[string]any{"at": kn, "count": 1, "names": []string{"x"}, "unasked": "x"})
	if err := os.WriteFile(filepath.Join(root, NotInDestinationFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
	rec, ok, err := ReadNotInDestination(root)
	if err != nil || !ok || !slices.Equal(rec.NotAsked, []string{"x"}) {
		t.Fatalf("record %+v ok=%v err=%v: want x as not asked", rec, ok, err)
	}
}

// Sending again with credentials without s3:ListBucket (#2689): the retry's
// "is it there already" is refused for every file not there yet, and each is
// sent instead of failing the whole send; a failure that is not a refusal
// still fails it.
func TestDenied_theRetrySendsWhatItCannotAskAbout(t *testing.T) {
	outputDir := t.TempDir()
	snap := filepath.Join(outputDir, "2025-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{filepath.Join(snap, "shop", "orders.parquet"), filepath.Join(snap, SuccessMarker)} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	run := func(existsErr func(key string) error) ([]string, error) {
		var uploaded []string
		ops := s3UploadOps{
			putEmpty:     func(context.Context, string) error { return nil },
			uploadFile:   func(_ context.Context, _, k string) error { uploaded = append(uploaded, k); return nil },
			objectExists: func(_ context.Context, k string) (bool, error) { return false, existsErr(k) },
			deleteObject: func(context.Context, string) error { return nil },
		}
		_, err := uploadWithOps(context.Background(), outputDir, "p", true, lockedOps(ops))
		return uploaded, err
	}
	uploaded, err := run(func(k string) error { return deniedErr(k) })
	if err != nil || len(uploaded) != 2 {
		t.Fatalf("refused questions: uploaded %v, err %v; want both files sent", uploaded, err)
	}
	if _, err := run(func(string) error { return errors.New("dial tcp: i/o timeout") }); err == nil {
		t.Fatal("a network error while asking was taken for a refusal")
	}
}
