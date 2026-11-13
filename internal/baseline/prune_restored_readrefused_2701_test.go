package baseline

import (
	"slices"
	"testing"
	"time"
)

// #2701 point 1: the restore's result in a bucket that takes uploads and
// refuses to read them back. It was asked about on a rule of its own, which
// never read the record of snapshots sent again, so every round sent it once
// more and S3 refused the next question again, for ever (bounded only by the
// hour's wait). Now it is sent once, and after one refusal that followed that
// send it is listed as refused to read, and not sent again.
func TestReadRefused_theRestoreResultIsSentOnce(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	restored := snapName(7 * time.Minute)
	makeSnapshot(t, root, restored, true, "shop/orders")
	if err := MarkRestored(root, restored); err != nil {
		t.Fatal(err)
	}
	// Everything ordinary is in S3 and readable; the restore's result is
	// not there yet, and once sent S3 refuses to read it.
	b := &writeOnlyBucket{has: map[string]bool{}, unreadable: map[string]bool{restored: true}}
	for _, n := range s {
		b.has[n] = true
	}
	var sentPerCycle []int
	var res PruneResult
	for i := 0; i < 4; i++ {
		var sent int
		res, sent = cycle(t, root, 1, b.probe, b.has)
		sentPerCycle = append(sentPerCycle, sent)
	}
	if !slices.Equal(sentPerCycle, []int{1, 0, 0, 0}) {
		t.Fatalf("sends per cycle %v, want [1 0 0 0]: the restore's result was sent again after S3 refused to read it", sentPerCycle)
	}
	if !slices.Equal(res.ReadRefused, []string{restored}) || res.RestoredUnasked != restored || !slices.Contains(res.NotAtDestination, restored) {
		t.Fatalf("the restore's result after the refusal: %+v", res)
	}
	reason := failureReason(t, root)
	t.Logf("failure record: %q", reason)
	if reason != "1 snapshot was sent again, and S3 took it but refuses to read it back: either the credentials lack s3:GetObject on this prefix, or they are no longer valid; check both. It is kept here and not sent again." {
		t.Errorf("the failure record says %q", reason)
	}
	// A refusal that followed no send is a question S3 would not answer,
	// not a refused read: it is sent (once per round, as before).
	root2 := t.TempDir()
	s2 := five(t, root2)
	makeSnapshot(t, root2, restored, true, "shop/orders")
	if err := MarkRestored(root2, restored); err != nil {
		t.Fatal(err)
	}
	b2 := &noListBucket{has: map[string]bool{}}
	for _, n := range s2 {
		b2.has[n] = true
	}
	res = s3Count(t, root2, 1, b2.probe)
	if len(res.ReadRefused) != 0 || !slices.Equal(res.NotAsked, []string{restored}) {
		t.Fatalf("an unsent restore result refused once: %+v", res)
	}
}

// S3 answering about the restore's result clears its mark: a later refusal
// is then a question S3 would not answer, not a refused read of that send.
func TestReadRefused_anAnswerAboutTheRestoreResultClearsItsMark(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	restored := snapName(7 * time.Minute)
	makeSnapshot(t, root, restored, true, "shop/orders")
	if err := MarkRestored(root, restored); err != nil {
		t.Fatal(err)
	}
	if err := MarkResent(root, restored, kn); err != nil {
		t.Fatal(err)
	}
	b := bucketWith(append(slices.Clone(s), restored)...)
	s3Count(t, root, 1, b.probe)
	if slices.Contains(ResentUnconfirmed(root), restored) {
		t.Fatalf("S3 confirmed the restore's result and its mark stays: %v", ResentUnconfirmed(root))
	}
}

// The same as S3 answers once s3:GetObject is taken away after the restore's
// result was sent: every question is refused from then on, and the result
// is still sent only once.
func TestReadRefused_theRestoreResultIsSentOnceWhenGetObjectGoes(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	restored := snapName(7 * time.Minute)
	makeSnapshot(t, root, restored, true, "shop/orders")
	if err := MarkRestored(root, restored); err != nil {
		t.Fatal(err)
	}
	b := &writeOnlyBucket{has: map[string]bool{}}
	for _, n := range s {
		b.has[n] = true
	}
	var per []int
	var res PruneResult
	for i := 0; i < 5; i++ {
		if i == 1 {
			b.getDenied = true
		}
		var sent int
		res, sent = cycle(t, root, 1, b.probe, b.has)
		per = append(per, sent)
	}
	if !slices.Equal(per, []int{1, 0, 0, 0, 0}) {
		t.Fatalf("sends per cycle %v, want [1 0 0 0 0]", per)
	}
	if !slices.Equal(res.ReadRefused, []string{restored}) {
		t.Fatalf("the restore's result: %+v", res)
	}
}
