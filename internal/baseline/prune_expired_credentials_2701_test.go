package baseline

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// #2701 point 3: credentials that stop working right after a re-send that S3
// took. Every question is refused, the re-sent snapshot's included, and the
// newest checked too, which is also what a destination without s3:GetObject
// does to the snapshots it holds. The two are told apart by a key no
// snapshot has: such a destination answers "not here" (it may list), and
// refused credentials refuse that as well.

// switchBucket answers as a readable bucket that may list (404 for what it
// lacks) until refuse is set, then refuses everything.
type switchBucket struct {
	has    map[string]bool
	refuse bool
	asked  []string
}

func (b *switchBucket) probe(_ context.Context, name string) (bool, error) {
	b.asked = append(b.asked, name)
	if b.refuse {
		return false, deniedErr(name)
	}
	return b.has[name], nil
}

func TestReadRefused_expiredCredentialsAreNotReadRefusals(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	// s[0] is missing from S3; the others are there.
	b := &switchBucket{has: map[string]bool{s[1]: true, s[2]: true, s[3]: true, s[4]: true}}

	// 1. S3 answers: s[0] is sent again and marked.
	if _, sent := cycle(t, root, 1, b.probe, b.has); sent != 1 {
		t.Fatalf("first cleanup sent %d, want 1", sent)
	}
	if !slices.Equal(ResentUnconfirmed(root), []string{s[0]}) {
		t.Fatalf("marks after the send: %v", ResentUnconfirmed(root))
	}

	// 2. The credentials stop working: everything is refused.
	b.refuse = true
	res, sent := cycle(t, root, 1, b.probe, b.has)
	if res.DestinationWriteOnly || len(res.ReadRefused) != 0 {
		t.Fatalf("refused credentials read as a destination that refuses reads: %+v", res)
	}
	if !res.DestinationDenied || !res.DestinationSilent || len(res.Pruned) != 0 || sent != 0 {
		t.Fatalf("want the general refusal, nothing removed, nothing sent: %+v (sent %d)", res, sent)
	}
	if !slices.Contains(b.asked, absentSnapshot) {
		t.Fatalf("the key no snapshot has was not asked: %v", b.asked)
	}
	reason := failureReason(t, root)
	t.Logf("failure record: %q", reason)
	if strings.Contains(reason, "s3:GetObject") || !strings.Contains(reason, "Check that the credentials are valid and that they may read this S3 prefix.") {
		t.Errorf("the failure record names the wrong cause: %q", reason)
	}
	if !slices.Equal(ResentUnconfirmed(root), []string{s[0]}) {
		t.Errorf("the mark of the re-send is lost while S3 refuses everything: %v", ResentUnconfirmed(root))
	}

	// 3. They work again: it heals, the mark goes, the record clears, and
	// s[0], now confirmed, goes past the count (the others went in 1).
	b.refuse = false
	res, _ = cycle(t, root, 1, b.probe, b.has)
	if res.DestinationDenied || res.DestinationWriteOnly || !slices.Equal(res.Pruned, []string{s[0]}) {
		t.Fatalf("after the credentials work again: %+v", res)
	}
	if m := ResentUnconfirmed(root); len(m) != 0 {
		t.Errorf("marks left after S3 answered: %v", m)
	}
	if reason := failureReason(t, root); reason != "" {
		t.Errorf("a healed folder still records %q", reason)
	}
}
