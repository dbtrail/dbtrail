package baseline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// A destination that takes uploads and refuses to read them back (#2696):
// credentials with s3:PutObject and without s3:GetObject. A HEAD needs
// s3:GetObject (#2701: AWS documents it, and a fake that answered 404 there
// hid that), so once it is gone EVERY question is refused, a key that does
// not exist included, whatever s3:ListBucket says. A send S3 took and a
// refusal after it is the one evidence the cleanup has.

// writeOnlyBucket answers as S3 does. Until getDenied is set it reads: 404
// for what it lacks, "here" for what it holds, and 403 for the objects in
// unreadable once they exist (an object whose read alone is denied). With
// getDenied, every HEAD is 403.
type writeOnlyBucket struct {
	has        map[string]bool
	unreadable map[string]bool
	getDenied  bool
	asked      []string
}

func (b *writeOnlyBucket) probe(_ context.Context, name string) (bool, error) {
	b.asked = append(b.asked, name)
	if b.getDenied || (b.has[name] && b.unreadable[name]) {
		return false, deniedErr(name)
	}
	return b.has[name], nil
}

// cycle runs one cleanup by count and then what the daemon's resend does
// with its result: send each listed snapshot that is not read-refused (S3
// takes it) and note it as sent. It returns the result and how many it sent.
func cycle(t *testing.T, root string, n int, probe durableProbe, has map[string]bool) (PruneResult, int) {
	t.Helper()
	res := s3Count(t, root, n, probe)
	sent := 0
	for _, name := range res.NotAtDestination {
		if slices.Contains(res.ReadRefused, name) {
			continue
		}
		has[name] = true
		if err := MarkResent(root, name, kn); err != nil {
			t.Fatal(err)
		}
		sent++
	}
	return res, sent
}

// The reviewer's case, as S3 answers it: s[0] is missing and sent once; then
// s3:GetObject is taken away, so every question is refused. The one refused
// question that followed a send, with the newest refused too, is a
// destination that takes uploads and refuses reads: nothing more is sent,
// nothing that needed an answer is removed, and the record names both
// causes S3's refusal can have.
func TestReadRefused_aWriteOnlyDestinationStopsTheSends(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := &writeOnlyBucket{has: map[string]bool{s[1]: true, s[2]: true, s[3]: true, s[4]: true}}
	var perCycle []int
	var res PruneResult
	for i := 0; i < 5; i++ {
		if i == 1 {
			b.getDenied = true
		}
		var sent int
		res, sent = cycle(t, root, 1, b.probe, b.has)
		perCycle = append(perCycle, sent)
		if i > 0 && len(res.Pruned) != 0 {
			t.Fatalf("cycle %d removed %v from a destination that confirms nothing", i+1, res.Pruned)
		}
	}
	if !slices.Equal(perCycle, []int{1, 0, 0, 0, 0}) {
		t.Fatalf("sends per cycle %v, want 1 then none", perCycle)
	}
	if !res.DestinationWriteOnly || !slices.Equal(res.ReadRefused, s[:1]) || res.DestinationSilent || res.ProbeErrors != 0 {
		t.Fatalf("want a write-only destination with s[0] read-refused: %+v", res)
	}
	wantOnDisk(t, root, s[0], s[4])
	reason := failureReason(t, root)
	t.Logf("failure record, as the page shows it:\n%s", reason)
	want := "S3 refuses to read back what it accepted: either the credentials lack s3:GetObject on this prefix, or they are no longer valid; check both. DBTrail cannot confirm any snapshot is there, so it keeps them here, and it does not send them again: S3 took each one and then refused to read it."
	if reason != want {
		t.Fatalf("failure record %q, want %q", reason, want)
	}
	rec, ok, err := ReadNotInDestination(root)
	if err != nil || !ok || !slices.Equal(rec.ReadRefused, s[:1]) {
		t.Fatalf("record %+v ok=%v err=%v: want s[0] as read-refused", rec, ok, err)
	}
}

// No s3:GetObject from the start: every question refused and no send ever
// made, so it is the general stop, with no send, and the record names
// s3:GetObject among what to check.
func TestReadRefused_writeOnlyWithNothingMissingIsTheGeneralStop(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := &writeOnlyBucket{has: map[string]bool{}, getDenied: true}
	for _, n := range s {
		b.has[n] = true
	}
	res, sent := cycle(t, root, 1, b.probe, b.has)
	if sent != 0 || !res.DestinationSilent || !res.DestinationDenied || len(res.Pruned) != 0 {
		t.Fatalf("sent %d: want the general stop and nothing sent: %+v", sent, res)
	}
	if reason := failureReason(t, root); !strings.Contains(reason, "Check that the credentials are valid and that they have s3:GetObject on this S3 prefix.") {
		t.Fatalf("failure record %q does not name s3:GetObject", reason)
	}
}

// One snapshot whose read S3 refuses while it reads the others: sent once,
// then kept and not sent again; the others go by the count as usual.
func TestReadRefused_oneSnapshotReadDenied(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := &writeOnlyBucket{has: map[string]bool{s[1]: true, s[2]: true, s[3]: true, s[4]: true},
		unreadable: map[string]bool{s[0]: true}}
	// s[0] is missing and, once sent, unreadable: 404 then 403.
	res, sent := cycle(t, root, 1, b.probe, b.has)
	if sent != 1 || !slices.Equal(res.NotAtDestination, []string{s[0]}) {
		t.Fatalf("first cycle sent %d, listed %v: want s[0] sent", sent, res.NotAtDestination)
	}
	for i := 0; i < 3; i++ {
		res, sent = cycle(t, root, 1, b.probe, b.has)
		if sent != 0 {
			t.Fatalf("cycle %d sent %d again", i+2, sent)
		}
	}
	if !slices.Equal(res.ReadRefused, []string{s[0]}) || res.DestinationWriteOnly || res.DestinationSilent {
		t.Fatalf("want s[0] read-refused, the destination not write-only (it reads the newest): %+v", res)
	}
	wantOnDisk(t, root, s[0], s[4])
	reason := failureReason(t, root)
	t.Logf("failure record, as the page shows it:\n%s", reason)
	if want := "1 snapshot was sent again, and S3 took it but refuses to read it back: either the credentials lack s3:GetObject on this prefix, or they are no longer valid; check both. It is kept here and not sent again."; reason != want {
		t.Fatalf("failure record %q, want %q", reason, want)
	}
}

// A snapshot S3 genuinely lacked (404) is sent, then confirmed and removed by
// the count; its note goes with it.
func TestReadRefused_aMissingSnapshotSentThenConfirmedGoes(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	in := map[string]bool{s[1]: true, s[2]: true, s[3]: true, s[4]: true}
	probe := func(_ context.Context, name string) (bool, error) { return in[name], nil }
	res, sent := cycle(t, root, 1, probe, in)
	if sent != 1 || !slices.Equal(ResentUnconfirmed(root), []string{s[0]}) {
		t.Fatalf("sent %d, noted %v", sent, ResentUnconfirmed(root))
	}
	res, sent = cycle(t, root, 1, probe, in)
	if sent != 0 || !slices.Equal(res.Pruned, []string{s[0]}) || len(ResentUnconfirmed(root)) != 0 {
		t.Fatalf("pruned %v, sent %d, noted %v: want s[0] gone and its note cleared", res.Pruned, sent, ResentUnconfirmed(root))
	}
}

// Credentials that expired: every question refused, the check too, and none
// of the refused was sent before. The general stop, nothing to send.
func TestReadRefused_expiredCredentialsSendNothing(t *testing.T) {
	root := t.TempDir()
	s := five(t, root)
	b := bucketWith(s...)
	b.refuseAll = true
	// One of them was sent once, long ago: not every refusal follows a send.
	if err := MarkResent(root, s[0], kn); err != nil {
		t.Fatal(err)
	}
	has := map[string]bool{}
	res, sent := cycle(t, root, 1, b.probe, has)
	if sent != 0 || !res.DestinationSilent || !res.DestinationDenied || res.DestinationWriteOnly || len(res.NotAtDestination) != 0 {
		t.Fatalf("sent %d: want the general stop and nothing to send: %+v", sent, res)
	}
}

// A 403 whose code says the signature did not match (credentials S3 does not
// accept) is a refusal like any 403: with every question refused, the general
// stop.
func TestReadRefused_signatureMismatchIsARefusal(t *testing.T) {
	root := t.TempDir()
	five(t, root)
	sig := func(_ context.Context, name string) (bool, error) {
		return false, fmt.Errorf("head %s: %w", name, &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
			Err:      &smithy.GenericAPIError{Code: "SignatureDoesNotMatch", Message: "x"},
		})
	}
	res := s3Count(t, root, 1, sig)
	if len(res.Pruned) != 0 || !res.DestinationDenied || !res.DestinationSilent || len(res.NotAtDestination) != 0 {
		t.Fatalf("%+v", res)
	}
}

// After a downgrade, an older build reads only "unasked": the record carries
// the first not-asked snapshot there too.
func TestReadRefused_theRecordKeepsTheOlderField(t *testing.T) {
	root := t.TempDir()
	if err := writeNotInDestination(root, NotInDestination{At: kn, Count: 2, Names: []string{"a", "b"}, NotAsked: []string{"b"}}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, NotInDestinationFile))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["unasked"] != "b" || !strings.Contains(string(raw), `"not_asked":["b"]`) {
		t.Fatalf("record on disk %s: want unasked b beside not_asked", raw)
	}
	rec, _, _ := ReadNotInDestination(root)
	if !slices.Equal(rec.NotAsked, []string{"b"}) || rec.Unasked != "" {
		t.Fatalf("read back %+v", rec)
	}
}
