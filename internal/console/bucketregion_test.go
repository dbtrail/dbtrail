package console

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/dbtrail/dbtrail/internal/storage"
	"github.com/dbtrail/dbtrail/internal/views"
)

func detected(r string) bucketRegionEntry {
	return bucketRegionEntry{region: r, detected: true, at: time.Now()}
}

// A failed detection returns cfg.Region, NOT "" — that is the whole hazard, so
// the fixtures must carry a plausible region with detected=false. An earlier
// version of this test seeded "" for "unresolvable", a value production cannot
// produce, and so asserted the desired behavior for a case it never exercised.
func fellBack(ambient string) bucketRegionEntry {
	return bucketRegionEntry{region: ambient, detected: false, at: time.Now()}
}

// TestLayoutBuckets: both halves of a layout count, because archives and
// baselines can sit in different buckets and the generated views read both.
func TestLayoutBuckets(t *testing.T) {
	in := views.Input{
		ArchiveSources: []string{
			"s3://archives/events/bintrail_id=aaa",
			"/local/archives/bintrail_id=bbb", // not S3: contributes no bucket
			"s3://archives/events/bintrail_id=ccc",
		},
		Baselines: []views.BaselineTable{
			{Path: "s3://baselines/2026-06-10T12-00-00Z/shop/orders.parquet"},
			{Path: "/local/baselines/x.parquet"},
		},
	}
	if got, want := layoutBuckets(in), []string{"archives", "baselines"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("layoutBuckets = %v, want %v", got, want)
	}
	if b := layoutBuckets(views.Input{ArchiveSources: []string{"/only/local"}}); len(b) != 0 {
		t.Errorf("a local-only layout yielded buckets: %v", b)
	}
}

// TestArchiveRegion_pinsOnlyWhatWasDetected is the regression guard for the
// defect this resolver most easily reintroduces. s3:GetBucketLocation is
// deliberately absent from bintrail's documented minimal IAM policy, so a
// failed detection is the COMMON path, and it falls back to the daemon's own
// ambient region — a plausible, confident, wrong value. Writing that into a
// file that runs on someone else's machine OVERRIDES their correct
// configuration; leaving it out lets their credential chain resolve the right
// region on its own. Silence beats a guess here, which is the opposite of the
// read path's trade-off.
func TestArchiveRegion_pinsOnlyWhatWasDetected(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	layout := views.Input{
		ArchiveSources: []string{"s3://archives/events/bintrail_id=aaa"},
		Baselines:      []views.BaselineTable{{Path: "s3://baselines/x/shop/orders.parquet"}},
	}
	cases := []struct {
		name          string
		seed          map[string]bucketRegionEntry
		wantRegion    string
		wantAmbiguous bool
	}{
		{"both detected in one region",
			map[string]bucketRegionEntry{"archives": detected("eu-central-1"), "baselines": detected("eu-central-1")},
			"eu-central-1", false},
		// Two DETECTIONS that differ: a fact, so the file may state it.
		{"two detections disagree",
			map[string]bucketRegionEntry{"archives": detected("eu-central-1"), "baselines": detected("us-west-2")},
			"", true},
		// The regression: one bucket detected, the other only guessed. Pinning
		// the detection would export a value that is right for one bucket and
		// unverified for the other.
		{"one detected, one only guessed",
			map[string]bucketRegionEntry{"archives": detected("eu-central-1"), "baselines": fellBack("us-east-1")},
			"", false},
		// And it must NOT be reported as a disagreement: the two values differ,
		// but one of them is not evidence of anything. Claiming it sends the
		// operator off to split a file that is fine.
		{"a guess that differs is not a disagreement",
			map[string]bucketRegionEntry{"archives": detected("eu-central-1"), "baselines": fellBack("ap-south-1")},
			"", false},
		{"nothing detected",
			map[string]bucketRegionEntry{"archives": fellBack("us-east-1"), "baselines": fellBack("us-east-1")},
			"", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{bucketRegions: tc.seed}
			region, ambiguous := s.archiveRegion(context.Background(), layout)
			if region != tc.wantRegion || ambiguous != tc.wantAmbiguous {
				t.Fatalf("got (%q, %v), want (%q, %v)", region, ambiguous, tc.wantRegion, tc.wantAmbiguous)
			}
		})
	}

	// A local-only layout asks nothing of AWS at all.
	s := &Server{}
	if r, amb := s.archiveRegion(context.Background(), views.Input{ArchiveSources: []string{"/local"}}); r != "" || amb {
		t.Errorf("a local layout resolved a region: (%q, %v)", r, amb)
	}
}

// A detection is a property of the bucket and never expires. A FAILURE is not:
// its cheap causes are transient (a credential refresh, a blip, an operator
// closing the download tab, which cancels the request context), and remembering
// one forever leaves this daemon and the files it hands out disagreeing until
// somebody restarts it.
func TestCachedBucketRegion_failureExpiresButDetectionDoesNot(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	// A cancelled context makes the re-attempt fail immediately, so this test
	// needs no network: what it observes is WHETHER an attempt happened.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stale := time.Now().Add(-negativeRegionTTL - time.Minute)
	s := &Server{bucketRegions: map[string]bucketRegionEntry{
		"fresh-miss": {region: "us-east-1", at: time.Now()},
		"stale-miss": {region: "us-east-1", at: stale},
		"hit":        {region: "eu-central-1", detected: true, at: stale},
	}}
	var cfg aws.Config
	var loaded bool

	if _, ok := s.cachedBucketRegion(ctx, &cfg, &loaded, "hit"); !ok {
		t.Error("a detection older than the negative TTL was discarded; a bucket's region does not move")
	}
	if got := s.bucketRegions["hit"].at; !got.Equal(stale) {
		t.Error("the detection was re-attempted despite being cached")
	}

	// Compared against the value BEFORE the call. The first version of this
	// asserted `!at.Equal(stale) && at.Before(now-1s)` on an entry seeded to
	// time.Now(): the second half is false by construction, so the whole leg
	// was unreachable and removing the negative memoization outright left it
	// green.
	freshBefore := s.bucketRegions["fresh-miss"].at
	s.cachedBucketRegion(ctx, &cfg, &loaded, "fresh-miss")
	if !s.bucketRegions["fresh-miss"].at.Equal(freshBefore) {
		t.Error("a failure still inside the TTL was re-attempted; the memoization is what keeps this off the SQL panel's per-query path")
	}

	before := s.bucketRegions["stale-miss"].at
	s.cachedBucketRegion(ctx, &cfg, &loaded, "stale-miss")
	if s.bucketRegions["stale-miss"].at.Equal(before) {
		t.Error("a failed detection older than the TTL was never retried; a transient cause would stick forever")
	}
}

// A bucket with its own store is signed by its own scoped secret, which
// always names a region; it has no say in the file-wide pin. Asking would go
// to the ambient endpoint (AWS for a MinIO bucket), and a cached answer from
// before the store was set would keep speaking for it.
func TestArchiveRegion_skipsBucketsWithTheirOwnStore(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	st, err := storage.NewBucketStore("http://minio:9000", "", "")
	if err != nil {
		t.Fatal(err)
	}
	storage.SetBucketStores(map[string]storage.BucketStore{"archives": st})
	t.Cleanup(func() { storage.SetBucketStores(nil) })
	layout := views.Input{
		ArchiveSources: []string{"s3://archives/events/bintrail_id=aaa"},
		Baselines:      []views.BaselineTable{{Path: "s3://baselines/x/shop/orders.parquet"}},
	}
	for name, seed := range map[string]bucketRegionEntry{
		"a stale detection": detected("us-west-2"),
		"a guess":           fellBack("us-east-1"),
	} {
		s := &Server{bucketRegions: map[string]bucketRegionEntry{"archives": seed, "baselines": detected("eu-central-1")}}
		region, ambiguous := s.archiveRegion(context.Background(), layout)
		if region != "eu-central-1" || ambiguous {
			t.Errorf("%s cached for the routed bucket: got (%q, %v), want (eu-central-1, false)", name, region, ambiguous)
		}
	}
}
