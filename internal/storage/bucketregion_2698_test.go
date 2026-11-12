package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// #2698, item 3: a caller whose deadline is shorter than the question's own
// time limit used to cut the question short, and a cut-short question was
// remembered as nothing, so every such caller paid the wait again. The
// question now runs on its own context.
func TestDiscoverBucketRegion_shortDeadlineDoesNotCutTheQuestion_2698(t *testing.T) {
	isolateAWSEnv(t)
	var asked atomic.Int32
	release := make(chan struct{})
	questionCut := make(chan error, 1)
	restore := SetBucketRegionDiscoveryForTest(func(ctx context.Context, _, _ string) (string, error) {
		asked.Add(1)
		select {
		case <-release:
			return "us-east-1", nil
		case <-ctx.Done():
			questionCut <- ctx.Err()
			return "", ctx.Err()
		}
	})
	t.Cleanup(restore)
	cfg := aws.Config{Region: "us-west-2"}

	// A: a 20 ms deadline. It stops waiting and gets nothing, at once.
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if r, ok := DiscoverBucketRegion(short, cfg, "slow"); ok || r != "" {
		t.Errorf("the short caller got (%q, %v), want nothing", r, ok)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("the short caller waited %v, past its own deadline", took)
	}

	// B arrives while the question is still out, and waits for it.
	got := make(chan string)
	go func() {
		r, _ := DiscoverBucketRegion(context.Background(), cfg, "slow")
		got <- r
	}()
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-questionCut:
		t.Fatalf("the question was cut short with the caller: %v", err)
	default:
	}
	close(release)
	if r := <-got; r != "us-east-1" {
		t.Errorf("the waiter got %q, want the question's answer us-east-1", r)
	}

	// C finds it remembered: still one question in all.
	if r, ok := DiscoverBucketRegion(context.Background(), cfg, "slow"); r != "us-east-1" || !ok {
		t.Errorf("after the answer: (%q, %v)", r, ok)
	}
	if asked.Load() != 1 {
		t.Errorf("asked %d times, want 1", asked.Load())
	}
}

// Many callers at once, most of them with short deadlines, the first one
// included: one question, and the patient ones all get its answer.
func TestDiscoverBucketRegion_concurrentShortDeadlinesOneQuestion_2698(t *testing.T) {
	isolateAWSEnv(t)
	var asked atomic.Int32
	release := make(chan struct{})
	restore := SetBucketRegionDiscoveryForTest(func(context.Context, string, string) (string, error) {
		asked.Add(1)
		<-release
		return "eu-west-1", nil
	})
	t.Cleanup(restore)
	cfg := aws.Config{Region: "us-west-2"}

	// The first caller, with a short deadline, starts the question and leaves.
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	DiscoverBucketRegion(short, cfg, "busy")

	const n = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	patient := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx := context.Background()
			if i%2 == 0 {
				c, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
				defer cancel()
				ctx = c
			}
			patient[i], _ = DiscoverBucketRegion(ctx, cfg, "busy")
		}()
	}
	close(start)
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if asked.Load() != 1 {
		t.Errorf("%d callers asked %d questions, want 1", n+1, asked.Load())
	}
	for i := 1; i < n; i += 2 {
		if patient[i] != "eu-west-1" {
			t.Errorf("patient caller %d got %q, want eu-west-1", i, patient[i])
		}
	}
}

// A question that hits its own time limit is remembered as a failure for
// bucketRegionMissTTL, like any other failure.
func TestDiscoverBucketRegion_timeLimitIsRememberedAsAFailure_2698(t *testing.T) {
	isolateAWSEnv(t)
	var asked atomic.Int32
	restore := SetBucketRegionDiscoveryForTest(func(ctx context.Context, _, _ string) (string, error) {
		asked.Add(1)
		return "", context.DeadlineExceeded
	})
	t.Cleanup(restore)
	for range 3 {
		DiscoverBucketRegion(context.Background(), aws.Config{Region: "us-west-2"}, "timeout")
	}
	if asked.Load() != 1 {
		t.Errorf("a timed-out question was asked %d times inside its window, want 1", asked.Load())
	}
}

// #2698, item 2: a bucket deleted and created again in another region while
// the process runs. The remembered region is dropped on the 301, once, and
// the next client asks again.
func TestRegionMismatch_301DropsTheRememberedRegionOnce_2698(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_REGION", "us-east-2")
	f := withFakeDiscovery(t, map[string]string{"moved": "us-west-2"})
	ctx := context.Background()

	// The process learned us-west-2 for the bucket.
	if got := clientRegion(t, "moved", ""); got != "us-west-2" {
		t.Fatalf("first client region = %q", got)
	}
	// The bucket is now in us-east-1. A request signed for us-west-2 is
	// redirected (the fake S3 stands for AWS; its client signs us-west-2).
	f.mu.Lock()
	f.answers["moved"] = "us-east-1"
	f.mu.Unlock()
	c := mismatchClient(fake301(t, "us-east-1"))
	_, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("moved"), MaxKeys: aws.Int32(1)})
	var rm *RegionMismatchError
	if !errors.As(err, &rm) || !rm.Forgotten {
		t.Fatalf("a 301 on the remembered region: %v (Forgotten=%v), want it dropped", err, rm != nil && rm.Forgotten)
	}
	want := `the S3 bucket "moved" is now in region us-east-1, not in region us-west-2, which this process had found for it earlier: its region is looked up again the next time bintrail opens the bucket, so retry`
	if !strings.Contains(err.Error(), want) {
		t.Errorf("message:\n%v\nwant it to contain:\n%s", err, want)
	}
	t.Logf("operator sees (dropped, header present): %v", err)

	// The next client asks again, once, and signs for the new region.
	if got := clientRegion(t, "moved", ""); got != "us-east-1" || f.count("moved") != 2 {
		t.Errorf("after the 301: region %q after %d questions, want us-east-1 after 2", got, f.count("moved"))
	}
	// A second 301 signed for the OLD region (a client kept from before)
	// does not drop the new answer: nothing loops.
	_, err = c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("moved"), MaxKeys: aws.Int32(1)})
	if errors.As(err, &rm) && rm.Forgotten {
		t.Error("a 301 on a region the process no longer remembers dropped the new answer")
	}
	for range 3 {
		clientRegion(t, "moved", "")
	}
	if f.count("moved") != 2 {
		t.Errorf("questions = %d after more clients, want still 2", f.count("moved"))
	}
	t.Logf("operator sees (old client, not dropped): %v", err)
}

// No header, a remembered failure, a region the operator passed: none of
// those is a remembered success for the signed region, so nothing is dropped
// (except in the first case, which is one).
func TestRegionMismatch_dropOnlyARememberedSuccessForThatRegion_2698(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_REGION", "us-east-2")
	f := withFakeDiscovery(t, map[string]string{"moved": "us-west-2"})
	ctx := context.Background()
	clientRegion(t, "moved", "")

	// Header absent: still dropped (the signed region is the remembered one).
	c := mismatchClient(fake301(t, ""))
	_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("moved"), Key: aws.String("k")})
	var rm *RegionMismatchError
	if !errors.As(err, &rm) || !rm.Forgotten {
		t.Fatalf("no header: %v, want dropped", err)
	}
	want := `the S3 bucket "moved" is no longer in region us-west-2, which this process had found for it earlier, so S3 answered 301 PermanentRedirect: its region is looked up again the next time bintrail opens the bucket, so retry`
	if !strings.Contains(err.Error(), want) {
		t.Errorf("message:\n%v\nwant it to contain:\n%s", err, want)
	}
	t.Logf("operator sees (dropped, no header): %v", err)

	// A remembered FAILURE keeps its window: not dropped, not asked again.
	clientRegion(t, "down", "") // fails, remembered
	_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("down"), Key: aws.String("k")})
	if errors.As(err, &rm) && rm.Forgotten {
		t.Error("a remembered failure was dropped")
	}
	clientRegion(t, "down", "")
	if f.count("down") != 1 {
		t.Errorf("a remembered failure was asked again after a 301 (%d)", f.count("down"))
	}

	// A region the operator passed is not what was discovered: the 301 of a
	// client built with it leaves the remembered answer alone, and the
	// message keeps naming the setting to change.
	f2 := withFakeDiscovery(t, map[string]string{"far": "us-east-1"})
	clientRegion(t, "far", "")
	_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("far"), Key: aws.String("k")}) // signed us-west-2
	if errors.As(err, &rm) && rm.Forgotten {
		t.Error("a 301 on another region than the remembered one dropped it")
	}
	if !strings.Contains(err.Error(), "AWS_REGION, or the command's S3 region option") {
		t.Errorf("explicit-region 301 lost its remedy: %v", err)
	}
	clientRegion(t, "far", "")
	if f2.count("far") != 1 {
		t.Errorf("far asked %d times, want 1", f2.count("far"))
	}
}

// The same 301 drops DetectBucketRegionCached's own answer (archive reads,
// the views.sql pin), kept up to bucketRegionTTL otherwise.
func TestRegionMismatch_301DropsTheDetectionCacheToo_2698(t *testing.T) {
	isolateAWSEnv(t)
	resetBucketRegionCache()
	t.Cleanup(resetBucketRegionCache)
	withFakeDiscovery(t, map[string]string{"moved": "us-west-2"})
	cfg := aws.Config{Region: "us-east-2"}
	if r, ok := DetectBucketRegionCached(context.Background(), cfg, "moved"); r != "us-west-2" || !ok {
		t.Fatalf("detected (%q, %v)", r, ok)
	}
	if !forgetDiscoveredRegion("moved", "us-west-2") {
		t.Fatal("nothing dropped")
	}
	bucketRegionMu.Lock()
	n := len(bucketRegionCache)
	bucketRegionMu.Unlock()
	if n != 0 {
		t.Errorf("the detection cache still holds %d entries for the moved bucket", n)
	}
}

// The discovery question itself is answered 301 routinely (that is where the
// region header comes from). Its client must not carry the explainer, or a
// question would drop its own answer.
func TestNewAWSOnlyS3Client_hasNoRegionExplainer_2698(t *testing.T) {
	isolateAWSEnv(t)
	srv := fake301(t, "us-east-1")
	c := newAWSOnlyS3Client(aws.Config{Region: "us-west-2", Credentials: aws.AnonymousCredentials{}}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	_, err := c.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String("x")})
	var rm *RegionMismatchError
	if err == nil || errors.As(err, &rm) {
		t.Errorf("discovery client error = %v; want the plain SDK error, not a RegionMismatchError", err)
	}
}
