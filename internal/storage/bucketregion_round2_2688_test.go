package storage

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Twenty connections on a cold process wait for ONE question, not twenty.
func TestDiscoverBucketRegion_concurrentAskersShareOneQuestion_2688(t *testing.T) {
	isolateAWSEnv(t)
	var asked atomic.Int32
	release := make(chan struct{})
	restore := SetBucketRegionDiscoveryForTest(func(_ context.Context, _, bucket string) (string, error) {
		asked.Add(1)
		<-release
		return "us-east-1", nil
	})
	t.Cleanup(restore)

	const n = 20
	var wg sync.WaitGroup
	regions := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			regions[i], _ = DiscoverBucketRegion(context.Background(), aws.Config{Region: "us-west-2"}, "cold")
		}()
	}
	// Let every goroutine reach the cache before the one question returns.
	deadline := time.Now().Add(2 * time.Second)
	for asked.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if got := asked.Load(); got != 1 {
		t.Errorf("%d concurrent askers asked %d questions, want 1", n, got)
	}
	for i, r := range regions {
		if r != "us-east-1" {
			t.Fatalf("asker %d got %q, want the shared answer", i, r)
		}
	}
}

// A waiter whose own context ends stops waiting; and when the asker gives up
// (its context cancelled), a waiter asks in its place instead of inheriting
// nothing forever.
func TestDiscoverBucketRegion_waitersAndCancelledAsker_2688(t *testing.T) {
	isolateAWSEnv(t)
	var asked atomic.Int32
	first := make(chan struct{})
	restore := SetBucketRegionDiscoveryForTest(func(ctx context.Context, _, _ string) (string, error) {
		if asked.Add(1) == 1 {
			close(first)
			<-ctx.Done()
			return "", ctx.Err()
		}
		return "eu-west-3", nil
	})
	t.Cleanup(restore)

	askerCtx, cancelAsker := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		DiscoverBucketRegion(askerCtx, aws.Config{}, "b")
		close(done)
	}()
	<-first

	short, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	if _, ok := DiscoverBucketRegion(short, aws.Config{}, "b"); ok {
		t.Error("a waiter whose context ended got an answer")
	}

	waiter := make(chan string)
	go func() {
		r, _ := DiscoverBucketRegion(context.Background(), aws.Config{}, "b")
		waiter <- r
	}()
	time.Sleep(20 * time.Millisecond)
	cancelAsker()
	<-done
	if r := <-waiter; r != "eu-west-3" {
		t.Errorf("the waiter got %q after the asker gave up, want its own answer", r)
	}
	if asked.Load() != 2 {
		t.Errorf("asked %d times, want 2 (the cancelled asker, then the waiter)", asked.Load())
	}
}

// An endpoint_url in ~/.aws/config is a custom endpoint too: the SDK only
// resolves it when a client is built, and the bucket is not asked about.
func TestDiscoverBucketRegion_endpointURLInSharedConfig_2688(t *testing.T) {
	isolateAWSEnv(t)
	cfgFile := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(cfgFile, []byte("[default]\nregion = us-west-2\nendpoint_url = http://127.0.0.1:9000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", cfgFile)
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "")
	f := withFakeDiscovery(t, map[string]string{"minio": "us-east-1"})
	c, err := NewS3ClientForBucket(context.Background(), "minio", "")
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(c.Options().BaseEndpoint) != "http://127.0.0.1:9000" {
		t.Fatalf("the shared config's endpoint_url did not apply (%q): the test proves nothing", aws.ToString(c.Options().BaseEndpoint))
	}
	if r := c.Options().Region; r != "us-west-2" || f.count("minio") != 0 {
		t.Errorf("region = %q after %d questions, want us-west-2 and none", r, f.count("minio"))
	}
}

// The real question is bounded: one attempt, cut at bucketRegionProbeTimeout,
// against a server that accepts and never answers.
func TestHeadBucketRegion_timeLimitAndOneAttempt_2688(t *testing.T) {
	isolateAWSEnv(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var accepted atomic.Int32
	var conns []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			mu.Lock()
			conns = append(conns, c) // held open, never answered
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	cfg := aws.Config{Region: "us-west-2", BaseEndpoint: aws.String("http://" + ln.Addr().String())}
	start := time.Now()
	_, err = realHeadBucketRegion(context.Background(), cfg, "silent-bucket")
	took := time.Since(start)
	if err == nil {
		t.Fatal("a server that never answers produced a region")
	}
	if took < bucketRegionProbeTimeout-500*time.Millisecond || took > bucketRegionProbeTimeout+2*time.Second {
		t.Errorf("the question took %v, want about %v", took, bucketRegionProbeTimeout)
	}
	if n := accepted.Load(); n != 1 {
		t.Errorf("%d connection attempts, want exactly 1 (no retries)", n)
	}
	t.Logf("gave up after %v, %d attempt: %v", took.Round(time.Millisecond), accepted.Load(), err)
}

// A bucket with its own store in the web interface takes its region from that
// store; a redirect says to fix the store, not AWS_REGION.
func TestRegionMismatch_bucketStorePointsAtTheStore_2688(t *testing.T) {
	isolateAWSEnv(t)
	ctx := context.Background()
	for _, tc := range []struct{ header, want string }{
		{"us-east-1", `the S3 bucket "pinned" is in region us-east-1, and its S3 store in the web interface signs for region eu-west-1: set that store's region to us-east-1`},
		{"", `the S3 bucket "pinned" is in another region than the one its S3 store in the web interface signs for (eu-west-1), so S3 answered 301 PermanentRedirect: set that store's region to the bucket's`},
	} {
		srv := fake301(t, tc.header)
		withBucketStores(t, map[string]BucketStore{"pinned": mustStore(t, srv.URL, "path", "eu-west-1")})
		c, err := NewS3ClientForBucket(ctx, "pinned", "")
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("pinned")})
		var rm *RegionMismatchError
		if !errors.As(err, &rm) || !rm.Store {
			t.Fatalf("err = %v, want a store RegionMismatchError", err)
		}
		if !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "AWS_REGION") {
			t.Errorf("message:\n%s\nwant it to contain:\n%s\nand not name AWS_REGION", err, tc.want)
		}
		t.Logf("operator sees (store, header %q): %v", tc.header, err)
	}
}

// newAWSOnlyS3Client ignores the operator's endpoint, and the chokepoint
// guard exempts the file it lives in; so it gets a guard of its own: only the
// discovery question, which is never asked with a custom endpoint, uses it.
func TestNewAWSOnlyS3Client_onlyDiscoveryCallsIt_2688(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		filepath.Join("internal", "storage", "s3url.go"):                 true, // the definition
		filepath.Join("internal", "storage", "bucketregion_discover.go"): true, // the one caller
	}
	var offenders []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); (name != "." && strings.HasPrefix(name, ".")) || name == "node_modules" || name == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), "newAWSOnlyS3Client(") && !allowed[rel] {
			offenders = append(offenders, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Errorf("newAWSOnlyS3Client bypasses the custom S3 endpoint and is only for the region question; also called from: %v", offenders)
	}
}

// The commands that know their bucket build the client for it, so it signs for
// the bucket's own region (#2688). Read from the source: these commands talk
// to S3 inside, with no seam for a test to see the client they built.
func TestBucketKnownCommandsBuildABucketClient_2688(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ file, fn string }{
		{"cliapp/upload.go", "runUpload"},
		{"cliapp/init.go", "verifyS3Bucket"},
		{"internal/doctor/objectlock.go", "CheckArchiveObjectLock"},
	} {
		calls := storageCalls(t, filepath.Join(root, c.file), c.fn)
		if calls["NewS3Client"] > 0 || calls["NewS3ClientForBucket"] == 0 {
			t.Errorf("%s %s: storage calls %v, want NewS3ClientForBucket and no NewS3Client", c.file, c.fn, calls)
		}
	}
	// init --s3-arn documents --s3-region as ignored: its us-east-1 default
	// must not reach the check.
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(root, "cliapp/init.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "verifyS3Bucket" && len(call.Args) == 3 {
			found = true
			if lit, ok := call.Args[2].(*ast.BasicLit); !ok || lit.Value != `""` {
				t.Errorf("init passes %s as the region to verifyS3Bucket, want \"\"", exprString(fset, call.Args[2]))
			}
		}
		return true
	})
	if !found {
		t.Error("no verifyS3Bucket call found in cliapp/init.go")
	}
}

func storageCalls(t *testing.T, path, fn string) map[string]int {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]int{}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != fn {
			continue
		}
		ast.Inspect(fd, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "storage" {
					calls[sel.Sel.Name]++
				}
			}
			return true
		})
		return calls
	}
	t.Fatalf("function %s not found in %s", fn, path)
	return nil
}

func exprString(fset *token.FileSet, e ast.Expr) string {
	return fset.Position(e.Pos()).String()
}
