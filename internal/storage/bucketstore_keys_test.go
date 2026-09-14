package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Per-bucket keys (#1575, second slice). A store may carry its own access key
// and secret key; they sign for that bucket instead of the ambient chain.

func TestBucketStore_WithKeys(t *testing.T) {
	base := mustStore(t, "http://minio:9000", "", "")
	st, err := base.WithKeys(" AKIASTORE ", " s3cr3t/with+slash ")
	if err != nil {
		t.Fatal(err)
	}
	if st.AccessKeyID != "AKIASTORE" || st.SecretKey != "s3cr3t/with+slash" || !st.HasKeys() {
		t.Errorf("keys not trimmed or not set: id=%q hasKeys=%v", st.AccessKeyID, st.HasKeys())
	}
	if st.Endpoint != base.Endpoint || st.Region != base.Region {
		t.Error("WithKeys changed the routing")
	}
	none, err := base.WithKeys("  ", "")
	if err != nil || none.HasKeys() {
		t.Errorf("both blank is no keys: %+v, %v", none, err)
	}
	for _, tc := range []struct{ name, id, secret string }{
		{"id alone", "AKIASTORE", ""},
		{"secret alone", "", "LEAKYsecretVALUE"},
		{"inner space in id", "AKIA STORE", "LEAKYsecretVALUE"},
		{"newline in secret", "AKIASTORE", "LEAKY\nsecretVALUE"},
		{"tab in secret", "AKIASTORE", "LEAKY\tsecretVALUE"},
		{"control char in id", "AKIA\x00STORE", "LEAKYsecretVALUE"},
	} {
		_, err := base.WithKeys(tc.id, tc.secret)
		if !errors.Is(err, ErrBucketStoreConfig) {
			t.Errorf("%s: err = %v, want ErrBucketStoreConfig", tc.name, err)
			continue
		}
		if strings.Contains(err.Error(), "LEAKY") || strings.Contains(err.Error(), "AKIA") {
			t.Errorf("%s: the error echoes a key: %v", tc.name, err)
		}
	}
}

func TestBucketStore_keysInIsZeroAndEqual(t *testing.T) {
	// An AWS bucket in another account: no endpoint, its own keys, and the
	// bucket's region. Without an endpoint nothing else names a region both
	// halves agree on: DuckDB's secret would sign as us-east-1 and the SDK
	// with the daemon's region.
	if _, err := (BucketStore{}).WithKeys("AKIASTORE", "secret"); !errors.Is(err, ErrBucketStoreConfig) || !strings.Contains(err.Error(), "region") {
		t.Errorf("keys with no endpoint and no region: err = %v, want a refusal naming the region", err)
	}
	keysOnly, err := mustStore(t, "", "", "eu-west-1").WithKeys("AKIASTORE", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if (BucketStore{AccessKeyID: "AKIASTORE", SecretKey: "secret"}).IsZero() {
		t.Error("a store built with keys and nothing else must not be zero: it would be dropped from the table and sign with the ambient chain")
	}
	if keysOnly.IsZero() {
		t.Error("a store with keys alone must not be the zero store: it signs differently from the ambient chain")
	}
	if got := keysOnly.SigningRegion(); got != "eu-west-1" {
		t.Errorf("a keys-only store signs with region %q, want its own", got)
	}
	base := mustStore(t, "http://minio:9000", "", "")
	a, _ := base.WithKeys("AKIASTORE", "secret")
	sameKeys, _ := base.WithKeys("AKIASTORE", "secret")
	otherSecret, _ := base.WithKeys("AKIASTORE", "rotated")
	otherID, _ := base.WithKeys("AKIAOTHER", "secret")
	if !a.Equal(sameKeys) {
		t.Error("same routing and same keys must be equal")
	}
	for name, o := range map[string]BucketStore{"no keys": base, "other secret": otherSecret, "other id": otherID} {
		if a.Equal(o) || o.Equal(a) {
			t.Errorf("%s: a bucket has one pair of keys; stores that sign differently must not be equal", name)
		}
	}
	if !a.SameRouting(otherSecret) || a.SameRouting(mustStore(t, "http://minio:9001", "", "")) {
		t.Error("SameRouting compares endpoint, style and region only")
	}
}

// Every way a store can reach a log line or an error string must hide the
// secret: %v, %+v, %#v, %s, slog text and JSON handlers, and json.Marshal.
func TestBucketStore_secretNeverRendered(t *testing.T) {
	st, err := mustStore(t, "http://minio:9000", "", "").WithKeys("AKIASTORE", "SECRETVALUE42")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		out = append(out, fmt.Sprintf(verb, st), fmt.Sprintf(verb, &st))
	}
	out = append(out, fmt.Sprintf("%v", map[string]BucketStore{"b": st}))
	var text, js bytes.Buffer
	slog.New(slog.NewTextHandler(&text, nil)).Info("x", "store", st)
	slog.New(slog.NewJSONHandler(&js, nil)).Info("x", "store", st)
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, text.String(), js.String(), string(b))
	for _, s := range out {
		if strings.Contains(s, "SECRETVALUE42") {
			t.Errorf("the secret is rendered: %s", s)
		}
	}
	// The masking must not hide what an operator needs to tell stores apart.
	if s := fmt.Sprintf("%v", st); !strings.Contains(s, "minio:9000") || !strings.Contains(s, "AKIASTORE") {
		t.Errorf("the rendering lost the endpoint or the access key id: %s", s)
	}
}

// fakeS3 answers every request 200 and records the Authorization header.
type fakeS3 struct {
	mu    sync.Mutex
	auths []string
	srv   *httptest.Server
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	f := &fakeS3{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auths = append(f.auths, r.Header.Get("Authorization"))
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeS3) seen() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.auths, "\n")
}

func headBucket(t *testing.T, c *s3.Client, bucket string) {
	t.Helper()
	if _, err := c.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("HeadBucket %s: %v", bucket, err)
	}
}

// A bucket whose store has keys is signed with them, not with the ambient
// chain (the environment holds testdummykey here).
func TestNewS3ClientForBucket_signsWithTheStoreKeys(t *testing.T) {
	isolateAWSEnv(t)
	fake := newFakeS3(t)
	keyed, err := mustStore(t, fake.srv.URL, "", "").WithKeys("AKIATABLESTORE", "tablesecret")
	if err != nil {
		t.Fatal(err)
	}
	withBucketStores(t, map[string]BucketStore{"keyed": keyed, "plain": mustStore(t, fake.srv.URL, "", "")})
	for _, b := range []string{"keyed", "plain"} {
		c, err := NewS3ClientForBucket(context.Background(), b, "")
		if err != nil {
			t.Fatal(err)
		}
		headBucket(t, c, b)
	}
	got := strings.Split(fake.seen(), "\n")
	if len(got) != 2 {
		t.Fatalf("requests = %d, want 2:\n%s", len(got), fake.seen())
	}
	if !strings.Contains(got[0], "Credential=AKIATABLESTORE/") {
		t.Errorf("the keyed bucket was not signed with its store's key: %s", got[0])
	}
	if !strings.Contains(got[1], "Credential=testdummykey/") {
		t.Errorf("a store without keys must sign with the ambient chain: %s", got[1])
	}
}

// A store with keys and no endpoint (an AWS bucket in another account) signs
// with its keys wherever the ambient endpoint is, at its own region.
func TestNewS3ClientForBucket_keysOnlyStoreSignsAtTheAmbientEndpoint(t *testing.T) {
	isolateAWSEnv(t)
	ambient := newFakeS3(t)
	t.Setenv(EnvS3Endpoint, ambient.srv.URL)
	keysOnly, err := mustStore(t, "", "", "eu-west-1").WithKeys("AKIAKEYSONLY", "keysonlysecret")
	if err != nil {
		t.Fatal(err)
	}
	withBucketStores(t, map[string]BucketStore{"other-account": keysOnly})
	c, err := NewS3ClientForBucket(context.Background(), "other-account", "")
	if err != nil {
		t.Fatal(err)
	}
	if c.Options().Region != "eu-west-1" {
		t.Errorf("region = %q, want the store's", c.Options().Region)
	}
	headBucket(t, c, "other-account")
	if !strings.Contains(ambient.seen(), "Credential=AKIAKEYSONLY/") || !strings.Contains(ambient.seen(), "/eu-west-1/s3/") {
		t.Errorf("a keys-only store did not sign with its keys and region at the ambient endpoint:\n%s", ambient.seen())
	}
}

// A keys-only store's bucket belongs to another account: GetBucketLocation
// with the daemon's credentials would be refused or, worse, answered about a
// different bucket. The store's own region is the answer.
func TestDetectBucketRegion_keysOnlyStoreIsNotAsked(t *testing.T) {
	isolateAWSEnv(t)
	keysOnly, err := mustStore(t, "", "", "ap-south-1").WithKeys("AKIAKEYSONLY", "keysonlysecret")
	if err != nil {
		t.Fatal(err)
	}
	withBucketStores(t, map[string]BucketStore{"acct": keysOnly})
	asked := newFakeS3(t)
	cfg := aws.Config{Region: "us-west-2", BaseEndpoint: aws.String(asked.srv.URL)}
	if r, ok := DetectBucketRegion(context.Background(), cfg, "acct"); r != "ap-south-1" || !ok {
		t.Errorf("keys-only store: (%q, %v), want (ap-south-1, true)", r, ok)
	}
	if asked.seen() != "" {
		t.Errorf("region detection asked about a keyed bucket with the daemon's credentials:\n%s", asked.seen())
	}
}

// An explicit S3Config.Endpoint is the caller owning the routing: the store's
// keys belong to the store's endpoint and must never be sent to another host.
func TestNewS3Client_explicitEndpointNeverCarriesStoreKeys(t *testing.T) {
	isolateAWSEnv(t)
	storeHost, elsewhere := newFakeS3(t), newFakeS3(t)
	keyed, err := mustStore(t, storeHost.srv.URL, "", "").WithKeys("AKIATABLESTORE", "tablesecret")
	if err != nil {
		t.Fatal(err)
	}
	withBucketStores(t, map[string]BucketStore{"keyed": keyed})
	if _, err := NewS3Backend(context.Background(), S3Config{Bucket: "keyed", Endpoint: elsewhere.srv.URL}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(elsewhere.seen(), "AKIATABLESTORE") || !strings.Contains(elsewhere.seen(), "Credential=testdummykey/") {
		t.Errorf("an explicit endpoint got the store's keys: %s", elsewhere.seen())
	}
	if storeHost.seen() != "" {
		t.Errorf("an explicit endpoint reached the store anyway: %s", storeHost.seen())
	}
}

// The Test connection probe builds its client from a CANDIDATE store, one
// that is not saved: the table may hold nothing for the bucket, or a
// different store. The candidate must win in both cases.
func TestNewS3ClientForStore_ignoresTheTable(t *testing.T) {
	isolateAWSEnv(t)
	candidateHost, tableHost := newFakeS3(t), newFakeS3(t)
	saved, err := mustStore(t, tableHost.srv.URL, "", "").WithKeys("AKIASAVED", "savedsecret")
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := mustStore(t, candidateHost.srv.URL, "", "").WithKeys("AKIACANDIDATE", "candidatesecret")
	if err != nil {
		t.Fatal(err)
	}
	for name, table := range map[string]map[string]BucketStore{"empty table": nil, "other store saved": {"b": saved}} {
		withBucketStores(t, table)
		retries := 0
		c, err := NewS3ClientForStore(context.Background(), candidate, func(o *s3.Options) { retries = 1; o.RetryMaxAttempts = 1 })
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if retries != 1 || c.Options().RetryMaxAttempts != 1 {
			t.Errorf("%s: the extra options were not applied", name)
		}
		headBucket(t, c, "b")
		if tableHost.seen() != "" {
			t.Errorf("%s: the probe consulted the table: %s", name, tableHost.seen())
		}
	}
	if n := strings.Count(candidateHost.seen(), "Credential=AKIACANDIDATE/"); n != 2 {
		t.Errorf("candidate requests signed with its own key = %d, want 2:\n%s", n, candidateHost.seen())
	}
	// A candidate with no keys signs with the ambient chain.
	plain, err := NewS3ClientForStore(context.Background(), mustStore(t, candidateHost.srv.URL, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	headBucket(t, plain, "b")
	if !strings.Contains(candidateHost.seen(), "Credential=testdummykey/") {
		t.Errorf("a keyless candidate did not use the ambient chain:\n%s", candidateHost.seen())
	}
}
