package storage

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// A BucketStore says where ONE bucket lives when that is not the ambient
// endpoint every other bucket uses (#1575): an S3-compatible store (MinIO,
// Wasabi), a pinned region, or both. The console registers one per bucket
// from its per-server settings; the process-wide environment
// (BINTRAIL_S3_ENDPOINT) keeps covering every bucket that has none.
//
// The store is keyed by BUCKET, not by server: an upload and the DuckDB read
// of the same object must resolve the same endpoint, and neither side knows
// which server a path belongs to — both know the bucket.
type BucketStore struct {
	// Endpoint is the store's URL and addressing style. The zero value means
	// the ambient endpoint (the environment's, then AWS).
	Endpoint S3Endpoint
	// Region signs requests for this bucket. Empty means whatever the ambient
	// configuration resolves, or us-east-1 when Endpoint is set and nothing
	// resolves (a custom store needs SOME region to sign with, and MinIO
	// accepts any).
	Region string
}

// ErrBucketStoreConfig marks an endpoint, addressing style or region value a
// BucketStore cannot be built from. A configuration fault, like
// ErrS3EndpointConfig.
var ErrBucketStoreConfig = errors.New("S3 store configuration")

// Addressing styles a BucketStore accepts. Empty means "the default for the
// endpoint": path style when one is set, since that is what MinIO and
// LocalStack need, and what BINTRAIL_S3_ENDPOINT defaults to.
const (
	PathStyleAuto  = ""
	PathStylePath  = "path"
	PathStyleVHost = "vhost"
)

// regionShape is deliberately loose: AWS and Wasabi regions (us-east-1,
// eu-central-1), and the "auto" some stores want, all fit; whitespace and
// quotes, which would otherwise travel into a DuckDB statement, do not.
var regionShape = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

// NewBucketStore validates the three operator-typed values and builds the
// store. endpoint "" with a style set is refused: the style has nothing to
// apply to, and accepting it would let a form save a setting that does
// nothing. endpoint "" with only a region is a store: a cross-region pin on
// AWS, the case DuckDB needs a per-bucket REGION for.
func NewBucketStore(endpoint, style, region string) (BucketStore, error) {
	endpoint = strings.TrimSpace(endpoint)
	style = strings.ToLower(strings.TrimSpace(style))
	region = strings.TrimSpace(region)
	var s BucketStore
	if endpoint != "" {
		u, err := NormalizeEndpointURL(endpoint)
		if err != nil {
			return BucketStore{}, fmt.Errorf("%w: endpoint: %w", ErrBucketStoreConfig, err)
		}
		s.Endpoint = S3Endpoint{URL: u, PathStyle: true}
	}
	switch style {
	case PathStyleAuto:
	case PathStylePath, PathStyleVHost:
		if endpoint == "" {
			return BucketStore{}, fmt.Errorf("%w: addressing style %q needs an endpoint to apply to", ErrBucketStoreConfig, style)
		}
		s.Endpoint.PathStyle = style == PathStylePath
		s.Endpoint.pathStyleExplicit = true
	default:
		return BucketStore{}, fmt.Errorf("%w: addressing style %q: want %q or %q", ErrBucketStoreConfig, style, PathStylePath, PathStyleVHost)
	}
	if region != "" && !regionShape.MatchString(region) {
		return BucketStore{}, fmt.Errorf("%w: region %q: want letters, digits and hyphens", ErrBucketStoreConfig, region)
	}
	s.Region = region
	return s, nil
}

// IsZero reports a store that routes nothing: no endpoint and no region.
func (s BucketStore) IsZero() bool { return !s.Endpoint.Set() && s.Region == "" }

// Style renders the addressing mode the way the console stores it.
func (s BucketStore) Style() string {
	if !s.Endpoint.Set() {
		return PathStyleAuto
	}
	if s.Endpoint.PathStyle {
		return PathStylePath
	}
	return PathStyleVHost
}

// Equal compares what matters for routing: the URL, the effective addressing
// style and the region. Whether the style was typed or defaulted is not a
// routing difference.
func (s BucketStore) Equal(o BucketStore) bool {
	return s.Endpoint.URL == o.Endpoint.URL && s.Endpoint.PathStyle == o.Endpoint.PathStyle && s.Region == o.Region
}

// NormalizeEndpointURL is the validation BINTRAIL_S3_ENDPOINT gets, exported
// so a store typed into the console is held to the same shape:
// scheme://host[:port], no path, query or credentials, trailing slash dropped.
func NormalizeEndpointURL(raw string) (string, error) {
	return normalizeEndpointURL(strings.TrimSpace(raw))
}

var bucketStores struct {
	mu sync.RWMutex
	m  map[string]BucketStore
}

// SetBucketStores replaces the process-wide bucket→store table with a copy of
// m. The console calls it whenever its registry loads or changes; a CLI
// process never calls it and keeps the environment-only behaviour. nil or
// empty clears the table.
func SetBucketStores(m map[string]BucketStore) {
	cp := make(map[string]BucketStore, len(m))
	for b, s := range m {
		if b == "" || s.IsZero() {
			continue
		}
		cp[b] = s
	}
	bucketStores.mu.Lock()
	bucketStores.m = cp
	bucketStores.mu.Unlock()
}

// BucketStoreFor returns the store registered for bucket, if any.
func BucketStoreFor(bucket string) (BucketStore, bool) {
	bucketStores.mu.RLock()
	defer bucketStores.mu.RUnlock()
	s, ok := bucketStores.m[bucket]
	return s, ok
}

// BucketStores returns a copy of the whole table, for the DuckDB half (one
// scoped secret per bucket) and the downloadable views.sql.
func BucketStores() map[string]BucketStore {
	bucketStores.mu.RLock()
	defer bucketStores.mu.RUnlock()
	cp := make(map[string]BucketStore, len(bucketStores.m))
	for b, s := range bucketStores.m {
		cp[b] = s
	}
	return cp
}

// resolveBucketRouting is what a client built for one bucket applies on top
// of the ambient configuration: the region to load the SDK config with, and
// the endpoint (nil when the ambient one stands). region, when the caller
// passed one, wins over the store's: it is more specific (a detected or
// flag-given value for this very call).
func resolveBucketRouting(bucket, region string) (effectiveRegion string, ep *S3Endpoint) {
	store, ok := BucketStoreFor(bucket)
	if !ok {
		return region, nil
	}
	if region == "" {
		region = store.Region
	}
	if store.Endpoint.Set() {
		if region == "" {
			region = "us-east-1"
		}
		e := store.Endpoint
		return region, &e
	}
	return region, nil
}
