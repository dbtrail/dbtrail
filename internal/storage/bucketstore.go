package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"unicode"
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
	// AccessKeyID and SecretKey sign requests for this bucket instead of the
	// ambient credential chain. Both or neither (WithKeys). The secret never
	// renders: String, GoString and LogValue mask it, and JSON skips it.
	AccessKeyID string
	SecretKey   string `json:"-"`
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
	region = strings.ToLower(strings.TrimSpace(region))
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

// WithKeys returns s signing with its own access key and secret key. Both are
// trimmed; both blank means no keys. One without the other is refused, and so
// is a key with a space or a control character inside: no provider issues
// one, and a pasted line break would otherwise save a key that never signs.
// Keys with no endpoint need a region.
// The errors never carry a key value.
func (s BucketStore) WithKeys(id, secret string) (BucketStore, error) {
	id, secret = strings.TrimSpace(id), strings.TrimSpace(secret)
	switch {
	case id == "" && secret == "":
		s.AccessKeyID, s.SecretKey = "", ""
		return s, nil
	case id == "":
		return BucketStore{}, fmt.Errorf("%w: a secret key needs its access key", ErrBucketStoreConfig)
	case secret == "":
		return BucketStore{}, fmt.Errorf("%w: an access key needs its secret key", ErrBucketStoreConfig)
	case strings.IndexFunc(id, badKeyRune) >= 0:
		return BucketStore{}, fmt.Errorf("%w: the access key contains a space or a control character", ErrBucketStoreConfig)
	case strings.IndexFunc(secret, badKeyRune) >= 0:
		return BucketStore{}, fmt.Errorf("%w: the secret key contains a space or a control character", ErrBucketStoreConfig)
	}
	if !s.Endpoint.Set() && s.Region == "" {
		// Nothing else names a region both halves agree on: the DuckDB
		// secret would sign as us-east-1 and the SDK with the daemon's.
		return BucketStore{}, fmt.Errorf("%w: S3 keys with no endpoint (an AWS bucket in another account) need the bucket's region", ErrBucketStoreConfig)
	}
	s.AccessKeyID, s.SecretKey = id, secret
	return s, nil
}

func badKeyRune(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }

// HasKeys reports a store that signs with its own keys.
func (s BucketStore) HasKeys() bool { return s.AccessKeyID != "" }

// IsZero reports a store that changes nothing: no endpoint, no region and no
// keys. A store with keys alone is not zero: it signs differently from the
// ambient chain (an AWS bucket in another account).
func (s BucketStore) IsZero() bool { return !s.Endpoint.Set() && s.Region == "" && !s.HasKeys() }

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

// Equal compares what a bucket's reads and writes depend on: the routing
// (SameRouting) and the keys. A bucket has one pair of keys, as it has one
// endpoint.
func (s BucketStore) Equal(o BucketStore) bool {
	return s.SameRouting(o) && s.AccessKeyID == o.AccessKeyID && s.SecretKey == o.SecretKey
}

// SameRouting compares the URL, the effective addressing style and the
// region. Whether the style was typed or defaulted is not a routing
// difference.
func (s BucketStore) SameRouting(o BucketStore) bool {
	return s.Endpoint.URL == o.Endpoint.URL && s.Endpoint.PathStyle == o.Endpoint.PathStyle && s.Region == o.Region
}

// String renders the store with the secret masked: an exported field would
// otherwise print with %v in any log line or error that formats a store.
func (s BucketStore) String() string {
	return fmt.Sprintf("BucketStore{endpoint=%q style=%q region=%q access_key_id=%q secret_key=%s}",
		s.Endpoint.URL, s.Style(), s.Region, s.AccessKeyID, maskSecret(s.SecretKey))
}

// GoString is String for %#v.
func (s BucketStore) GoString() string { return s.String() }

// LogValue keeps slog handlers, the JSON one included, from walking the
// exported fields.
func (s BucketStore) LogValue() slog.Value { return slog.StringValue(s.String()) }

func maskSecret(secret string) string {
	if secret == "" {
		return `""`
	}
	return "<redacted>"
}

// SigningRegion is the region requests for this bucket are signed with, the
// ONE answer both halves use: the SDK client and the DuckDB scoped secret.
// The typed region when there is one; us-east-1 for an endpoint with none (a
// custom store needs SOME region, and MinIO accepts any); "" for no store.
// Measured: a DuckDB secret left without REGION signs with the session's
// s3_region instead, so the two halves would sign differently.
func (s BucketStore) SigningRegion() string {
	if s.Region == "" && s.Endpoint.Set() {
		return "us-east-1"
	}
	return s.Region
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
