package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// ParseS3URL parses an S3 URL of the form s3://bucket or s3://bucket/prefix
// and returns the bucket name and prefix (without leading slash).
func ParseS3URL(u string) (bucket, prefix string, err error) {
	if !strings.HasPrefix(u, "s3://") {
		return "", "", fmt.Errorf("must start with s3://, got %q", u)
	}
	rest := strings.TrimPrefix(u, "s3://")
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("bucket name is empty in %q", u)
	}
	return bucket, prefix, nil
}

// NewS3Client creates an S3 client using the default AWS credential chain.
// region is optional — if empty, the SDK resolves it from AWS_REGION env var
// or ~/.aws/config. A caller that knows which bucket it is about to touch
// uses NewS3ClientForBucket instead, so a per-bucket store applies.
func NewS3Client(ctx context.Context, region string) (*s3.Client, error) {
	awsCfg, err := LoadAWSConfig(ctx, region)
	if err != nil {
		return nil, err
	}
	return NewS3ClientFromConfig(awsCfg), nil
}

// NewS3ClientForBucket is NewS3Client for one bucket: when the console has
// registered a store for it (#1575), the client goes there, with that store's
// addressing style and region. With no store it is NewS3Client exactly.
func NewS3ClientForBucket(ctx context.Context, bucket, region string) (*s3.Client, error) {
	return newS3Client(ctx, S3Config{Bucket: bucket, Region: region})
}

// NewS3ClientForStore builds a client for store alone, never consulting the
// bucket table: the console's Test connection probes a store the operator
// typed and has not saved, which the table does not hold, or holds as a
// different store. It signs with the store's keys when it has them, else with
// the ambient chain, and at the store's SigningRegion. extra options apply
// last and win.
func NewS3ClientForStore(ctx context.Context, store BucketStore, extra ...func(*s3.Options)) (*s3.Client, error) {
	return newStoreClient(ctx, store, store.SigningRegion(), extra...)
}

// newStoreClient is the client for one bucket store at region. An endpoint
// store is built without the unmirrored-endpoint warning: the DuckDB half
// carries the same store as a secret scoped to the bucket, and a probe of an
// unsaved candidate is one HeadBucket that DuckDB never reads through. A store
// with no endpoint (a region pin, keys alone) goes through the shared
// constructor, which applies the process-wide endpoint like any other client.
func newStoreClient(ctx context.Context, store BucketStore, region string, extra ...func(*s3.Options)) (*s3.Client, error) {
	awsCfg, err := LoadAWSConfig(ctx, region)
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	if store.HasKeys() {
		// Static, per client: a plain provider func, so the ambient chain
		// (environment, profile, instance role) is never asked.
		id, secret := store.AccessKeyID, store.SecretKey
		awsCfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: id, SecretAccessKey: secret, Source: "bintrail S3 store keys"}, nil
		})
	}
	if !store.Endpoint.Set() {
		return NewS3ClientFromConfig(awsCfg, extra...), nil
	}
	url, pathStyle := store.Endpoint.URL, store.Endpoint.PathStyle
	opts := append([]func(*s3.Options){func(o *s3.Options) {
		o.BaseEndpoint = aws.String(url)
		o.UsePathStyle = pathStyle
	}}, extra...)
	return newS3ClientRouted(awsCfg, opts...), nil
}

// NewS3ClientFromConfig is the one constructor every S3 client in the tree
// goes through (#1453). It carries the addressing style that goes with a
// custom endpoint: the SDK config can hold the endpoint URL itself
// (LoadAWSConfig sets BaseEndpoint), but path-style addressing is a client
// option with no environment knob, so a caller that built its client with a
// bare s3.NewFromConfig would reach MinIO at a virtual-hosted URL and fail on
// DNS. extra options are applied AFTER the addressing option and win.
func NewS3ClientFromConfig(cfg aws.Config, extra ...func(*s3.Options)) *s3.Client {
	client := newS3ClientRouted(cfg, extra...)
	warnIfEndpointUnmirrored(client)
	return client
}

// newS3ClientRouted is NewS3ClientFromConfig without the unmirrored-endpoint
// warning, for a client whose endpoint came from a bucket store: that store
// IS mirrored to DuckDB, as a secret scoped to the bucket, so the warning
// would be false there.
func newS3ClientRouted(cfg aws.Config, extra ...func(*s3.Options)) *s3.Client {
	return s3.NewFromConfig(cfg, append(S3ClientOptions(), extra...)...)
}

// warnIfEndpointUnmirrored reports an endpoint the SDK resolved that bintrail
// could not pass to DuckDB. It reads the CLIENT's endpoint, not the config's:
// the SDK resolves a service-specific endpoint (AWS_ENDPOINT_URL_S3, or
// services.<name>.s3.endpoint_url in ~/.aws/config) when the client is built,
// so cfg.BaseEndpoint is nil for exactly the shape that most needs the
// warning — S3 alone pointed at a compatible store, uploads landing there
// while every Parquet read goes to AWS.
func warnIfEndpointUnmirrored(client *s3.Client) {
	opts := client.Options()
	if opts.BaseEndpoint == nil || *opts.BaseEndpoint == "" {
		return
	}
	if ep, err := S3EndpointFromEnv(); err != nil || ep.Set() {
		return // mirrored, or the caller is about to fail on the config error
	}
	// Redacted: this branch is REACHED BY a value bintrail refused, and one of
	// the refused shapes is a URL carrying credentials. The scheme and host are
	// what identify the store; the rest is not ours to print.
	warnUnmirroredEndpoint(redactURL(*opts.BaseEndpoint))
}

// S3ClientOptions returns the client options bintrail's own endpoint needs.
// It takes no config on purpose: everything it decides comes from the
// environment, and consulting cfg.BaseEndpoint is what made an earlier version
// miss the service-specific endpoint the SDK resolves at client build time.
//
// The endpoint is pinned on the CLIENT, not only in cfg.BaseEndpoint, because
// the SDK's service-specific AWS_ENDPOINT_URL_S3 overrides cfg.BaseEndpoint
// when the client is built (verified against the pinned SDK version). Without
// this pin, an operator with both variables set would have SDK writes go to
// one store and DuckDB reads to the other — the split this whole change
// exists to prevent. Client options are applied last and win.
//
// Path style is forced only for BINTRAIL_S3_ENDPOINT (defaulting to what MinIO
// and LocalStack need) or when BINTRAIL_S3_PATH_STYLE names it. An endpoint
// the SDK resolved on its own — its AWS_ENDPOINT_URL* variables, or
// endpoint_url in ~/.aws/config — keeps the SDK's virtual-hosted addressing:
// overriding it would break a setup that worked before this option existed.
func S3ClientOptions() []func(*s3.Options) {
	ep, err := S3EndpointFromEnv()
	if err != nil {
		// LoadAWSConfig validated this already, so the environment changed
		// under us. Leave the SDK's own resolution alone rather than guess.
		return nil
	}
	var opts []func(*s3.Options)
	if ep.Managed() {
		endpoint := ep.URL
		opts = append(opts, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint) })
	}
	// PathStyleExplicit applies with no endpoint of our own too: the SDK has no
	// shared-config setting for addressing style, so BINTRAIL_S3_PATH_STYLE is
	// the only lever an operator who configured endpoint_url in ~/.aws/config
	// has, and dropping it sends uploads to bucket.host and fails on DNS.
	if ep.Managed() || ep.PathStyleExplicit() {
		pathStyle := ep.PathStyle
		opts = append(opts, func(o *s3.Options) { o.UsePathStyle = pathStyle })
	}
	return opts
}

// redactURL keeps scheme://host and drops everything else, including any
// userinfo. A value that does not parse is reported as a fixed placeholder
// rather than echoed.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "(unparseable)"
	}
	return u.Scheme + "://" + u.Host
}

// warnUnmirroredEndpoint fires once per process for an endpoint the SDK
// resolved and bintrail could not mirror: SDK writes reach the store while
// DuckDB reads go to AWS, and nothing else in the system can notice.
var warnUnmirroredOnce sync.Once

func warnUnmirroredEndpoint(endpoint string) {
	warnUnmirroredOnce.Do(func() {
		slog.Warn("an S3 endpoint is configured for the AWS SDK that bintrail cannot pass to DuckDB (an ~/.aws/config endpoint_url, or a value it cannot parse): uploads will reach it while Parquet reads go to AWS; set "+EnvS3Endpoint+" to the same value so both halves agree",
			"endpoint", endpoint)
	})
}

// LoadAWSConfig loads the default AWS config (credential chain, region) for
// S3 access. region is optional — if empty, the SDK resolves it from
// AWS_REGION/AWS_DEFAULT_REGION env var or ~/.aws/config.
//
// LoadDefaultConfig resolves region from AWS_REGION/AWS_DEFAULT_REGION and the
// shared config — but NOT from EC2/ECS IMDS. In an IAM-role-only deployment
// with no AWS_REGION set (e.g. the bundled console on EC2), the region ends
// up empty and every S3 request fails with "region was not a valid DNS name".
// This surfaces even when the caller goes on to probe a bucket's own region
// (e.g. via GetBucketLocation) as a fallback, since that probe typically
// itself requires an IAM permission (s3:GetBucketLocation) a minimal,
// least-privilege policy may not grant — leaving the caller with only this
// empty region to fall back to. So every caller that loads AWS config for S3
// access should go through this shared helper, not awsconfig.LoadDefaultConfig
// directly, to get the same IMDS fallback.
//
// BINTRAIL_S3_ENDPOINT (see S3EndpointFromEnv) is applied here as
// cfg.BaseEndpoint, so every SDK client built from this config reaches the
// S3-compatible store (#1453), and an invalid value fails the load rather
// than falling back to AWS. With it set and no region anywhere, the region
// defaults to us-east-1: SigV4 needs one, MinIO and friends accept any, and
// the IMDS probe would only cost a 2s timeout on a host that is not an EC2
// instance. The AWS SDK's own endpoint variables are deliberately NOT given
// that treatment — the SDK already applies them, and suppressing the IMDS
// region fallback for an operator who set AWS_ENDPOINT_URL against real AWS
// would sign their requests for the wrong region.
func LoadAWSConfig(ctx context.Context, region string) (aws.Config, error) {
	ep, err := S3EndpointFromEnv()
	if err != nil {
		return aws.Config{}, fmt.Errorf("load AWS config: %w", err)
	}
	opts := []func(*awsconfig.LoadOptions) error{}
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return awsCfg, fmt.Errorf("load AWS config: %w", err)
	}
	if ep.Managed() {
		awsCfg.BaseEndpoint = aws.String(ep.URL)
		if awsCfg.Region == "" {
			awsCfg.Region = "us-east-1"
		}
		return awsCfg, nil
	}
	if region == "" && awsCfg.Region == "" {
		if r := imdsRegion(ctx, awsCfg); r != "" {
			awsCfg.Region = r
		}
	}
	return awsCfg, nil
}

// imdsRegion best-effort fetches the EC2/ECS instance region from IMDS. Returns
// "" off-instance or on any error; a short timeout keeps it from hanging where
// IMDS is unreachable (a non-AWS host, or a hop-limited container).
func imdsRegion(ctx context.Context, cfg aws.Config) string {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out, err := imds.NewFromConfig(cfg).GetRegion(ctx, &imds.GetRegionInput{})
	if err != nil || out == nil {
		return ""
	}
	return out.Region
}

// BuildS3Key constructs the S3 object key for a file by computing its path
// relative to baseDir and prepending prefix (if non-empty). This is the
// shared key-building logic used by both the baseline upload and the rotate
// archive loop.
func BuildS3Key(baseDir, filePath, prefix string) (string, error) {
	rel, err := filepath.Rel(baseDir, filePath)
	if err != nil {
		return "", err
	}
	key := filepath.ToSlash(rel)
	if prefix != "" {
		key = strings.TrimSuffix(prefix, "/") + "/" + key
	}
	return key, nil
}

// S3ObjectExists checks whether an object already exists in S3 by issuing a
// HeadObject request. Returns true when the object is found, false on 404,
// and an error for any other failure.
func S3ObjectExists(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// The SDK wraps NotFound as a modeled error type.
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		// HeadObject also surfaces 404 as a generic HTTP 404 response error
		// when the bucket itself has no matching key (some S3-compatible
		// backends use this path).
		var re *smithyhttp.ResponseError
		if errors.As(err, &re) && re.Response.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("head s3://%s/%s: %w", bucket, key, err)
	}
	return true, nil
}

// uploadReader streams body to s3://bucket/key via the AWS SDK managed
// Uploader. The Uploader transparently switches to a multipart upload for
// bodies larger than its part size (~5 MiB, retried and checksummed per part)
// and falls back to a single PutObject for small bodies — so it is a drop-in
// for a plain PutObject that also handles partitions above S3's 5 GiB
// single-PUT ceiling (EntityTooLarge). Callers keep their existing
// signatures; only the transport changes.
//
// Note: an interrupted multipart upload can leave orphaned parts in the
// bucket. Operators should attach an AbortIncompleteMultipartUpload
// lifecycle rule to the archive bucket to reap them (follow-up, documented in
// docs/deployment.md).
func uploadReader(ctx context.Context, client manager.UploadAPIClient, bucket, key string, body io.Reader) error {
	uploader := manager.NewUploader(client)
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

// UploadFile opens a single local file and uploads it to S3. It is a separate
// function so that defer f.Close() runs when UploadFile returns — not when a
// WalkDir callback returns — preventing file descriptor accumulation over
// large directory trees. Uploads stream through the managed Uploader, which
// automatically uses a multipart upload for files above S3's 5 GiB single-PUT
// limit.
func UploadFile(ctx context.Context, client *s3.Client, path, bucket, key string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	if err := uploadReader(ctx, client, bucket, key, f); err != nil {
		return fmt.Errorf("upload %s → s3://%s/%s: %w", path, bucket, key, err)
	}
	return nil
}

// PutEmptyObject writes a zero-byte object at key. It is used to publish the
// _INCOMPLETE / completeness markers on the S3 upload path without needing a
// local file to walk (the local _INCOMPLETE marker is already removed once a
// run succeeds).
func PutEmptyObject(ctx context.Context, client *s3.Client, bucket, key string) error {
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(""),
	}); err != nil {
		return fmt.Errorf("put empty object s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}

// DeleteObject removes a single object. Used to clean up the _INCOMPLETE marker
// once an S3 snapshot upload completes; callers treat its failure as harmless
// because completeness is decided by _SUCCESS-present first.
func DeleteObject(ctx context.Context, client *s3.Client, bucket, key string) error {
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		return fmt.Errorf("delete s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}
