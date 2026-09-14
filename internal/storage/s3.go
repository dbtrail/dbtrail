package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// s3API is the subset of the S3 client interface used by S3Backend.
// Defined as an interface for testability. It embeds the multipart-upload
// operations (UploadPart/CreateMultipartUpload/CompleteMultipartUpload/
// AbortMultipartUpload) so the value satisfies manager.UploadAPIClient — Put
// streams through the managed Uploader, which switches to a multipart upload
// for bodies above S3's 5 GiB single-PUT ceiling (EntityTooLarge).
type s3API interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	HeadBucket(ctx context.Context, input *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	UploadPart(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// S3Config holds the configuration for an S3 storage backend.
type S3Config struct {
	// Bucket is the S3 bucket name (required).
	Bucket string

	// Region is the AWS region. If empty, the SDK resolves it from the
	// standard AWS configuration chain (environment, shared config,
	// instance metadata).
	Region string

	// Prefix is an optional key prefix applied to all operations.
	// For example, "bintrail/" causes Put("foo.parquet", r) to write
	// to "bintrail/foo.parquet" in the bucket.
	Prefix string

	// Endpoint is an optional custom S3 endpoint URL for S3-compatible
	// services (MinIO, LocalStack). Leave empty for standard AWS S3.
	Endpoint string
}

// S3Backend implements Backend using AWS S3 (or any S3-compatible service).
type S3Backend struct {
	client s3API
	bucket string
	prefix string
}

// NewS3Backend creates an S3Backend and validates that the credentials and
// bucket are accessible by issuing a HeadBucket request. Returns an error
// if the bucket does not exist or credentials are invalid.
func NewS3Backend(ctx context.Context, cfg S3Config) (*S3Backend, error) {
	client, err := newS3Client(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return newS3BackendFromClient(ctx, client, cfg)
}

// newS3Client creates an S3 client from config. The region the SDK config
// loads with is the caller's, then the bucket store's (#1575), then whatever
// the ambient chain resolves; a store with an endpoint and no region signs as
// us-east-1, which is also DuckDB's default for a secret that pins none.
func newS3Client(ctx context.Context, cfg S3Config) (*s3.Client, error) {
	region, ep := resolveBucketRouting(cfg.Bucket, cfg.Region)
	awsCfg, err := LoadAWSConfig(ctx, region)
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}

	// An explicit S3Config.Endpoint wins over everything: the caller owns the
	// routing, and the bucket table is not consulted.
	if cfg.Endpoint != "" {
		return NewS3ClientFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // required for MinIO / LocalStack
		}), nil
	}
	// A bucket with its own store (#1575) is routed there, as a CLIENT option:
	// the SDK resolves AWS_ENDPOINT_URL_S3 over cfg.BaseEndpoint, and the
	// option is what wins over both. No "unmirrored" warning for this shape:
	// the DuckDB half carries the same store as a secret scoped to the bucket.
	// With none, NewS3ClientFromConfig applies the shared BINTRAIL_S3_ENDPOINT
	// routing so this backend and every other client agree on where S3 is
	// (#1453).
	if ep != nil {
		url, pathStyle := ep.URL, ep.PathStyle
		return newS3ClientRouted(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(url)
			o.UsePathStyle = pathStyle
		}), nil
	}
	return NewS3ClientFromConfig(awsCfg), nil
}

// newS3BackendFromClient creates an S3Backend from an existing s3API client.
// Used by NewS3Backend and tests.
func newS3BackendFromClient(ctx context.Context, client s3API, cfg S3Config) (*S3Backend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("storage: S3 bucket name is required")
	}

	// Validate credentials and bucket access.
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	}); err != nil {
		return nil, fmt.Errorf("storage: validate bucket %q: %w", cfg.Bucket, err)
	}

	prefix := cfg.Prefix
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}

	return &S3Backend{
		client: client,
		bucket: cfg.Bucket,
		prefix: prefix,
	}, nil
}

// validateKey rejects empty keys and keys with a leading slash.
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("storage: key must not be empty")
	}
	if strings.HasPrefix(key, "/") {
		return fmt.Errorf("storage: key must not start with /")
	}
	return nil
}

// fullKey returns the full S3 key by prepending the configured prefix.
func (b *S3Backend) fullKey(key string) string {
	return b.prefix + key
}

// relKey strips the configured prefix from a full S3 key.
func (b *S3Backend) relKey(fullKey string) string {
	return strings.TrimPrefix(fullKey, b.prefix)
}

// Put uploads the content from r to the given key. Uploads stream through the
// managed Uploader, which automatically switches to a multipart upload for
// bodies above S3's 5 GiB single-PUT limit.
func (b *S3Backend) Put(ctx context.Context, key string, r io.Reader) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := uploadReader(ctx, b.client, b.bucket, b.fullKey(key), r); err != nil {
		return fmt.Errorf("storage: put %q: %w", key, err)
	}
	return nil
}

// PutIfAbsent uploads the content from r to the given key only if no object
// exists there, using an S3 conditional write (If-None-Match: *). When an
// object is already present S3 answers 412 and PutIfAbsent returns an error
// wrapping ErrObjectExists. S3 also documents a 409 ConditionalRequestConflict
// response when a conflicting write races the same key during upload — the
// exact two-writer scenario this method exists to protect — and that is
// treated the same as the losing 412 case rather than surfaced as a hard
// error. The body is buffered in memory — this is meant for small control
// files (markers), not data uploads. S3-compatible services that do not
// support conditional writes (NotImplemented/501) fall back to an
// unconditional Put, i.e. the pre-conditional non-atomic behavior; that
// fallback is logged so the loss of the atomicity guarantee is visible to
// operators.
func (b *S3Backend) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	if err := validateKey(key); err != nil {
		return err
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("storage: put-if-absent %q: read body: %w", key, err)
	}
	_, err = b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(b.fullKey(key)),
		Body:        bytes.NewReader(body),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isAPIErrorCode(err, "PreconditionFailed") || isHTTPStatus(err, http.StatusPreconditionFailed) ||
			isAPIErrorCode(err, "ConditionalRequestConflict") || isHTTPStatus(err, http.StatusConflict) {
			return fmt.Errorf("storage: put-if-absent %q: %w", key, ErrObjectExists)
		}
		if isAPIErrorCode(err, "NotImplemented") || isHTTPStatus(err, http.StatusNotImplemented) {
			slog.Warn("storage: backend does not support S3 conditional writes (If-None-Match); falling back to an unconditional Put — the put-if-absent atomicity guarantee is inactive for this key and a concurrent writer can silently overwrite it",
				"key", key)
			return b.Put(ctx, key, bytes.NewReader(body))
		}
		return fmt.Errorf("storage: put-if-absent %q: %w", key, err)
	}
	return nil
}

// isAPIErrorCode reports whether err carries the given S3 API error code.
func isAPIErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == code
}

// isHTTPStatus reports whether err carries the given HTTP status. Some
// S3-compatible backends surface errors as generic HTTP response errors
// without an S3 error code.
func isHTTPStatus(err error, status int) bool {
	var re *smithyhttp.ResponseError
	return errors.As(err, &re) && re.Response.StatusCode == status
}

// Get returns a reader for the content at the given key.
func (b *S3Backend) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	resp, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.fullKey(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("storage: get %q: %w", key, err)
	}
	return resp.Body, nil
}

// List returns all keys under the given prefix.
func (b *S3Backend) List(ctx context.Context, prefix string) ([]string, error) {
	infos, err := b.ListInfo(ctx, prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(infos))
	for _, o := range infos {
		keys = append(keys, o.Key)
	}
	return keys, nil
}

// ListInfo returns every object under prefix with its size and last-modified
// time, paginating exactly like List. ListObjectsV2 already carries both
// fields on every page; List used to drop them, and the console's backup
// detail/download surfaces need them without one HeadObject per file.
func (b *S3Backend) ListInfo(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	fullPrefix := b.fullKey(prefix)
	var infos []ObjectInfo

	var continuationToken *string
	for {
		resp, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("storage: list %q (after %d keys): %w", prefix, len(infos), err)
		}
		for _, obj := range resp.Contents {
			if obj.Key != nil {
				infos = append(infos, ObjectInfo{
					Key:          b.relKey(*obj.Key),
					Size:         aws.ToInt64(obj.Size),
					LastModified: aws.ToTime(obj.LastModified),
				})
			}
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	return infos, nil
}

// Delete removes the object at the given key.
func (b *S3Backend) Delete(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if _, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.fullKey(key)),
	}); err != nil {
		return fmt.Errorf("storage: delete %q: %w", key, err)
	}
	return nil
}

// Exists checks whether an object exists at the given key.
func (b *S3Backend) Exists(ctx context.Context, key string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.fullKey(key)),
	})
	if err != nil {
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		// Some S3-compatible backends (Ceph, Wasabi) return NoSuchKey
		// from HeadObject instead of NotFound.
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return false, nil
		}
		// Some S3-compatible backends surface 404 as a generic HTTP error.
		var re *smithyhttp.ResponseError
		if errors.As(err, &re) && re.Response.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("storage: exists %q: %w", key, err)
	}
	return true, nil
}

var _ InfoLister = (*S3Backend)(nil)
