package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3CopySingleMax is the largest object one CopyObject may copy (5 GiB); a
// larger one is copied in parts (UploadPartCopy).
const S3CopySingleMax int64 = 5 << 30

// s3CopyPartMin is the part size a multipart copy starts from. S3's floor is
// 5 MiB and its ceiling 10,000 parts; 512 MiB keeps a 5 TB object (S3's
// largest) under the ceiling while each part stays a short server-side
// operation.
const s3CopyPartMin int64 = 512 << 20

// s3CopyAPI is the part of the S3 client a server-side copy uses.
type s3CopyAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPartCopy(context.Context, *s3.UploadPartCopyInput, ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// CopyObject copies s3://srcBucket/srcKey to s3://dstBucket/dstKey inside S3
// (#2212): no byte passes through this process. The request goes through
// client, which must be the DESTINATION bucket's (its region and
// credentials); S3 reads the source itself. Objects over 5 GiB are copied in
// parts, and a part that fails aborts the upload so no billed parts stay
// behind.
func CopyObject(ctx context.Context, client *s3.Client, srcBucket, srcKey, dstBucket, dstKey string) error {
	return copyS3Object(ctx, client, srcBucket, srcKey, dstBucket, dstKey)
}

func copyS3Object(ctx context.Context, api s3CopyAPI, srcBucket, srcKey, dstBucket, dstKey string) error {
	head, err := api.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(srcBucket), Key: aws.String(srcKey)})
	if err != nil {
		return fmt.Errorf("size s3://%s/%s before copying it: %w", srcBucket, srcKey, err)
	}
	size := aws.ToInt64(head.ContentLength)
	src := s3CopySource(srcBucket, srcKey)
	if size <= S3CopySingleMax {
		if _, err := api.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket: aws.String(dstBucket), Key: aws.String(dstKey), CopySource: aws.String(src),
		}); err != nil {
			return fmt.Errorf("copy s3://%s/%s → s3://%s/%s: %w", srcBucket, srcKey, dstBucket, dstKey, err)
		}
		return nil
	}
	return multipartCopy(ctx, api, src, size, dstBucket, dstKey)
}

func multipartCopy(ctx context.Context, api s3CopyAPI, src string, size int64, dstBucket, dstKey string) error {
	part := max(s3CopyPartMin, (size+9999)/10000)
	up, err := api.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(dstBucket), Key: aws.String(dstKey)})
	if err != nil {
		return fmt.Errorf("start a multipart copy of %s → s3://%s/%s: %w", src, dstBucket, dstKey, err)
	}
	abort := func(cause error) error {
		// On a context that outlives the caller's: the usual reason a part
		// fails here is that the caller was cancelled, and an abort sent on
		// that context would never leave, leaving the parts billed.
		actx := context.WithoutCancel(ctx)
		if _, aerr := api.AbortMultipartUpload(actx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: up.UploadId,
		}); aerr != nil {
			return fmt.Errorf("%w (and the multipart upload %s could not be aborted, so its parts stay in the bucket until a lifecycle rule removes them: %v)",
				cause, aws.ToString(up.UploadId), aerr)
		}
		return cause
	}
	var parts []types.CompletedPart
	for n, lo := int32(1), int64(0); lo < size; n, lo = n+1, lo+part {
		hi := min(lo+part, size) - 1
		out, err := api.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: up.UploadId,
			PartNumber: aws.Int32(n), CopySource: aws.String(src),
			CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", lo, hi)),
		})
		if err != nil {
			return abort(fmt.Errorf("copy part %d of %s → s3://%s/%s: %w", n, src, dstBucket, dstKey, err))
		}
		var etag *string
		if out.CopyPartResult != nil {
			etag = out.CopyPartResult.ETag
		}
		parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(n), ETag: etag})
	}
	if _, err := api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(dstBucket), Key: aws.String(dstKey), UploadId: up.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		return abort(fmt.Errorf("finish the multipart copy of %s → s3://%s/%s: %w", src, dstBucket, dstKey, err))
	}
	return nil
}

// s3CopySource spells the x-amz-copy-source header: "bucket/key" with every
// byte outside the unreserved set percent-encoded, '/' kept. S3 URL-decodes
// the header, so a key holding a space, '+', '#', '%', '?' or non-ASCII
// would otherwise name a different object, or none.
func s3CopySource(bucket, key string) string {
	var b strings.Builder
	for _, c := range []byte(bucket + "/" + key) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~', c == '/':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
