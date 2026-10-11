package copymirror

import (
	"context"
	"fmt"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// OpenS3 is the Source for a snapshot root given as an s3:// URL, with or
// without a trailing slash. The client is the bucket's own: a store the
// console registered for it (#1575) applies, as it does for the upload.
//
// Open one per round and do not keep it: when the bucket's region cannot be
// learned the client is built for us-east-1 (storage.NewS3BackendUnprobed),
// and one kept would go on failing after the region could be learned.
func OpenS3(ctx context.Context, s3URL, region string) (Source, error) {
	bucket, prefix, err := storage.ParseS3URL(s3URL)
	if err != nil {
		return nil, fmt.Errorf("copymirror: %w", err)
	}
	return storage.NewS3BackendUnprobed(ctx, storage.S3Config{Bucket: bucket, Prefix: prefix, Region: region})
}
