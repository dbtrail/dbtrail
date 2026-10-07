package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// #2212: an unchanged table of an S3-only server's update is published by a
// copy INSIDE S3. The edge cases, written before the code:
//   - up to and including 5 GiB: one CopyObject, no multipart;
//   - one byte over: a multipart copy whose ranges cover the object exactly,
//     in order, with no gap and no overlap, under 10,000 parts;
//   - a part that fails: the multipart upload is ABORTED (on a context that
//     survives the caller's cancellation), and the error is returned;
//   - a key with a space, a '+', a '#', a '%' or non-ASCII: the copy source
//     is percent-encoded so S3 reads the same key back;
//   - the source cannot be sized (HEAD fails): an error, nothing copied.

type fakeCopyAPI struct {
	mu        sync.Mutex
	size      int64
	headErr   error
	partErrAt int // 1-based part number that fails; 0 = none
	copies    []s3.CopyObjectInput
	parts     []s3.UploadPartCopyInput
	created   int
	completed *s3.CompleteMultipartUploadInput
	aborted   int
	abortCtx  error
}

func (f *fakeCopyAPI) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if f.headErr != nil {
		return nil, f.headErr
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(f.size)}, nil
}

func (f *fakeCopyAPI) CopyObject(_ context.Context, in *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.copies = append(f.copies, *in)
	return &s3.CopyObjectOutput{}, nil
}

func (f *fakeCopyAPI) CreateMultipartUpload(_ context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.created++
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("up-1")}, nil
}

func (f *fakeCopyAPI) UploadPartCopy(_ context.Context, in *s3.UploadPartCopyInput, _ ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.parts = append(f.parts, *in)
	if f.partErrAt != 0 && int(aws.ToInt32(in.PartNumber)) == f.partErrAt {
		return nil, errors.New("part copy failed")
	}
	return &s3.UploadPartCopyOutput{CopyPartResult: &types.CopyPartResult{ETag: aws.String(fmt.Sprintf("e%d", aws.ToInt32(in.PartNumber)))}}, nil
}

func (f *fakeCopyAPI) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	f.completed = in
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeCopyAPI) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.aborted++
	f.abortCtx = ctx.Err()
	return &s3.AbortMultipartUploadOutput{}, nil
}

func TestCopyS3Object_atTheSinglePutLimitIsOneCopy(t *testing.T) {
	f := &fakeCopyAPI{size: S3CopySingleMax}
	if err := copyS3Object(context.Background(), f, "src", "a/t.parquet", "dst", "b/t.parquet"); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != 1 || f.created != 0 {
		t.Fatalf("copies=%d multipart=%d, want one CopyObject and no multipart at exactly 5 GiB", len(f.copies), f.created)
	}
	c := f.copies[0]
	if aws.ToString(c.Bucket) != "dst" || aws.ToString(c.Key) != "b/t.parquet" || aws.ToString(c.CopySource) != "src/a/t.parquet" {
		t.Fatalf("copy = %s/%s from %s", aws.ToString(c.Bucket), aws.ToString(c.Key), aws.ToString(c.CopySource))
	}
}

func TestCopyS3Object_oneByteOverIsAMultipartCopyCoveringEveryByte(t *testing.T) {
	size := S3CopySingleMax + 1
	f := &fakeCopyAPI{size: size}
	if err := copyS3Object(context.Background(), f, "src", "k", "dst", "k2"); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != 0 || f.created != 1 || f.completed == nil || f.aborted != 0 {
		t.Fatalf("copies=%d created=%d completed=%v aborted=%d", len(f.copies), f.created, f.completed != nil, f.aborted)
	}
	if len(f.parts) > 10000 || len(f.parts) < 2 {
		t.Fatalf("%d parts", len(f.parts))
	}
	var next int64
	for i, p := range f.parts {
		if int(aws.ToInt32(p.PartNumber)) != i+1 {
			t.Fatalf("part %d numbered %d", i, aws.ToInt32(p.PartNumber))
		}
		var lo, hi int64
		if _, err := fmt.Sscanf(aws.ToString(p.CopySourceRange), "bytes=%d-%d", &lo, &hi); err != nil {
			t.Fatal(err)
		}
		if lo != next || hi < lo {
			t.Fatalf("part %d range %s, want it to start at %d", i+1, aws.ToString(p.CopySourceRange), next)
		}
		if i < len(f.parts)-1 && hi-lo+1 < 5<<20 {
			t.Fatalf("part %d is %d bytes, under S3's 5 MiB minimum", i+1, hi-lo+1)
		}
		next = hi + 1
	}
	if next != size {
		t.Fatalf("the parts end at %d, the object is %d bytes", next, size)
	}
	if got := len(f.completed.MultipartUpload.Parts); got != len(f.parts) {
		t.Fatalf("completed %d parts of %d", got, len(f.parts))
	}
}

func TestCopyS3Object_aFailedPartAbortsTheUpload(t *testing.T) {
	f := &fakeCopyAPI{size: S3CopySingleMax * 2, partErrAt: 2}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := copyS3Object(ctx, f, "src", "k", "dst", "k2")
	if err == nil {
		t.Fatal("a failed part copy returned no error")
	}
	if f.aborted != 1 || f.completed != nil {
		t.Fatalf("aborted=%d completed=%v, want one abort and no completion", f.aborted, f.completed != nil)
	}
	if f.abortCtx != nil {
		t.Fatalf("the abort ran on a dead context: %v", f.abortCtx)
	}
}

func TestCopyS3Object_sourceThatCannotBeSizedCopiesNothing(t *testing.T) {
	f := &fakeCopyAPI{headErr: errors.New("403")}
	if err := copyS3Object(context.Background(), f, "src", "k", "dst", "k2"); err == nil || len(f.copies)+f.created != 0 {
		t.Fatalf("err=%v copies=%d created=%d", err, len(f.copies), f.created)
	}
}

func TestS3CopySource_encodesEveryByteS3WouldReadDifferently(t *testing.T) {
	cases := map[string]string{
		"shop/orders.parquet": "b/shop/orders.parquet",
		"my shop/a+b.parquet": "b/my%20shop/a%2Bb.parquet",
		"s/t#1%.parquet":      "b/s/t%231%25.parquet",
		"s/ñandú.parquet":     "b/s/%C3%B1and%C3%BA.parquet",
		"s/t.000000.posdel":   "b/s/t.000000.posdel",
		"s/a~b_c-d.parquet":   "b/s/a~b_c-d.parquet",
		"s/q?x=1&y.parquet":   "b/s/q%3Fx%3D1%26y.parquet",
	}
	for key, want := range cases {
		if got := s3CopySource("b", key); got != want {
			t.Errorf("s3CopySource(%q) = %q, want %q", key, got, want)
		}
	}
	if strings.Contains(s3CopySource("b", "a b"), " ") {
		t.Fatal("a space survived")
	}
}
