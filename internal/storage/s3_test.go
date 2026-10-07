package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// mockS3 implements s3API for testing.
type mockS3 struct {
	objects map[string][]byte // key → content

	headBucketErr error
	putErr        error
	getErr        error
	headErr       error
	deleteErr     error
	listErr       error

	// condUnsupported makes PutObject reject If-None-Match with
	// NotImplemented, like S3-compatible services without conditional-write
	// support.
	condUnsupported bool

	// condConflict makes PutObject reject If-None-Match with the 409
	// ConditionalRequestConflict AWS documents for a write racing another
	// conditional write to the same key, instead of the more common 412
	// PreconditionFailed.
	condConflict bool

	// Multipart-upload state, exercised by the managed Uploader for bodies
	// larger than its part size. mpMu guards concurrent UploadPart calls.
	mpMu           sync.Mutex
	mpUploads      map[string]*mpMockUpload
	createMPUCount int // number of multipart uploads initiated
	nextUploadID   int
}

// mpMockUpload accumulates the parts of one in-progress multipart upload.
type mpMockUpload struct {
	key   string
	parts map[int32][]byte
}

func newMockS3() *mockS3 {
	return &mockS3{objects: make(map[string][]byte)}
}

func (m *mockS3) HeadBucket(_ context.Context, _ *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	return &s3.HeadBucketOutput{}, m.headBucketErr
}

func (m *mockS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putErr != nil {
		return nil, m.putErr
	}
	if aws.ToString(input.IfNoneMatch) == "*" {
		if m.condUnsupported {
			return nil, &smithy.GenericAPIError{Code: "NotImplemented", Message: "conditional writes not supported"}
		}
		if m.condConflict {
			return nil, &smithy.GenericAPIError{Code: "ConditionalRequestConflict", Message: "a conflicting conditional operation is currently in progress against this resource"}
		}
		if _, exists := m.objects[*input.Key]; exists {
			return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "at least one of the pre-conditions you specified did not hold"}
		}
	}
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.objects[*input.Key] = data
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	data, ok := m.objects[*input.Key]
	if !ok {
		return nil, &types.NoSuchKey{Message: aws.String("not found")}
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *mockS3) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.headErr != nil {
		return nil, m.headErr
	}
	if _, ok := m.objects[*input.Key]; !ok {
		return nil, &types.NotFound{Message: aws.String("not found")}
	}
	return &s3.HeadObjectOutput{}, nil
}

func (m *mockS3) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.deleteErr != nil {
		return nil, m.deleteErr
	}
	delete(m.objects, *input.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	prefix := aws.ToString(input.Prefix)
	var contents []types.Object
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			contents = append(contents, types.Object{Key: aws.String(k)})
		}
	}
	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: aws.Bool(false),
	}, nil
}

func (m *mockS3) CreateMultipartUpload(_ context.Context, input *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if m.putErr != nil {
		return nil, m.putErr
	}
	m.mpMu.Lock()
	defer m.mpMu.Unlock()
	if m.mpUploads == nil {
		m.mpUploads = make(map[string]*mpMockUpload)
	}
	m.createMPUCount++
	m.nextUploadID++
	id := fmt.Sprintf("upload-%d", m.nextUploadID)
	m.mpUploads[id] = &mpMockUpload{key: aws.ToString(input.Key), parts: make(map[int32][]byte)}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(id)}, nil
}

func (m *mockS3) UploadPart(_ context.Context, input *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.mpMu.Lock()
	defer m.mpMu.Unlock()
	up, ok := m.mpUploads[aws.ToString(input.UploadId)]
	if !ok {
		return nil, fmt.Errorf("no such upload %q", aws.ToString(input.UploadId))
	}
	n := aws.ToInt32(input.PartNumber)
	up.parts[n] = data
	return &s3.UploadPartOutput{ETag: aws.String(fmt.Sprintf("\"etag-%d\"", n))}, nil
}

func (m *mockS3) CompleteMultipartUpload(_ context.Context, input *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	m.mpMu.Lock()
	defer m.mpMu.Unlock()
	up, ok := m.mpUploads[aws.ToString(input.UploadId)]
	if !ok {
		return nil, fmt.Errorf("no such upload %q", aws.ToString(input.UploadId))
	}
	nums := make([]int32, 0, len(up.parts))
	for n := range up.parts {
		nums = append(nums, n)
	}
	slices.Sort(nums)
	var buf []byte
	for _, n := range nums {
		buf = append(buf, up.parts[n]...)
	}
	m.objects[up.key] = buf
	delete(m.mpUploads, aws.ToString(input.UploadId))
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (m *mockS3) AbortMultipartUpload(_ context.Context, input *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	m.mpMu.Lock()
	defer m.mpMu.Unlock()
	delete(m.mpUploads, aws.ToString(input.UploadId))
	return &s3.AbortMultipartUploadOutput{}, nil
}

func newTestBackend(t *testing.T, mock *mockS3, prefix string) *S3Backend {
	t.Helper()
	ctx := context.Background()
	b, err := newS3BackendFromClient(ctx, mock, S3Config{
		Bucket: "test-bucket",
		Prefix: prefix,
	})
	if err != nil {
		t.Fatalf("newS3BackendFromClient: %v", err)
	}
	return b
}

func TestPutAndGet(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	content := "hello world"
	if err := b.Put(ctx, "test.parquet", strings.NewReader(content)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := b.Get(ctx, "test.parquet")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != content {
		t.Errorf("Get returned %q, want %q", data, content)
	}
}

func TestPutWithPrefix(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "bintrail/archives")
	ctx := context.Background()

	if err := b.Put(ctx, "2026/data.parquet", strings.NewReader("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The mock should have the full key with prefix.
	if _, ok := mock.objects["bintrail/archives/2026/data.parquet"]; !ok {
		keys := make([]string, 0, len(mock.objects))
		for k := range mock.objects {
			keys = append(keys, k)
		}
		t.Errorf("expected key with prefix, got keys: %v", keys)
	}
}

func TestList(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "pfx")
	ctx := context.Background()

	// Add objects directly to mock with full prefixed keys.
	mock.objects["pfx/a/1.parquet"] = []byte("a1")
	mock.objects["pfx/a/2.parquet"] = []byte("a2")
	mock.objects["pfx/b/3.parquet"] = []byte("b3")

	keys, err := b.List(ctx, "a/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(keys) != 2 {
		t.Fatalf("List returned %d keys, want 2: %v", len(keys), keys)
	}

	// Keys should be relative (prefix stripped).
	for _, k := range keys {
		if !strings.HasPrefix(k, "a/") {
			t.Errorf("key %q does not start with 'a/'", k)
		}
	}
}

func TestListAll(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	mock.objects["x.parquet"] = []byte("x")
	mock.objects["y.parquet"] = []byte("y")

	keys, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("List returned %d keys, want 2", len(keys))
	}
}

func TestDelete(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	mock.objects["doomed.parquet"] = []byte("bye")

	if err := b.Delete(ctx, "doomed.parquet"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok := mock.objects["doomed.parquet"]; ok {
		t.Error("Delete did not remove the object")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	// S3 DeleteObject on a non-existent key is a no-op (not an error).
	if err := b.Delete(ctx, "ghost.parquet"); err != nil {
		t.Fatalf("Delete non-existent: %v", err)
	}
}

func TestDeleteError(t *testing.T) {
	mock := newMockS3()
	mock.deleteErr = fmt.Errorf("delete forbidden")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	err := b.Delete(ctx, "any.parquet")
	if err == nil || !strings.Contains(err.Error(), "delete forbidden") {
		t.Errorf("expected delete error, got: %v", err)
	}
}

func TestGetError(t *testing.T) {
	mock := newMockS3()
	mock.getErr = fmt.Errorf("access denied")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	_, err := b.Get(ctx, "any.parquet")
	if err == nil || !strings.Contains(err.Error(), "access denied") {
		t.Errorf("expected get error, got: %v", err)
	}
}

func TestExists(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	mock.objects["present.parquet"] = []byte("here")

	ok, err := b.Exists(ctx, "present.parquet")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Error("Exists returned false for existing object")
	}

	ok, err = b.Exists(ctx, "missing.parquet")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if ok {
		t.Error("Exists returned true for missing object")
	}
}

func TestExistsNoSuchKey(t *testing.T) {
	mock := newMockS3()
	// Some S3-compatible backends (Ceph, Wasabi) return NoSuchKey
	// from HeadObject instead of NotFound.
	mock.headErr = &types.NoSuchKey{Message: aws.String("not found")}
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	ok, err := b.Exists(ctx, "anything")
	if err != nil {
		t.Fatalf("Exists with NoSuchKey: %v", err)
	}
	if ok {
		t.Error("Exists returned true for NoSuchKey")
	}
}

func TestExistsHTTP404(t *testing.T) {
	mock := newMockS3()
	// Override HeadObject to return a generic HTTP 404 (some S3-compatible
	// backends use this instead of types.NotFound).
	mock.headErr = &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: 404},
		},
		Err: fmt.Errorf("not found"),
	}
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	ok, err := b.Exists(ctx, "anything")
	if err != nil {
		t.Fatalf("Exists with HTTP 404: %v", err)
	}
	if ok {
		t.Error("Exists returned true for HTTP 404")
	}
}

func TestExistsRealError(t *testing.T) {
	mock := newMockS3()
	mock.headErr = fmt.Errorf("network timeout")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	_, err := b.Exists(ctx, "anything")
	if err == nil {
		t.Fatal("Exists should return error for non-404 failures")
	}
	if !strings.Contains(err.Error(), "network timeout") {
		t.Errorf("error should wrap original: %v", err)
	}
}

func TestNewS3BackendValidation(t *testing.T) {
	ctx := context.Background()

	// Empty bucket — use newS3BackendFromClient directly to avoid loading
	// real AWS config from the environment.
	_, err := newS3BackendFromClient(ctx, newMockS3(), S3Config{})
	if err == nil || !strings.Contains(err.Error(), "bucket name is required") {
		t.Errorf("expected bucket validation error, got: %v", err)
	}
}

func TestNewS3BackendHeadBucketFailure(t *testing.T) {
	mock := newMockS3()
	mock.headBucketErr = fmt.Errorf("access denied")
	ctx := context.Background()

	_, err := newS3BackendFromClient(ctx, mock, S3Config{Bucket: "locked-bucket"})
	if err == nil || !strings.Contains(err.Error(), "validate bucket") {
		t.Errorf("expected bucket validation error, got: %v", err)
	}
}

func TestPutError(t *testing.T) {
	mock := newMockS3()
	mock.putErr = fmt.Errorf("write failed")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	err := b.Put(ctx, "fail.parquet", strings.NewReader("data"))
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Errorf("expected put error, got: %v", err)
	}
}

func TestGetMissing(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	_, err := b.Get(ctx, "missing.parquet")
	if err == nil {
		t.Fatal("Get missing key should return error")
	}
}

func TestListError(t *testing.T) {
	mock := newMockS3()
	mock.listErr = fmt.Errorf("permission denied")
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	_, err := b.List(ctx, "")
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("expected list error, got: %v", err)
	}
}

func TestKeyValidation(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	cases := []struct {
		key     string
		wantMsg string
	}{
		{"", "must not be empty"},
		{"/bad", "must not start with /"},
	}

	for _, tc := range cases {
		type op struct {
			name string
			fn   func() error
		}
		ops := []op{
			{"Put", func() error { return b.Put(ctx, tc.key, strings.NewReader("x")) }},
			{"Get", func() error { _, err := b.Get(ctx, tc.key); return err }},
			{"Delete", func() error { return b.Delete(ctx, tc.key) }},
			{"Exists", func() error { _, err := b.Exists(ctx, tc.key); return err }},
		}
		for _, o := range ops {
			err := o.fn()
			if err == nil || !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("%s(%q): want error containing %q, got: %v", o.name, tc.key, tc.wantMsg, err)
			}
		}
	}
}

func TestPrefixNormalization(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"bintrail", "bintrail/"},
		{"bintrail/", "bintrail/"},
		{"a/b/c", "a/b/c/"},
		{"a/b/c/", "a/b/c/"},
	}

	for _, tt := range tests {
		mock := newMockS3()
		b := newTestBackend(t, mock, tt.input)
		if b.prefix != tt.want {
			t.Errorf("prefix %q → %q, want %q", tt.input, b.prefix, tt.want)
		}
	}
}

func TestListPagination(t *testing.T) {
	// Build a mock that returns results in two pages.
	mock := &paginatedMockS3{
		mockS3: mockS3{objects: make(map[string][]byte)},
		pages: [][]string{
			{"a.parquet", "b.parquet"},
			{"c.parquet"},
		},
	}
	ctx := context.Background()
	b, err := newS3BackendFromClient(ctx, mock, S3Config{Bucket: "test"})
	if err != nil {
		t.Fatalf("newS3BackendFromClient: %v", err)
	}

	keys, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("List returned %d keys, want 3: %v", len(keys), keys)
	}
}

// paginatedMockS3 extends mockS3 with paginated List responses.
type paginatedMockS3 struct {
	mockS3
	pages    [][]string
	listCall int
}

func (m *paginatedMockS3) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.listCall >= len(m.pages) {
		return &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}, nil
	}
	page := m.pages[m.listCall]
	m.listCall++

	var contents []types.Object
	for _, k := range page {
		contents = append(contents, types.Object{Key: aws.String(k)})
	}

	hasMore := m.listCall < len(m.pages)
	var nextToken *string
	if hasMore {
		nextToken = aws.String(fmt.Sprintf("token-%d", m.listCall))
	}

	return &s3.ListObjectsV2Output{
		Contents:              contents,
		IsTruncated:           aws.Bool(hasMore),
		NextContinuationToken: nextToken,
	}, nil
}

// Verify S3Backend satisfies the Backend interface at compile time.
var _ Backend = (*S3Backend)(nil)

// TestS3BackendPutMultipart verifies Put routes through the managed Uploader:
// small bodies take the single PutObject path, while bodies above the
// Uploader's part size (~5 MiB) transparently switch to a multipart upload —
// the fix for partitions above S3's 5 GiB single-PUT ceiling. The reassembled
// object must byte-match the input (part ordering is load-bearing).
func TestS3BackendPutMultipart(t *testing.T) {
	tests := []struct {
		name          string
		size          int
		wantMultipart bool
	}{
		{"small single-put", 4096, false},
		{"large multipart", 6 * 1024 * 1024, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := newMockS3()
			b := newTestBackend(t, mock, "prefix/")

			payload := make([]byte, tc.size)
			for i := range payload {
				payload[i] = byte((i * 7) % 251) // position-dependent so misordered parts fail
			}

			if err := b.Put(context.Background(), "big.parquet", bytes.NewReader(payload)); err != nil {
				t.Fatalf("Put: %v", err)
			}

			got := mock.objects["prefix/big.parquet"]
			if !bytes.Equal(got, payload) {
				t.Fatalf("stored %d bytes, want %d (or wrong order)", len(got), len(payload))
			}
			if usedMultipart := mock.createMPUCount > 0; usedMultipart != tc.wantMultipart {
				t.Fatalf("multipart used = %v (createMPUCount=%d), want %v", usedMultipart, mock.createMPUCount, tc.wantMultipart)
			}
		})
	}
}

// TestUploadFileMultipart verifies the file-upload path (rotation archive /
// baseline snapshots) also streams a large file through the multipart Uploader
// rather than a single PutObject. It exercises uploadReader directly with the
// multipart-capable mock so no *s3.Client is required.
func TestUploadFileMultipart(t *testing.T) {
	mock := newMockS3()
	mock.objects = map[string][]byte{}

	payload := make([]byte, 6*1024*1024)
	for i := range payload {
		payload[i] = byte((i * 13) % 251)
	}

	if err := uploadReader(context.Background(), mock, "bucket", "archive/big.parquet", bytes.NewReader(payload)); err != nil {
		t.Fatalf("uploadReader: %v", err)
	}
	if mock.createMPUCount == 0 {
		t.Fatal("expected multipart upload for a 6 MiB body, got single PutObject")
	}
	if got := mock.objects["archive/big.parquet"]; !bytes.Equal(got, payload) {
		t.Fatalf("stored %d bytes, want %d (or wrong order)", len(got), len(payload))
	}
}

func TestPutIfAbsentCreatesWhenAbsent(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "bintrail")
	ctx := context.Background()

	if err := b.PutIfAbsent(ctx, "marker", strings.NewReader("v1")); err != nil {
		t.Fatalf("PutIfAbsent: %v", err)
	}
	if got := string(mock.objects["bintrail/marker"]); got != "v1" {
		t.Errorf("stored content under prefixed key = %q, want v1", got)
	}
}

func TestPutIfAbsentExistingObjectRefused(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	ctx := context.Background()
	mock.objects["marker"] = []byte("original")

	err := b.PutIfAbsent(ctx, "marker", strings.NewReader("intruder"))
	if !errors.Is(err, ErrObjectExists) {
		t.Fatalf("want ErrObjectExists, got: %v", err)
	}
	if got := string(mock.objects["marker"]); got != "original" {
		t.Errorf("existing object was overwritten: %q", got)
	}
}

func TestPutIfAbsentHTTP412Refused(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	// Some S3-compatible backends surface the conditional-write conflict as
	// a generic HTTP 412 without an S3 error code.
	mock.putErr = &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: http.StatusPreconditionFailed},
		},
		Err: fmt.Errorf("precondition failed"),
	}

	err := b.PutIfAbsent(context.Background(), "marker", strings.NewReader("x"))
	if !errors.Is(err, ErrObjectExists) {
		t.Fatalf("want ErrObjectExists for generic HTTP 412, got: %v", err)
	}
}

// TestPutIfAbsentConditionalRequestConflictRefused pins the #806 review
// finding: AWS documents a 409 ConditionalRequestConflict response (as
// opposed to the more common 412 PreconditionFailed) when a conflicting
// write races the same key during upload — the exact two-writer race this
// method exists to protect. Before the fix this error code fell through to
// the generic error branch, so the losing writer of the race got a hard
// error instead of ErrObjectExists and the caller's read-and-compare
// fallback was never reached.
func TestPutIfAbsentConditionalRequestConflictRefused(t *testing.T) {
	mock := newMockS3()
	mock.condConflict = true
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	err := b.PutIfAbsent(ctx, "marker", strings.NewReader("intruder"))
	if !errors.Is(err, ErrObjectExists) {
		t.Fatalf("want ErrObjectExists for 409 ConditionalRequestConflict, got: %v", err)
	}
}

// TestPutIfAbsentHTTP409Refused mirrors TestPutIfAbsentHTTP412Refused for the
// generic-HTTP-status form of the same 409 ConditionalRequestConflict case,
// for backends that surface it without an S3 error code.
func TestPutIfAbsentHTTP409Refused(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	mock.putErr = &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: http.StatusConflict},
		},
		Err: fmt.Errorf("conditional request conflict"),
	}

	err := b.PutIfAbsent(context.Background(), "marker", strings.NewReader("x"))
	if !errors.Is(err, ErrObjectExists) {
		t.Fatalf("want ErrObjectExists for generic HTTP 409, got: %v", err)
	}
}

func TestPutIfAbsentNotImplementedFallsBack(t *testing.T) {
	mock := newMockS3()
	mock.condUnsupported = true
	b := newTestBackend(t, mock, "")
	ctx := context.Background()

	// Capture slog to assert the degraded-guarantee warning fired — before
	// the fix this fallback was silent, so operators on S3-compatible
	// backends without conditional-write support had no signal that the
	// put-if-absent atomicity guarantee was inactive.
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	// Backends without conditional-write support degrade to a plain Put —
	// the pre-conditional behavior — instead of failing the write.
	if err := b.PutIfAbsent(ctx, "marker", strings.NewReader("v1")); err != nil {
		t.Fatalf("PutIfAbsent with NotImplemented backend: %v", err)
	}
	if got := string(mock.objects["marker"]); got != "v1" {
		t.Errorf("fallback Put did not store object, got %q", got)
	}
	if !strings.Contains(logBuf.String(), "does not support S3 conditional writes") {
		t.Errorf("want a warning logged for the NotImplemented fallback, got log output: %q", logBuf.String())
	}
}

func TestPutIfAbsentOtherErrorPropagates(t *testing.T) {
	mock := newMockS3()
	b := newTestBackend(t, mock, "")
	mock.putErr = errors.New("s3 down")

	err := b.PutIfAbsent(context.Background(), "marker", strings.NewReader("x"))
	if err == nil || errors.Is(err, ErrObjectExists) {
		t.Fatalf("want plain propagated error, got: %v", err)
	}
}

// #2181 review: a snapshot upload sends files under 16 MiB eight at a time.
// With the SDK's 5 MiB default part size a 6 to 16 MiB file was a multipart
// upload, and a sibling's failure cancelled it mid-way, leaving its parts
// behind in the bucket (the abort runs on the cancelled context). A FILE
// body is read in place, not buffered, so the file path uses parts of
// UploadFileSinglePutMax: anything below it is one PUT, nothing to orphan.
func TestUploadFileBody_singlePutBelowTheLimit_2181(t *testing.T) {
	for _, tc := range []struct {
		size          int
		wantMultipart bool
	}{
		{16 << 20, false},
		{UploadFileSinglePutMax - 1, false},
		{UploadFileSinglePutMax + 1, true},
	} {
		mock := newMockS3()
		mock.objects = map[string][]byte{}
		payload := make([]byte, tc.size)
		for i := range payload {
			payload[i] = byte((i * 13) % 251)
		}
		if err := uploadFileBody(context.Background(), mock, "bucket", "k", bytes.NewReader(payload)); err != nil {
			t.Fatalf("%d bytes: %v", tc.size, err)
		}
		if got := mock.createMPUCount > 0; got != tc.wantMultipart {
			t.Errorf("%d bytes: multipart = %v, want %v", tc.size, got, tc.wantMultipart)
		}
		if !bytes.Equal(mock.objects["k"], payload) {
			t.Errorf("%d bytes: stored content differs", tc.size)
		}
	}
}
