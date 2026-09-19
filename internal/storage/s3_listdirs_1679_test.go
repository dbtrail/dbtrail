package storage

import (
	"context"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// dirListMock answers ListObjectsV2 from scripted pages and records every
// input, so a test can pin what was ASKED (delimiter, start-after key) and
// not only what came back.
type dirListMock struct {
	mockS3
	inputs []s3.ListObjectsV2Input
	pages  []*s3.ListObjectsV2Output
}

func (m *dirListMock) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.inputs = append(m.inputs, *in)
	i := len(m.inputs) - 1
	if i >= len(m.pages) {
		return &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}, nil
	}
	return m.pages[i], nil
}

func newDirListBackend(t *testing.T, m *dirListMock) *S3Backend {
	t.Helper()
	m.mockS3 = *newMockS3()
	b, err := newS3BackendFromClient(context.Background(), m, S3Config{Bucket: "test-bucket", Prefix: "base/"})
	if err != nil {
		t.Fatal(err)
	}
	m.inputs = nil // HeadBucket is not a listing; and the constructor made none
	return b
}

func TestS3Backend_ListDirs(t *testing.T) {
	m := &dirListMock{pages: []*s3.ListObjectsV2Output{
		{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("base/2026-09-18T00-00-46Z/")}, {Prefix: aws.String("base/2026-09-18T12-00-44Z/")}},
			Contents:    []types.Object{{Key: aws.String("base/views.sql")}}, // a file at the top level is not a directory
			IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("t1")},
		{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("base/current/")}}, IsTruncated: aws.Bool(false)},
	}}
	b := newDirListBackend(t, m)
	got, err := b.ListDirs(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"2026-09-18T00-00-46Z", "2026-09-18T12-00-44Z", "current"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListDirs = %v, want %v", got, want)
	}
	if len(m.inputs) != 2 || aws.ToString(m.inputs[0].Delimiter) != "/" || aws.ToString(m.inputs[0].Prefix) != "base/" ||
		aws.ToString(m.inputs[1].ContinuationToken) != "t1" {
		t.Fatalf("inputs = %+v, want a delimiter listing under base/ continued with t1", m.inputs)
	}
	// A sub-prefix gets its own trailing slash.
	if _, err := b.ListDirs(context.Background(), "2026-09-18T12-00-44Z"); err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(m.inputs[2].Prefix); got != "base/2026-09-18T12-00-44Z/" {
		t.Fatalf("sub-prefix listed as %q", got)
	}
}

func TestS3Backend_ListInfoFrom(t *testing.T) {
	m := &dirListMock{pages: []*s3.ListObjectsV2Output{
		{Contents: []types.Object{{Key: aws.String("base/2026-09-18T12-00-44Z/_SUCCESS"), Size: aws.Int64(0)}, {Key: aws.String("base/2026-09-18T12-00-44Z/shop/orders.parquet"), Size: aws.Int64(7)}}, IsTruncated: aws.Bool(false)},
	}}
	b := newDirListBackend(t, m)
	got, err := b.ListInfoFrom(context.Background(), "", "2026-09-18T12-00-44Z")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Key != "2026-09-18T12-00-44Z/_SUCCESS" || got[1].Key != "2026-09-18T12-00-44Z/shop/orders.parquet" || got[1].Size != 7 {
		t.Fatalf("ListInfoFrom = %+v", got)
	}
	if in := m.inputs[0]; aws.ToString(in.StartAfter) != "base/2026-09-18T12-00-44Z" || aws.ToString(in.Prefix) != "base/" || in.Delimiter != nil {
		t.Fatalf("input = %+v, want StartAfter under the backend's prefix and no delimiter", in)
	}
	// On a continued page the token carries the position: StartAfter is
	// not re-sent (a compatible store that honoured both would restart).
	m2 := &dirListMock{pages: []*s3.ListObjectsV2Output{
		{Contents: []types.Object{{Key: aws.String("base/a/_SUCCESS")}}, IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("t1")},
		{Contents: []types.Object{{Key: aws.String("base/b/_SUCCESS")}}, IsTruncated: aws.Bool(false)},
	}}
	b2 := newDirListBackend(t, m2)
	if got, err := b2.ListInfoFrom(context.Background(), "", "a"); err != nil || len(got) != 2 {
		t.Fatalf("got=%v err=%v", got, err)
	}
	if aws.ToString(m2.inputs[0].StartAfter) != "base/a" || m2.inputs[1].StartAfter != nil || aws.ToString(m2.inputs[1].ContinuationToken) != "t1" {
		t.Fatalf("inputs = %+v, want StartAfter on the first page only and the token on the second", m2.inputs)
	}
	// "" starts at the beginning: no StartAfter at all (ListInfo's shape).
	if _, err := b.ListInfo(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if m.inputs[1].StartAfter != nil {
		t.Fatalf("ListInfo sent StartAfter %q", aws.ToString(m.inputs[1].StartAfter))
	}
}
