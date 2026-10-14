package storage

import (
	"reflect"
	"testing"
)

// TestBucketStoresForChild_RoundTrip: the table crosses to a child whole,
// the secret and the explicit path style included, which BucketStore's own
// JSON drops. A field added to either type later must be added to the wire
// too: the counts below fail until it is.
func TestBucketStoresForChild_RoundTrip(t *testing.T) {
	if n := reflect.TypeFor[BucketStore]().NumField(); n != 4 {
		t.Fatalf("BucketStore has %d fields; carry the new one in bucketStoreWire, then update this count", n)
	}
	if n := reflect.TypeFor[S3Endpoint]().NumField(); n != 4 {
		t.Fatalf("S3Endpoint has %d fields; carry the new one in bucketStoreWire, then update this count", n)
	}
	minio, err := NewBucketStore("http://minio:9000", PathStylePath, "eu-west-1")
	if err != nil {
		t.Fatal(err)
	}
	if minio, err = minio.WithKeys("AKID", "s3cr3t"); err != nil {
		t.Fatal(err)
	}
	if !minio.Endpoint.PathStyleExplicit() {
		t.Fatal("fixture: the explicit path style is not set, so the test would not see it lost")
	}
	regionOnly, err := NewBucketStore("", "", "ap-south-1")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]BucketStore{"copies": minio, "other": regionOnly}
	b, err := EncodeBucketStoresForChild(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBucketStoresForChild(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("crossed as %#v, want %#v", got, want)
	}
	if got["copies"].SecretKey != "s3cr3t" {
		t.Error("the secret did not cross")
	}
	if empty, err := DecodeBucketStoresForChild(nil); err != nil || len(empty) != 0 {
		t.Errorf("an empty input = %v, %v; want an empty table", empty, err)
	}
}
