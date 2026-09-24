package consoleapp

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// Every upload the console makes goes through uploadSnapshot, and
// uploadSnapshot is the upload followed by dropping the cached S3 directory
// listing (#1847): the snapshot just written must be on the Snapshots page,
// in the coverage card and under the scheduler's next fold at once. Pinned
// on the wiring, since the package's other tests replace uploadSnapshot
// whole and the real function never runs there.
func TestUploadSnapshotInvalidatesTheS3Inventory(t *testing.T) {
	if reflect.ValueOf(uploadSnapshot).Pointer() != reflect.ValueOf(uploadAndInvalidate).Pointer() {
		t.Fatal("uploadSnapshot is not uploadAndInvalidate: an upload would not invalidate the cached listing")
	}
	prevUpload, prevInvalidate := baselineUpload, invalidateS3Inventory
	t.Cleanup(func() { baselineUpload, invalidateS3Inventory = prevUpload, prevInvalidate })
	for _, tc := range []struct {
		name string
		err  error
	}{{"upload succeeds", nil}, {"upload fails", errors.New("AccessDenied")}} {
		t.Run(tc.name, func(t *testing.T) {
			var invalidated []string
			baselineUpload = func(context.Context, string, string, string, bool) (int, error) { return 3, tc.err }
			invalidateS3Inventory = func(u string) { invalidated = append(invalidated, u) }
			n, err := uploadAndInvalidate(context.Background(), "/tmp/snap", "s3://b/base/2026-09-24T01-30-07Z", "", false)
			if n != 3 || !errors.Is(err, tc.err) {
				t.Fatalf("n=%d err=%v, want the upload's own answer", n, err)
			}
			// On failure too: a partial upload left a directory with its
			// _INCOMPLETE marker, and the listing should see it.
			if !reflect.DeepEqual(invalidated, []string{"s3://b/base/2026-09-24T01-30-07Z"}) {
				t.Fatalf("invalidated = %v, want the snapshot's own URL once", invalidated)
			}
		})
	}
}
