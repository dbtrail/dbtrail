package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2052: a snapshot whose data and _SUCCESS reached S3 but whose root pointer
// did not is uploaded: the console's single upload door reports success (and
// logs the stale pointer), so no caller treats a published backup as lost.
// Every other upload error still fails.
func TestUploadAndInvalidate_stalePointerIsNotALostUpload_2052(t *testing.T) {
	saveU, saveI := baselineUpload, invalidateS3Inventory
	defer func() { baselineUpload, invalidateS3Inventory = saveU, saveI }()
	invalidateS3Inventory = func(string) {}

	baselineUpload = func(context.Context, string, string, string, bool) (int, error) {
		return 7, fmt.Errorf("%w: snapshot 2026-10-04T12-00-03Z; writing p/_NEWEST: AccessDenied", baseline.ErrNewestPointer)
	}
	if n, err := uploadAndInvalidate(context.Background(), "/x", "s3://b/p/2026-10-04T12-00-03Z", "", false); err != nil || n != 7 {
		t.Errorf("stale pointer: n=%d err=%v, want 7 and no error", n, err)
	}

	boom := errors.New("upload failed")
	baselineUpload = func(context.Context, string, string, string, bool) (int, error) { return 3, boom }
	if _, err := uploadAndInvalidate(context.Background(), "/x", "s3://b/p/x", "", false); !errors.Is(err, boom) {
		t.Errorf("a real upload failure: err=%v, want it returned", err)
	}
}
