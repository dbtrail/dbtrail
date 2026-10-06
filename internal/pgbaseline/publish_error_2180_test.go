package pgbaseline

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A vanished marker is not reported as "snapshot complete" (#2180), and
// errors.Is still finds it.
func TestPublishError_vanishedMarkerIsNotCalledComplete(t *testing.T) {
	vanished := fmt.Errorf("%w in /x", baseline.ErrIncompleteMarkerVanished)
	got := publishError(vanished)
	if !errors.Is(got, baseline.ErrIncompleteMarkerVanished) || strings.Contains(got.Error(), "snapshot complete") {
		t.Fatalf("vanished: %v", got)
	}
	other := publishError(errors.New("disk full"))
	if !strings.Contains(other.Error(), "snapshot complete") {
		t.Fatalf("a marker-write failure lost its wording: %v", other)
	}
}
