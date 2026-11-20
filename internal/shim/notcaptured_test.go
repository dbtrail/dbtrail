package shim

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
)

// The not-captured marker stands for a column of a PostgreSQL earlier row that
// was never sent (a table below REPLICA IDENTITY FULL). The shim serves rows as
// of a past time and must never put a marker's JSON on the wire as if it were
// the value: every cell path refuses it, as it refuses the unchanged-TOAST
// marker (#592). It should never reach the shim at all.
func TestShimCells_refuseNotCapturedMarker(t *testing.T) {
	marker := map[string]any{event.NotCapturedKey: true}
	images := []map[string]any{
		{"id": "1", "body": "fine"},
		{"id": "2", "body": marker},
	}
	check := func(t *testing.T, where string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected a refusal for a not-captured cell", where)
		}
		for _, want := range []string{"body", "was not captured", "REPLICA IDENTITY FULL", "cannot serve"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: message missing %q:\n%s", where, want, err)
			}
		}
		if strings.Contains(err.Error(), "cannot undo") {
			t.Errorf("%s: the shim serves rows; it must not talk about undo:\n%s", where, err)
		}
	}

	_, err := imagesToResult(images, []string{"id", "body"})
	check(t, "imagesToResult", err)
	_, err = imagesToResultVerbatim(images, []string{"body"})
	check(t, "imagesToResultVerbatim", err)
	if _, err := imagesToResultVerbatim(images, []string{"id"}); err != nil {
		t.Fatalf("a marker in an unprojected column must not refuse: %v", err)
	}
	single := map[string]any{"id": "2", "body": marker}
	_, err = imageToResult(single, []string{"id", "body"})
	check(t, "imageToResult", err)
	_, err = imageToResultVerbatim(single, []string{"body"})
	check(t, "imageToResultVerbatim", err)

	h := floatsHandler()
	floats := h.timeTravelFloats(TimeTravelQuery{Schema: "myapp", Table: "m", AsOf: time.Now()})
	_, err = h.projectCell(floats, "myapp", "m", "body", marker)
	check(t, "projectCell", err)
	if !strings.Contains(err.Error(), "myapp.m") {
		t.Errorf("projectCell knows the table and must name it:\n%s", err)
	}
	t.Logf("message as shown: %s", err)

	// The unchanged-TOAST refusal keeps its exact #592 text.
	_, err = h.projectCell(floats, "myapp", "m", "body", map[string]any{event.UnchangedToastKey: true})
	if want := event.UnresolvedToastError("myapp", "m", "", []string{"body"}).Error(); err == nil || err.Error() != want {
		t.Fatalf("got  %v\nwant %s", err, want)
	}
}
