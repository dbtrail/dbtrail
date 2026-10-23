package consoleapp

import (
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/status"
)

// The snapshot update's read of the newest change is the newest row by key
// (#2509), not the exact MAX over the newest partition.
func TestReadNewestIndexed_isTheNewestRowByKey(t *testing.T) {
	if reflect.ValueOf(newestIndexedRead).Pointer() != reflect.ValueOf(status.NewestIndexedRow).Pointer() {
		t.Fatal("readNewestIndexedFromDB must read status.NewestIndexedRow, not NewestIndexedEvent")
	}
}
