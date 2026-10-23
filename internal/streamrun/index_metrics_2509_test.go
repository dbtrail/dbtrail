package streamrun

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/status"
)

// The scraper takes the gauges' snapshot (#2509), the one whose event span
// never reads a partition's rows, and nothing else: the seam's default is
// pinned to it, and a scrape goes through the seam.
func TestScrapeIndexMetrics_takesTheGaugesSnapshot(t *testing.T) {
	if reflect.ValueOf(collectForGauges).Pointer() != reflect.ValueOf(status.CollectStatusForGauges).Pointer() {
		t.Fatal("the scraper's snapshot must be status.CollectStatusForGauges, not CollectStatus")
	}
	prev := collectForGauges
	defer func() { collectForGauges = prev }()
	calls := 0
	collectForGauges = func(ctx context.Context, db *sql.DB, dbName string) (*status.StatusData, error) {
		calls++
		return &status.StatusData{}, nil
	}
	scrapeIndexMetrics(context.Background(), nil, "idx", observe.IndexForSource("t2509"))
	if calls != 1 {
		t.Fatalf("the scrape took %d snapshots through the seam, want 1", calls)
	}
}
