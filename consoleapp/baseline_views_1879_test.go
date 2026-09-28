package consoleapp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #1879: a full read of a source that holds views says how many it left out,
// on the published status, on the final status and on the run record.
func TestDump_countsTheViewsItSkipped(t *testing.T) {
	at := time.Date(2026, 9, 27, 11, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name  string
		views []string
		want  int
	}{
		{"two views", []string{"shop.big_orders", "shop.totals"}, 2},
		{"no views", nil, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			local := t.TempDir()
			ds := stubDumpUpload(t, map[string]bool{}, nil)
			sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
			sup.history = openTestHistory(t)
			req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
			sup.jobs["a"] = &console.BaselineStatus{State: "running"}
			out := dumpOutcomeAt(t, local, at)
			out.stats.ViewsSkipped = tt.views

			done := make(chan struct{})
			go func() { defer close(done); completeDumpOwn(sup, req, out, nil) }()
			published := waitDump(t, sup, "a", func(st console.BaselineStatus) bool { return st.Published && st.Uploading },
				"published, uploading")
			if published.ViewsSkipped != tt.want {
				t.Errorf("published status: ViewsSkipped = %d, want %d", published.ViewsSkipped, tt.want)
			}
			close(ds.hold)
			<-done

			st := sup.Status("a")
			if st.State != "succeeded" || st.ViewsSkipped != tt.want {
				t.Errorf("final status = %+v, want succeeded with ViewsSkipped %d", st, tt.want)
			}
			runs := sup.history.List("a")
			if len(runs) != 1 || runs[0].ViewsSkipped != tt.want {
				t.Fatalf("history = %+v, want one run with ViewsSkipped %d", runs, tt.want)
			}

			// What travels to the browser: the count, and no key at all when
			// there was nothing to count.
			for what, v := range map[string]any{"status": st, "run record": runs[0]} {
				raw, err := json.Marshal(v)
				if err != nil {
					t.Fatal(err)
				}
				has := strings.Contains(string(raw), `"views_skipped":2`)
				if tt.want == 2 && !has {
					t.Errorf("%s JSON %s lacks \"views_skipped\":2", what, raw)
				}
				if tt.want == 0 && strings.Contains(string(raw), "views_skipped") {
					t.Errorf("%s JSON %s reports views for a source that has none", what, raw)
				}
			}
		})
	}
}
