package console

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The boot server's downloads are capped by the rule its folder is pruned
// by, through the real call site (#2691): the boot server is not in the
// registry, so its destination is the daemon's own --baseline-s3, and its
// folder is pruned by age when that is set. A call site that read the
// destination from anywhere else (the registry entry it does not have)
// would find none, apply no rule, and let downloads hold any number of old
// snapshots. Nothing here replaces downloadRule.
func TestBaselineDownload_theBootServersCapComesFromTheDaemonsBucket(t *testing.T) {
	for _, c := range []struct {
		bootS3 string
		want   int
	}{
		{"s3://bkt/boot", http.StatusTooManyRequests},
		// No bucket: nothing prunes the boot folder by age, so no cap.
		{"", http.StatusOK},
	} {
		dir := newDetailFixture(t)
		olderSnapshot(t, dir, "2026-06-12T12-00-00Z") // newer: the fixture's is past the age
		srv := newBaselineServer(t, dir, true)
		srv.localPruneLoop = true
		srv.backupSettingsDefaults.BaselineRetain = "24h"
		srv.cm.defaultBaselineS3 = c.bootS3
		// The cap of a folder pruned by age alone: three old snapshots held
		// by other downloads.
		for _, n := range []string{"2026-06-01T12-00-00Z", "2026-06-02T12-00-00Z", "2026-06-03T12-00-00Z"} {
			olderSnapshot(t, dir, n)
			release, ok := baseline.PinDownload(dir, n, time.Hour, nil, DefaultLocalKeepNewest, DefaultLocalKeepNewest*downloadCeilingPerCap)
			if !ok {
				t.Fatalf("filling the cap: %s refused", n)
			}
			t.Cleanup(release)
		}
		w := httptest.NewRecorder()
		srv.handleBaselineDownload(w, httptest.NewRequest("GET", "/api/baselines/download"+detailQuery(detailSnapAt), nil))
		if w.Code != c.want {
			t.Fatalf("boot server with --baseline-s3 %q: the download answered %d, want %d", c.bootS3, w.Code, c.want)
		}
		if c.want == http.StatusTooManyRequests {
			t.Logf("the refusal as the client reads it: %s", w.Body.String())
		}
	}
}
