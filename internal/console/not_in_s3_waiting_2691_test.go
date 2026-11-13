package console

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// Each snapshot's wait reaches the page (#2691), with when it ends in UTC
// and whether it is for an answer S3 could not give.
func TestNotInS3_rowCarriesEachSnapshotsWait(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	_, dto := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://bucket/prod/"}`)
	e, _ := srv.cm.reg.Get(dto.ID)
	until := time.Date(2026, 11, 11, 4, 0, 0, 0, time.FixedZone("COT", -5*3600))
	rec := baseline.NotInDestination{At: time.Date(2026, 11, 11, 3, 0, 0, 0, time.UTC), Count: 2,
		Names: []string{"2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"}, Resending: true,
		Waiting: []baseline.SendWait{{Name: "2026-11-10T03-00-00Z", Until: until}, {Name: "2026-11-10T03-05-00Z", Until: until.Add(10 * time.Minute), ForAnswer: true}}}
	if err := baseline.RewriteNotInDestination(e.BaselineDir, rec); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(srv.backupSettingsServerDTO(e).NotInS3)
	t.Logf("not_in_s3 on the wire: %s", raw)
	if !strings.Contains(string(raw), `"waiting":[{"name":"2026-11-10T03-00-00Z","until":"2026-11-11T09:00:00Z"},{"name":"2026-11-10T03-05-00Z","until":"2026-11-11T09:10:00Z","for_answer":true}]`) {
		t.Fatalf("the waits are not on the wire as the page reads them: %s", raw)
	}
	rec.Waiting = nil
	if err := baseline.RewriteNotInDestination(e.BaselineDir, rec); err != nil {
		t.Fatal(err)
	}
	if raw, _ = json.Marshal(srv.backupSettingsServerDTO(e).NotInS3); strings.Contains(string(raw), "waiting") {
		t.Fatalf("no wait, and the row says one: %s", raw)
	}
}
