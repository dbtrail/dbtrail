package console

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// The snapshots S3 could not be asked about, and when the next send is
// tried, reach the page from the record the cleanup writes (#2689). Before,
// the row carried neither, so the page said "S3 does not have it" about a
// copy S3 had only refused to answer for, and "at the next cleanup" while
// the sends waited an hour.
func TestNotInS3_rowCarriesNotAskedAndNextSend(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	_, dto := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://bucket/prod/"}`)
	e, _ := srv.cm.reg.Get(dto.ID)
	next := time.Date(2026, 11, 11, 4, 0, 0, 0, time.FixedZone("COT", -5*3600))
	rec := baseline.NotInDestination{At: time.Date(2026, 11, 11, 3, 0, 0, 0, time.UTC), Count: 2,
		Names: []string{"2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"}, Resending: true,
		NotAsked: []string{"2026-11-10T03-05-00Z"}, NextSend: &next, ReadRefused: []string{"2026-11-10T03-05-00Z"}, WaitsForAnswer: true}
	if err := baseline.RewriteNotInDestination(e.BaselineDir, rec); err != nil {
		t.Fatal(err)
	}
	row := srv.backupSettingsServerDTO(e)
	raw, _ := json.Marshal(row.NotInS3)
	t.Logf("not_in_s3 on the wire: %s", raw)
	if row.NotInS3 == nil || !slices.Equal(row.NotInS3.NotAsked, rec.NotAsked) || row.NotInS3.NextSend != "2026-11-11T09:00:00Z" {
		t.Fatalf("the row does not carry what S3 could not be asked about, or when the next send is: %s", raw)
	}
	if !strings.Contains(string(raw), `"not_asked":["2026-11-10T03-05-00Z"]`) || !strings.Contains(string(raw), `"next_send":"2026-11-11T09:00:00Z"`) ||
		!strings.Contains(string(raw), `"read_refused":["2026-11-10T03-05-00Z"]`) || !strings.Contains(string(raw), `"waits_for_answer":true`) {
		t.Fatalf("the wire names are not the ones the page reads: %s", raw)
	}
	// A record with neither says neither.
	rec.NotAsked, rec.NextSend, rec.ReadRefused, rec.WaitsForAnswer = nil, nil, nil, false
	if err := baseline.RewriteNotInDestination(e.BaselineDir, rec); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(srv.backupSettingsServerDTO(e).NotInS3)
	if strings.Contains(string(raw), "not_asked") || strings.Contains(string(raw), "next_send") || strings.Contains(string(raw), "read_refused") || strings.Contains(string(raw), "waits_for_answer") {
		t.Fatalf("fields with nothing to say are sent: %s", raw)
	}
}
