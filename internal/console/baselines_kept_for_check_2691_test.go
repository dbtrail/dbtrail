package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// writeProvenanceSnapshot writes <dir>/<at>/shop/orders.parquet with the
// footer a dump (read zero) or a fold carrying the read `read` writes.
func writeProvenanceSnapshot(t *testing.T, dir string, at, read time.Time) {
	t.Helper()
	const create = "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols, err := baseline.ParseSchemaText(create)
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{baseline.MetaKeyCreateTableSQL: create, baseline.MetaKeySnapshotTimestamp: at.UTC().Format(time.RFC3339)}
	if read.IsZero() {
		md[baseline.MetaKeySnapshotProducer], md[baseline.MetaKeyLastDumpAt] = baseline.ProducerDump, at.UTC().Format(time.RFC3339)
	} else {
		md[baseline.MetaKeySnapshotProducer], md[baseline.MetaKeyLastDumpAt] = baseline.ProducerReconstruct, read.UTC().Format(time.RFC3339)
	}
	path := filepath.Join(dir, reconstruct.SnapshotDirName(at), "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(filepath.Dir(filepath.Dir(path))); err != nil {
		t.Fatal(err)
	}
}

// GET /api/baselines marks the two rows a check compares, on a server
// with a count and no S3 (#2691), and none on one with S3.
func TestBaselinesList_marksThePairKeptForTheCheck(t *testing.T) {
	for _, withS3 := range []bool{false, true} {
		srv, id, dir := s3CopyServerWith(t, &stubScheduleReporter{full: true}, withS3)
		srv.localPruneLoop = true
		e, _ := srv.cm.reg.Get(id)
		e.LocalKeepNewest = 2
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC().Truncate(time.Second)
		at := func(h int) time.Time { return now.Add(-time.Duration(h) * time.Hour) }
		writeProvenanceSnapshot(t, dir, at(4), at(5))
		writeProvenanceSnapshot(t, dir, at(3), time.Time{}) // the last full read
		writeProvenanceSnapshot(t, dir, at(2), at(3))
		writeProvenanceSnapshot(t, dir, at(1), at(3))
		listings := map[string]s3CopyListing{dir: {snaps: []time.Time{at(1), at(2), at(3), at(4)}}}
		if withS3 {
			listings["s3://bkt/snaps"] = s3CopyListing{snaps: []time.Time{at(1), at(2), at(3), at(4)}}
		}
		stubListings(listings)
		rec, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
		if rec.Code != 200 {
			t.Fatalf("GET /api/baselines: %d %s", rec.Code, body)
		}
		var listed struct {
			Snapshots []struct {
				Time         string
				KeptForCheck bool `json:"kept_for_check"`
				Going        bool
				PastCount    bool `json:"past_count"`
			}
		}
		if err := json.Unmarshal(body, &listed); err != nil {
			t.Fatal(err)
		}
		var kept []string
		for _, sn := range listed.Snapshots {
			if sn.KeptForCheck {
				kept = append(kept, sn.Time)
			}
		}
		want := []string{at(3).Format(consoleTSFormat), at(4).Format(consoleTSFormat)}
		if withS3 {
			want = nil
		}
		if !slices.Equal(kept, want) {
			t.Fatalf("with S3 %v: rows kept for the check %v, want %v", withS3, kept, want)
		}
		// Past the count, the pair is not going; with S3 there is no pair, and
		// both go once S3 has them (#2675).
		for _, sn := range listed.Snapshots {
			past := sn.Time == at(3).Format(consoleTSFormat) || sn.Time == at(4).Format(consoleTSFormat)
			if sn.PastCount != past || sn.Going != (past && withS3) {
				t.Errorf("with S3 %v: %s past_count %v going %v, want %v and %v", withS3, sn.Time, sn.PastCount, sn.Going, past, past && withS3)
			}
		}
	}
}
