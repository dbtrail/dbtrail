package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// chainFixtureTable writes <root>/<snapshot>/<schema>/<table>.parquet and the
// pairs 0..last of a chain of deltas beside it, every pair recording
// chainStart. last = -1 writes the table file alone.
func chainFixtureTable(t *testing.T, root, snapshot, schema, table string, last int, chainStart string) string {
	t.Helper()
	dir := filepath.Join(root, snapshot, schema)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, table+".parquet")
	cols := []baseline.Column{{Name: "id", MySQLType: "int", ParquetType: parquet.Leaf(parquet.Int32Type)}}
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
		baseline.MetaKeyBinlogFile: "binlog.000042", baseline.MetaKeyBinlogPos: "12345",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for seq := 0; seq <= last; seq++ {
		md := map[string]string{
			baseline.MetaKeyBinlogFile: "binlog.000042", baseline.MetaKeyBinlogPos: strconv.Itoa(20000 + seq),
			baseline.MetaKeyDeltaBaseAnchor: "binlog.000042:12345", baseline.MetaKeyDeltaBaseSize: "1",
		}
		if chainStart != "" {
			md[baseline.MetaKeyDeltaChainStart] = chainStart
		}
		if err := baseline.WriteTableDeltaPair(base, seq, cols, baseline.WithDeltaSeq(md, seq), nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	return base
}

// TestStatusBaselines_gradesOnTheChainStart (#1707) walks a real folder the
// way `bintrail status --baseline-dir` does and grades what the walk found.
// The snapshot folder is 12:00 and the index covers from 06:00, so graded on
// the folder every table reads ok; a restore of orders fetches events from
// 02:00, where its chain of deltas started, and refuses.
func TestStatusBaselines_gradesOnTheChainStart(t *testing.T) {
	root := t.TempDir()
	const snap = "2026-09-27T12-00-00Z"
	dir := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	chainFixtureTable(t, root, snap, "shop", "orders", 3, "2026-09-27T02:00:00Z")
	chainFixtureTable(t, root, snap, "shop", "users", 0, "2026-09-27T11:00:00Z")
	chainFixtureTable(t, root, snap, "shop", "orders_2", -1, "")
	chainFixtureTable(t, root, snap, "shop", "nostart", 0, "")
	half := chainFixtureTable(t, root, snap, "shop", "half", 1, "2026-09-27T02:00:00Z")
	posdel, _ := baseline.TableDeltaPaths(half, 1)
	if err := os.Remove(posdel); err != nil {
		t.Fatal(err)
	}
	// The same table name in another schema, with a chain inside coverage.
	chainFixtureTable(t, root, snap, "other", "orders", 0, "2026-09-27T09:00:00Z")

	found, unreadable, err := baseline.DiscoverBaselinesReport(root)
	if err != nil || len(unreadable) != 0 {
		t.Fatalf("err = %v, unreadable = %v", err, unreadable)
	}
	got := statusBaselines(found)
	if len(got) != 6 {
		t.Fatalf("listed %d files, want 6: %+v", len(got), got)
	}
	floor := status.DeltaFloor{Hour: time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)}
	now := time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC)
	status.AnnotateBaselineStaleness(got, floor, now)

	want := map[string]struct {
		verdict status.BaselineStalenessVerdict
		from    time.Time
		unread  bool
	}{
		"shop.orders":   {status.BaselineBroken, time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC), false},
		"shop.users":    {status.BaselineOK, time.Date(2026, 9, 27, 11, 0, 0, 0, time.UTC), false},
		"shop.orders_2": {status.BaselineOK, dir, false},
		"shop.nostart":  {status.BaselineUnknown, dir, true},
		"shop.half":     {status.BaselineUnknown, dir, true},
		"other.orders":  {status.BaselineOK, time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC), false},
	}
	for _, b := range got {
		k := b.Database + "." + b.Table
		w, ok := want[k]
		if !ok {
			t.Errorf("%s: not expected", k)
			continue
		}
		if b.Staleness != w.verdict {
			t.Errorf("%s: verdict %q, want %q", k, b.Staleness, w.verdict)
		}
		if b.Bound.Unread != w.unread {
			t.Errorf("%s: unread = %v, want %v", k, b.Bound.Unread, w.unread)
		}
		if w.unread {
			continue
		}
		from := b.Bound.From(b.SnapshotTime)
		if !from.Equal(w.from) {
			t.Errorf("%s: graded on %s, want %s", k, from, w.from)
		}
		// The seam: what a reader of this very file fetches events from.
		path, reader, _, err := reconstruct.FindBaseline(context.Background(), root, b.Database, b.Table, dir.Add(time.Minute))
		if err != nil {
			t.Fatalf("FindBaseline %s: %v", k, err)
		}
		if path != b.Path {
			t.Fatalf("%s: a reader opens %s, status graded %s", k, path, b.Path)
		}
		if !from.Equal(reader) {
			t.Errorf("%s: status grades on %s, a reader fetches from %s", k, from, reader)
		}
	}
	if overall := status.OverallBaselineStaleness(got); overall != status.BaselineBroken {
		t.Errorf("headline %q, want broken", overall)
	}

	// The report an operator reads, from the same entries.
	var buf bytes.Buffer
	(&status.StatusData{Baselines: got}).Write(&buf)
	text := buf.String()
	checked := 0
	for _, line := range strings.Split(text, "\n") {
		// SNAPSHOT is a date and a time, so the schema is the third field.
		f := strings.Fields(line)
		if len(f) < 4 || f[2] != "shop" {
			continue
		}
		switch f[3] {
		case "orders":
			checked++
			if !strings.Contains(line, "2026-09-27 02:00:00") || !strings.Contains(line, "⚠ broken") {
				t.Errorf("orders row: %q", line)
			}
		case "half", "nostart":
			checked++
			if !strings.Contains(line, "unreadable") || !strings.HasSuffix(strings.TrimSpace(line), "unknown") {
				t.Errorf("%s row: %q", f[3], line)
			}
		}
	}
	if checked != 3 {
		t.Errorf("checked %d rows of the report, want 3:\n%s", checked, text)
	}
	if !strings.Contains(text, "BASELINE STALE") {
		t.Errorf("no banner for the broken table:\n%s", text)
	}
}
