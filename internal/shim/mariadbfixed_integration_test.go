//go:build integration

package shim

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestShimMariaDBUUIDKey: a MariaDB table keyed by UUID, as capture indexes
// it since #1944: pk_values holds the key's bytes (spelled by
// event.BuildPKValues), the row images hold base64 of UUID/INET6 bytes. A
// client writes the key as text, in any spelling MariaDB accepts. Before,
// _flashback and _diff looked the text up verbatim and found nothing, and
// _snapshot served the baseline row without the later change: an answer that
// looks valid and is stale. Values came back as base64.
func TestShimMariaDBUUIDKey(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	hourTop := time.Now().UTC().Truncate(time.Hour)
	addHourlyPartition(t, db, hourTop)
	snapTime := hourTop.Add(1 * time.Minute)
	eventTS := hourTop.Add(5 * time.Minute)
	asOf := hourTop.Add(10 * time.Minute)

	snapTS := snapTime.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, "mdb", "devices", "u", 1, "PRI", "uuid", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, "mdb", "devices", "ip", 2, "", "inet6", "YES")

	// 7c5c... puts '|' and '\' bytes in the key: escaped inside pk_values.
	const keyText = "7c5c7c5c-5c7c-0000-0000-000000000000"
	keyBytes, _ := hex.DecodeString("7c5c7c5c5c7c00000000000000000000")
	ipBefore, _ := hex.DecodeString("00000000000000000000000000000001") // ::1
	ipAfter, _ := hex.DecodeString("00000000000000000000ffff00000000")  // ::ffff:0.0.0.0
	pkCols := []metadata.ColumnMeta{{Name: "u", DataType: "uuid", IsPK: true}}
	storedPK := event.BuildPKValues(pkCols, map[string]any{"u": keyBytes})
	b64 := base64.StdEncoding.EncodeToString
	before, _ := json.Marshal(map[string]any{"u": b64(keyBytes), "ip": b64(ipBefore)})
	after, _ := json.Marshal(map[string]any{"u": b64(keyBytes), "ip": b64(ipAfter)})
	testutil.InsertEvent(t, db, "mariadb-bin.000001", 100, 200, eventTS.Format("2006-01-02 15:04:05"), nil,
		"mdb", "devices", 2 /*update*/, storedPK, nil, before, after)

	cols := []baseline.Column{
		{Name: "u", MySQLType: "uuid", ParquetType: baseline.MysqlToParquetNode("uuid")},
		{Name: "ip", MySQLType: "inet6", ParquetType: baseline.MysqlToParquetNode("inet6")},
	}
	baselineDir := writeBaselineSnapshot(t, snapTime, "mdb", "devices", cols, [][]string{
		{keyText, "::1"},
		{"ffffffff-ffff-ffff-ffff-ffffffffffff", "::ffff:10.0.0.1"}, // untouched since the baseline
	})

	h := NewHandlerWithConfig(db, Config{
		AllowGaps: true, NoArchive: true, IndexDBName: dbName, BaselineDir: baselineDir,
	}, slog.Default())
	want := []string{keyText, "::ffff:0.0.0.0"}

	// A row with no event since the baseline: only _snapshot knows it, and
	// only if the UUID key is matched against the baseline's text.
	quiet := TimeTravelQuery{Type: TypeSnapshot, Schema: "mdb", Table: "devices", PKColumn: "u",
		PKValue: "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", AsOf: asOf}
	res, err := h.runSnapshot(quiet)
	if err != nil {
		t.Fatalf("_snapshot of the untouched row: %v", err)
	}
	if cells := rowCells(t, res.Resultset); len(cells) != 1 || !slices.Equal(cells[0], []string{"ffffffff-ffff-ffff-ffff-ffffffffffff", "::ffff:10.0.0.1"}) {
		t.Errorf("_snapshot of the untouched row = %v, want its baseline row", cells)
	}

	for _, typed := range []string{keyText, "7C5C7C5C5C7C00000000000000000000"} {
		q := TimeTravelQuery{Type: TypeFlashback, Schema: "mdb", Table: "devices", PKColumn: "u", PKValue: typed, AsOf: asOf}
		res, err := h.runPointInTime(q)
		if err != nil {
			t.Fatalf("_flashback %q: %v", typed, err)
		}
		if cells := rowCells(t, res.Resultset); len(cells) != 1 || !slices.Equal(cells[0], want) {
			t.Errorf("_flashback %q = %v, want one row %v", typed, cells, want)
		}

		q.Type = TypeSnapshot
		res, err = h.runSnapshot(q)
		if err != nil {
			t.Fatalf("_snapshot %q: %v", typed, err)
		}
		if cells := rowCells(t, res.Resultset); len(cells) != 1 || !slices.Equal(cells[0], want) {
			t.Errorf("_snapshot %q = %v, want one row %v (the baseline row plus the later UPDATE)", typed, cells, want)
		}

		q.Type = TypeDiff
		q.Since, q.Until = snapTime, asOf
		res, err = h.runDiff(q)
		if err != nil {
			t.Fatalf("_diff %q: %v", typed, err)
		}
		if n := len(res.Resultset.RowDatas); n != 1 {
			t.Errorf("_diff %q: %d rows, want the one UPDATE", typed, n)
		}
	}
}
