//go:build integration

package icebergexport

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// narrowStatus records what capture writes for an ALTER that narrowed
// orders.status from utf8mb4 to latin1 at binlog.000001:450, base+22m: a
// schema snapshot taken for it (id 2) and its schema_changes row. Snapshot 1
// is given the character set the column had before.
func (f fixture) narrowStatus(t *testing.T) {
	t.Helper()
	testutil.MustExec(t, f.db, `UPDATE schema_snapshots SET character_set_name = 'utf8mb4'
		WHERE snapshot_id = 1 AND column_name = 'status'`)
	at := f.base.Add(22 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.MustExec(t, f.db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type, character_set_name, is_nullable)
		SELECT 2, ?, schema_name, table_name, column_name, ordinal_position, column_key, data_type, column_type,
			IF(column_name = 'status', 'latin1', character_set_name), is_nullable
		FROM schema_snapshots WHERE snapshot_id = 1`, at)
	testutil.MustExec(t, f.db, `INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
		VALUES (?, 'binlog.000001', 450, ?, 'orders', 'ALTER TABLE', 'ALTER TABLE orders MODIFY status VARCHAR(20) CHARACTER SET latin1', 2)`,
		at, f.schema)
}

// #2735 in the Iceberg export. A window holding an ALTER that narrowed a
// column's character set:
//
//   - a row the window writes from a change made BEFORE the ALTER, with a
//     character latin1 cannot hold, is refused: the source has '?' there.
//   - rows the window does not touch are already in the Iceberg table from an
//     earlier run and are not read back, so the outcome says so instead of
//     guessing, and a row written after the ALTER exports as it is.
func TestIntegrationExport_charsetNarrowing(t *testing.T) {
	t.Run("a value the ALTER replaced with '?' refuses the window", func(t *testing.T) {
		f := seedFixture(t)
		f.seedFirstWindow(t)
		warehouse := t.TempDir()
		if o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute))); o.Verdict != VerdictLoaded {
			t.Fatalf("run 1 = %+v (%s), want loaded", o, o.Detail)
		}
		f.event(t, 420, 440, 21*time.Minute, 2, "1",
			`{"id":1,"status":"new","amount":10.00,"updated_at":"2026-08-28 12:00:00"}`,
			`{"id":1,"status":"Ñandú ✓","amount":10.00,"updated_at":"2026-08-28 12:00:00"}`)
		f.narrowStatus(t)
		o := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
		if o.Verdict != VerdictRefusedDDL || !strings.Contains(o.Detail, "from utf8mb4 to latin1") {
			t.Fatalf("run 2 = %+v (%s), want refused-ddl naming the narrowing", o, o.Detail)
		}
		if strings.Contains(o.Detail, "✓") {
			t.Errorf("the refusal carries the row's value: %s", o.Detail)
		}
	})

	t.Run("a first load checks every baseline row, so it warns of nothing", func(t *testing.T) {
		f := seedFixture(t)
		f.seedFirstWindow(t)
		f.narrowStatus(t)
		o := runOne(t, f.config(t.TempDir(), f.base.Add(40*time.Minute)))
		if o.Verdict != VerdictLoaded {
			t.Fatalf("run 1 = %+v (%s), want loaded: every status latin1 holds", o, o.Detail)
		}
		if strings.Contains(o.Detail, "Rows this run did not change") {
			t.Errorf("run 1 detail = %q: the rows it loaded were all checked, so nothing was left unchecked", o.Detail)
		}
	})

	t.Run("rows the window leaves in place are a warning, not a guess", func(t *testing.T) {
		f := seedFixture(t)
		f.seedFirstWindow(t)
		warehouse := t.TempDir()
		if o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute))); o.Verdict != VerdictLoaded {
			t.Fatalf("run 1 = %+v (%s), want loaded", o, o.Detail)
		}
		f.narrowStatus(t)
		f.event(t, 500, 600, 25*time.Minute, 2, "2",
			`{"id":2,"status":"paid","amount":22.5,"updated_at":"2026-08-28 13:00:00"}`,
			`{"id":2,"status":"shipped","amount":22.5,"updated_at":"2026-08-28 14:00:00"}`)
		o := runOne(t, f.config(warehouse, f.base.Add(40*time.Minute)))
		if o.Verdict != VerdictExported || o.Upserts != 1 {
			t.Fatalf("run 2 = %+v (%s), want exported with one upsert", o, o.Detail)
		}
		if !strings.Contains(o.Detail, "column status to latin1") || !strings.Contains(o.Detail, "Rows this run did not change") {
			t.Errorf("run 2 detail = %q, want the warning about the rows it left in place", o.Detail)
		}
	})
}
