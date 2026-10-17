//go:build integration

package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// LoadPGShapes against a real index: the OID is written and read back, the
// newest shape per OID wins (a rename appears once, under the new name),
// MySQL rows are ignored, a reused name keeps both OIDs, and an OID-less shape
// written before #2441 is dropped once an identified shape was written under
// its name, even after that table was renamed away.
func TestLoadPGShapes_integration(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	ctx := context.Background()

	write := func(oid uint32, table string, cols ...string) int {
		t.Helper()
		r := &PGRelationSchema{OID: oid, Schema: "public", Table: table}
		for i, c := range cols {
			r.Columns = append(r.Columns, PGRelationColumn{Name: c, Ordinal: i + 1, IsPK: i == 0, TypeOID: 23, TypeMod: -1})
		}
		id, err := WritePGSnapshot(ctx, indexDB, r)
		if err != nil {
			t.Fatalf("WritePGSnapshot(%s): %v", table, err)
		}
		return id
	}

	shapes, err := LoadPGShapes(ctx, indexDB, 0)
	if err != nil || len(shapes) != 0 {
		t.Fatalf("empty index: %d shapes, err %v", len(shapes), err)
	}

	write(0, "legacy_kept", "id")             // pre-#2441, never identified since
	write(0, "Legacy_kept", "id")             // another table: PostgreSQL names are case-sensitive
	write(0, "renamed_away", "id")            // pre-#2441 ...
	write(20, "renamed_away", "id")           // ... then identified ...
	renamed := write(20, "renamed_now", "id") // ... then renamed
	write(30, "orders", "id")
	altered := write(30, "orders", "id", "note") // ALTER: newest per OID wins
	reused := write(31, "orders", "id")          // dropped and recreated under the same name
	testutil.MustExec(t, indexDB, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable)
		VALUES (999, NOW(), 'shop', 'mysql_table', 'id', 1, 'PRI', 'int', 'NO')`)

	shapes, err = LoadPGShapes(ctx, indexDB, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int]PGShape{}
	for _, s := range shapes {
		got[s.SnapshotID] = s
	}
	if len(shapes) != 5 {
		t.Fatalf("got %d shapes %+v, want 5", len(shapes), shapes)
	}
	if s := got[renamed]; s.Relation.OID != 20 || s.Relation.Table != "renamed_now" {
		t.Errorf("renamed table = %+v", s.Relation)
	}
	if s := got[altered]; s.Relation.OID != 30 || len(s.Relation.Columns) != 2 || !s.Relation.Columns[0].IsPK ||
		s.Relation.Columns[1].Name != "note" || s.Relation.Columns[1].TypeMod != -1 {
		t.Errorf("altered table = %+v", s.Relation)
	}
	if s := got[reused]; s.Relation.OID != 31 || s.Relation.Table != "orders" {
		t.Errorf("reused name = %+v", s.Relation)
	}
	legacy := 0
	for _, s := range shapes {
		if s.Relation.OID == 0 {
			legacy++
			if s.Relation.Table != "legacy_kept" && s.Relation.Table != "Legacy_kept" {
				t.Errorf("legacy shape %q returned; only legacy_kept and Legacy_kept should be", s.Relation.Table)
			}
		}
	}
	if legacy != 2 {
		t.Errorf("%d legacy shapes, want 2 (case-distinct names are two tables)", legacy)
	}
	for i := 1; i < len(shapes); i++ {
		if shapes[i-1].SnapshotID >= shapes[i].SnapshotID {
			t.Errorf("not ordered by snapshot: %v", ids(shapes))
		}
	}
}

// For a resume position, every shape seen from it on is returned, plus the
// one in effect where PostgreSQL's resend starts; the position and time each
// was seen read back.
func TestLoadPGShapes_fromAResumePosition(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	ctx := context.Background()
	at := time.Date(2026, 10, 17, 12, 0, 0, 123456000, time.UTC)
	write := func(lsn uint64, cols ...string) int {
		t.Helper()
		r := &PGRelationSchema{OID: 40, Schema: "public", Table: "t", SeenLSN: lsn, SeenAt: at.Add(time.Duration(lsn) * time.Second)}
		for i, c := range cols {
			r.Columns = append(r.Columns, PGRelationColumn{Name: c, Ordinal: i + 1, TypeOID: 23, TypeMod: -1})
		}
		id, err := WritePGSnapshot(ctx, indexDB, r)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	old := write(100, "id")
	write(200, "id", "x")
	for _, c := range []struct {
		from uint64
		n    int
	}{{150, 2}, {0, 1}, {300, 1}, {50, 2}} {
		shapes, err := LoadPGShapes(ctx, indexDB, c.from)
		if err != nil || len(shapes) != c.n {
			t.Fatalf("from %d: %d shapes, err %v, want %d", c.from, len(shapes), err, c.n)
		}
	}
	shapes, _ := LoadPGShapes(ctx, indexDB, 150)
	if s := shapes[0]; s.SnapshotID != old || s.Relation.SeenLSN != 100 || !s.Relation.SeenAt.Equal(at.Add(100*time.Second)) {
		t.Errorf("base shape = id %d seen %d@%v", s.SnapshotID, s.Relation.SeenLSN, s.Relation.SeenAt)
	}
}
