//go:build integration

package query

import (
	"context"
	"database/sql"
	"reflect"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedMySQLDroppedTable3110 builds a MySQL-layout index where the two loaders
// disagree: "gone" is in the first whole-schema snapshot and was dropped
// before the second. NewResolver(db, 0) does not know it; the per-table loader
// does. stream is the stream_state row to write: a flavor, "" for no row, or
// "unreadable" to drop the table so the flavor read fails.
func seedMySQLDroppedTable3110(t *testing.T, stream string) (*sql.DB, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.MustExec(t, db, `CREATE TABLE gone (id INT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
	testutil.MustExec(t, db, `CREATE TABLE kept (id INT NOT NULL PRIMARY KEY, v INT) ENGINE=InnoDB`)
	if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	testutil.MustExec(t, db, `DROP TABLE gone`)
	if _, err := metadata.TakeSnapshot(db, db, []string{dbName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	switch stream {
	case "":
	case "unreadable":
		testutil.MustExec(t, db, `DROP TABLE stream_state`)
	default:
		testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1, '{}')`, stream)
	}
	return db, dbName
}

// seedPGShapes3110 builds a PostgreSQL index the way the capture writes it:
// one table per snapshot, orders saved twice (legacy dropped in between), then
// two other tables. Returns the snapshot that holds orders WITH legacy.
func seedPGShapes3110(t *testing.T) (*sql.DB, int) {
	t.Helper()
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ctx := context.Background()
	first, err := metadata.WritePGSnapshot(ctx, db, pgRel3110("orders", "status", "legacy"))
	if err != nil {
		t.Fatal(err)
	}
	for _, rel := range []*metadata.PGRelationSchema{pgRel3110("orders", "status"), pgRel3110("users", "name"), pgRel3110("items", "name")} {
		if _, err := metadata.WritePGSnapshot(ctx, db, rel); err != nil {
			t.Fatal(err)
		}
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1, '{}')`)
	return db, int(first)
}

func pgRel3110(table string, cols ...string) *metadata.PGRelationSchema {
	rel := &metadata.PGRelationSchema{Schema: "public", Table: table,
		Columns: []metadata.PGRelationColumn{{Name: "id", Ordinal: 1, IsPK: true, TypeOID: 23, TypeMod: -1}}}
	for i, c := range cols {
		rel.Columns = append(rel.Columns, metadata.PGRelationColumn{Name: c, Ordinal: i + 2, TypeOID: 25, TypeMod: -1})
	}
	return rel
}

// #3110: every index that is not known to be PostgreSQL gets exactly what
// metadata.NewResolver(db, 0) gave before the fork, including when the flavor
// cannot be read.
func TestLatestSchemaResolver_MySQLFamilyUnchanged_3110(t *testing.T) {
	for _, stream := range []string{"mysql", "mariadb", "", "unreadable"} {
		t.Run("stream="+stream, func(t *testing.T) {
			db, dbName := seedMySQLDroppedTable3110(t, stream)
			want, err := metadata.NewResolver(db, 0)
			if err != nil {
				t.Fatal(err)
			}
			got, err := LatestSchemaResolver(db)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("LatestSchemaResolver differs from NewResolver(db, 0) on a %q index", stream)
			}
			if _, err := got.Resolve(dbName, "gone"); err == nil {
				t.Errorf("the dropped table resolves: the per-table loader was used on a %q index", stream)
			}
			// The fixture discriminates: the per-table loader knows the dropped table.
			pt, err := metadata.NewLatestPerTableResolver(db)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pt.Resolve(dbName, "gone"); err != nil {
				t.Fatalf("fixture: the per-table loader does not know the dropped table: %v", err)
			}
		})
	}
}

func TestLatestSchemaResolver_PGEveryTable_3110(t *testing.T) {
	db, _ := seedPGShapes3110(t)
	r, err := LatestSchemaResolver(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{"orders", "users", "items"} {
		if _, err := r.Resolve("public", tbl); err != nil {
			t.Errorf("public.%s: %v", tbl, err)
		}
	}
	tm, err := r.Resolve("public", "orders")
	if err != nil {
		t.Fatal(err)
	}
	var cols []string
	for _, c := range tm.Columns {
		cols = append(cols, c.Name)
	}
	if !slices.Equal(cols, []string{"id", "status"}) {
		t.Errorf("public.orders columns = %v, want its newest shape [id status]", cols)
	}
}
