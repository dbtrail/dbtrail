//go:build integration

package reconstruct

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// SpellIndexPKFilter is the seam every --pk/--pks lookup goes through (CLI
// query and recover, MCP, console): for a system-versioned table it must
// turn the declared-key value into the stored spellings (#2007), and leave
// any other table's lookup as it was.
func TestSpellIndexPKFilter_sysVersionedKey_2007(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	for _, row := range []string{
		"(1, NOW(), 'shop', 'prices', 'id', 1, 'PRI', 'int', 'NO', 0)",
		"(1, NOW(), 'shop', 'prices', 'row_start', 2, '', 'timestamp', 'NO', 1)",
		"(1, NOW(), 'shop', 'prices', 'row_end', 3, 'PRI', 'timestamp', 'NO', 1)",
		"(1, NOW(), 'shop', 'plain', 'id', 1, 'PRI', 'int', 'NO', 0)",
	} {
		testutil.MustExec(t, db, "INSERT INTO schema_snapshots (snapshot_id, snapshot_time, schema_name, table_name, "+
			"column_name, ordinal_position, column_key, data_type, is_nullable, is_generated) VALUES "+row)
	}

	opts := query.Options{Schema: "shop", Table: "prices", PKValues: "2"}
	if _, err := SpellIndexPKFilter(context.Background(), db, &opts); err != nil {
		t.Fatal(err)
	}
	want := "2,2|2038-01-19 03:14:07.999999,2|2106-02-07 06:28:15.999999"
	if opts.PKValues != "" || opts.PKValuesAlt != "" || strings.Join(opts.PKValuesIn, ",") != want {
		t.Fatalf("versioned --pk: PKValues=%q Alt=%q In=%q, want only In=%q", opts.PKValues, opts.PKValuesAlt, opts.PKValuesIn, want)
	}

	opts = query.Options{Schema: "shop", Table: "prices", PKValuesIn: []string{"2", "3"}}
	if _, err := SpellIndexPKFilter(context.Background(), db, &opts); err != nil {
		t.Fatal(err)
	}
	if len(opts.PKValuesIn) != 6 {
		t.Fatalf("versioned --pks: In=%q, want both values with both markers", opts.PKValuesIn)
	}

	opts = query.Options{Schema: "shop", Table: "plain", PKValues: "2"}
	if _, err := SpellIndexPKFilter(context.Background(), db, &opts); err != nil {
		t.Fatal(err)
	}
	if opts.PKValues != "2" || len(opts.PKValuesIn) != 0 {
		t.Fatalf("plain table: PKValues=%q In=%q, want the lookup unchanged", opts.PKValues, opts.PKValuesIn)
	}
}
