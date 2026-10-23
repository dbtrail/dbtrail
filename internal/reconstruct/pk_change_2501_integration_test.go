//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2501, through the real fold, a real index and the delta writer
// (publishWithTableDelta): a window holding an UPDATE that moves a row to
// another key publishes a pair whose .posdel names the old key's base row and
// whose .upserts carries the tombstone of the old key (when it ends dead) and
// the row under the new key, and the state read over the pair equals the
// source. Windows C (`UPDATE 1→4; INSERT 1`) and E (`DELETE 2; UPDATE 1→2`)
// of #782, over the base {1=new, 2=paid, 3=shipped}.
func TestReconstructParquet_pkChangeInWindow_2501(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	type want struct {
		state   []string
		posdel  []int64
		upserts []string // "key=op"
	}
	cases := []struct {
		name   string
		events func(ev func(start uint64, evType uint8, pk, before, after string))
		want   want
	}{
		{
			name: "C: UPDATE 1→4; INSERT 1",
			events: func(ev func(uint64, uint8, string, string, string)) {
				ev(100, 2, "1", `{"id":1,"status":"new"}`, `{"id":4,"status":"moved"}`)
				ev(200, 1, "1", "", `{"id":1,"status":"again"}`)
			},
			want: want{state: []string{"1=again", "2=paid", "3=shipped", "4=moved"}, posdel: []int64{0}, upserts: []string{"1=u", "4=u"}},
		},
		{
			name: "E: DELETE 2; UPDATE 1→2",
			events: func(ev func(uint64, uint8, string, string, string)) {
				ev(100, 3, "2", `{"id":2,"status":"paid"}`, "")
				ev(200, 2, "1", `{"id":1,"status":"new"}`, `{"id":2,"status":"took its place"}`)
			},
			want: want{state: []string{"2=took its place", "3=shipped"}, posdel: []int64{0, 1}, upserts: []string{"1=d", "2=u"}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			db, dbName := testutil.CreateTestDB(t)
			if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
				t.Fatalf("CreateIndexTables: %v", err)
			}
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			dsn := testutil.BaseDSN() + "/" + dbName
			const schema = "shop"
			h0 := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
			seedOrdersSnapshot(t, db, schema, h0)
			c.events(func(start uint64, evType uint8, pk, before, after string) {
				var b, a []byte
				if before != "" {
					b = []byte(before)
				}
				if after != "" {
					a = []byte(after)
				}
				testutil.InsertEvent(t, db, "binlog.000001", start, start+100, h0.Add(10*time.Minute).Format("2006-01-02 15:04:05"),
					nil, schema, "orders", evType, pk, nil, b, a)
			})
			root := t.TempDir()
			seedSourceBaseline(t, root, h0, schema)

			at := h0.Add(30 * time.Minute)
			reports, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
				IndexDSN: dsn, BaselineSrc: root, Tables: []string{schema + ".orders"},
				At: at, OutputDir: root, OutputFormat: reconstruct.OutputFormatParquet, TableDeltas: true,
			})
			if err != nil {
				t.Fatalf("ReconstructTables: %v (failures %v)", err, failures)
			}
			if len(reports) != 1 {
				t.Fatalf("%d reports, want 1", len(reports))
			}
			rep := reports[0]
			if !rep.ArithmeticChecked || len(rep.ArithmeticProblems) != 0 {
				t.Errorf("the refresh's arithmetic: checked %v, problems %v", rep.ArithmeticChecked, rep.ArithmeticProblems)
			}
			if rep.EventsApplied != 2 || rep.WindowInserts+rep.WindowDeletes != 1 {
				t.Errorf("report counts %d events, window inserts %d, deletes %d; the PK change must count once, as an update", rep.EventsApplied, rep.WindowInserts, rep.WindowDeletes)
			}
			base, _, _, err := reconstruct.FindBaseline(ctx, root, schema, "orders", at)
			if err != nil {
				t.Fatalf("FindBaseline: %v", err)
			}
			if got := readOrdersState(t, base); !equalStrings(got, c.want.state) {
				t.Errorf("state = %v, want %v", got, c.want.state)
			}
			posdel, upserts := baseline.TableDeltaPaths(base, 0)
			if got := readPosdel(t, posdel); fmt.Sprint(got) != fmt.Sprint(c.want.posdel) {
				t.Errorf(".posdel = %v, want %v (the base rows of the keys the window touched)", got, c.want.posdel)
			}
			if got := readUpserts(t, upserts); !equalStrings(got, c.want.upserts) {
				t.Errorf(".upserts = %v, want %v", got, c.want.upserts)
			}
		})
	}
}

// readPosdel reads the base row numbers a .posdel names, ascending.
func readPosdel(t *testing.T, path string) []int64 {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	rows, err := ddb.Query(fmt.Sprintf("SELECT \"%s\" FROM read_parquet('%s') ORDER BY 1", baseline.TableDeltaPosColumn, path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

// readUpserts reads a .upserts as sorted "key=op" strings.
func readUpserts(t *testing.T, path string) []string {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	rows, err := ddb.Query(fmt.Sprintf("SELECT \"%s\", \"%s\" FROM read_parquet('%s')", baseline.TableDeltaPKColumn, baseline.TableDeltaOpColumn, path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var pk, op string
		if err := rows.Scan(&pk, &op); err != nil {
			t.Fatal(err)
		}
		out = append(out, pk+"="+op)
	}
	sort.Strings(out)
	return strings.Split(strings.Join(out, ","), ",")
}
