//go:build integration

package pgbaseline_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// parquetFiles lists every Parquet file under root, relative to it.
func parquetFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".parquet") {
			rel, _ := filepath.Rel(root, p)
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Against a real PostgreSQL: a table whose schema or table name is not one
// path component writes nothing, anywhere, is reported, and the table beside
// it is published. A name that is odd but is one component is stored as ever.
func TestPGBaseline_UnstorableNames_3113(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	pg, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	for i, c := range []struct {
		schema, table string
		stored        bool
	}{
		{"public", "a/b", false},
		{"public", "../t3113", false},
		{"public", "../../t3113", false},
		{"..", "t3113", false},
		{".", "t3113", false},
		{"s/3113", "t", false},
		{"_SUCCESS", "t", false},
		{"_INCOMPLETE", "t", false},
		{"_MANIFEST", "t", false},
		{"views.sql", "t", false},
		{"public", ".views.sql.tmp3113", false},
		{"public", `a\b`, true},
		{"public", "..", true},
		{"public", ".", true},
		{`s\3113`, "t", true},
		{"public", " sp ace 3113 ", true},
		{"public", "Ñandú3113", true},
		{"public", "UPPER3113", true},
		{"public", strings.Repeat("x", 63), true},
	} {
		t.Run(fmt.Sprintf("%q.%q", c.schema, c.table), func(t *testing.T) {
			pub, slot := fmt.Sprintf("t3113_p%d", i), fmt.Sprintf("t3113_s%d", i)
			keepName := fmt.Sprintf("t3113_keep%d", i)
			sch := pgx.Identifier{c.schema}.Sanitize()
			odd, keep := pgx.Identifier{c.schema, c.table}.Sanitize(), pgx.Identifier{"public", keepName}.Sanitize()
			clean := func() {
				bg := context.Background()
				_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
				_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", slot)
				_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+odd+", "+keep)
				if c.schema != "public" {
					_, _ = pg.Exec(bg, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
				}
			}
			clean()
			t.Cleanup(clean)
			var stmts []string
			if c.schema != "public" {
				stmts = append(stmts, "CREATE SCHEMA "+sch)
			}
			stmts = append(stmts,
				"CREATE TABLE "+odd+" (id int PRIMARY KEY)", "INSERT INTO "+odd+" VALUES (1)",
				"CREATE TABLE "+keep+" (id int PRIMARY KEY)", "INSERT INTO "+keep+" VALUES (1)",
				"CREATE PUBLICATION "+pub+" FOR TABLE "+odd+", "+keep)
			for _, q := range stmts {
				if _, err := pg.Exec(ctx, q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			root := t.TempDir()
			out := filepath.Join(root, "l1", "l2", "out")
			st, err := pgbaseline.Run(ctx, pgbaseline.Config{QueryDSN: dsn, ReplDSN: replDSN(dsn),
				SlotName: slot, Publication: pub, OutputDir: out, Parallelism: 2})
			if err != nil {
				t.Fatalf("the run failed: %v", err)
			}
			snap := reconstruct.SnapshotDirName(st.SnapshotTime)
			want := []string{filepath.Join("l1/l2/out", snap, "public", keepName+".parquet")}
			if c.stored {
				want = append(want, filepath.Join("l1/l2/out", snap, c.schema, c.table+".parquet"))
			}
			got := parquetFiles(t, root)
			if len(got) != len(want) {
				t.Fatalf("files = %q, want %q", got, want)
			}
			for _, w := range want {
				found := false
				for _, g := range got {
					found = found || g == w
				}
				if !found {
					t.Fatalf("files = %q, want %q", got, want)
				}
			}
			if _, err := os.Stat(filepath.Join(out, snap, baseline.SuccessMarker)); err != nil {
				t.Fatalf("the snapshot was not published: %v", err)
			}
			if c.stored {
				if len(st.TablesLeftOut) != 0 {
					t.Fatalf("left out: %+v", st.TablesLeftOut)
				}
				return
			}
			if len(st.TablesLeftOut) != 1 || st.TablesLeftOut[0].Schema != c.schema || st.TablesLeftOut[0].Name != c.table || st.TablesLeftOut[0].Reason == "" {
				t.Fatalf("left out: %+v", st.TablesLeftOut)
			}
			t.Logf("SCREEN %s: %s", st.TablesLeftOut[0].Table, st.TablesLeftOut[0].Reason)
		})
	}
}

// Only an unstorable table in the publication: an error that says which and
// why, no snapshot folder and no replication slot.
func TestPGBaseline_EveryTableUnstorable_3113(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	pg, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	const pub, slot = "t3113_only_p", "t3113_only_s"
	odd := pgx.Identifier{"public", "only/3113"}.Sanitize()
	clean := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", slot)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+odd)
	}
	clean()
	t.Cleanup(clean)
	for _, q := range []string{"CREATE TABLE " + odd + " (id int PRIMARY KEY)", "CREATE PUBLICATION " + pub + " FOR TABLE " + odd} {
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(t.TempDir(), "out")
	st, err := pgbaseline.Run(ctx, pgbaseline.Config{QueryDSN: dsn, ReplDSN: replDSN(dsn), SlotName: slot, Publication: pub, OutputDir: out})
	if err == nil || strings.Contains(err.Error(), "only/3113") || !strings.Contains(err.Error(), "log names each one") {
		t.Fatalf("err = %v", err)
	}
	t.Logf("SCREEN %v", err)
	if len(st.TablesLeftOut) != 1 {
		t.Fatalf("left out: %+v", st.TablesLeftOut)
	}
	var slots int
	if err := pg.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if slots != 0 {
		t.Fatal("a run with nothing to store left a replication slot on the source")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("a run with nothing to store created its folder: %v", err)
	}
}

// A file of an earlier version under an odd name is still found and read.
func TestExistingSnapshotWithOddNameStillFound_3113(t *testing.T) {
	root := t.TempDir()
	at := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	snap := filepath.Join(root, reconstruct.SnapshotDirName(at))
	for _, table := range []string{`a\b`, ".."} {
		path := filepath.Join(snap, "public", table+".parquet")
		w, err := baseline.NewWriter(path, []baseline.Column{{Name: "id", RawText: true}}, baseline.WriterConfig{
			Compression: "zstd", RowGroupSize: 10,
			Metadata: map[string]string{baseline.MetaKeySnapshotTimestamp: at.Format(time.RFC3339)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		got, _, _, err := reconstruct.FindBaseline(context.Background(), root, "public", table, at.Add(time.Hour))
		if err != nil {
			t.Fatalf("table %q: %v", table, err)
		}
		if got != path {
			t.Fatalf("table %q found at %q, want %q", table, got, path)
		}
		rows, err := reconstruct.ReadBaselineRows(context.Background(), got, map[string]string{"id": "1"}, 5)
		if err != nil || len(rows) != 1 {
			t.Fatalf("table %q: read %v rows, err %v", table, rows, err)
		}
	}
}
