//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2684 points 1 and 4 against a real index. shop.t has no snapshot and two
// recorded inserts, and the output folder is one an earlier run wrote into.
//
// Point 1: the refused table's files from the earlier run are removed, so the
// folder holds nothing for it, as the refusal says; files of other tables and
// files this package never writes stay. A table written again with fewer data
// files than before loses the earlier run's higher-numbered ones.
//
// Point 4: when the newest recorded CREATE is a CREATE TABLE IF NOT EXISTS,
// the schema file holds it as a plain CREATE TABLE.
func TestReconstruct2684_reusedFolderAndIfNotExists(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	at := base.Add(50 * time.Minute)

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ts := func(tm time.Time) string { return tm.Format("2006-01-02 15:04:05") }
	testutil.InsertSnapshot(t, db, 1, ts(base), "shop", "t", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts(base), "shop", "t", "v", 2, "", "varchar", "YES")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts(base.Add(10*time.Minute)), nil,
		"shop", "t", 1, "1", nil, nil, []byte(`{"id":1,"v":"one"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts(base.Add(30*time.Minute)), nil,
		"shop", "t", 1, "2", nil, nil, []byte(`{"id":2,"v":"two"}`))

	out := t.TempDir()
	earlier := []string{"shop.t-schema.sql", "shop.t.00000.sql", "shop.t.00001.sql", "shop.t.00002.sql"}
	unrelated := []string{"notes.txt", "shop-schema-create.sql", "shop.t_archive-schema.sql", "shop.t_archive.00000.sql",
		"shop.t-schema-triggers.sql", "shop.t.00000.sql.bak"}
	seed := func(t *testing.T) {
		t.Helper()
		for _, n := range append(append([]string{}, earlier...), unrelated...) {
			if err := os.WriteFile(filepath.Join(out, n), []byte("INSERT INTO `shop`.`t` VALUES (99,'earlier run');\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	run := func(allow bool) ([]*reconstruct.TableReport, error) {
		reports, _, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
			IndexDSN:         testutil.BaseDSN() + "/" + dbName,
			BaselineSrc:      t.TempDir(),
			Tables:           []string{"shop.t"},
			At:               at,
			OutputDir:        out,
			OutputFormat:     reconstruct.OutputFormatMydumper,
			AllowChangesOnly: allow,
		})
		return reports, err
	}
	names := func(t *testing.T) []string {
		t.Helper()
		ents, err := os.ReadDir(out)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, e := range ents {
			got = append(got, e.Name())
		}
		return got
	}

	t.Run("a refused table keeps no file of the earlier run", func(t *testing.T) {
		seed(t)
		_, err := run(false)
		if !errors.Is(err, reconstruct.ErrChangesOnly) {
			t.Fatalf("err = %v, want the refusal", err)
		}
		got := names(t)
		for _, n := range earlier {
			if slices.Contains(got, n) {
				t.Errorf("%s from the earlier run is still in the folder of a refused table", n)
			}
		}
		for _, n := range unrelated {
			if !slices.Contains(got, n) {
				t.Errorf("%s, which is not a file of shop.t, was removed", n)
			}
		}
	})

	// A complete dump of an earlier run, markers included, stays complete
	// after a run refused before any table starts: its files AND its markers.
	for _, refused := range []struct {
		name, baselineSrc, wantErr string
		tables                     []string
	}{
		{"a bad --tables entry", t.TempDir(), "invalid --tables entry", []string{"shop.t", "shop."}},
		{"a snapshot folder that does not exist", filepath.Join(t.TempDir(), "typo"), "cannot be read", []string{"shop.t"}},
	} {
		t.Run("a run refused up front by "+refused.name+" leaves the folder as it was", func(t *testing.T) {
			seed(t)
			os.Remove(filepath.Join(out, baseline.IncompleteMarker))
			for _, m := range []string{baseline.SuccessMarker, baseline.ChangesOnlyMarkerFile} {
				if err := os.WriteFile(filepath.Join(out, m), []byte("{\"schema\":\"shop\",\"table\":\"x\"}\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			before := names(t)
			_, _, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
				IndexDSN:     testutil.BaseDSN() + "/" + dbName,
				BaselineSrc:  refused.baselineSrc,
				Tables:       refused.tables,
				At:           at,
				OutputDir:    out,
				OutputFormat: reconstruct.OutputFormatMydumper,
			})
			if err == nil || !strings.Contains(err.Error(), refused.wantErr) {
				t.Fatalf("err = %v, want %q", err, refused.wantErr)
			}
			t.Logf("rendered: %s", err)
			if after := names(t); !slices.Equal(after, before) {
				t.Errorf("the folder changed:\nbefore %v\nafter  %v", before, after)
			}
		})
	}

	if _, err := db.Exec(`INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES (?, 'binlog.000001', 50, 'shop', 't', 'CREATE TABLE', 'CREATE TABLE t (id int NOT NULL, v varchar(16), PRIMARY KEY (id))'),
		       (?, 'binlog.000001', 60, 'shop', 't', 'CREATE TABLE', 'CREATE TABLE IF NOT EXISTS t (id int NOT NULL, v varchar(16), PRIMARY KEY (id))')`,
		base.Add(5*time.Minute), base.Add(6*time.Minute)); err != nil {
		t.Fatalf("seed schema_changes: %v", err)
	}

	t.Run("a table written again keeps no higher chunk of the earlier run, and its CREATE has no IF NOT EXISTS", func(t *testing.T) {
		seed(t)
		reports, err := run(true)
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if len(reports) != 1 || len(reports[0].Files) != 2 {
			t.Fatalf("reports = %+v, want one table with a schema file and one data file", reports)
		}
		got := names(t)
		for _, n := range []string{"shop.t.00001.sql", "shop.t.00002.sql"} {
			if slices.Contains(got, n) {
				t.Errorf("%s from the earlier run is still in the folder", n)
			}
		}
		for _, n := range unrelated {
			if !slices.Contains(got, n) {
				t.Errorf("%s, which is not a file of shop.t, was removed", n)
			}
		}
		data, err := os.ReadFile(filepath.Join(out, "shop.t.00000.sql"))
		if err != nil || strings.Contains(string(data), "earlier run") {
			t.Errorf("data file = %q, %v", data, err)
		}
		schemaFile, err := os.ReadFile(filepath.Join(out, "shop.t-schema.sql"))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("rendered:\n%s", schemaFile)
		// The comment above the definition names the clause on purpose; the
		// statement a loader runs must not carry it.
		for _, line := range strings.Split(string(schemaFile), "\n") {
			if !strings.HasPrefix(line, "--") && strings.Contains(strings.ToUpper(line), "IF NOT EXISTS") {
				t.Errorf("the statement still carries IF NOT EXISTS: %q", line)
			}
		}
		if !strings.Contains(string(schemaFile), "at\n-- the source it may have created nothing") {
			t.Error("the schema file does not say the definition came from an IF NOT EXISTS")
		}
		if !strings.Contains(string(schemaFile), "CREATE TABLE t (id int NOT NULL, v varchar(16), PRIMARY KEY (id))") {
			t.Error("the schema file lost the recorded definition")
		}
	})
}
