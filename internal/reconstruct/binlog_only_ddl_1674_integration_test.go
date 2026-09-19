//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestBinlogOnly_destructiveDDLInsideTheWindowRefuses is #1674 through the real
// full-table path. shop.t has no backup, so it is rebuilt from its recorded
// changes alone (#766). A TRUNCATE between two of those changes wrote no row
// events, so without a refusal the row inserted before it comes back in the
// output. A statement outside the window read, before the oldest change or
// after the target, is no reason to refuse.
func TestBinlogOnly_destructiveDDLInsideTheWindowRefuses(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	at := base.Add(50 * time.Minute)

	run := func(t *testing.T, truncateAt time.Time, prep ...func(*sql.DB)) (*reconstruct.TableReport, error) {
		t.Helper()
		db, dbName := testutil.CreateTestDB(t)
		if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
			t.Fatalf("CreateIndexTables: %v", err)
		}
		if err := indexer.EnsureSchema(db); err != nil {
			t.Fatalf("EnsureSchema: %v", err)
		}
		ts := func(tm time.Time) string { return tm.Format("2006-01-02 15:04:05") }
		for _, table := range []string{"t", "other"} {
			testutil.InsertSnapshot(t, db, 1, ts(base), "shop", table, "id", 1, "PRI", "int", "NO")
			testutil.InsertSnapshot(t, db, 1, ts(base), "shop", table, "v", 2, "", "varchar", "YES")
		}

		// A real snapshot that holds only shop.other, so shop.t has no backup
		// and takes the binlog-only fallback.
		root := t.TempDir()
		snapDir := filepath.Join(root, strings.ReplaceAll(base.Format(time.RFC3339), ":", "-"))
		const createSQL = "CREATE TABLE `other` (\n  `id` int NOT NULL,\n  `v` varchar(16) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
		cols, err := baseline.ParseSchemaText(createSQL)
		if err != nil {
			t.Fatalf("ParseSchemaText: %v", err)
		}
		w, err := baseline.NewWriter(filepath.Join(snapDir, "shop", "other.parquet"), cols, baseline.WriterConfig{
			Compression: "none", RowGroupSize: 100,
			Metadata: map[string]string{
				baseline.MetaKeyCreateTableSQL: createSQL,
				baseline.MetaKeyBinlogFile:     "binlog.000001",
				baseline.MetaKeyBinlogPos:      "4",
			},
		})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := baseline.WriteSuccessMarker(snapDir); err != nil {
			t.Fatalf("WriteSuccessMarker: %v", err)
		}

		// id=1 before the TRUNCATE (when it is inside the window), id=2 after.
		testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts(base.Add(10*time.Minute)), nil,
			"shop", "t", 1, "1", nil, nil, []byte(`{"id":1,"v":"gone"}`))
		testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts(base.Add(30*time.Minute)), nil,
			"shop", "t", 1, "2", nil, nil, []byte(`{"id":2,"v":"kept"}`))
		if _, err := db.Exec(`INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
			VALUES (?, 'binlog.000001', 250, 'shop', 't', 'TRUNCATE TABLE', 'TRUNCATE TABLE t')`, truncateAt); err != nil {
			t.Fatalf("seed schema_changes: %v", err)
		}

		for _, p := range prep {
			p(db)
		}

		reports, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
			IndexDSN:    testutil.BaseDSN() + "/" + dbName,
			BaselineSrc: root,
			Tables:      []string{"shop.t"},
			At:          at,
			OutputDir:   t.TempDir(),
		})
		for _, f := range failures {
			if f.Table == "t" {
				return nil, f.Err
			}
		}
		if err != nil {
			return nil, err
		}
		for _, r := range reports {
			if r.Table == "t" {
				return r, nil
			}
		}
		t.Fatalf("no report and no failure for shop.t: reports=%d failures=%d", len(reports), len(failures))
		return nil, nil
	}

	refuses := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
			t.Fatalf("err = %v, want ErrDestructiveDDL: the row inserted before the TRUNCATE would come back", err)
		}
		t.Log(err)
		for _, want := range []string{"no backup", "TRUNCATE TABLE"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q does not say %q", err, want)
			}
		}
	}

	// An index too old to record DDL cannot answer the question. Rebuilding
	// is still allowed — a refusal there would deny every such index the
	// fallback — but the operator is told, because this check is the only
	// thing standing between a TRUNCATE and rows coming back.
	t.Run("an index that records no DDL says it could not check", func(t *testing.T) {
		logs := &warnCapture{}
		prev := slog.Default()
		slog.SetDefault(slog.New(logs))
		t.Cleanup(func() { slog.SetDefault(prev) })

		rep, err := run(t, base.Add(20*time.Minute), func(db *sql.DB) {
			if _, err := db.Exec("DROP TABLE schema_changes"); err != nil {
				t.Fatalf("drop schema_changes: %v", err)
			}
		})
		if err != nil {
			t.Fatalf("an index without schema_changes must still rebuild, got: %v", err)
		}
		if rep == nil {
			t.Fatal("no report for shop.t")
		}
		if !logs.saw("could not be checked") {
			t.Error("the rebuild could not check for a TRUNCATE and said nothing: " + logs.text())
		}
	})

	t.Run("inside the window read", func(t *testing.T) {
		_, err := run(t, base.Add(20*time.Minute))
		refuses(t, err)
	})
	t.Run("same second as the oldest change", func(t *testing.T) {
		// The statement may follow that change inside the second: refuse.
		_, err := run(t, base.Add(10*time.Minute))
		refuses(t, err)
	})
	t.Run("before the oldest change read", func(t *testing.T) {
		rep, err := run(t, base.Add(5*time.Minute))
		if err != nil {
			t.Fatalf("a TRUNCATE older than every retained change cannot bring a row back, but the run refused: %v", err)
		}
		if !rep.BinlogOnly || rep.InsertsEmitted != 2 {
			t.Errorf("report = {BinlogOnly:%v InsertsEmitted:%d}, want the binlog-only fallback with both rows", rep.BinlogOnly, rep.InsertsEmitted)
		}
	})
	t.Run("after the target", func(t *testing.T) {
		rep, err := run(t, at.Add(time.Minute))
		if err != nil {
			t.Fatalf("a TRUNCATE after the target is outside the window, but the run refused: %v", err)
		}
		if rep.InsertsEmitted != 2 {
			t.Errorf("InsertsEmitted = %d, want 2", rep.InsertsEmitted)
		}
	})
}

// warnCapture records slog messages so a test can assert on a line that must
// be said. Level-agnostic: what matters is that the operator is told.
type warnCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (c *warnCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *warnCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, r.Message)
	return nil
}
func (c *warnCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *warnCapture) WithGroup(string) slog.Handler      { return c }

func (c *warnCapture) saw(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.msgs {
		if strings.Contains(m, substr) {
			return true
		}
	}
	return false
}

func (c *warnCapture) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.msgs, " | ")
}
