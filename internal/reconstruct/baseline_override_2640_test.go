package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A table read again alone (#2640): what a BaselineOverride entry must be
// before a fold starts from it. Every case here is a refusal or the one
// accepted shape; the folds themselves are in the integration tests.

// stagedRead writes a finished read of schema.table under staging, in a
// folder named after at, and returns the table file's path.
func stagedRead(t *testing.T, staging string, at time.Time, schema, table string) string {
	t.Helper()
	dir := filepath.Join(staging, SnapshotDirName(at))
	p := filepath.Join(dir, schema, table+".parquet")
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(p, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1", "new"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	return p
}

func overrideCfg(root string, at time.Time, tables []string, override map[string]string) FullTableConfig {
	return FullTableConfig{
		IndexDSN: "unused", BaselineSrc: root, OutputDir: root, Tables: tables, At: at,
		OutputFormat: OutputFormatParquet, BaselineOverride: override,
	}
}

func TestBaselineOverride_noEntryLooksNothingUp(t *testing.T) {
	cfg := overrideCfg(t.TempDir(), time.Now(), []string{"shop.orders"}, map[string]string{"shop.items": "/nowhere"})
	if _, ok, err := baselineOverrideFor(context.Background(), cfg, "shop", "orders"); ok || err != nil {
		t.Fatalf("a table with no entry: ok=%v err=%v, want neither", ok, err)
	}
	cfg.BaselineOverride = nil
	if err := checkBaselineOverrides(context.Background(), cfg); err != nil {
		t.Fatalf("no entries at all: %v", err)
	}
}

// The names a table can carry. A space, a dot and upper case are held by the
// layout as written. A path separator is not, and is refused.
func TestBaselineOverride_tableNames(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	for _, table := range []string{"orders", "Orders", "order items", "order.items", " padded ", "ñandú"} {
		t.Run(table, func(t *testing.T) {
			root, staging := t.TempDir(), t.TempDir()
			p := stagedRead(t, staging, base, "Shop", table)
			cfg := overrideCfg(root, base.Add(time.Minute), []string{"Shop." + table}, map[string]string{"Shop." + table: p})
			if err := checkBaselineOverrides(context.Background(), cfg); err != nil {
				t.Fatalf("checkBaselineOverrides: %v", err)
			}
			got, ok, err := baselineOverrideFor(context.Background(), cfg, "Shop", table)
			if err != nil || !ok {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			if got.path != p || !got.at.Equal(base) || got.dir != filepath.Join(staging, SnapshotDirName(base)) {
				t.Fatalf("got %+v, want the file %s read at %s", got, p, base)
			}
		})
	}
	t.Run("a path separator", func(t *testing.T) {
		root, staging := t.TempDir(), t.TempDir()
		p := stagedRead(t, staging, base, "shop", "a/b")
		cfg := overrideCfg(root, base.Add(time.Minute), []string{"shop.a/b"}, map[string]string{"shop.a/b": p})
		if err := checkBaselineOverrides(context.Background(), cfg); !errors.Is(err, ErrBaselineOverride) {
			t.Fatalf("a table name with a path separator: %v, want ErrBaselineOverride", err)
		}
	})
}

// An entry spelled another way than the run's table must not leave the table
// on its old snapshot without a word.
func TestBaselineOverride_entryForATableTheRunDoesNotFold(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	root, staging := t.TempDir(), t.TempDir()
	p := stagedRead(t, staging, base, "shop", "orders")
	for _, key := range []string{"shop.Orders", "SHOP.orders", "shop.orders ", " shop.orders", "shop.gone", "orders", ""} {
		cfg := overrideCfg(root, base.Add(time.Minute), []string{"shop.orders", "shop.items"}, map[string]string{key: p})
		err := checkBaselineOverrides(context.Background(), cfg)
		if !errors.Is(err, ErrBaselineOverride) {
			t.Errorf("entry %q: %v, want ErrBaselineOverride", key, err)
		}
	}
}

func TestBaselineOverride_refusals(t *testing.T) {
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	target := base.Add(time.Minute)
	type setup struct {
		root, staging, path string
		cfg                 FullTableConfig
	}
	fresh := func(t *testing.T) *setup {
		s := &setup{root: t.TempDir(), staging: t.TempDir()}
		s.path = stagedRead(t, s.staging, base, "shop", "orders")
		s.cfg = overrideCfg(s.root, target, []string{"shop.orders"}, map[string]string{"shop.orders": s.path})
		return s
	}
	dirOf := func(s *setup) string { return filepath.Dir(filepath.Dir(s.path)) }
	mustWrite := func(t *testing.T, p string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name   string
		break_ func(t *testing.T, s *setup)
		want   error
		says   string
	}{
		{"the accepted shape", func(*testing.T, *setup) {}, nil, ""},
		{"a run that writes a dump", func(_ *testing.T, s *setup) { s.cfg.OutputFormat = OutputFormatMydumper }, ErrBaselineOverride, "writes a snapshot"},
		{"an empty path", func(_ *testing.T, s *setup) { s.cfg.BaselineOverride["shop.orders"] = "" }, ErrBaselineOverride, "absolute local path"},
		{"a relative path", func(_ *testing.T, s *setup) {
			s.cfg.BaselineOverride["shop.orders"] = "staging/2026-01-01T00-00-00Z/shop/orders.parquet"
		}, ErrBaselineOverride, "absolute local path"},
		{"an s3 path", func(_ *testing.T, s *setup) {
			s.cfg.BaselineOverride["shop.orders"] = "s3://bucket/staging/2026-01-01T00-00-00Z/shop/orders.parquet"
		}, ErrBaselineOverride, "absolute local path"},
		// The table was dropped on the source between the refusal and the
		// read: the read finished and left no file for it.
		{"no file for the table", func(t *testing.T, s *setup) {
			if err := os.Remove(s.path); err != nil {
				t.Fatal(err)
			}
		}, ErrBaselineOverride, "cannot be read"},
		{"a directory where the file would be", func(t *testing.T, s *setup) {
			if err := os.Remove(s.path); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(s.path, 0o755); err != nil {
				t.Fatal(err)
			}
		}, ErrBaselineOverride, "is not a file"},
		// A daemon restarted in the middle of the read.
		{"a read still marked unfinished", func(t *testing.T, s *setup) {
			if err := os.Remove(filepath.Join(dirOf(s), baseline.SuccessMarker)); err != nil {
				t.Fatal(err)
			}
			if err := baseline.WriteIncompleteMarker(dirOf(s)); err != nil {
				t.Fatal(err)
			}
		}, ErrBaselineOverride, "not marked as a finished read"},
		{"a read with no marker at all", func(t *testing.T, s *setup) {
			if err := os.Remove(filepath.Join(dirOf(s), baseline.SuccessMarker)); err != nil {
				t.Fatal(err)
			}
		}, ErrBaselineOverride, "not marked as a finished read"},
		{"both markers", func(t *testing.T, s *setup) {
			mustWrite(t, filepath.Join(dirOf(s), baseline.IncompleteMarker))
		}, ErrBaselineOverride, "unfinished read"},
		{"a folder not named after a time", func(t *testing.T, s *setup) {
			to := filepath.Join(s.staging, "reread")
			if err := os.Rename(dirOf(s), to); err != nil {
				t.Fatal(err)
			}
			s.cfg.BaselineOverride["shop.orders"] = filepath.Join(to, "shop", "orders.parquet")
		}, ErrBaselineOverride, "not named after the time of a read"},
		{"another table's file", func(t *testing.T, s *setup) {
			other := stagedRead(t, s.staging, base, "shop", "items")
			s.cfg.BaselineOverride["shop.orders"] = other
		}, ErrBaselineOverride, "is not this table's file"},
		{"the file under another schema", func(t *testing.T, s *setup) {
			other := stagedRead(t, s.staging, base, "shop2", "orders")
			s.cfg.BaselineOverride["shop.orders"] = other
		}, ErrBaselineOverride, "is not this table's file"},
		{"staged inside the snapshots root", func(t *testing.T, s *setup) {
			s.cfg.BaselineOverride["shop.orders"] = stagedRead(t, s.root, base, "shop", "orders")
		}, ErrBaselineOverride, "inside the snapshots root"},
		{"staged in a folder of the snapshots root", func(t *testing.T, s *setup) {
			s.cfg.BaselineOverride["shop.orders"] = stagedRead(t, filepath.Join(s.root, ".staging"), base, "shop", "orders")
		}, ErrBaselineOverride, "inside the snapshots root"},
		{"staged inside another output root", func(t *testing.T, s *setup) {
			s.cfg.OutputDir = s.staging
		}, ErrBaselineOverride, "inside the snapshots root"},
		{"inside a snapshots root given as a relative path", func(t *testing.T, s *setup) {
			t.Chdir(filepath.Dir(s.staging))
			s.cfg.OutputDir = filepath.Base(s.staging)
		}, ErrBaselineOverride, "inside the snapshots root"},
		{"inside the snapshots root through a link", func(t *testing.T, s *setup) {
			real := stagedRead(t, filepath.Join(s.root, "staging"), base, "shop", "orders")
			link := filepath.Join(t.TempDir(), "link")
			if err := os.Symlink(filepath.Join(s.root, "staging"), link); err != nil {
				t.Fatal(err)
			}
			s.cfg.BaselineOverride["shop.orders"] = filepath.Join(link, filepath.Base(filepath.Dir(filepath.Dir(real))), "shop", "orders.parquet")
		}, ErrBaselineOverride, "inside the snapshots root"},
		{"a link where the file would be", func(t *testing.T, s *setup) {
			other := stagedRead(t, t.TempDir(), base, "shop", "orders")
			if err := os.Remove(s.path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(other, s.path); err != nil {
				t.Fatal(err)
			}
		}, ErrBaselineOverride, "is not a file"},
		{"a table delta beside the file", func(t *testing.T, s *setup) {
			posdel, upserts := baseline.TableDeltaPaths(s.path, 0)
			mustWrite(t, posdel)
			mustWrite(t, upserts)
		}, ErrBaselineOverride, "so it is not a read from the source"},
		{"half a table delta beside the file", func(t *testing.T, s *setup) {
			_, upserts := baseline.TableDeltaPaths(s.path, 0)
			mustWrite(t, upserts)
		}, ErrBaselineOverride, "cannot tell whether"},
		// The read finished after the point the run was asked for.
		{"read after the run's target", func(_ *testing.T, s *setup) { s.cfg.At = base.Add(-time.Second) }, ErrBaselineOverrideAhead, "after this run's target"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := fresh(t)
			tc.break_(t, s)
			_, ok, err := baselineOverrideFor(ctx, s.cfg, "shop", "orders")
			if !ok {
				t.Fatalf("the entry was not seen")
			}
			if tc.want == nil {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if !strings.Contains(err.Error(), "shop.orders") || !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("the refusal does not name the table and say %q: %v", tc.says, err)
			}
			// The same refusal up front, for the whole run.
			if rerr := checkBaselineOverrides(ctx, s.cfg); !errors.Is(rerr, tc.want) {
				t.Fatalf("checkBaselineOverrides: %v, want %v", rerr, tc.want)
			}
		})
	}
}

// A run refused for an entry creates nothing: no snapshot directory is left
// under the root, and the index is never opened (the DSN here opens nothing).
func TestBaselineOverride_aRefusedRunCreatesNothing(t *testing.T) {
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	root, staging := t.TempDir(), t.TempDir()
	p := stagedRead(t, staging, base, "shop", "orders")
	if err := os.Remove(filepath.Join(filepath.Dir(filepath.Dir(p)), baseline.SuccessMarker)); err != nil {
		t.Fatal(err)
	}
	cfg := overrideCfg(root, base.Add(time.Minute), []string{"shop.orders"}, map[string]string{"shop.orders": p})
	_, failures, err := ReconstructTablesDetailed(context.Background(), cfg)
	if !errors.Is(err, ErrBaselineOverride) {
		t.Fatalf("got %v, want ErrBaselineOverride", err)
	}
	if len(failures) != 0 {
		t.Fatalf("failures = %v, want none: no table was started", failures)
	}
	entries, rerr := os.ReadDir(root)
	if rerr != nil || len(entries) != 0 {
		t.Fatalf("the root holds %v (err %v), want nothing", entries, rerr)
	}
}

func TestCheckOverrideReached(t *testing.T) {
	readAt := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	at := func(file string, pos int64) baseline.DumpMetadata {
		return baseline.DumpMetadata{BinlogFile: file, BinlogPos: pos, Producer: baseline.ProducerDump, SnapshotTimestamp: readAt}
	}
	with := func(m baseline.DumpMetadata, f func(*baseline.DumpMetadata)) baseline.DumpMetadata { f(&m); return m }
	cut := func(file string, pos uint64) *query.BinlogPos { return &query.BinlogPos{File: file, Pos: pos} }
	ok := at("binlog.000003", 700)
	cases := []struct {
		name     string
		meta     baseline.DumpMetadata
		postgres bool
		cut      *query.BinlogPos
		want     error
	}{
		{"the cut is past the read", ok, false, cut("binlog.000003", 900), nil},
		{"the cut is exactly the read", ok, false, cut("binlog.000003", 700), nil},
		{"the cut is in a later file", ok, false, cut("binlog.000004", 4), nil},
		{"the cut is past a file name that grew", at("binlog.999999", 700), false, cut("binlog.1000000", 4), nil},
		{"capture is behind the read, same file", ok, false, cut("binlog.000003", 699), ErrBaselineOverrideAhead},
		{"capture is behind the read, earlier file", ok, false, cut("binlog.000002", 9000), ErrBaselineOverrideAhead},
		{"capture is behind a file name that grew", at("binlog.1000000", 4), false, cut("binlog.999999", 9000), ErrBaselineOverrideAhead},
		{"the index holds no change", ok, false, nil, ErrBaselineOverrideAhead},
		{"the file records no coordinate", at("", 0), false, cut("binlog.000003", 900), ErrBaselineOverride},
		{"the file records a file and no position", at("binlog.000003", 0), false, cut("binlog.000003", 900), ErrBaselineOverride},
		{"a PostgreSQL file", with(ok, func(m *baseline.DumpMetadata) { m.LSN = 42 }), false, cut("binlog.000003", 900), ErrBaselineOverride},
		{"a PostgreSQL index", ok, true, cut("binlog.000003", 900), ErrBaselineOverride},
		{"a cut that is an LSN", ok, false, cut("0/16B3748", 23803720), ErrBaselineOverrideAhead},
		{"the output of a fold", with(ok, func(m *baseline.DumpMetadata) { m.Producer = baseline.ProducerReconstruct }), false, cut("binlog.000003", 900), ErrBaselineOverride},
		{"a file that does not say what wrote it", with(ok, func(m *baseline.DumpMetadata) { m.Producer = "" }), false, cut("binlog.000003", 900), ErrBaselineOverride},
		{"a file written before its folder's time", with(ok, func(m *baseline.DumpMetadata) { m.SnapshotTimestamp = readAt.Add(-time.Minute) }), false, cut("binlog.000003", 900), ErrBaselineOverride},
		{"a file written after its folder's time", with(ok, func(m *baseline.DumpMetadata) { m.SnapshotTimestamp = readAt.Add(time.Second) }), false, cut("binlog.000003", 900), ErrBaselineOverride},
		{"a file that records no time", with(ok, func(m *baseline.DumpMetadata) { m.SnapshotTimestamp = time.Time{} }), false, cut("binlog.000003", 900), ErrBaselineOverride},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkOverrideReached("shop", "orders", tc.meta, readAt, tc.postgres, tc.cut)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if !strings.Contains(err.Error(), "shop.orders") {
				t.Fatalf("the refusal does not name the table: %v", err)
			}
		})
	}
}
