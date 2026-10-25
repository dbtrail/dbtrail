package icebergexport

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// These drive the FIRST-LOAD commit (firstLoad) without an index: the
// baseline is a real Parquet snapshot on disk and the catalog a local
// warehouse, wrapped to fail or race the way Glue and REST do.

const loadCreateSQL = "CREATE TABLE `orders` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `status` varchar(20) DEFAULT NULL,\n" +
	"  `amount` decimal(10,2) DEFAULT NULL,\n" +
	"  `updated_at` datetime DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB;\n"

var loadSnapAt = time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

// writeLoadBaseline writes one baseline snapshot of shop.orders with rows
// 1..3 and returns its root.
func writeLoadBaseline(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	snapDir := filepath.Join(root, strings.ReplaceAll(loadSnapAt.Format(time.RFC3339), ":", "-"))
	cols, err := baseline.ParseSchemaText(loadCreateSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, "shop", "orders.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: loadCreateSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			baseline.MetaKeyBinlogPos:      "100",
			"bintrail.snapshot_timestamp":  loadSnapAt.Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"1", "2", "3"} {
		if err := w.WriteRow([]string{id, "new", "10.00", "2026-08-28 12:00:00"}, []bool{false, false, false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
	return root
}

var loadTM = &metadata.TableMeta{Schema: "shop", Table: "orders", PKColumns: []string{"id"}, Columns: []metadata.ColumnMeta{
	{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int", ColumnType: "int"},
	{Name: "status", OrdinalPosition: 2, DataType: "varchar", ColumnType: "varchar(20)"},
	{Name: "amount", OrdinalPosition: 3, DataType: "decimal", ColumnType: "decimal(10,2)"},
	{Name: "updated_at", OrdinalPosition: 4, DataType: "datetime", ColumnType: "datetime"},
}}

// loadDeps returns deps for a first load into a fresh local warehouse,
// with the catalog handed to wrap (under the guard) and the commits
// OnCommit reported collected in *commits.
func loadDeps(t *testing.T, kind CatalogKind, wrap func(catalog.Catalog) catalog.Catalog) (*deps, catalog.Catalog, string, *[]Commit) {
	t.Helper()
	dir := t.TempDir()
	cat, release, err := openWarehouse(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	inner := cat.(guardedCatalog).Catalog
	if wrap != nil {
		inner = wrap(inner)
	}
	if err := ensureNamespace(context.Background(), cat, "shop"); err != nil {
		t.Fatal(err)
	}
	var commits []Commit
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	d := &deps{
		cfg: Config{BaselineSrc: writeLoadBaseline(t), At: loadSnapAt.Add(time.Hour), Catalog: CatalogConfig{Kind: kind},
			OnCommit: func(c Commit) { commits = append(commits, c) }},
		resolver: metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.orders": loadTM}),
		cat:      guard(inner),
		location: abs,
		sourceID: "11111111-1111-1111-1111-111111111111",
		mem:      memory.DefaultAllocator,
	}
	return d, cat, abs, &commits
}

func firstLoadOf(d *deps) (*table.Table, error) {
	tbl, _, _, err := d.firstLoad(context.Background(), "shop", "orders", loadTM, loadTM.PKColumnMetas(),
		catalog.ToIdentifier("shop", "orders"), nil)
	return tbl, err
}

// TestFirstLoad_commitThatLandedIsDone: the first load's commit lands and
// the catalog still answers with a 5xx or a timeout. The load is durable,
// so it is reported as done and the audit callback fires once.
func TestFirstLoad_commitThatLandedIsDone(t *testing.T) {
	for _, cerr := range []error{rest.ErrCommitStateUnknown, errors.New("read tcp: i/o timeout")} {
		t.Run(cerr.Error(), func(t *testing.T) {
			d, cat, _, commits := loadDeps(t, CatalogHadoop, func(c catalog.Catalog) catalog.Catalog {
				return landsThenFails{Catalog: c, err: cerr}
			})
			if _, err := firstLoadOf(d); err != nil {
				t.Fatalf("a first load that landed was reported as %v", err)
			}
			if len(*commits) != 1 || (*commits)[0].Kind != CommitLoad || (*commits)[0].Rows != 3 {
				t.Fatalf("OnCommit = %+v, want one load of 3 rows", *commits)
			}
			equalRows(t, "after the load", scanRows(t, reload(t, cat)), []string{"1=new", "2=new", "3=new"})
		})
	}
}

// TestFirstLoad_lostRaceWritesNothing: another writer commits to the table
// between the guard's check and the catalog applying this run's load. The
// load is refused as a conflict, nothing of it is in the table, and no
// audit event fires.
func TestFirstLoad_lostRaceWritesNothing(t *testing.T) {
	var rc *racingCatalog
	d, cat, _, commits := loadDeps(t, CatalogHadoop, func(c catalog.Catalog) catalog.Catalog {
		rc = &racingCatalog{Catalog: c}
		return rc
	})
	cWin := cursor{File: "binlog.000001", Pos: 900, At: loadSnapAt.Add(30 * time.Minute)}
	rc.other = func() {
		tbl := reload(t, cat)
		cols, err := columnsFromSchema(tbl.Schema())
		if err != nil {
			t.Fatal(err)
		}
		commit(t, tbl, cols, foldOps(t, ordersPK,
			ev(1, event.EventInsert, "9", nil, orderRow(9, "other", "9.00", "2026-08-28 12:00:00"))), cWin)
	}
	_, err := firstLoadOf(d)
	if !errors.Is(err, ErrConcurrentCommit) {
		t.Fatalf("err = %v, want ErrConcurrentCommit", err)
	}
	if o := refusal("shop", "orders", err); o.Verdict != VerdictRefusedConflict {
		t.Fatalf("verdict = %s, want refused-conflict", o.Verdict)
	}
	if len(*commits) != 0 {
		t.Fatalf("OnCommit fired for a refused load: %+v", *commits)
	}
	after := reload(t, cat)
	equalRows(t, "after the lost load", scanRows(t, after), []string{"9=other"})
	if got, err := readCursor(after.Properties()); err != nil || got == nil || *got != cWin {
		t.Fatalf("cursor = %v (%v), want the other writer's %v", got, err, cWin)
	}
}

// recordingCatalog records the location each CreateTable asked for.
type recordingCatalog struct {
	catalog.Catalog
	locations *[]string
}

func (r recordingCatalog) CreateTable(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	cfg := catalog.NewCreateTableCfg()
	for _, o := range opts {
		o(&cfg)
	}
	*r.locations = append(*r.locations, cfg.Location)
	return r.Catalog.CreateTable(ctx, ident, sc, opts...)
}

// TestFirstLoad_tableLocation: on Glue and REST the table is created at
// <warehouse>/<schema>/<table> explicitly; the local catalog is never given
// a location (it places tables itself, and refuses any other).
func TestFirstLoad_tableLocation(t *testing.T) {
	for _, kind := range []CatalogKind{CatalogHadoop, CatalogGlue, CatalogREST} {
		t.Run(string(kind), func(t *testing.T) {
			var locs []string
			d, _, abs, _ := loadDeps(t, kind, func(c catalog.Catalog) catalog.Catalog {
				return recordingCatalog{Catalog: c, locations: &locs}
			})
			if _, err := firstLoadOf(d); err != nil {
				t.Fatal(err)
			}
			want := ""
			if kind != CatalogHadoop {
				want = abs + "/shop/orders"
			}
			if len(locs) != 1 || locs[0] != want {
				t.Fatalf("CreateTable locations = %q, want [%q]", locs, want)
			}
		})
	}
}

// TestFirstLoad_stampsProvenance: a new table records the source server
// and the exact source schema.table on every catalog.
func TestFirstLoad_stampsProvenance(t *testing.T) {
	d, cat, _, _ := loadDeps(t, CatalogHadoop, nil)
	if _, err := firstLoadOf(d); err != nil {
		t.Fatal(err)
	}
	props := reload(t, cat).Properties()
	if props[propSource] != d.sourceID || props[propSourceTable] != "shop.orders" {
		t.Fatalf("provenance = %q / %q, want %q / shop.orders", props[propSource], props[propSourceTable], d.sourceID)
	}
}
