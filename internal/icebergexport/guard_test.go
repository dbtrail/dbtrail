package icebergexport

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// These pin the optimistic-commit guard: a run that loses a race to another
// writer refuses the table and commits NOTHING, so a window folded before
// the other commit is never applied on top of it. They use the local
// filesystem catalog because the guard sits between the export and ANY
// catalog; the AWS leg (aws_integration) drives the same path on Glue.

var t0 = time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)

// loadedTable returns a catalog and a table holding the rows 1..3, as after
// a first load.
func loadedTable(t *testing.T) (catalog.Catalog, *table.Table, []column) {
	t.Helper()
	cat, tbl, cols := newTestTable(t, ordersCols, []string{"id"})
	tbl = commit(t, tbl, cols, foldOps(t, ordersPK,
		ev(1, event.EventInsert, "1", nil, orderRow(1, "new", "10.00", "2026-08-28 12:00:00")),
		ev(2, event.EventInsert, "2", nil, orderRow(2, "new", "20.00", "2026-08-28 12:00:00")),
		ev(3, event.EventInsert, "3", nil, orderRow(3, "new", "30.00", "2026-08-28 12:00:00")),
	), cursor{File: "binlog.000001", Pos: 100, At: t0})
	return cat, tbl, cols
}

func reload(t *testing.T, cat catalog.Catalog) *table.Table {
	t.Helper()
	tbl, err := cat.LoadTable(context.Background(), catalog.ToIdentifier("shop", "orders"))
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

// TestConcurrentWriters_loserRefusedAndCommitsNothing: two runs read the
// table at the same cursor. The first commits its window. The second, whose
// window was folded against the state BEFORE that commit, must refuse with
// ErrConcurrentCommit and leave the winner's rows and cursor untouched.
func TestConcurrentWriters_loserRefusedAndCommitsNothing(t *testing.T) {
	for _, tc := range []struct {
		name      string
		loserOps  bool // false: the loser had nothing to fold and commits the cursor alone
		retries   string
		winnerOps bool
	}{
		{"loser with a delta", true, "", true},
		{"loser moving only its cursor", false, "", true},
		// Neither run had rows to fold: the snapshot does not move, only the
		// cursor. No snapshot requirement can see that; the cursor
		// comparison does, and the loser must not move the cursor back.
		{"both moving only their cursors", false, "", false},
		// The table asks the library to retry. The guard's own check refuses
		// first and never returns table.ErrCommitFailed, so no retry starts;
		// TestGuard_libraryNeverRetries pins the retry half.
		{"table asks the library to retry", true, "3", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, tbl, cols := loadedTable(t)
			if tc.retries != "" {
				tx := tbl.NewTransaction()
				if err := tx.SetProperties(iceberg.Properties{table.CommitNumRetriesKey: tc.retries, table.CommitMinRetryWaitMsKey: "1", table.CommitMaxRetryWaitMsKey: "2"}); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Commit(withCommitBase(context.Background(), tbl, nil)); err != nil {
					t.Fatal(err)
				}
			}
			winner, loser := reload(t, cat), reload(t, cat)

			cWin := cursor{File: "binlog.000001", Pos: 300, At: t0.Add(2 * time.Hour)}
			var winOps []*netOp
			wantRows := []string{"1=new", "2=new", "3=new"}
			if tc.winnerOps {
				winOps = foldOps(t, ordersPK,
					ev(4, event.EventUpdate, "2", orderRow(2, "new", "20.00", "2026-08-28 12:00:00"), orderRow(2, "paid", "22.50", "2026-08-28 13:00:00")))
				wantRows = []string{"1=new", "2=paid", "3=new"}
			}
			commit(t, winner, cols, winOps, cWin)

			var ops []*netOp
			if tc.loserOps {
				ops = foldOps(t, ordersPK,
					ev(5, event.EventDelete, "1", orderRow(1, "new", "10.00", "2026-08-28 12:00:00"), nil))
			}
			cLose := cursor{File: "binlog.000001", Pos: 200, At: t0.Add(time.Hour)}
			_, err := writeDelta(context.Background(), memory.DefaultAllocator, loser, cols, ops, nil, cLose)
			if !errors.Is(err, ErrConcurrentCommit) {
				t.Fatalf("loser commit err = %v, want ErrConcurrentCommit", err)
			}
			if errors.Is(err, table.ErrCommitFailed) {
				t.Fatalf("the conflict must not wrap table.ErrCommitFailed (iceberg-go retries on it): %v", err)
			}

			after := reload(t, cat)
			equalRows(t, "after the lost race", scanRows(t, after), wantRows)
			got, err := readCursor(after.Properties())
			if err != nil || got == nil || *got != cWin {
				t.Fatalf("cursor after the lost race = %v (%v), want the winner's %v", got, err, cWin)
			}
		})
	}
}

// TestConcurrentWriters_twoCommitsOnAnEmptyTable: two writers hold the same
// empty table (created, nothing committed) and both commit rows. The second
// must not append on top of the first. This is the writer level; the
// first-load path itself is TestFirstLoad_lostRaceWritesNothing.
func TestConcurrentWriters_twoCommitsOnAnEmptyTable(t *testing.T) {
	cat, tbl, cols := newTestTable(t, ordersCols, []string{"id"})
	_ = tbl
	a, b := reload(t, cat), reload(t, cat)
	load := foldOps(t, ordersPK, ev(1, event.EventInsert, "1", nil, orderRow(1, "new", "10.00", "2026-08-28 12:00:00")))
	c := cursor{File: "binlog.000001", Pos: 100, At: t0, FromBaseline: true}
	commit(t, a, cols, load, c)
	_, err := writeDelta(context.Background(), memory.DefaultAllocator, b, cols, load, nil, c)
	if !errors.Is(err, ErrConcurrentCommit) {
		t.Fatalf("second load err = %v, want ErrConcurrentCommit", err)
	}
	equalRows(t, "after two loads", scanRows(t, reload(t, cat)), []string{"1=new"})
}

// racingCatalog lets another writer commit between the guard's own check
// and the catalog's commit: the window a REST server closes by validating
// the requirements under its own lock, and Glue by its version id.
type racingCatalog struct {
	catalog.Catalog
	other func()
}

func (r *racingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if r.other != nil {
		r.other()
		r.other = nil
	}
	return r.Catalog.CommitTable(ctx, ident, reqs, updates)
}

// TestGuard_raceAfterTheCheck: the other writer commits after the guard
// compared states and before the catalog applies this run's commit. The
// catalog must still refuse it (the guard adds the snapshot requirement a
// cursor-only commit lacks), and the guard must report the refusal as the
// lost race it is.
func TestGuard_raceAfterTheCheck(t *testing.T) {
	cat, _, cols := loadedTable(t)
	inner := cat.(guardedCatalog).Catalog
	rc := &racingCatalog{Catalog: inner}
	g := guard(rc)
	loser := reload(t, g)
	cWin := cursor{File: "binlog.000001", Pos: 300, At: t0.Add(2 * time.Hour)}
	rc.other = func() {
		commit(t, reload(t, cat), cols, foldOps(t, ordersPK,
			ev(4, event.EventDelete, "3", orderRow(3, "new", "30.00", "2026-08-28 12:00:00"), nil)), cWin)
	}
	_, err := writeDelta(context.Background(), memory.DefaultAllocator, loser, cols, nil, nil,
		cursor{File: "binlog.000001", Pos: 200, At: t0.Add(time.Hour)})
	if !errors.Is(err, ErrConcurrentCommit) {
		t.Fatalf("err = %v, want ErrConcurrentCommit", err)
	}
	after := reload(t, cat)
	equalRows(t, "after the race", scanRows(t, after), []string{"1=new", "2=new"})
	if got, err := readCursor(after.Properties()); err != nil || got == nil || *got != cWin {
		t.Fatalf("cursor = %v (%v), want the winner's %v", got, err, cWin)
	}
}

// failingCatalog makes CommitTable fail the way a catalog does, without the
// table moving: Glue's ConcurrentModificationException and a REST 409 both
// arrive as table.ErrCommitFailed; a REST 5xx as rest.ErrCommitStateUnknown.
type failingCatalog struct {
	catalog.Catalog
	err error
}

func (f failingCatalog) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	return nil, "", f.err
}

func TestGuard_catalogErrors(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		wantConflict bool
		wantUnknown  bool
	}{
		{"catalog reports a conflict", fmt.Errorf("%w: ConcurrentModificationException", table.ErrCommitFailed), true, false},
		{"rest 409", rest.ErrCommitFailed, true, false},
		{"rest 5xx", rest.ErrCommitStateUnknown, false, true},
		{"anything else", errors.New("access denied"), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cat, _, cols := loadedTable(t)
			inner := cat.(guardedCatalog).Catalog
			g := guard(failingCatalog{Catalog: inner, err: tc.err})
			tbl := reload(t, g)
			_, err := writeDelta(context.Background(), memory.DefaultAllocator, tbl, cols, nil, nil,
				cursor{File: "binlog.000001", Pos: 200, At: t0.Add(time.Hour)})
			if err == nil {
				t.Fatal("commit succeeded through a failing catalog")
			}
			if got := errors.Is(err, ErrConcurrentCommit); got != tc.wantConflict {
				t.Fatalf("errors.Is(err, ErrConcurrentCommit) = %v, want %v (err %v)", got, tc.wantConflict, err)
			}
			if errors.Is(err, table.ErrCommitFailed) {
				t.Fatalf("the guard passed table.ErrCommitFailed through, and iceberg-go retries on it: %v", err)
			}
			o := refusal("shop", "orders", err)
			if tc.wantConflict {
				if o.Verdict != VerdictRefusedConflict || !strings.Contains(o.Detail, "another writer committed to this table") ||
					!strings.Contains(o.Detail, "the next run resumes from") {
					t.Fatalf("conflict outcome = %+v", o)
				}
			}
			if tc.wantUnknown && !strings.Contains(o.Detail, "may or may not have been applied") {
				t.Fatalf("unknown-state outcome does not say so: %+v", o)
			}
			if !tc.wantConflict && o.Verdict == VerdictRefusedConflict {
				t.Fatalf("a non-conflict was reported as a conflict: %+v", o)
			}
		})
	}
}

// TestGuard_commitWithoutBaseRefused: every commit the export makes names
// the table state it was computed against. A commit path that forgets to
// would silently lose the guard, so the guard refuses it loudly instead.
func TestGuard_commitWithoutBaseRefused(t *testing.T) {
	_, tbl, _ := loadedTable(t)
	tx := tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{"x": "y"}); err != nil {
		t.Fatal(err)
	}
	_, err := tx.Commit(context.Background())
	if err == nil || !strings.Contains(err.Error(), "without the table state it was computed against") {
		t.Fatalf("err = %v, want the missing-base refusal", err)
	}
}

// ownershipDeps is a deps whose resolver knows shop.orders and whose catalog
// is a fresh local warehouse; nothing reaches the index before the
// ownership check, so db stays nil.
func ownershipDeps(t *testing.T) (*deps, catalog.Catalog) {
	t.Helper()
	cat, release, err := openWarehouse(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	tm := &metadata.TableMeta{Schema: "shop", Table: "orders", PKColumns: []string{"id"}, Columns: []metadata.ColumnMeta{
		{Name: "id", IsPK: true, DataType: "bigint", ColumnType: "bigint"},
		{Name: "status", DataType: "varchar", ColumnType: "varchar(20)"},
	}}
	d := &deps{
		cfg:      Config{BaselineSrc: t.TempDir(), At: t0.Add(time.Hour)},
		resolver: metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{"shop.orders": tm}),
		cat:      cat,
		mem:      memory.DefaultAllocator,
	}
	return d, cat
}

// TestRunTable_unownedTableRefused: a table that already exists in the
// catalog and carries no bintrail.export.version was created by someone
// else. It is refused, never adopted, whether it holds data or not; before
// this, an empty one with the right columns was loaded into.
func TestRunTable_unownedTableRefused(t *testing.T) {
	for _, withData := range []bool{false, true} {
		t.Run(fmt.Sprintf("data=%v", withData), func(t *testing.T) {
			d, cat := ownershipDeps(t)
			ctx := context.Background()
			if err := ensureNamespace(ctx, cat, "shop"); err != nil {
				t.Fatal(err)
			}
			cols, err := buildColumns(ordersCols, []string{"id"})
			if err != nil {
				t.Fatal(err)
			}
			props := tableProperties()
			delete(props, propVersion)
			tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("shop", "orders"), icebergSchema(cols), catalog.WithProperties(props))
			if err != nil {
				t.Fatal(err)
			}
			if withData {
				// A foreign writer: rows, and no bintrail property at all.
				arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
				if err != nil {
					t.Fatal(err)
				}
				ops := foldOps(t, ordersPK, ev(1, event.EventInsert, "1", nil, orderRow(1, "x", "1.00", "2026-08-28 12:00:00")))
				var files []iceberg.DataFile
				for df, err := range table.WriteRecords(ctx, tbl, arrowSchema, upsertBatches(memory.DefaultAllocator, arrowSchema, cols, ops)) {
					if err != nil {
						t.Fatal(err)
					}
					files = append(files, df)
				}
				tx := tbl.NewTransaction()
				rd := tx.NewRowDelta(nil)
				rd.AddRows(files...)
				if err := rd.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Commit(withCommitBase(ctx, tbl, nil)); err != nil {
					t.Fatal(err)
				}
			}
			o := d.runTable(ctx, "shop", "orders")
			if o.Verdict != VerdictRefused || !strings.Contains(o.Detail, "bintrail.export.version") ||
				!strings.Contains(o.Detail, "did not create") {
				t.Fatalf("outcome = %+v, want the not-ours refusal", o)
			}
			after := reload(t, cat)
			if after.CurrentSnapshot() != nil && !withData {
				t.Fatal("the refused table was loaded into")
			}
		})
	}
}

// TestRunTable_newerVersionRefused: the version check that predates this
// change still fires before anything is written.
func TestRunTable_newerVersionRefused(t *testing.T) {
	d, cat := ownershipDeps(t)
	ctx := context.Background()
	if err := ensureNamespace(ctx, cat, "shop"); err != nil {
		t.Fatal(err)
	}
	cols, err := buildColumns(ordersCols, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	props := tableProperties()
	props[propVersion] = "2"
	if _, err := cat.CreateTable(ctx, catalog.ToIdentifier("shop", "orders"), icebergSchema(cols), catalog.WithProperties(props)); err != nil {
		t.Fatal(err)
	}
	o := d.runTable(ctx, "shop", "orders")
	if o.Verdict != VerdictRefused || !strings.Contains(o.Detail, "newer export") {
		t.Fatalf("outcome = %+v, want the newer-version refusal", o)
	}
}

// TestRunTables_nameRulesBeforeAnyCatalogWrite: a name the catalog would
// fold is refused before the namespace is created, so a refused table
// leaves no empty Glue database behind; the other tables still run.
func TestRunTables_nameRulesBeforeAnyCatalogWrite(t *testing.T) {
	d, cat := ownershipDeps(t)
	d.cfg.Catalog = CatalogConfig{Kind: CatalogGlue}
	d.sourceID = "11111111-1111-1111-1111-111111111111"
	outs := d.runTables(context.Background(), []string{"Shop.Orders"})
	if len(outs) != 1 || outs[0].Verdict != VerdictRefused || !strings.Contains(outs[0].Detail, "Shop (schema), Orders (table)") {
		t.Fatalf("outcomes = %+v, want the Glue name refusal listing both names", outs)
	}
	assertNoNamespace(t, cat, "Shop", "shop")
}

// TestRunTables_caseCollisionBeforeAnyCatalogWrite: two source schemas that
// differ only in case are both refused, on every catalog (the local one
// too: on a case-insensitive filesystem they share a directory), before a
// namespace is created for either.
func TestRunTables_caseCollisionBeforeAnyCatalogWrite(t *testing.T) {
	for _, kind := range []CatalogKind{CatalogHadoop, CatalogREST} {
		t.Run(string(kind), func(t *testing.T) {
			d, cat := ownershipDeps(t)
			d.cfg.Catalog = CatalogConfig{Kind: kind}
			d.sourceID = "11111111-1111-1111-1111-111111111111"
			outs := d.runTables(context.Background(), []string{"Shop.orders", "shop.orders"})
			if len(outs) != 2 {
				t.Fatalf("outcomes = %d, want 2", len(outs))
			}
			for _, o := range outs {
				if o.Verdict != VerdictRefused || !strings.Contains(o.Detail, "Shop and shop") {
					t.Fatalf("outcome %s.%s = %+v, want the collision refusal naming both", o.Schema, o.Table, o)
				}
			}
			assertNoNamespace(t, cat, "Shop", "shop")
		})
	}
}

func assertNoNamespace(t *testing.T, cat catalog.Catalog, names ...string) {
	t.Helper()
	for _, ns := range names {
		exists, err := cat.CheckNamespaceExists(context.Background(), catalog.ToIdentifier(ns))
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatalf("namespace %s was created for a refused table", ns)
		}
	}
}

// TestRunTables_remedyNamesTheCatalog: a refusal that tells a local user to
// remove the table directory tells a Glue or REST user to drop the table
// from the catalog, through the real run path.
func TestRunTables_remedyNamesTheCatalog(t *testing.T) {
	for _, kind := range []CatalogKind{CatalogHadoop, CatalogGlue, CatalogREST} {
		t.Run(string(kind), func(t *testing.T) {
			d, cat := ownershipDeps(t)
			d.cfg.Catalog = CatalogConfig{Kind: kind}
			d.sourceID = "11111111-1111-1111-1111-111111111111"
			// A partial cursor: readCursor's refusal ends in the remedy.
			props := ourTableProps(d.sourceID, "shop.orders")
			props[propFile] = "binlog.000001"
			createRaw(t, cat, props)
			outs := d.runTables(context.Background(), []string{"shop.orders"})
			if len(outs) != 1 || !strings.Contains(outs[0].Detail, "partial export cursor") {
				t.Fatalf("outcomes = %+v, want the partial-cursor refusal", outs)
			}
			wantDir := kind == CatalogHadoop
			if got := strings.Contains(outs[0].Detail, "remove the table directory"); got != wantDir {
				t.Fatalf("%s: detail %q; names the directory = %v, want %v", kind, outs[0].Detail, got, wantDir)
			}
			if !wantDir && !strings.Contains(outs[0].Detail, "drop the table from the catalog") {
				t.Fatalf("%s: detail %q does not say to drop the table from the catalog", kind, outs[0].Detail)
			}
		})
	}
}

// ourTableProps is what the export stamps on a table it creates.
func ourTableProps(source, sourceTable string) iceberg.Properties {
	p := tableProperties()
	if source != "" {
		p[propSource] = source
	}
	if sourceTable != "" {
		p[propSourceTable] = sourceTable
	}
	return p
}

// createRaw creates shop.orders directly in the catalog with props.
func createRaw(t *testing.T, cat catalog.Catalog, props iceberg.Properties) {
	t.Helper()
	ctx := context.Background()
	if err := ensureNamespace(ctx, cat, "shop"); err != nil {
		t.Fatal(err)
	}
	cols, err := buildColumns(ordersCols, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable(ctx, catalog.ToIdentifier("shop", "orders"), icebergSchema(cols), catalog.WithProperties(props)); err != nil {
		t.Fatal(err)
	}
}

// countingCatalog counts CommitTable calls reaching the catalog and fails
// each with err.
type countingCatalog struct {
	catalog.Catalog
	err   error
	calls *int
}

func (c countingCatalog) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	*c.calls++
	return nil, "", c.err
}

// TestGuard_libraryNeverRetries: iceberg-go retries a commit that failed
// with table.ErrCommitFailed as many times as the TABLE's
// commit.retry.num-retries says, refreshing and replaying it each time. The
// guard never returns that sentinel, so even with retries set the catalog
// sees the commit once.
func TestGuard_libraryNeverRetries(t *testing.T) {
	cat, tbl, cols := loadedTable(t)
	tx := tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{table.CommitNumRetriesKey: "3", table.CommitMinRetryWaitMsKey: "1", table.CommitMaxRetryWaitMsKey: "2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(withCommitBase(context.Background(), tbl, nil)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	g := guard(countingCatalog{Catalog: cat.(guardedCatalog).Catalog, err: fmt.Errorf("%w: conflict", table.ErrCommitFailed), calls: &calls})
	_, err := writeDelta(context.Background(), memory.DefaultAllocator, reload(t, g), cols, nil, nil,
		cursor{File: "binlog.000001", Pos: 200, At: t0.Add(time.Hour)})
	if !errors.Is(err, ErrConcurrentCommit) {
		t.Fatalf("err = %v, want ErrConcurrentCommit", err)
	}
	if calls != 1 {
		t.Fatalf("the catalog saw the commit %d times, want 1 (no library retry)", calls)
	}
}

// TestGuard_tableDroppedMidRun: a table dropped between the run's read and
// its commit is a lost race, not a commit into nothing.
func TestGuard_tableDroppedMidRun(t *testing.T) {
	cat, tbl, cols := loadedTable(t)
	if err := cat.DropTable(context.Background(), catalog.ToIdentifier("shop", "orders")); err != nil {
		t.Fatal(err)
	}
	_, err := writeDelta(context.Background(), memory.DefaultAllocator, tbl, cols, nil, nil,
		cursor{File: "binlog.000001", Pos: 200, At: t0.Add(time.Hour)})
	if !errors.Is(err, ErrConcurrentCommit) {
		t.Fatalf("err = %v, want ErrConcurrentCommit", err)
	}
}

// landsThenFails applies the commit and then reports an error, the way a
// REST 5xx or a timeout after Glue's UpdateTable can: the commit is durable
// and the caller is told it failed.
type landsThenFails struct {
	catalog.Catalog
	err error
}

func (l landsThenFails) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if _, _, err := l.Catalog.CommitTable(ctx, ident, reqs, updates); err != nil {
		return nil, "", err
	}
	return nil, "", l.err
}

// TestGuard_commitThatLandedIsSuccess: when the reload after a failed call
// shows this run's own cursor, the commit landed. Reporting it as refused
// would skip the audit event for a commit that exists, and tell the operator
// nothing was written.
func TestGuard_commitThatLandedIsSuccess(t *testing.T) {
	for _, cerr := range []error{rest.ErrCommitStateUnknown, errors.New("read tcp: i/o timeout")} {
		t.Run(cerr.Error(), func(t *testing.T) {
			cat, _, cols := loadedTable(t)
			g := guard(landsThenFails{Catalog: cat.(guardedCatalog).Catalog, err: cerr})
			c := cursor{File: "binlog.000001", Pos: 200, At: t0.Add(time.Hour)}
			ops := foldOps(t, ordersPK, ev(5, event.EventDelete, "1", orderRow(1, "new", "10.00", "2026-08-28 12:00:00"), nil))
			got, err := writeDelta(context.Background(), memory.DefaultAllocator, reload(t, g), cols, ops, nil, c)
			if err != nil {
				t.Fatalf("a commit that landed was reported as %v", err)
			}
			if cur, err := readCursor(got.Properties()); err != nil || cur == nil || *cur != c {
				t.Fatalf("returned table cursor = %v (%v), want %v", cur, err, c)
			}
			equalRows(t, "after the landed commit", scanRows(t, reload(t, cat)), []string{"2=new", "3=new"})
		})
	}
}

// createdByOther creates the table (as the run that won would) and then
// fails the way Glue reports it: an AWS AlreadyExistsException, which is not
// catalog.ErrTableAlreadyExists.
type createdByOther struct{ catalog.Catalog }

func (c createdByOther) CreateTable(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	if _, err := c.Catalog.CreateTable(ctx, ident, sc, opts...); err != nil {
		return nil, err
	}
	return nil, errors.New("failed to create table: AlreadyExistsException: Table already exists")
}

// TestCreateTable_lostRaceIsAConflict: two first loads racing on Glue. The
// loser's CreateTable fails because the winner's table now exists; that is
// the same lost race as a commit conflict and is reported as one. A
// same-named table bintrail did not create is not: it stays the plain error.
func TestCreateTable_lostRaceIsAConflict(t *testing.T) {
	cols, err := buildColumns(ordersCols, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	for _, ours := range []bool{true, false} {
		t.Run(fmt.Sprintf("ours=%v", ours), func(t *testing.T) {
			d, cat := ownershipDeps(t)
			if err := ensureNamespace(context.Background(), cat, "shop"); err != nil {
				t.Fatal(err)
			}
			d.cat = guard(createdByOther{cat.(guardedCatalog).Catalog})
			props := tableProperties()
			if !ours {
				delete(props, propVersion)
			}
			_, err := d.createTable(context.Background(), catalog.ToIdentifier("shop", "orders"), icebergSchema(cols), catalog.WithProperties(props))
			if got := errors.Is(err, ErrConcurrentCommit); got != ours {
				t.Fatalf("err = %v; errors.Is(ErrConcurrentCommit) = %v, want %v", err, got, ours)
			}
		})
	}
}

// TestRunTable_provenance: on Glue and REST every table must say which
// source server (bintrail.export.source_id) and which exact source
// schema.table (bintrail.export.source_table) it was created from, and both
// must match the run. Those catalogs are shared by an account, and a REST
// server that compares names without case maps Shop.orders and shop.orders
// to one table across runs. The local catalog checks the table name when a
// table carries it (tables from earlier releases do not) and never the
// source, as before.
func TestRunTable_provenance(t *testing.T) {
	const a, b = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
	cases := []struct {
		name               string
		kind               CatalogKind
		tblSource, tblName string // "" = property absent
		runSource          string
		want               string // substring of the refusal; "" = not refused by provenance
	}{
		{"glue same source and name", CatalogGlue, a, "shop.orders", a, ""},
		{"glue other source", CatalogGlue, a, "shop.orders", b, "another source"},
		{"glue table without a source", CatalogGlue, "", "shop.orders", a, "carries no " + propSource},
		{"glue table without a name", CatalogGlue, a, "", a, "carries no " + propSourceTable},
		{"rest name differs in case", CatalogREST, a, "Shop.orders", a, "Shop.orders"},
		{"hadoop other source accepted", CatalogHadoop, a, "shop.orders", b, ""},
		{"hadoop table from an earlier release", CatalogHadoop, "", "", "", ""},
		{"hadoop name differs in case", CatalogHadoop, "", "Shop.orders", "", "Shop.orders"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, cat := ownershipDeps(t)
			d.sourceID = tc.runSource
			d.cfg.Catalog = CatalogConfig{Kind: tc.kind}
			createRaw(t, cat, ourTableProps(tc.tblSource, tc.tblName))
			o := d.runTable(context.Background(), "shop", "orders")
			provenance := strings.Contains(o.Detail, "another source") || strings.Contains(o.Detail, "carries no bintrail.export.source") ||
				strings.Contains(o.Detail, "was created for the source table")
			switch {
			case tc.want == "" && provenance:
				t.Fatalf("refused on provenance: %+v", o)
			case tc.want != "" && (!provenance || !strings.Contains(o.Detail, tc.want)):
				t.Fatalf("outcome = %+v, want a provenance refusal containing %q", o, tc.want)
			}
			if tc.want == "Shop.orders" && !strings.Contains(o.Detail, "shop.orders") {
				t.Fatalf("name refusal does not name both: %s", o.Detail)
			}
		})
	}
}
