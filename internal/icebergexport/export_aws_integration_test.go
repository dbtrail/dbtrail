//go:build integration && aws_integration

package icebergexport

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The AWS leg runs the real export path (index MySQL on 13306, a baseline
// Parquet, Run) against a real AWS Glue catalog and against Glue's Iceberg
// REST endpoint signed with SigV4, then reads the tables back through the
// same catalogs. It needs:
//
//	BINTRAIL_AWS_IT_WAREHOUSE  s3://bucket/prefix the tables are written under
//	BINTRAIL_AWS_IT_ACCOUNT    the AWS account id (the Glue REST warehouse)
//	AWS_REGION                 the region of the bucket and the catalog
//
// plus credentials in the default chain. Tables and databases are dropped at
// the end unless BINTRAIL_AWS_IT_KEEP=1 (to read them with Athena); the
// objects under the warehouse are the caller's to delete.

func awsEnv(t *testing.T) (warehouse, account string) {
	t.Helper()
	warehouse, account = os.Getenv("BINTRAIL_AWS_IT_WAREHOUSE"), os.Getenv("BINTRAIL_AWS_IT_ACCOUNT")
	if warehouse == "" || account == "" || os.Getenv("AWS_REGION") == "" {
		t.Skip("set BINTRAIL_AWS_IT_WAREHOUSE, BINTRAIL_AWS_IT_ACCOUNT and AWS_REGION to run the AWS leg")
	}
	return warehouse, account
}

func awsSchema(prefix string) string {
	return fmt.Sprintf("%s_%d_%04d", prefix, time.Now().Unix(), rand.IntN(10000))
}

func dropAfter(t *testing.T, cc CatalogConfig, loc, schema string) {
	t.Cleanup(func() {
		if os.Getenv("BINTRAIL_AWS_IT_KEEP") == "1" {
			t.Logf("kept %s for reading", schema)
			return
		}
		ctx := context.Background()
		cat, release, ctx, err := openCatalog(ctx, cc, loc)
		if err != nil {
			t.Errorf("cleanup: %v", err)
			return
		}
		defer release()
		if err := cat.DropTable(ctx, catalog.ToIdentifier(schema, "orders")); err != nil {
			t.Errorf("cleanup drop table: %v", err)
		}
		if err := cat.DropNamespace(ctx, catalog.ToIdentifier(schema)); err != nil {
			t.Errorf("cleanup drop namespace: %v", err)
		}
	})
}

// exportTwoWindows runs the first load plus window 1, then window 2, and
// checks each verdict and the rows the catalog serves after each.
func exportTwoWindows(t *testing.T, f fixture, cc CatalogConfig, warehouse string) {
	t.Helper()
	// Glue and REST tables record their source server, so the index must
	// register one.
	testutil.MustExec(t, f.db, serverid.DDLBintrailServers)
	testutil.MustExec(t, f.db, `DELETE FROM bintrail_servers`)
	testutil.MustExec(t, f.db, `INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, username) VALUES
		('11111111-1111-1111-1111-111111111111', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'db-a', 'repl')`)
	cfg := f.config(warehouse, f.base.Add(20*time.Minute))
	cfg.Catalog = cc
	o := runOne(t, cfg)
	if o.Verdict != VerdictLoaded || o.RowsLoaded != 3 || o.Events != 3 || o.Upserts != 2 || o.Deletes != 1 {
		t.Fatalf("run 1 = %+v, want loaded: 3 rows, 3 events, 2 upserts, 1 delete (%s)", o, o.Detail)
	}
	t.Logf("run 1: %s %s -> %s", o.Verdict, o.Detail, o.Location)
	loc, err := CheckCatalog(cc, warehouse)
	if err != nil {
		t.Fatal(err)
	}
	read := func() []string {
		ctx := context.Background()
		cat, release, ctx, err := openCatalog(ctx, cc, loc)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(f.schema, "orders"))
		if err != nil {
			t.Fatal(err)
		}
		return scanRows(t, tbl)
	}
	equalRows(t, "after run 1", read(), []string{"1=new", "2=paid", "4=new"})

	f.seedSecondWindow(t)
	cfg = f.config(warehouse, f.base.Add(40*time.Minute))
	cfg.Catalog = cc
	o = runOne(t, cfg)
	if o.Verdict != VerdictExported || o.Events != 3 || o.Upserts != 2 || o.Deletes != 1 {
		t.Fatalf("run 2 = %+v, want exported: 3 events, 2 upserts, 1 delete (%s)", o, o.Detail)
	}
	t.Logf("run 2: %s %s cursor %s snapshot %v", o.Verdict, o.Detail, o.Cursor, *o.SnapshotID)
	equalRows(t, "after run 2", read(), []string{"2=paid", "4=shipped", "5=new"})
}

func TestAWSIntegrationExport_glue(t *testing.T) {
	warehouse, _ := awsEnv(t)
	schema := awsSchema("bt2552b_glue")
	f := seedFixtureSchema(t, schema)
	f.seedFirstWindow(t)
	cc := CatalogConfig{Kind: CatalogGlue}
	loc, err := CheckCatalog(cc, warehouse)
	if err != nil {
		t.Fatal(err)
	}
	dropAfter(t, cc, loc, schema)
	exportTwoWindows(t, f, cc, warehouse)
	t.Logf("GLUE_TABLE=%s.orders", schema)
}

func TestAWSIntegrationExport_restSigV4(t *testing.T) {
	warehouse, account := awsEnv(t)
	schema := awsSchema("bt2552b_rest")
	f := seedFixtureSchema(t, schema)
	f.seedFirstWindow(t)
	cc := CatalogConfig{Kind: CatalogREST, URI: "https://glue." + os.Getenv("AWS_REGION") + ".amazonaws.com/iceberg",
		Warehouse: account, SigV4: true}
	loc, err := CheckCatalog(cc, warehouse)
	if err != nil {
		t.Fatal(err)
	}
	dropAfter(t, cc, loc, schema)
	exportTwoWindows(t, f, cc, warehouse)
	t.Logf("REST_TABLE=%s.orders", schema)
}

// TestAWSIntegrationExport_glueConflict: two writers read the same Glue
// table; the second commit is refused as a lost race and commits nothing.
func TestAWSIntegrationExport_glueConflict(t *testing.T) {
	warehouse, _ := awsEnv(t)
	schema := awsSchema("bt2552b_race")
	cc := CatalogConfig{Kind: CatalogGlue}
	loc, err := CheckCatalog(cc, warehouse)
	if err != nil {
		t.Fatal(err)
	}
	dropAfter(t, cc, loc, schema)
	ctx := context.Background()
	cat, release, ctx, err := openCatalog(ctx, cc, loc)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := ensureNamespace(ctx, cat, schema); err != nil {
		t.Fatal(err)
	}
	cols, err := buildColumns(ordersCols, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	ident := catalog.ToIdentifier(schema, "orders")
	if _, err := cat.CreateTable(ctx, ident, icebergSchema(cols), catalog.WithProperties(tableProperties()),
		catalog.WithLocation(tableLocation(loc, schema, "orders"))); err != nil {
		t.Fatal(err)
	}
	a, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	b, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	opsA := foldOps(t, ordersPK, ev(1, event.EventInsert, "1", nil, orderRow(1, "a", "1.00", "2026-08-28 12:00:00")))
	opsB := foldOps(t, ordersPK, ev(1, event.EventInsert, "2", nil, orderRow(2, "b", "2.00", "2026-08-28 12:00:00")))
	cA := cursor{File: "binlog.000001", Pos: 200, At: t0}
	if _, err := writeDelta(ctx, memory.DefaultAllocator, a, cols, opsA, nil, cA); err != nil {
		t.Fatalf("winner: %v", err)
	}
	_, err = writeDelta(ctx, memory.DefaultAllocator, b, cols, opsB, nil, cursor{File: "binlog.000001", Pos: 300, At: t0})
	if !errors.Is(err, ErrConcurrentCommit) {
		t.Fatalf("loser err = %v, want ErrConcurrentCommit", err)
	}
	t.Logf("loser refused: %v", err)
	after, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	equalRows(t, "after the race", scanRows(t, after), []string{"1=a"})
	if got, err := readCursor(after.Properties()); err != nil || got == nil || *got != cA {
		t.Fatalf("cursor = %v (%v), want the winner's %v", got, err, cA)
	}
}
