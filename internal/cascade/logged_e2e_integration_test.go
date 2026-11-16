//go:build integration

package cascade_test

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationRecoverCascadeRestoresTheSource is recover-cascade end to
// end, the way the commands run it (Options.Logging loaded from the index,
// cascaderecover.EmitSQL), on whatever server the cell runs: a parent delete
// that cascades two levels down and nulls a SET NULL child, then a parent key
// update that rewrites an ON UPDATE CASCADE child and nulls an ON UPDATE SET
// NULL one. The script is applied to the source and every table must be back
// to its state before the two changes.
//
// On MySQL 8.0 and 8.4 (and 9.7 with innodb_native_foreign_keys = ON) the
// cascade is not in the binary log and the children are synthesized; on 9.7
// with the default the cascade's rows are in the index and are reversed as
// they are. The test asserts which path ran, so a cell cannot pass on the
// wrong one.
func TestIntegrationRecoverCascadeRestoresTheSource(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (id INT PRIMARY KEY, name VARCHAR(32)) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE child (id INT PRIMARY KEY, pid INT, payload VARCHAR(32),
		CONSTRAINT fk_child FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE CASCADE) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE grandchild (id INT PRIMARY KEY, cid INT, note VARCHAR(32),
		CONSTRAINT fk_gc FOREIGN KEY (cid) REFERENCES child(id) ON DELETE CASCADE) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE nullkid (id INT PRIMARY KEY, pid INT, note VARCHAR(32),
		CONSTRAINT fk_nk FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE SET NULL ON UPDATE SET NULL) ENGINE=InnoDB`)

	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	src, err := metadata.ReadSourceFKLogging(ctx, sourceDB)
	if err != nil {
		t.Fatalf("ReadSourceFKLogging: %v", err)
	}

	tables := []string{"parent", "child", "grandchild", "nullkid"}
	want := map[string]int64{}
	captureAndIndex(t, sourceDB, indexDB, resolver, sourceName, func() {
		testutil.MustExec(t, sourceDB, "INSERT INTO parent VALUES (1,'p1'),(2,'p2'),(3,'p3')")
		testutil.MustExec(t, sourceDB, "INSERT INTO child VALUES (10,1,'c10'),(11,1,'c11'),(12,2,'c12'),(13,1,'c13'),(14,1,'c14')")
		testutil.MustExec(t, sourceDB, "INSERT INTO grandchild VALUES (100,10,'g100'),(101,10,'g101'),(102,11,'g102'),(103,12,'g103')")
		testutil.MustExec(t, sourceDB, "INSERT INTO nullkid VALUES (50,1,'n50'),(51,2,'n51'),(52,3,'n52')")
		testutil.MustExec(t, sourceDB, "UPDATE child SET pid=3 WHERE id=13") // re-parented: not part of the cascade
		testutil.MustExec(t, sourceDB, "DELETE FROM child WHERE id=14")      // gone before the cascade: stays gone
		time.Sleep(1100 * time.Millisecond)
		for _, tb := range tables {
			want[tb] = checksum(t, sourceDB, tb)
		}
		testutil.MustExec(t, sourceDB, "DELETE FROM parent WHERE id=1")
		time.Sleep(1100 * time.Millisecond)
		testutil.MustExec(t, sourceDB, "UPDATE parent SET id=20 WHERE id=2")
	})

	// What a running capture does once a minute: read the source's start
	// again, which proves it ran through the changes above.
	if _, err := metadata.RecordSourceFKLogging(ctx, sourceDB, indexDB); err != nil {
		t.Fatalf("RecordSourceFKLogging: %v", err)
	}

	eng := query.New(indexDB)
	del, upd := event.EventDelete, event.EventUpdate
	parentDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "parent", EventType: &del, Order: "ASC"})
	parentUpdates := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "parent", EventType: &upd, Order: "ASC"})
	childDeletes := mustFetch(t, eng, query.Options{Schema: sourceName, Table: "child", EventType: &del})
	if logs := len(childDeletes) > 1; logs != src.LogsCascades() {
		t.Fatalf("the index holds %d child deletes, but the source says logs-cascades=%v (innodb_native_foreign_keys=%q)",
			len(childDeletes), src.LogsCascades(), src.NativeForeignKeys)
	}
	logging, err := cascade.LoadLogging(ctx, indexDB)
	if err != nil {
		t.Fatalf("LoadLogging: %v", err)
	}

	parentEvents := append(append([]query.ResultRow{}, parentDeletes...), parentUpdates...)
	groups, fkCaveats, err := cascade.GroupParentDeletesByFKGraph(ctx, indexDB, sourceName, parentEvents)
	if err != nil {
		t.Fatalf("GroupParentDeletesByFKGraph: %v", err)
	}
	if len(fkCaveats) != 0 {
		t.Fatalf("FK graph caveats: %v", fkCaveats)
	}
	var results []cascade.Result
	for _, g := range groups {
		r, err := cascade.SynthesizeVictims(ctx, eng, g.FKs, g.Roots, cascade.Options{Logging: logging, PKMetas: cascade.PKMetasFromResolver(resolver)})
		if err != nil {
			t.Fatalf("SynthesizeVictims: %v", err)
		}
		results = append(results, r)
	}
	res := cascade.MergeResults(results...)
	if len(res.Incomplete) != 0 {
		t.Fatalf("incomplete: %v", res.Incomplete)
	}
	if src.LogsCascades() {
		if len(res.SetNullRows) != 0 || len(res.KeyUpdates) != 0 {
			t.Errorf("a logging source got synthesized restores: %d SET NULL, %d key", len(res.SetNullRows), len(res.KeyUpdates))
		}
		for _, v := range res.Victims {
			if v.EventID == 0 {
				t.Errorf("a logging source got a victim that is not an indexed event: %+v", v)
			}
		}
	} else if len(res.SetNullRows) == 0 || len(res.KeyUpdates) == 0 {
		t.Errorf("a source that does not log its cascades got no synthesized restores: %d SET NULL, %d key", len(res.SetNullRows), len(res.KeyUpdates))
	}

	parents, order := cascaderecover.MergeParentRoots(parentDeletes, res.KeyUpdateParents, query.BinlogOrderProof(ctx, indexDB))
	rows := append(append([]query.ResultRow{}, parents...), res.Victims...)
	var buf bytes.Buffer
	if _, err := cascaderecover.EmitSQL(&buf, recovery.New(indexDB, resolver), rows, res.SetNullRows, res.KeyUpdates, resolver, cascaderecover.Header{
		Schema: sourceName, Table: "parent", Parents: len(parents), Children: len(res.Victims),
		Warnings: append(res.Warnings, cascaderecover.OrderNotes(order)...),
	}); err != nil {
		t.Fatalf("EmitSQL: %v", err)
	}

	applyDB, err := sql.Open("mysql", testutil.BaseDSN()+"/"+sourceName+"?multiStatements=true")
	if err != nil {
		t.Fatalf("open apply conn: %v", err)
	}
	defer applyDB.Close()
	if _, err := applyDB.ExecContext(ctx, buf.String()); err != nil {
		t.Fatalf("apply: %v\n%s", err, buf.String())
	}
	for _, tb := range tables {
		if got := checksum(t, sourceDB, tb); got != want[tb] {
			t.Errorf("%s checksum %d after the recovery, want %d (before the changes)", tb, got, want[tb])
		}
	}
	if t.Failed() {
		t.Logf("logs-cascades=%v; script:\n%s", src.LogsCascades(), buf.String())
	}
}
