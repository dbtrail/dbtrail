//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The update that folds from a read of some tables, end to end (#2640): the
// real mydumper, the supervisor's real capture, the real fold. Run by hand: it
// needs a mydumper on PATH (a wrapper around the pinned image works).

type rereadRig struct {
	ctx         context.Context
	src, index  *sql.DB
	srcName     string
	sup         *baselineSupervisor
	reg         *console.Registry
	work, local string
	req         refreshRequest
	source      console.BaselineRequest
	beat        atomic.Int64
}

var rereadRigTables = []string{"orders", "items", "plain"}

func newRereadRig(t *testing.T) *rereadRig {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	if _, err := exec.LookPath("mydumper"); err != nil {
		t.Skip("no mydumper on PATH: this test runs the real one")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	src, srcName := testutil.CreateTestDB(t)
	for _, table := range rereadRigTables {
		testutil.MustExec(t, src, "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, val VARCHAR(40) NOT NULL, n INT NOT NULL)")
		for i := 1; i <= 50; i++ {
			testutil.MustExec(t, src, fmt.Sprintf("INSERT INTO %s VALUES (%d, 'seed', 0)", table, i))
		}
	}
	testutil.MustExec(t, src, "CREATE TABLE beat (id BIGINT PRIMARY KEY)")

	index, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, index)
	if err := indexer.EnsureSchema(index); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	var hours []time.Time
	for h := now.Truncate(time.Hour).Add(-3 * time.Hour); !h.After(now.Add(2 * time.Hour)); h = h.Add(time.Hour) {
		hours = append(hours, h)
	}
	testutil.SetupPartitionedTable(t, index, indexName, hours)

	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	local := t.TempDir()
	indexDSN := testutil.IntegrationDSN(indexName)
	ent, err := reg.Add(console.ServerEntry{Name: "reread-fold", DSN: indexDSN, SourceDSN: testutil.BaseDSN() + "/",
		Schemas: srcName, BaselineDir: local})
	if err != nil {
		t.Fatal(err)
	}
	capture := newMonitorSupervisor(ctx, indexDSN, nil, nil)
	if err := capture.Start(ctx, ent); err != nil {
		t.Fatalf("start the capture: %v", err)
	}
	t.Cleanup(func() { _ = capture.Stop(context.Background(), ent.ID) })
	waitStreamLive(t, src, index, srcName, "beat", "id")

	work := t.TempDir()
	// lock-all: the MySQL of these tests is shared, and it locks only the
	// tables read.
	sup := newBaselineSupervisor(ctx, work, baseline.LockModeLockAll)
	sup.lockModeChosen = true
	source := console.BaselineRequestFor(ent)
	r := &rereadRig{ctx: ctx, src: src, index: index, srcName: srcName, sup: sup, reg: reg, work: work, local: local, source: source,
		req: refreshRequest{ServerID: ent.ID, ServerName: ent.Name, IndexDSN: indexDSN, BaselineDir: local, CarryForwardUnchanged: true}}
	if _, err := sup.execute(source); err != nil {
		t.Fatalf("the first full read: %v", err)
	}
	return r
}

// captured writes one row and waits for the index to hold it, so every write
// before this call is indexed.
func (r *rereadRig) captured(t *testing.T) {
	t.Helper()
	id := r.beat.Add(1)
	testutil.MustExec(t, r.src, fmt.Sprintf("INSERT INTO beat VALUES (%d)", id))
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		var n int
		_ = r.index.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND table_name = 'beat' AND pk_values = ?", r.srcName, fmt.Sprint(id)).Scan(&n)
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("capture did not reach the last write")
		}
	}
}

func (r *rereadRig) refresh(req refreshRequest) console.BaselineStatus {
	// A snapshot folder is named to the second and written once: a run in the
	// second of the previous one is refused for that alone.
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(1100 * time.Millisecond)))
	r.sup.mu.Lock()
	r.sup.refreshes[req.ServerID] = &console.BaselineStatus{State: "running"}
	r.sup.mu.Unlock()
	r.sup.runRefresh(req, time.Time{}, time.Hour)
	return r.sup.RefreshStatus(req.ServerID)
}

func (r *rereadRig) rereadReq(tables ...string) refreshRequest {
	req := r.req
	order := rereadOrder{Source: r.source}
	for _, table := range tables {
		order.Tables = append(order.Tables, r.srcName+"."+table)
	}
	req.Reread = &order
	return req
}

// truncateBoth leaves orders and items refused for any update.
func (r *rereadRig) truncateBoth(t *testing.T) {
	t.Helper()
	for _, table := range []string{"orders", "items"} {
		testutil.MustExec(t, r.src, "TRUNCATE TABLE "+table)
		testutil.MustExec(t, r.src, "INSERT INTO "+table+" VALUES (1000, 'after-truncate', 1), (1001, 'after-truncate', 1)")
	}
	r.captured(t)
}

func (r *rereadRig) wantWorkEmpty(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(r.work)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Fatalf("the working folder still holds %s", e.Name())
	}
}

// wantSource compares every table of the newest snapshot with the source.
func (r *rereadRig) wantSource(t *testing.T) {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	for _, table := range rereadRigTables {
		p, _, _, err := reconstruct.FindBaseline(r.ctx, r.local, r.srcName, table, time.Now().UTC().Add(time.Minute))
		if err != nil {
			t.Fatalf("FindBaseline(%s): %v", table, err)
		}
		got := rereadGTIDRows(t, ddb, fmt.Sprintf("SELECT id, val, n FROM parquet_scan('%s')", p))
		want := rereadGTIDRows(t, r.src, "SELECT id, val, n FROM "+table)
		if !maps.Equal(got, want) {
			t.Fatalf("%s: the snapshot holds %d rows and the source %d, or their values differ", table, len(got), len(want))
		}
	}
}

// Two tables truncated and one that is not, under writes that do not stop:
// the ordinary update refuses, the update with a read publishes all of them
// at one coordinate, and the next ordinary update ends equal to the source.
func TestIntegrationRereadFold_twoReadTablesAndAPlainOne_2640(t *testing.T) {
	r := newRereadRig(t)
	r.truncateBoth(t)
	if st := r.refresh(r.req); st.State != "failed" || len(st.RefusedTables) != 2 {
		t.Fatalf("the ordinary update: %q, refused %+v (%s); want it to refuse the two tables", st.State, st.RefusedTables, st.LastError)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 2000; !stop.Load(); i++ {
			for _, table := range rereadRigTables {
				_, _ = r.src.Exec(fmt.Sprintf("INSERT INTO %s VALUES (%d, 'during', 2)", table, i))
				_, _ = r.src.Exec(fmt.Sprintf("UPDATE %s SET n = n + 1 WHERE id = %d", table, i-1))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	st := r.refresh(r.rereadReq("orders", "items"))
	stop.Store(true)
	wg.Wait()
	if st.State != "succeeded" || !st.Published {
		t.Fatalf("the update with a read: %q published=%v: %s", st.State, st.Published, st.LastError)
	}
	r.wantWorkEmpty(t)

	// One point in time: every table of the snapshot ends at the same place.
	at, err := time.Parse(time.RFC3339, st.At)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(r.local, reconstruct.SnapshotDirName(at))
	var file string
	var pos int64
	for i, table := range rereadRigTables {
		m, err := baseline.ReadParquetMetadata(filepath.Join(dir, r.srcName, table+".parquet"))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			file, pos = m.BinlogFile, m.BinlogPos
		} else if m.BinlogFile != file || m.BinlogPos != pos {
			t.Fatalf("%s ends at %s:%d and the first table at %s:%d", table, m.BinlogFile, m.BinlogPos, file, pos)
		}
	}

	r.captured(t)
	if st := r.refresh(r.req); st.State != "succeeded" {
		t.Fatalf("the update after it: %q: %s", st.State, st.LastError)
	}
	r.wantSource(t)
}

// The table is truncated again after the read: the update refuses it and
// publishes nothing, and the staged read is gone.
func TestIntegrationRereadFold_truncatedAgainAfterTheRead_2640(t *testing.T) {
	r := newRereadRig(t)
	r.truncateBoth(t)
	before, _, err := reconstruct.NewestSnapshot(r.ctx, r.local)
	if err != nil {
		t.Fatal(err)
	}
	realReached := rereadCaptureReached
	t.Cleanup(func() { rereadCaptureReached = realReached })
	var once sync.Once
	rereadCaptureReached = func(ctx context.Context, cfg reconstruct.OverrideProbe) error {
		once.Do(func() {
			testutil.MustExec(t, r.src, "TRUNCATE TABLE orders")
			testutil.MustExec(t, r.src, "INSERT INTO orders VALUES (5000, 'second-truncate', 3)")
			r.captured(t)
		})
		cfg.At = time.Now().UTC()
		return realReached(ctx, cfg)
	}
	st := r.refresh(r.rereadReq("orders", "items"))
	if st.State != "failed" || st.Published {
		t.Fatalf("status %q published=%v: %s", st.State, st.Published, st.LastError)
	}
	found := false
	for _, rt := range st.RefusedTables {
		if rt.Name == r.srcName+".orders" && rt.Verdict == reconstruct.RefreshVerdictRefusedDDL {
			found = true
		}
	}
	if !found {
		t.Fatalf("refused = %+v, want orders refused for the second TRUNCATE", st.RefusedTables)
	}
	r.wantWorkEmpty(t)
	after, _, err := reconstruct.NewestSnapshot(r.ctx, r.local)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Equal(before) {
		t.Fatalf("the newest snapshot moved from %s to %s", before, after)
	}
}

// A server whose copy lives in a bucket: the other tables are read from S3,
// the read tables from the local staged files. It needs a bucket the test can
// write to, in BINTRAIL_TEST_REREAD_S3 (s3://bucket/prefix).
func TestIntegrationRereadFold_aServerWhoseCopyIsInS3_2640(t *testing.T) {
	url := os.Getenv("BINTRAIL_TEST_REREAD_S3")
	if url == "" {
		t.Skip("BINTRAIL_TEST_REREAD_S3 is not set")
	}
	r := newRereadRig(t)
	url = fmt.Sprintf("%s/%s", url, r.srcName)
	at, _, err := reconstruct.NewestSnapshot(r.ctx, r.local)
	if err != nil {
		t.Fatal(err)
	}
	snap := filepath.Join(r.local, reconstruct.SnapshotDirName(at))
	if _, err := baseline.Upload(r.ctx, snap, url+"/"+reconstruct.SnapshotDirName(at), "", true); err != nil {
		t.Fatalf("upload the first snapshot: %v", err)
	}
	// Only the bucket holds it now.
	if err := os.RemoveAll(snap); err != nil {
		t.Fatal(err)
	}
	r.truncateBoth(t)

	// One write after the read, so the cut can reach it.
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(100000); !stop.Load(); i++ {
			_, _ = r.src.Exec(fmt.Sprintf("INSERT INTO beat VALUES (%d)", i))
			time.Sleep(50 * time.Millisecond)
		}
	}()
	req := r.rereadReq("orders", "items")
	req.BaselineS3 = url
	st := r.refresh(req)
	stop.Store(true)
	wg.Wait()
	if st.State != "succeeded" || !st.Published {
		t.Fatalf("the update with a read, over a bucket: %q published=%v: %s", st.State, st.Published, st.LastError)
	}
	r.wantWorkEmpty(t)
	r.wantSource(t)
	newest, tables, err := reconstruct.NewestSnapshot(r.ctx, url)
	if err != nil {
		t.Fatalf("list the bucket: %v", err)
	}
	if newest.Equal(at) || len(tables) != 4 {
		t.Fatalf("the bucket's newest snapshot is %s with %v; want a new one with the four tables", newest, tables)
	}
}
