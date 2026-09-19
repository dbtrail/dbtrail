//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1720 through the real fold and a real index. Two things the unit tests
// cannot pin: that the stamp on the pair is the id the INDEX assigned to the
// last event the fold applied, and that the floor is live on a stream-written
// index and absent on a file-mode one.

// insertOrdersEvent writes one orders event with an EXPLICIT event_id, so a
// test can put ids out of binlog order the way `bintrail index --files` does
// when the files are given out of order.
func insertOrdersEvent(t *testing.T, db *sql.DB, schema string, id, start uint64, at time.Time, evType uint8, pk, after string) {
	t.Helper()
	var afterJSON, before []byte
	if after != "" {
		afterJSON = []byte(after)
	}
	if evType == 3 {
		before = []byte(`{"id":` + pk + `,"status":"gone"}`)
	}
	if _, err := db.Exec(`INSERT INTO binlog_events
		(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
		VALUES (?, 'binlog.000001', ?, ?, ?, ?, 'orders', ?, ?, ?, ?)`,
		id, start, start+100, at.Format("2006-01-02 15:04:05"), schema, evType, pk, before, afterJSON); err != nil {
		t.Fatalf("insert event %d: %v", id, err)
	}
}

func markStreamCaptured(t *testing.T, db *sql.DB) {
	t.Helper()
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id)
		VALUES (1, 'position', 'binlog.000001', 4, NOW(), 1)`)
}

type floorRig struct {
	ctx    context.Context
	db     *sql.DB
	dsn    string
	root   string
	schema string
	h0     time.Time
}

func newFloorRig(t *testing.T) *floorRig {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	r := &floorRig{ctx: ctx, db: db, dsn: testutil.BaseDSN() + "/" + dbName, root: t.TempDir(), schema: "shop",
		h0: time.Now().UTC().Truncate(time.Hour).Add(time.Hour)}
	seedOrdersSnapshot(t, db, r.schema, r.h0)
	seedSourceBaseline(t, r.root, r.h0, r.schema)
	return r
}

func (r *floorRig) run(t *testing.T, at time.Time) string {
	t.Helper()
	p, _ := r.runWith(t, at, true)
	return p
}

// runWith refreshes with deltas on or off and returns the newest table file
// and its report.
func (r *floorRig) runWith(t *testing.T, at time.Time, deltas bool) (string, *reconstruct.TableReport) {
	t.Helper()
	reps, err := reconstruct.ReconstructTables(r.ctx, reconstruct.FullTableConfig{
		IndexDSN: r.dsn, BaselineSrc: r.root, Tables: []string{r.schema + ".orders"},
		At: at, OutputDir: r.root, OutputFormat: reconstruct.OutputFormatParquet, TableDeltas: deltas,
	})
	if err != nil {
		t.Fatalf("ReconstructTables(at=%s, deltas=%v): %v", at.Format(time.RFC3339), deltas, err)
	}
	if len(reps) != 1 {
		t.Fatalf("ReconstructTables returned %d reports, want 1", len(reps))
	}
	p, _, _, err := reconstruct.FindBaseline(r.ctx, r.root, r.schema, "orders", at)
	if err != nil {
		t.Fatalf("FindBaseline(%s): %v", at.Format(time.RFC3339), err)
	}
	return p, reps[0]
}

// lastPairStamp reads MetaKeyLastEventID off the chain's newest .upserts.
func lastPairStamp(t *testing.T, base string) uint64 {
	t.Helper()
	chain, err := baseline.ListTableDelta(context.Background(), base)
	if err != nil || chain == nil {
		t.Fatalf("ListTableDelta(%s): chain=%v err=%v", base, chain, err)
	}
	m, err := baseline.ReadParquetMetadata(chain.Last().Upserts)
	if err != nil {
		t.Fatalf("footer of %s: %v", chain.Last().Upserts, err)
	}
	return m.LastEventID
}

// TestReconstructParquet_stampsTheIndexAssignedLastEventID: on a stream
// index, every refresh stamps the highest id it applied, an empty refresh
// carries the previous stamp, and the floor is live from the second refresh
// on without changing what the fold produces.
func TestReconstructParquet_stampsTheIndexAssignedLastEventID(t *testing.T) {
	r := newFloorRig(t)
	markStreamCaptured(t, r.db)
	hour := func(n int, d time.Duration) time.Time { return r.h0.Add(time.Duration(n)*time.Hour + d) }
	// Ids as a stream assigns them: monotone with position. Gaps between ids
	// stand for other tables' events.
	insertOrdersEvent(t, r.db, r.schema, 10, 100, hour(0, 10*time.Minute), 2, "1", `{"id":1,"status":"A"}`)
	insertOrdersEvent(t, r.db, r.schema, 25, 200, hour(1, 10*time.Minute), 3, "2", "")
	insertOrdersEvent(t, r.db, r.schema, 31, 300, hour(2, 10*time.Minute), 1, "4", `{"id":4,"status":"D"}`)

	base, rep := r.runWith(t, hour(0, 30*time.Minute), true)
	if got := lastPairStamp(t, base); got != 10 {
		t.Fatalf("after hour 0: stamp = %d, want 10", got)
	}
	// A walked window that folded a page has time on both sides.
	if rep.FetchDuration <= 0 || rep.FoldDuration <= 0 {
		t.Fatalf("walked window: fetch %s, fold %s, want both positive", rep.FetchDuration, rep.FoldDuration)
	}
	// On a stream index the floor by construction removes nothing the
	// position gate admits, so these runs pin the stamp and the state; that
	// the floor is applied is what the gated test's first case proves.
	base = r.run(t, hour(1, 30*time.Minute))
	if got := lastPairStamp(t, base); got != 25 {
		t.Fatalf("after hour 1: stamp = %d, want 25", got)
	}
	if got := readOrdersState(t, base); !equalStrings(got, []string{"1=A", "3=shipped"}) {
		t.Fatalf("after hour 1: state = %v", got)
	}
	// An empty window between two events: the cut is the anchor, so the
	// fetch is skipped (no time on either side), no pair is written, and
	// the stamp the next fold floors on is still 25.
	base, rep = r.runWith(t, hour(1, 50*time.Minute), true)
	if got := lastPairStamp(t, base); got != 25 {
		t.Fatalf("after the empty window: stamp = %d, want 25 carried", got)
	}
	if rep.FetchDuration != 0 || rep.FoldDuration != 0 {
		t.Fatalf("window empty by position: fetch %s, fold %s, want both zero (no fetch ran)", rep.FetchDuration, rep.FoldDuration)
	}
	base = r.run(t, hour(2, 30*time.Minute))
	if got := lastPairStamp(t, base); got != 31 {
		t.Fatalf("after hour 2: stamp = %d, want 31", got)
	}
	if got := readOrdersState(t, base); !equalStrings(got, []string{"1=A", "3=shipped", "4=D"}) {
		t.Fatalf("after hour 2: state = %v", got)
	}
}

// TestReconstructParquet_eventIDFloorGatedOnStreamCapture pins the gate with
// the one input that tells the two apart: an event whose id is BELOW the
// previous fold's stamp but whose position is AFTER its anchor — what a
// file-mode index produces when a later binlog file is indexed first. With
// stream_state present the fold trusts id order and skips it; with no
// stream_state it applies it. The exact position gate admits it either way,
// which is why the floor must stay off wherever id order is not binlog order.
//
// The first case cannot arise from real writers: AUTO_INCREMENT never hands
// out a lower id later, so on an index a stream wrote the floor skips
// nothing the position gate admits. It is the synthetic input that proves
// the floor is LIVE; the second proves it is off. The third is the real
// hazard: a fold made while files were indexed out of order, then a stream
// started on the same index. That fold must have left NO stamp, so the
// stream-era fold has no file-era id to trust.
//
// The cut needs an event PAST the target with a higher id (ResolveSnapshotCut
// reads commit order as ascending id too): without one the cut is the newest
// id's end position, which sits before the out-of-order event, and the
// position gate alone defers it — the floor would then be untestable here.
func TestReconstructParquet_eventIDFloorGatedOnStreamCapture(t *testing.T) {
	for _, tc := range []struct {
		name          string
		capturedFirst bool // stream_state present at the first refresh
		capturedNext  bool // ... and at the second
		wantStamp     uint64
		want          []string
	}{
		{"stream index: floor live, out-of-order id skipped", true, true, 40, []string{"1=A", "2=paid", "3=shipped"}},
		{"file-mode index: no floor, out-of-order id applied", false, false, 0, []string{"1=A", "3=shipped"}},
		{"file-era fold then a stream: no stamp to trust, the event is applied", false, true, 0, []string{"1=A", "3=shipped"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newFloorRig(t)
			if tc.capturedFirst {
				markStreamCaptured(t, r.db)
			}
			hour := func(n int, d time.Duration) time.Time { return r.h0.Add(time.Duration(n)*time.Hour + d) }
			insertOrdersEvent(t, r.db, r.schema, 40, 100, hour(0, 10*time.Minute), 2, "1", `{"id":1,"status":"A"}`)
			base := r.run(t, hour(0, 30*time.Minute))
			if got := lastPairStamp(t, base); got != tc.wantStamp {
				t.Fatalf("first refresh: stamp = %d, want %d", got, tc.wantStamp)
			}
			if tc.capturedNext && !tc.capturedFirst {
				markStreamCaptured(t, r.db)
			}
			// Written AFTER the first refresh, with a lower id and a later position,
			// plus a later event past the target that fixes the cut after it.
			insertOrdersEvent(t, r.db, r.schema, 7, 200, hour(1, 10*time.Minute), 3, "2", "")
			insertOrdersEvent(t, r.db, r.schema, 50, 300, hour(2, 10*time.Minute), 1, "4", `{"id":4,"status":"D"}`)
			base = r.run(t, hour(1, 30*time.Minute))
			if got := readOrdersState(t, base); !equalStrings(got, tc.want) {
				t.Fatalf("state = %v, want %v", got, tc.want)
			}
			// A stamp never drops below the previous one; the file-era chain
			// gets its first stamp only once a stream writes the index, and
			// that stamp is the highest id the stream-era fold applied.
			wantAfter := tc.wantStamp
			if tc.capturedNext && !tc.capturedFirst {
				wantAfter = 7
			}
			if got := lastPairStamp(t, base); got != wantAfter {
				t.Fatalf("second refresh: stamp = %d, want %d", got, wantAfter)
			}
		})
	}
}

// TestReconstructParquet_plainTablesCarryTheStampToo: the default path — a
// table rewritten in full, no deltas, the only path an S3-hosted previous
// snapshot can take — stamps the table file itself, and the next refresh
// reads the stamp off that file (fetchFloor with no chain). Without this the
// fix would hold only for the opt-in delta layout.
func TestReconstructParquet_plainTablesCarryTheStampToo(t *testing.T) {
	r := newFloorRig(t)
	markStreamCaptured(t, r.db)
	hour := func(n int, d time.Duration) time.Time { return r.h0.Add(time.Duration(n)*time.Hour + d) }
	insertOrdersEvent(t, r.db, r.schema, 10, 100, hour(0, 10*time.Minute), 2, "1", `{"id":1,"status":"A"}`)
	insertOrdersEvent(t, r.db, r.schema, 25, 200, hour(1, 10*time.Minute), 3, "2", "")
	stamp := func(path string) uint64 {
		t.Helper()
		m, err := baseline.ReadParquetMetadata(path)
		if err != nil {
			t.Fatalf("footer of %s: %v", path, err)
		}
		return m.LastEventID
	}
	base, _ := r.runWith(t, hour(0, 30*time.Minute), false)
	if got := stamp(base); got != 10 {
		t.Fatalf("first rewrite: stamp = %d, want 10", got)
	}
	base, rep := r.runWith(t, hour(1, 30*time.Minute), false)
	if got := stamp(base); got != 25 {
		t.Fatalf("second rewrite: stamp = %d, want 25", got)
	}
	if rep.EventsApplied != 1 {
		t.Fatalf("second rewrite applied %d events, want 1 (the floor must not hide the hour-1 delete)", rep.EventsApplied)
	}
	if got := readOrders(t, base); !equalStrings(got, []string{"1=A", "3=shipped"}) {
		t.Fatalf("state = %v", got)
	}
	// A carried-forward table (no events, carry-forward on) keeps the
	// previous file, stamp included, and reports the fetch it made with no
	// fold behind it. Its manifest takes the previous snapshot's digest for
	// the linked file and hashes nothing (#1717): the plain carry-forward
	// path, not only a delta chain, feeds the manifest its priors.
	var manifest baselineintegrity.ManifestStats
	defer reconstruct.CountManifestReuseForTest(&manifest)()
	reps, err := reconstruct.ReconstructTables(r.ctx, reconstruct.FullTableConfig{
		IndexDSN: r.dsn, BaselineSrc: r.root, Tables: []string{r.schema + ".orders"},
		At: hour(1, 50*time.Minute), OutputDir: r.root, OutputFormat: reconstruct.OutputFormatParquet,
		CarryForwardUnchanged: true,
	})
	if err != nil || len(reps) != 1 {
		t.Fatalf("carry-forward run: reps=%d err=%v", len(reps), err)
	}
	if !reps[0].CarriedForward || reps[0].FoldDuration != 0 {
		t.Fatalf("carry-forward: CarriedForward=%v fold=%s, want carried with no fold time", reps[0].CarriedForward, reps[0].FoldDuration)
	}
	if manifest.Reused != 1 || manifest.Hashed != 0 {
		t.Fatalf("carry-forward manifest: reused %d, hashed %d; want the linked file's digest reused and nothing hashed", manifest.Reused, manifest.Hashed)
	}
	if want := filepath.Dir(filepath.Dir(base)); reps[0].SourceSnapshotDir != want {
		t.Fatalf("SourceSnapshotDir = %q, want %q", reps[0].SourceSnapshotDir, want)
	}
	carried, _, _, err := reconstruct.FindBaseline(r.ctx, r.root, r.schema, "orders", hour(1, 50*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got := stamp(carried); got != 25 {
		t.Fatalf("carried-forward file: stamp = %d, want the previous 25", got)
	}
}
