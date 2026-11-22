//go:build integration

package pgbaseline_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// slotCreating3078 is a source whose slot is being created and cannot finish:
// PostgreSQL ends the creation of a logical slot only once every transaction
// that was open when it began has ended, and hold keeps one open. That is the
// state a busy source leaves capture in for as long as its transactions run
// (#3078: 80 seconds, under the load of a 54 GB table).
type slotCreating3078 struct {
	baseDSN, slot, pub, tbl string
	setup                   *pgx.Conn
	hold                    *pgx.Conn
	// created receives the creation's result: the slot's consistent point,
	// or the error it ended with.
	created chan slotCreated3078
	cancel  context.CancelFunc
}

type slotCreated3078 struct {
	point pglogrepl.LSN
	err   error
}

func newSlotCreating3078(t *testing.T, name string) *slotCreating3078 {
	t.Helper()
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	s := &slotCreating3078{
		baseDSN: baseDSN,
		slot:    "bintrail_pgb_3078_" + name,
		pub:     "bintrail_pgb_3078_pub_" + name,
		tbl:     "pgb_3078_" + name,
		created: make(chan slotCreated3078, 1),
	}
	var err error
	if s.setup, err = pgx.Connect(ctx, baseDSN); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { s.setup.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = s.setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+s.pub)
		_, _ = s.setup.Exec(bg, "DROP TABLE IF EXISTS "+s.tbl)
		for range 50 {
			_, _ = s.setup.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", s.slot)
			var n int
			if err := s.setup.QueryRow(bg, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", s.slot).Scan(&n); err != nil || n == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	drop()
	t.Cleanup(drop)
	s.exec(t, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", s.tbl))
	s.exec(t, fmt.Sprintf("INSERT INTO %s VALUES (1, 'before'), (2, 'before')", s.tbl))
	s.exec(t, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", s.pub, s.tbl))

	// The open transaction, with a transaction id of its own: only those
	// hold a slot's creation back.
	if s.hold, err = pgx.Connect(ctx, baseDSN); err != nil {
		t.Fatalf("connect (open transaction): %v", err)
	}
	t.Cleanup(func() { s.hold.Close(context.Background()) })
	if _, err := s.hold.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	var xid int64
	if err := s.hold.QueryRow(ctx, "SELECT txid_current()").Scan(&xid); err != nil {
		t.Fatalf("give the open transaction an id: %v", err)
	}
	return s
}

func (s *slotCreating3078) exec(t *testing.T, stmt string) {
	t.Helper()
	if _, err := s.setup.Exec(context.Background(), stmt); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

// startCreating begins the slot's creation as capture does, on a replication
// connection of its own, and returns once PostgreSQL lists the slot: listed,
// and without a position.
func (s *slotCreating3078) startCreating(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	t.Cleanup(cancel)
	go func() {
		repl, err := pgconn.Connect(ctx, replDSN(s.baseDSN))
		if err != nil {
			s.created <- slotCreated3078{err: err}
			return
		}
		defer repl.Close(context.Background())
		res, err := pglogrepl.CreateReplicationSlot(ctx, repl, s.slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
		if err != nil {
			s.created <- slotCreated3078{err: err}
			return
		}
		point, err := pglogrepl.ParseLSN(res.ConsistentPoint)
		s.created <- slotCreated3078{point: point, err: err}
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var hasPosition bool
		err := s.setup.QueryRow(context.Background(), "SELECT confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name = $1", s.slot).Scan(&hasPosition)
		if err == nil {
			if hasPosition {
				t.Fatal("the slot has a position while a transaction that was open before its creation is still open: the test's premise does not hold on this server")
			}
			return
		}
		select {
		case c := <-s.created:
			t.Fatalf("the slot's creation ended before it was seen in progress: point %s, err %v", c.point, c.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the slot being created was never listed in pg_replication_slots: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// release ends the open transaction, which lets a creation finish.
func (s *slotCreating3078) release(t *testing.T) {
	t.Helper()
	if _, err := s.hold.Exec(context.Background(), "COMMIT"); err != nil {
		t.Fatalf("end the open transaction: %v", err)
	}
}

func (s *slotCreating3078) config(outDir string) pgbaseline.Config {
	return pgbaseline.Config{
		QueryDSN:    s.baseDSN,
		ReplDSN:     replDSN(s.baseDSN),
		SlotName:    s.slot,
		Publication: s.pub,
		OutputDir:   outDir,
		Compression: "zstd",
		Parallelism: 1,
	}
}

type ran3078 struct {
	stats pgbaseline.Stats
	err   error
}

// The gap of #3078, at the snapshot itself. Capture is still creating its
// slot when the snapshot is asked for. A change committed after the snapshot
// has read its rows and before the slot reaches its consistent point is in
// neither: the snapshot does not see it, and a slot delivers only what commits
// after its consistent point. So the snapshot must not read anything before
// the slot has that point.
func TestPGBaseline_SlotBeingCreated_3078_Integration(t *testing.T) {
	s := newSlotCreating3078(t, "creating")
	s.startCreating(t)

	outDir := t.TempDir()
	done := make(chan ran3078, 1)
	go func() {
		stats, err := pgbaseline.Run(context.Background(), s.config(outDir))
		done <- ran3078{stats, err}
	}()

	// Time for a snapshot that does not wait to read its two rows.
	var early *ran3078
	select {
	case r := <-done:
		early = &r
	case <-time.After(3 * time.Second):
	}

	// The change in between: committed while the slot has no position.
	s.exec(t, fmt.Sprintf("UPDATE %s SET v = 'in between' WHERE id = 1", s.tbl))
	s.release(t)
	created := <-s.created
	if created.err != nil {
		t.Fatalf("the slot's creation failed: %v", created.err)
	}

	if early != nil {
		if early.err != nil {
			t.Fatalf("the snapshot failed while the slot was being created, want it to wait: %v", early.err)
		}
		t.Fatalf("the snapshot was read while the slot was still being created: its rows are as of %s and the slot delivers from %s, so the change committed between the two is in neither (floor recorded: %s)",
			pglogrepl.LSN(early.stats.AnchorLSN), created.point, pglogrepl.LSN(early.stats.DeltaStartLSN))
	}

	var r ran3078
	select {
	case r = <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("the snapshot did not end within a minute of the slot having its position")
	}
	if r.err != nil {
		t.Fatalf("the snapshot failed after the slot got its position: %v", r.err)
	}
	if r.stats.SlotCreated {
		t.Error("the snapshot says it created the slot; capture did")
	}
	// The ordering every reader relies on: the slot delivers from at or
	// before where the snapshot's rows are.
	if uint64(created.point) > r.stats.AnchorLSN {
		t.Fatalf("the slot delivers from %s, after the snapshot's rows (%s)", created.point, pglogrepl.LSN(r.stats.AnchorLSN))
	}
	if r.stats.DeltaStartLSN != uint64(created.point) {
		t.Errorf("the snapshot's floor is %s, want the slot's own position %s", pglogrepl.LSN(r.stats.DeltaStartLSN), created.point)
	}
	// And the change in between is in the snapshot.
	rows := readRawRows(t, filepath.Join(singleSnapshotDir(t, outDir), "public", s.tbl+".parquet"))
	if got := strOrNull(rowByCol(t, rows, "id", "1")["v"]); got != "in between" {
		t.Fatalf("the snapshot holds v = %q for row 1, want the change committed while the slot was being created", got)
	}
}

// Capture never gets its slot: the snapshot is refused at the cap, having
// read nothing and created nothing, and says what to wait for.
func TestPGBaseline_SlotNeverReady_Refused_3078_Integration(t *testing.T) {
	s := newSlotCreating3078(t, "never")
	s.startCreating(t)

	outDir := t.TempDir()
	cfg := s.config(outDir)
	cfg.SlotWait = time.Second
	began := time.Now()
	_, err := pgbaseline.Run(context.Background(), cfg)
	if !errors.Is(err, pgcapture.ErrSlotNotReady) {
		t.Fatalf("err = %v, want ErrSlotNotReady", err)
	}
	for _, want := range []string{"no table was read", "is still being created after 1s", "Run the snapshot again"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, err)
		}
	}
	if took := time.Since(began); took > 15*time.Second {
		t.Errorf("a 1s wait took %s", took)
	}
	if entries, _ := os.ReadDir(outDir); len(entries) != 0 {
		t.Errorf("the refused snapshot left %v in its folder", entries)
	}

	// It cures itself: once the slot has its position the same command runs.
	s.release(t)
	if c := <-s.created; c.err != nil {
		t.Fatalf("the slot's creation failed: %v", c.err)
	}
	stats, err := pgbaseline.Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("the snapshot after the slot got its position: %v", err)
	}
	if stats.SlotWaited != 0 || stats.SlotCreated {
		t.Errorf("stats = %+v, want no wait and no slot created", stats)
	}
}

// The slot is removed while the tables are read: the gap is known when the
// read ends, so the run fails and the folder is not published.
func TestPGBaseline_SlotGoneAtTheEnd_NotPublished_3078_Integration(t *testing.T) {
	s := newSlotCreating3078(t, "gone")
	s.release(t) // nothing holds the slot back in this test

	restore := pgbaseline.SetTestHookAfterSnapshot(func() {
		if _, err := s.setup.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", s.slot); err != nil {
			t.Errorf("drop the slot mid-read: %v", err)
		}
	})
	defer restore()
	outDir := t.TempDir()
	_, err := pgbaseline.Run(context.Background(), s.config(outDir))
	if err == nil || !strings.Contains(err.Error(), "no longer exists") || !strings.Contains(err.Error(), "the snapshot is not published") {
		t.Fatalf("err = %v, want the run to fail on the slot that was removed", err)
	}
	dir := singleSnapshotDir(t, outDir)
	if baseline.SnapshotComplete(dir) {
		t.Fatal("the snapshot was published although its slot was removed while it was read")
	}
}

// What the caller knows of a gap is asked before the publish, and its refusal
// leaves the folder unpublished.
func TestPGBaseline_BeforePublishRefusal_NotPublished_3078_Integration(t *testing.T) {
	s := newSlotCreating3078(t, "hook")
	s.release(t)

	outDir := t.TempDir()
	cfg := s.config(outDir)
	refusal := errors.New("capture started after this snapshot's floor")
	var asked pgbaseline.Stats
	cfg.BeforePublish = func(st pgbaseline.Stats) error { asked = st; return refusal }
	_, err := pgbaseline.Run(context.Background(), cfg)
	if !errors.Is(err, refusal) {
		t.Fatalf("err = %v, want the caller's refusal", err)
	}
	if asked.DeltaStartLSN == 0 || asked.TablesProcessed != 1 {
		t.Errorf("BeforePublish was given %+v, want the floor and the tables read", asked)
	}
	if baseline.SnapshotComplete(singleSnapshotDir(t, outDir)) {
		t.Fatal("the snapshot was published over the caller's refusal")
	}
	cfg.BeforePublish = func(pgbaseline.Stats) error { return nil }
	if _, err := pgbaseline.Run(context.Background(), s.config(t.TempDir())); err != nil {
		t.Fatalf("a run nothing refuses: %v", err)
	}
}
