//go:build integration

package pgcapture_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// syncWriter lets a test read a log the capture goroutine writes.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// nofullRun is a Capturer running against the test PostgreSQL, with what its
// tests read from it.
type nofullRun struct {
	t      *testing.T
	events chan event.Event
	runErr chan error
	cancel context.CancelFunc
	skips  *parser.SkipCounters
	logs   *bytes.Buffer
	logMu  *syncWriter
}

// startNofull starts a Capturer with the gate on tables below REPLICA
// IDENTITY FULL turned off (pgcapture.CaptureBelowFull): the tests of this
// file test the code that stores an earlier row in part, which no table
// reaches while the gate is on. startCapture(..., true) runs the gate.
func startNofull(t *testing.T, ctx context.Context, setup *pgx.Conn, baseDSN, slot, pub string) *nofullRun {
	t.Helper()
	return startCapture(t, ctx, setup, baseDSN, slot, pub, false)
}

func startCapture(t *testing.T, ctx context.Context, setup *pgx.Conn, baseDSN, slot, pub string, gate bool) *nofullRun {
	t.Helper()
	r := &nofullRun{t: t, skips: parser.NewSkipCounters(nil), logs: &bytes.Buffer{}}
	r.logMu = &syncWriter{w: r.logs}
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	cfg := pgcapture.Config{
		ReplDSN:         replDSN(baseDSN),
		QueryDSN:        baseDSN,
		SlotName:        slot,
		Publication:     pub,
		Skips:           r.skips,
		StandbyInterval: 200 * time.Millisecond,
		Logger:          slog.New(slog.NewTextHandler(r.logMu, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	if !gate {
		cfg = pgcapture.CaptureBelowFull(cfg)
	}
	cap := pgcapture.New(cfg)
	r.events = make(chan event.Event, 4096)
	r.runErr = make(chan error, 1)
	go func() { r.runErr <- cap.Run(runCtx, r.events) }()
	waitSlotActive(t, setup, slot, r.runErr)
	return r
}

func (r *nofullRun) logText() string {
	r.logMu.mu.Lock()
	defer r.logMu.mu.Unlock()
	return r.logs.String()
}

// until returns the row and DDL events read up to and including the first one
// done accepts, failing on a timeout or if Run returns.
func (r *nofullRun) until(done func(event.Event) bool) []event.Event {
	r.t.Helper()
	var out []event.Event
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case ev := <-r.events:
			if !isRow(ev) && ev.EventType != event.EventDDL {
				continue
			}
			out = append(out, ev)
			if done(ev) {
				return out
			}
		case err := <-r.runErr:
			r.t.Fatalf("Run stopped while changes were expected: %v\nlog:\n%s", err, r.logText())
		case <-deadline.C:
			r.t.Fatalf("timed out after %d events; log:\n%s", len(out), r.logText())
		}
	}
}

func (r *nofullRun) stop() {
	r.t.Helper()
	select {
	case err := <-r.runErr:
		r.t.Fatalf("Run stopped on its own: %v", err)
	default:
	}
	r.cancel()
	if err := <-r.runErr; err != nil {
		r.t.Fatalf("Run returned non-nil on graceful cancel: %v", err)
	}
}

func of(evs []event.Event, table string) []event.Event {
	var out []event.Event
	for _, ev := range evs {
		if ev.Table == table {
			out = append(out, ev)
		}
	}
	return out
}

// rowKinds spells a table's events: I, U, D for rows and T for a TRUNCATE.
func rowKinds(evs []event.Event) string {
	var b strings.Builder
	for _, ev := range evs {
		switch {
		case ev.EventType == event.EventInsert:
			b.WriteByte('I')
		case ev.EventType == event.EventUpdate:
			b.WriteByte('U')
		case ev.EventType == event.EventDelete:
			b.WriteByte('D')
		case ev.EventType == event.EventDDL && ev.DDLType == event.DDLTruncateTable:
			b.WriteByte('T')
		default:
			b.WriteByte('?')
		}
	}
	return b.String()
}

func refusedNoIdentity(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55000" && strings.Contains(pgErr.Message, "does not have a replica identity")
}

// TestCapturer_BelowFull_Integration runs the real Capturer on a live
// PostgreSQL over one table per replica identity and reads what it emits for
// each kind of change. The unit tests feed the decoder messages shaped like
// PostgreSQL's; here PostgreSQL makes them.
func TestCapturer_BelowFull_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_nofull_it"
	const pub = "bintrail_nofull_it_pub"
	const quoted = `"NF Quoted"`
	tables := []string{"nf_def", "nf_idx", "nf_idxnopk", "nf_nothing", "nf_nopk", "nf_idxonly", "nf_full", "nf_bigkey", quoted, "nf_pkinclude"}

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+strings.Join(tables, ", "))
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	text := func(q string) string {
		t.Helper()
		var s string
		if err := setup.QueryRow(ctx, q).Scan(&s); err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
		return s
	}

	// big is stored out of line, uncompressed, so an UPDATE that leaves it
	// alone sends it as 'u' (as in TestPgoutputReplicaIdentityWire_Integration).
	const bigVal = "(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 100) i)"
	const bigVal2 = "(SELECT string_agg(md5(i::text), '') FROM generate_series(101, 200) i)"
	const bigKey = "(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 80) i)"
	big, big2, key := text("SELECT "+bigVal), text("SELECT "+bigVal2), text("SELECT "+bigKey)

	mustExec("CREATE TABLE nf_def (id int PRIMARY KEY, v text, big text)")
	mustExec("CREATE TABLE nf_idx (id int PRIMARY KEY, u int NOT NULL, v text, big text)")
	mustExec("CREATE UNIQUE INDEX nf_idx_uk ON nf_idx (id, u)")
	mustExec("ALTER TABLE nf_idx REPLICA IDENTITY USING INDEX nf_idx_uk")
	mustExec("CREATE TABLE nf_idxnopk (id int PRIMARY KEY, u int NOT NULL, v text)")
	mustExec("CREATE UNIQUE INDEX nf_idxnopk_uk ON nf_idxnopk (u)")
	mustExec("ALTER TABLE nf_idxnopk REPLICA IDENTITY USING INDEX nf_idxnopk_uk")
	mustExec("CREATE TABLE nf_nothing (id int PRIMARY KEY, v text)")
	mustExec("ALTER TABLE nf_nothing REPLICA IDENTITY NOTHING")
	mustExec("CREATE TABLE nf_nopk (a int, v text)")
	mustExec("CREATE TABLE nf_idxonly (u int NOT NULL, v text)")
	mustExec("CREATE UNIQUE INDEX nf_idxonly_uk ON nf_idxonly (u)")
	mustExec("ALTER TABLE nf_idxonly REPLICA IDENTITY USING INDEX nf_idxonly_uk")
	mustExec("CREATE TABLE nf_full (id int PRIMARY KEY, v text, big text)")
	mustExec("ALTER TABLE nf_full REPLICA IDENTITY FULL")
	mustExec("CREATE TABLE nf_bigkey (k text PRIMARY KEY, v text)")
	mustExec("CREATE TABLE " + quoted + ` ("Order Id" int PRIMARY KEY, "NOTE" text, "Big Val" text)`)
	// A primary key with an INCLUDE column: the identity DEFAULT sends is the
	// key column alone, and the capture reads the included column as part of
	// the key, so the identity does not hold "the primary key" as read.
	mustExec("CREATE TABLE nf_pkinclude (id int, extra int, v text, PRIMARY KEY (id) INCLUDE (extra))")
	for _, tb := range []string{"nf_def", "nf_idx", "nf_full"} {
		mustExec("ALTER TABLE " + tb + " ALTER COLUMN big SET STORAGE EXTERNAL")
	}
	mustExec("ALTER TABLE " + quoted + ` ALTER COLUMN "Big Val" SET STORAGE EXTERNAL`)
	mustExec("ALTER TABLE nf_bigkey ALTER COLUMN k SET STORAGE EXTERNAL")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + strings.Join(tables, ", "))

	// What the doctor reads of these tables from the catalog.
	ids, err := pgcapture.QueryTableIdentities(ctx, setup, pub)
	if err != nil {
		t.Fatalf("QueryTableIdentities: %v", err)
	}
	gotIDs := map[string]string{}
	for _, ti := range ids {
		gotIDs[ti.Table] = fmt.Sprintf("%s none=%v misses=%v", ti.Identity, ti.NoIdentity, ti.MissesKey)
	}
	t.Logf("catalog identities: %v", gotIDs)
	wantIDs := map[string]string{
		"nf_def":       "d none=false misses=[]",
		"nf_idx":       "i none=false misses=[]",
		"nf_idxnopk":   "i none=false misses=[id]",
		"nf_nothing":   "n none=true misses=[]",
		"nf_nopk":      "d none=true misses=[]",
		"nf_idxonly":   "i none=false misses=[]",
		"nf_bigkey":    "d none=false misses=[]",
		"NF Quoted":    "d none=false misses=[]",
		"nf_pkinclude": "d none=false misses=[extra]",
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Errorf("QueryTableIdentities = %v\nwant %v (nf_full is at FULL and not listed)", gotIDs, wantIDs)
	}

	// What the health panel lists (production, whatever the gate of this
	// test's capture): every table below FULL as not captured, each with why,
	// and none as "without undo". A table outside the capture's scope is in
	// neither.
	listed, notCaptured := pgcapture.HealthTableLists(ids, nil, event.Filters{})
	wantListed := []string{}
	wantNot := []string{"public.NF Quoted (not at REPLICA IDENTITY FULL)", "public.nf_bigkey (not at REPLICA IDENTITY FULL)", "public.nf_def (not at REPLICA IDENTITY FULL)",
		"public.nf_idx (not at REPLICA IDENTITY FULL)", "public.nf_idxnopk (replica identity without the primary key)", "public.nf_idxonly (not at REPLICA IDENTITY FULL)",
		"public.nf_nopk (not at REPLICA IDENTITY FULL)", "public.nf_nothing (not at REPLICA IDENTITY FULL)", "public.nf_pkinclude (replica identity without the primary key)"}
	if !slices.Equal(listed, wantListed) || !slices.Equal(notCaptured, wantNot) {
		t.Errorf("health lists:\nwithout undo %q\nwant         %q\nnot captured %q\nwant         %q", listed, wantListed, notCaptured, wantNot)
	}
	scoped, scopedNot := pgcapture.HealthTableLists(ids, nil, event.Filters{Tables: map[string]bool{"public.nf_def": true}})
	if len(scoped) != 0 || !slices.Equal(scopedNot, []string{"public.nf_def (not at REPLICA IDENTITY FULL)"}) {
		t.Errorf("with the capture limited to nf_def: without undo %q, not captured %q", scoped, scopedNot)
	}

	r := startNofull(t, ctx, setup, baseDSN, slot, pub)

	// One statement per transaction.
	mustExec("INSERT INTO nf_def VALUES (1, 'a', " + bigVal + ")")
	mustExec("UPDATE nf_def SET v = 'b' WHERE id = 1")                         // no old tuple, big unsent
	mustExec("UPDATE nf_def SET id = 2 WHERE id = 1")                          // 'K', big unsent
	mustExec("UPDATE nf_def SET big = " + bigVal2 + ", v = NULL WHERE id = 2") // no old tuple, big sent
	mustExec("DELETE FROM nf_def WHERE id = 2")                                // 'K'

	mustExec("INSERT INTO nf_idx VALUES (1, 10, 'a', " + bigVal + ")")
	mustExec("UPDATE nf_idx SET v = 'b' WHERE id = 1")
	mustExec("UPDATE nf_idx SET u = 11 WHERE id = 1") // an identity column outside the primary key
	mustExec("DELETE FROM nf_idx WHERE id = 1")

	mustExec("INSERT INTO nf_idxnopk VALUES (1, 10, 'a')")
	mustExec("UPDATE nf_idxnopk SET id = 2 WHERE id = 1") // PostgreSQL sends no old tuple: the old key is gone
	mustExec("DELETE FROM nf_idxnopk WHERE id = 2")

	mustExec("INSERT INTO nf_pkinclude VALUES (1, 5, 'a')")

	mustExec("INSERT INTO nf_nothing VALUES (1, 'a')")
	mustExec("INSERT INTO nf_nopk VALUES (1, 'a')")
	for _, q := range []string{"UPDATE nf_nothing SET v = 'b'", "DELETE FROM nf_nothing", "UPDATE nf_nopk SET v = 'b'", "DELETE FROM nf_nopk"} {
		if _, err := setup.Exec(ctx, q); !refusedNoIdentity(err) {
			t.Errorf("%s: PostgreSQL must refuse it (55000, no replica identity), got %v", q, err)
		}
	}

	mustExec("INSERT INTO nf_idxonly VALUES (5, 'a')")
	mustExec("UPDATE nf_idxonly SET v = 'b' WHERE u = 5")
	mustExec("DELETE FROM nf_idxonly WHERE u = 5")

	mustExec("INSERT INTO nf_full VALUES (1, 'a', " + bigVal + ")")
	mustExec("UPDATE nf_full SET v = 'b' WHERE id = 1")
	mustExec("DELETE FROM nf_full WHERE id = 1")

	mustExec("INSERT INTO nf_bigkey VALUES (" + bigKey + ", 'a')")
	mustExec("UPDATE nf_bigkey SET v = 'b'") // the key is a large value left as it was
	mustExec("DELETE FROM nf_bigkey")

	mustExec("INSERT INTO " + quoted + " VALUES (1, 'a', " + bigVal + ")")
	mustExec("UPDATE " + quoted + ` SET "NOTE" = 'b' WHERE "Order Id" = 1`)
	mustExec("DELETE FROM " + quoted + ` WHERE "Order Id" = 1`)

	// DEFAULT, FULL, DEFAULT in mid stream, then a column added and one dropped.
	mustExec("INSERT INTO nf_def VALUES (7, 'a', " + bigVal + ")")
	mustExec("ALTER TABLE nf_def REPLICA IDENTITY FULL")
	mustExec("UPDATE nf_def SET v = 'full' WHERE id = 7")
	mustExec("ALTER TABLE nf_def REPLICA IDENTITY DEFAULT")
	mustExec("UPDATE nf_def SET v = 'default again' WHERE id = 7")
	mustExec("ALTER TABLE nf_def ADD COLUMN extra int")
	mustExec("UPDATE nf_def SET extra = 1 WHERE id = 7")
	mustExec("ALTER TABLE nf_def DROP COLUMN v")
	mustExec("UPDATE nf_def SET extra = 2 WHERE id = 7")
	mustExec("TRUNCATE nf_def")

	mustExec("INSERT INTO nf_full VALUES (999, 'end', 'end')")
	evs := r.until(func(ev event.Event) bool { return ev.Table == "nf_full" && ev.PKValues == "999" })
	r.stop()

	wantKinds := map[string]string{
		"nf_def":       "IUUUD" + "IUUUUT",
		"nf_idx":       "IUUD",
		"nf_idxnopk":   "",
		"nf_pkinclude": "",
		"nf_nothing":   "I",
		"nf_nopk":      "I",
		"nf_idxonly":   "IUD",
		"nf_full":      "IUDI",
		"nf_bigkey":    "IUD",
		"NF Quoted":    "IUD",
	}
	for tb, want := range wantKinds {
		if got := rowKinds(of(evs, tb)); got != want {
			t.Errorf("%s: events %q, want %q", tb, got, want)
		}
	}
	if t.Failed() {
		t.Fatalf("log:\n%s", r.logText())
	}

	def := of(evs, "nf_def")
	cols := []string{"id", "v", "big"}
	// INSERT.
	if def[0].RowBefore != nil || def[0].RowAfter["big"] != big || def[0].PKValues != "1" {
		t.Errorf("nf_def INSERT: pk=%q before=%v", def[0].PKValues, def[0].RowBefore)
	}
	// UPDATE that leaves the key alone: a synthesized earlier row.
	assertPartialBefore(t, def[1], cols, map[string]string{"id": "1"})
	if def[1].RowAfter["v"] != "b" || !event.IsUnchangedToastMarker(def[1].RowAfter["big"]) || def[1].PKValues != "1" {
		t.Errorf("nf_def UPDATE (key untouched): pk=%q after[v]=%v after[big]=%T", def[1].PKValues, def[1].RowAfter["v"], def[1].RowAfter["big"])
	}
	// UPDATE of the key: 'K'.
	assertPartialBefore(t, def[2], cols, map[string]string{"id": "1"})
	if def[2].PKValues != "1" || def[2].RowAfter["id"] != "2" || !event.IsUnchangedToastMarker(def[2].RowAfter["big"]) {
		t.Errorf("nf_def UPDATE (key changed): pk=%q after[id]=%v after[big]=%T", def[2].PKValues, def[2].RowAfter["id"], def[2].RowAfter["big"])
	}
	// UPDATE that writes the large value and a NULL: both are real in the new row.
	assertPartialBefore(t, def[3], cols, map[string]string{"id": "2"})
	if v, ok := def[3].RowAfter["v"]; def[3].RowAfter["big"] != big2 || !ok || v != nil {
		t.Errorf("nf_def UPDATE (big written, v NULL): after[v]=%#v after[big] is the new value: %v", v, def[3].RowAfter["big"] == big2)
	}
	// DELETE: 'K'.
	assertPartialBefore(t, def[4], cols, map[string]string{"id": "2"})
	if def[4].RowAfter != nil || def[4].PKValues != "2" {
		t.Errorf("nf_def DELETE: pk=%q after=%v", def[4].PKValues, def[4].RowAfter)
	}
	// DEFAULT → FULL → DEFAULT: exact per change.
	if u := def[6]; len(event.NotCapturedColumns(u.RowBefore)) != 0 || u.RowBefore["v"] != "a" || u.RowBefore["big"] != big || u.RowAfter["big"] != big {
		t.Errorf("nf_def UPDATE made at FULL must carry whole rows: before[v]=%v, markers %v, after[big] real=%v",
			u.RowBefore["v"], event.NotCapturedColumns(u.RowBefore), u.RowAfter["big"] == big)
	}
	assertPartialBefore(t, def[7], cols, map[string]string{"id": "7"})
	if !event.IsUnchangedToastMarker(def[7].RowAfter["big"]) {
		t.Errorf("nf_def UPDATE made back at DEFAULT: after[big] = %T, want the unchanged-TOAST marker", def[7].RowAfter["big"])
	}
	// A column added, then one dropped: the two images hold the same columns.
	assertPartialBefore(t, def[8], []string{"id", "v", "big", "extra"}, map[string]string{"id": "7"})
	assertPartialBefore(t, def[9], []string{"id", "big", "extra"}, map[string]string{"id": "7"})
	if def[10].DDLType != event.DDLTruncateTable {
		t.Errorf("nf_def TRUNCATE: %+v", def[10])
	}

	idx := of(evs, "nf_idx")
	idxCols := []string{"id", "u", "v", "big"}
	assertPartialBefore(t, idx[1], idxCols, map[string]string{"id": "1", "u": "10"})
	assertPartialBefore(t, idx[2], idxCols, map[string]string{"id": "1", "u": "10"})
	if idx[2].RowAfter["u"] != "11" || idx[2].PKValues != "1" {
		t.Errorf("nf_idx UPDATE of the index column: pk=%q after[u]=%v", idx[2].PKValues, idx[2].RowAfter["u"])
	}
	assertPartialBefore(t, idx[3], idxCols, map[string]string{"id": "1", "u": "11"})

	only := of(evs, "nf_idxonly")
	for _, ev := range only {
		if ev.PKValues != "" {
			t.Errorf("nf_idxonly has no primary key: pk_values = %q, want empty", ev.PKValues)
		}
	}
	assertPartialBefore(t, only[1], []string{"u", "v"}, map[string]string{"u": "5"})
	assertPartialBefore(t, only[2], []string{"u", "v"}, map[string]string{"u": "5"})

	full := of(evs, "nf_full")
	if u := full[1]; u.RowBefore["v"] != "a" || u.RowBefore["big"] != big || u.RowAfter["big"] != big || len(event.NotCapturedColumns(u.RowBefore)) != 0 {
		t.Errorf("nf_full UPDATE: whole rows and the large value resolved; before[v]=%v", u.RowBefore["v"])
	}
	if d := full[2]; d.RowBefore["big"] != big || d.RowBefore["v"] != "b" {
		t.Errorf("nf_full DELETE: the whole old row; before[v]=%v", d.RowBefore["v"])
	}

	bk := of(evs, "nf_bigkey")
	assertPartialBefore(t, bk[1], []string{"k", "v"}, map[string]string{"k": key})
	if bk[1].RowAfter["k"] != key || bk[1].PKValues != key {
		t.Errorf("nf_bigkey UPDATE: the unchanged large KEY must be real in the new row and in pk_values (after[k] is %T)", bk[1].RowAfter["k"])
	}
	assertPartialBefore(t, bk[2], []string{"k", "v"}, map[string]string{"k": key})

	q := of(evs, "NF Quoted")
	qCols := []string{"Order Id", "NOTE", "Big Val"}
	assertPartialBefore(t, q[1], qCols, map[string]string{"Order Id": "1"})
	if !event.IsUnchangedToastMarker(q[1].RowAfter["Big Val"]) || q[1].RowAfter["NOTE"] != "b" {
		t.Errorf(`"NF Quoted" UPDATE: after = %v`, q[1].RowAfter)
	}
	assertPartialBefore(t, q[2], qCols, map[string]string{"Order Id": "1"})

	// What was skipped, and only that.
	const reason = pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey
	if n := r.skips.Count(reason); n != 4 || r.skips.Total() != 4 {
		t.Errorf("want 4 skips under %q (3 of nf_idxnopk, 1 of nf_pkinclude) and none other; got %d of %d", reason, n, r.skips.Total())
	}
	assertLedgerNames(t, r.skips, reason, "public.nf_idxnopk", "public.nf_pkinclude")

	logs := r.logText()
	t.Logf("capture log:\n%s", logs)
	for _, want := range []string{
		// The startup warning is the production one (the gate is off for
		// this test's capture only): every table below FULL, not captured.
		"9 tables not at REPLICA IDENTITY FULL",
		"their changes are NOT captured",
		"table without undo",
		"has no replica identity",
		"replica identity does not hold its primary key",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("the log lacks %q", want)
		}
	}
}

// A partitioned table with one partition at DEFAULT and one at FULL.
// Published by its partitions, each is described with its own identity.
// Published through its root, the description carries the ROOT's identity
// and PostgreSQL labels every old row by it, while each row holds what its
// PARTITION logged (ALTER TABLE on the parent does not reach the partitions).
// Either way a change made in the partition at FULL is stored whole and can
// be undone, and one made in the partition at DEFAULT with its key only: one
// partition below FULL does not cost the others their undo.
func TestCapturer_BelowFull_Partitioned_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	for _, viaRoot := range []bool{false, true} {
		t.Run(fmt.Sprintf("publish_via_partition_root=%v", viaRoot), func(t *testing.T) {
			const slot = "bintrail_nofull_part_it"
			const pub = "bintrail_nofull_part_pub"
			setup, err := pgx.Connect(ctx, baseDSN)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			t.Cleanup(func() { setup.Close(context.Background()) })
			cleanup := func() {
				bg := context.Background()
				_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
				_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS nf_parted, nf_part_end")
				_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
			}
			cleanup()
			t.Cleanup(cleanup)
			mustExec := func(q string) {
				t.Helper()
				if _, err := setup.Exec(ctx, q); err != nil {
					t.Fatalf("exec %q: %v", q, err)
				}
			}
			const bigVal = "(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 100) i)"
			var big string
			if err := setup.QueryRow(ctx, "SELECT "+bigVal).Scan(&big); err != nil {
				t.Fatal(err)
			}
			mustExec("CREATE TABLE nf_parted (id int PRIMARY KEY, v text, big text) PARTITION BY RANGE (id)")
			mustExec("CREATE TABLE nf_parted_1 PARTITION OF nf_parted FOR VALUES FROM (0) TO (1000)")    // DEFAULT
			mustExec("CREATE TABLE nf_parted_2 PARTITION OF nf_parted FOR VALUES FROM (1000) TO (2000)") // FULL
			mustExec("ALTER TABLE nf_parted_2 REPLICA IDENTITY FULL")
			for _, part := range []string{"nf_parted_1", "nf_parted_2"} {
				mustExec("ALTER TABLE " + part + " ALTER COLUMN big SET STORAGE EXTERNAL")
			}
			mustExec("CREATE TABLE nf_part_end (id int PRIMARY KEY)")
			belowTable, fullTable := "nf_parted_1", "nf_parted_2"
			if viaRoot {
				belowTable, fullTable = "nf_parted", "nf_parted"
				mustExec("ALTER TABLE nf_parted REPLICA IDENTITY FULL") // the root only
			}
			mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE nf_parted, nf_part_end WITH (publish_via_partition_root = %v)", pub, viaRoot))

			// What the doctor and the health panel read: a root at FULL is
			// listed for its partition below FULL alone.
			ids, err := pgcapture.QueryTableIdentities(ctx, setup, pub)
			if err != nil {
				t.Fatalf("QueryTableIdentities: %v", err)
			}
			var gotIDs []string
			for _, ti := range ids {
				gotIDs = append(gotIDs, fmt.Sprintf("%s %s none=%v misses=%v parts=%v partsmiss=%v", ti.Table, ti.Identity, ti.NoIdentity, ti.MissesKey, ti.PartitionsBelowFull, ti.PartitionsMissKey))
			}
			wantIDs := []string{"nf_part_end d none=false misses=[] parts=[] partsmiss=[]", "nf_parted_1 d none=false misses=[] parts=[] partsmiss=[]"}
			if viaRoot {
				// The parent has an identity (it is at FULL): it is listed
				// for its partition, and is not a table without one.
				wantIDs = []string{`nf_part_end d none=false misses=[] parts=[] partsmiss=[]`, `nf_parted f none=false misses=[] parts=["public"."nf_parted_1"] partsmiss=[]`}
			}
			if !slices.Equal(gotIDs, wantIDs) {
				t.Errorf("QueryTableIdentities = %q, want %q", gotIDs, wantIDs)
			}

			r := startNofull(t, ctx, setup, baseDSN, slot, pub)
			for _, id := range []string{"1", "1001"} { // one row in each partition
				mustExec("INSERT INTO nf_parted VALUES (" + id + ", 'a', " + bigVal + ")")
				mustExec("UPDATE nf_parted SET v = 'b' WHERE id = " + id)
				mustExec("UPDATE nf_parted SET id = id + 1 WHERE id = " + id)
				mustExec("DELETE FROM nf_parted WHERE id = " + id + " + 1")
			}
			mustExec("INSERT INTO nf_part_end VALUES (1)")
			evs := r.until(func(ev event.Event) bool { return ev.Table == "nf_part_end" })
			r.stop()
			t.Logf("capture log:\n%s", r.logText())

			byPK := func(table string, low bool) []event.Event {
				var out []event.Event
				for _, ev := range of(evs, table) {
					if (len(ev.PKValues) < 4) == low {
						out = append(out, ev)
					}
				}
				return out
			}
			cols := []string{"id", "v", "big"}

			// The partition at DEFAULT: key-only earlier rows.
			got := byPK(belowTable, true)
			if rowKinds(got) != "IUUD" {
				t.Fatalf("%s, partition at DEFAULT: events %q, want IUUD (all events: %d)", belowTable, rowKinds(got), len(evs))
			}
			assertPartialBefore(t, got[1], cols, map[string]string{"id": "1"})
			if !event.IsUnchangedToastMarker(got[1].RowAfter["big"]) {
				t.Errorf("UPDATE: after[big] = %T, want the unchanged-TOAST marker", got[1].RowAfter["big"])
			}
			assertPartialBefore(t, got[2], cols, map[string]string{"id": "1"})
			if got[2].PKValues != "1" || got[2].RowAfter["id"] != "2" {
				t.Errorf("key-changing UPDATE: pk=%q after[id]=%v", got[2].PKValues, got[2].RowAfter["id"])
			}
			assertPartialBefore(t, got[3], cols, map[string]string{"id": "2"})

			// The partition at FULL: whole earlier rows, the large value the
			// UPDATE left alone taken from them, and undo possible.
			got = byPK(fullTable, false)
			if rowKinds(got) != "IUUD" {
				t.Fatalf("%s, partition at FULL: events %q, want IUUD", fullTable, rowKinds(got))
			}
			for i, ev := range got[1:] {
				if cols := event.NotCapturedColumns(ev.RowBefore); len(cols) > 0 || ev.RowBefore["big"] != big {
					t.Errorf("partition at FULL, change %d: the earlier row must be whole; not captured %v, big real=%v", i+1, cols, ev.RowBefore["big"] == big)
				}
				if ev.RowAfter != nil && ev.RowAfter["big"] != big {
					t.Errorf("partition at FULL, change %d: after[big] = %T, want the value taken from the whole earlier row", i+1, ev.RowAfter["big"])
				}
				if err := event.CheckUnresolvedToast(ev.Schema, ev.Table, ev.PKValues, stored(t, ev.RowBefore), stored(t, ev.RowAfter)); err != nil {
					t.Errorf("partition at FULL, change %d: it can be undone, got %v", i+1, err)
				}
			}
			if got[1].RowBefore["v"] != "a" || got[3].RowBefore["v"] != "b" {
				t.Errorf("partition at FULL: before[v] = %v, %v", got[1].RowBefore["v"], got[3].RowBefore["v"])
			}

			if r.skips.Total() != 0 {
				t.Errorf("nothing is skipped, got %d", r.skips.Total())
			}
			if viaRoot != strings.Contains(r.logText(), "partitioned table with partitions without undo") {
				t.Errorf("the warning about partitions below FULL must be given through the root only (viaRoot=%v)", viaRoot)
			}
		})
	}
}

// A partition with a replica identity of its own (USING INDEX), under a root
// published in its stead, the root at FULL, at DEFAULT and at USING INDEX.
// Capture cannot see which partition a change was made in; it reads the
// catalog, whatever the root's identity.
//   - The index leaves the primary key out: an UPDATE of the primary key made
//     there arrives with no earlier row at all, so the table is skipped, as a
//     table with such an identity is, and the doctor's list names the
//     partition. Under a root below FULL that change, stored from its new row,
//     would be filed under the NEW key and leave the old row in the copy for
//     good; capture, the health lists and the doctor's read must agree that
//     the table is not captured.
//   - The index holds the primary key and another column: a key-only row then
//     carries a value outside the key and looks whole, so every earlier row of
//     the table is stored with its key only.
func TestCapturer_PartitionOwnIdentity_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	type tcase struct {
		name, index, root string
		skipped           bool
	}
	var cases []tcase
	for _, root := range []string{"FULL", "DEFAULT", "USING INDEX nf_pid_pkey"} {
		cases = append(cases,
			tcase{"root at " + root + ", index without the primary key", "(u, id2)", root, true},
			tcase{"root at " + root + ", index wider than the primary key", "(id, id2, u)", root, false})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const slot = "bintrail_nofull_partid_it"
			const pub = "bintrail_nofull_partid_pub"
			setup, err := pgx.Connect(ctx, baseDSN)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			t.Cleanup(func() { setup.Close(context.Background()) })
			cleanup := func() {
				bg := context.Background()
				_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
				_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS nf_pid, nf_pid_end")
				_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
			}
			cleanup()
			t.Cleanup(cleanup)
			mustExec := func(q string) {
				t.Helper()
				if _, err := setup.Exec(ctx, q); err != nil {
					t.Fatalf("exec %q: %v", q, err)
				}
			}
			// id2 is the partition key too, so a unique index may leave id out.
			mustExec("CREATE TABLE nf_pid (id int, id2 int NOT NULL, u int NOT NULL, v text, PRIMARY KEY (id, id2)) PARTITION BY RANGE (id2)")
			mustExec("CREATE TABLE nf_pid_1 PARTITION OF nf_pid FOR VALUES FROM (0) TO (1000)")
			mustExec("CREATE UNIQUE INDEX nf_pid_1_uk ON nf_pid_1 " + tc.index)
			mustExec("ALTER TABLE nf_pid_1 REPLICA IDENTITY USING INDEX nf_pid_1_uk")
			mustExec("ALTER TABLE nf_pid REPLICA IDENTITY " + tc.root)
			mustExec("CREATE TABLE nf_pid_end (id int PRIMARY KEY)")
			mustExec("ALTER TABLE nf_pid_end REPLICA IDENTITY FULL")
			mustExec("CREATE PUBLICATION " + pub + " FOR TABLE nf_pid, nf_pid_end WITH (publish_via_partition_root = true)")

			ids, err := pgcapture.QueryTableIdentities(ctx, setup, pub)
			if err != nil || len(ids) != 1 {
				t.Fatalf("QueryTableIdentities: %+v, %v", ids, err)
			}
			wantMiss := []string{}
			if tc.skipped {
				wantMiss = []string{`"public"."nf_pid_1"`}
			}
			if !slices.Equal(ids[0].PartitionsMissKey, wantMiss) || ids[0].Captured() == tc.skipped {
				t.Errorf("the catalog read: %+v, want partitions without the key %v", ids[0], wantMiss)
			}
			// The health panel's two lists, from the same read.
			undo, not := pgcapture.HealthTableLists(ids, nil, event.Filters{})
			t.Logf("health lists: without undo %q, not captured %q", undo, not)
			if len(undo) != 0 || len(not) != 1 || !strings.HasPrefix(not[0], "public.nf_pid ") {
				t.Errorf("health lists: without undo %q, not captured %q; skipped=%v", undo, not, tc.skipped)
			}

			r := startNofull(t, ctx, setup, baseDSN, slot, pub)
			mustExec("INSERT INTO nf_pid VALUES (1, 1, 10, 'a')")
			mustExec("UPDATE nf_pid SET id = 2 WHERE id = 1") // the primary key
			mustExec("DELETE FROM nf_pid WHERE id = 2")
			mustExec("INSERT INTO nf_pid_end VALUES (1)")
			evs := r.until(func(ev event.Event) bool { return ev.Table == "nf_pid_end" })
			r.stop()
			got := of(evs, "nf_pid")
			const reason = pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey
			if tc.skipped {
				// What capture says must be what the health lists say: nothing
				// of the table is stored, least of all under the new key.
				if len(got) != 0 || r.skips.Count(reason) != 3 || r.skips.Total() != 3 {
					for _, ev := range got {
						t.Logf("stored: %v pk=%q before=%v after=%v", ev.EventType, ev.PKValues, ev.RowBefore, ev.RowAfter)
					}
					t.Fatalf("the table must be skipped: %d events, %d skips under %q of %d\nlog:\n%s", len(got), r.skips.Count(reason), reason, r.skips.Total(), r.logText())
				}
				assertLedgerNames(t, r.skips, reason, "public.nf_pid")
				return
			}
			if rowKinds(got) != "IUD" || r.skips.Total() != 0 {
				t.Fatalf("events %q (want IUD), skips %d\nlog:\n%s", rowKinds(got), r.skips.Total(), r.logText())
			}
			cols := []string{"id", "id2", "u", "v"}
			assertPartialBefore(t, got[1], cols, map[string]string{"id": "1", "id2": "1"})
			if got[1].PKValues != "1|1" || got[1].RowAfter["id"] != "2" {
				t.Errorf("key-changing UPDATE: pk=%q after=%v", got[1].PKValues, got[1].RowAfter)
			}
			assertPartialBefore(t, got[2], cols, map[string]string{"id": "2", "id2": "1"})
		})
	}
}

// A partitioned root with NO primary key, at FULL, published in the stead of
// its partitions: one has a primary key of its own and is at DEFAULT, the
// other is at FULL. PostgreSQL labels the old row of the first as a whole row
// and it holds that partition's key alone: it holds empty columns, so it is
// stored with its key only (here: no column at all, the root's key being
// empty), and NULL is never stored for a value PostgreSQL did not send. The
// rows of the partition at FULL hold a value in every column and stay whole.
func TestCapturer_RootWithoutKey_PartitionWithOwnKey_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_nofull_nokey_it"
	const pub = "bintrail_nofull_nokey_pub"
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS nf_nokey, nf_nokey_end")
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	const bigVal = "(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 100) i)"
	var big string
	if err := setup.QueryRow(ctx, "SELECT "+bigVal).Scan(&big); err != nil {
		t.Fatal(err)
	}
	mustExec("CREATE TABLE nf_nokey (id int NOT NULL, v text, big text) PARTITION BY RANGE (id)")
	mustExec("CREATE TABLE nf_nokey_1 PARTITION OF nf_nokey FOR VALUES FROM (0) TO (1000)")
	mustExec("ALTER TABLE nf_nokey_1 ADD PRIMARY KEY (id)") // its own key, at DEFAULT
	mustExec("CREATE TABLE nf_nokey_2 PARTITION OF nf_nokey FOR VALUES FROM (1000) TO (2000)")
	mustExec("ALTER TABLE nf_nokey_2 REPLICA IDENTITY FULL")
	for _, part := range []string{"nf_nokey_1", "nf_nokey_2"} {
		mustExec("ALTER TABLE " + part + " ALTER COLUMN big SET STORAGE EXTERNAL")
	}
	mustExec("ALTER TABLE nf_nokey REPLICA IDENTITY FULL") // the root only
	mustExec("CREATE TABLE nf_nokey_end (id int PRIMARY KEY)")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE nf_nokey, nf_nokey_end WITH (publish_via_partition_root = true)")

	r := startNofull(t, ctx, setup, baseDSN, slot, pub)
	for _, id := range []string{"1", "1001"} { // one row in each partition
		mustExec("INSERT INTO nf_nokey VALUES (" + id + ", 'a', " + bigVal + ")")
		mustExec("UPDATE nf_nokey SET v = 'b' WHERE id = " + id)
		mustExec("UPDATE nf_nokey SET id = id + 1 WHERE id = " + id)
		mustExec("DELETE FROM nf_nokey WHERE id = " + id + " + 1")
	}
	mustExec("INSERT INTO nf_nokey_end VALUES (1)")
	evs := r.until(func(ev event.Event) bool { return ev.Table == "nf_nokey_end" })
	r.stop()
	t.Logf("capture log:\n%s", r.logText())

	got := of(evs, "nf_nokey")
	if rowKinds(got) != "IUUDIUUD" || r.skips.Total() != 0 {
		t.Fatalf("events %q (want IUUDIUUD), skips %d", rowKinds(got), r.skips.Total())
	}
	cols := []string{"id", "v", "big"}
	for i, ev := range got {
		if ev.EventType == event.EventInsert {
			continue
		}
		if ev.PKValues != "" {
			t.Errorf("change %d: pk_values %q, want none (the table as published has no primary key)", i, ev.PKValues)
		}
		if i < 4 {
			// The partition at DEFAULT with a key of its own.
			for c, v := range ev.RowBefore {
				if v == nil {
					t.Errorf("change %d (%v): before[%s] is NULL: a value PostgreSQL did not send was stored as NULL", i, ev.EventType, c)
				}
			}
			assertPartialBefore(t, ev, cols, map[string]string{})
			if ev.EventType == event.EventUpdate && !event.IsUnchangedToastMarker(ev.RowAfter["big"]) {
				t.Errorf("change %d: after[big] = %T, want the unchanged-TOAST marker", i, ev.RowAfter["big"])
			}
			continue
		}
		// The partition at FULL: whole, and undoable.
		if nc := event.NotCapturedColumns(ev.RowBefore); len(nc) > 0 || ev.RowBefore["big"] != big {
			t.Errorf("change %d, partition at FULL: the earlier row must be whole; not captured %v, big real=%v", i, nc, ev.RowBefore["big"] == big)
		}
		if err := event.CheckUnresolvedToast(ev.Schema, ev.Table, ev.PKValues, stored(t, ev.RowBefore), stored(t, ev.RowAfter)); err != nil {
			t.Errorf("change %d, partition at FULL: it can be undone, got %v", i, err)
		}
	}
}

// The identity of a partition is changed between a change being written and
// capture reading it: a DELETE made while the partition's identity was an
// index wider than the primary key, the partition put at FULL (what the
// doctor asks for), and only then capture started again. PostgreSQL sends the
// old row labelled whole, holding the index's columns alone; today's catalog
// shows every partition at FULL. The row holds an empty column, so it is
// stored with its key only: never as a whole row with NULL in the columns
// PostgreSQL did not send.
func TestCapturer_PartitionIdentityChangedBeforeCaptureRead_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_nofull_stale_it"
	const pub = "bintrail_nofull_stale_pub"
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS nf_stale, nf_stale_end")
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec("CREATE TABLE nf_stale (id int PRIMARY KEY, u int NOT NULL, v text, w text) PARTITION BY RANGE (id)")
	mustExec("CREATE TABLE nf_stale_1 PARTITION OF nf_stale FOR VALUES FROM (0) TO (1000)")
	mustExec("CREATE UNIQUE INDEX nf_stale_1_uk ON nf_stale_1 (id, u)")
	mustExec("ALTER TABLE nf_stale_1 REPLICA IDENTITY USING INDEX nf_stale_1_uk")
	mustExec("ALTER TABLE nf_stale REPLICA IDENTITY FULL")
	mustExec("CREATE TABLE nf_stale_end (id int PRIMARY KEY)")
	mustExec("ALTER TABLE nf_stale_end REPLICA IDENTITY FULL")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE nf_stale, nf_stale_end WITH (publish_via_partition_root = true)")

	// A first run, so that the slot exists and holds the changes that follow.
	r := startNofull(t, ctx, setup, baseDSN, slot, pub)
	mustExec("INSERT INTO nf_stale VALUES (1, 10, 'a', 'w'), (2, 20, 'a', 'w'), (3, 30, 'a', 'w')")
	mustExec("INSERT INTO nf_stale_end VALUES (1)")
	r.until(func(ev event.Event) bool { return ev.Table == "nf_stale_end" })
	r.stop()

	// Capture is not running. Changes under the wide identity ...
	mustExec("DELETE FROM nf_stale WHERE id = 1")
	mustExec("UPDATE nf_stale SET u = 21 WHERE id = 2")
	// ... then the partition is put at FULL, and a change is made under that.
	mustExec("ALTER TABLE nf_stale_1 REPLICA IDENTITY FULL")
	mustExec("DELETE FROM nf_stale WHERE id = 3")
	mustExec("INSERT INTO nf_stale_end VALUES (2)")

	r = startNofull(t, ctx, setup, baseDSN, slot, pub)
	evs := r.until(func(ev event.Event) bool { return ev.Table == "nf_stale_end" && ev.PKValues == "2" })
	r.stop()
	t.Logf("capture log:\n%s", r.logText())
	var got []event.Event
	for _, ev := range of(evs, "nf_stale") {
		if ev.EventType != event.EventInsert {
			got = append(got, ev)
		}
	}
	if rowKinds(got) != "DUD" || r.skips.Total() != 0 {
		t.Fatalf("events %q (want DUD), skips %d", rowKinds(got), r.skips.Total())
	}
	cols := []string{"id", "u", "v", "w"}
	for i, ev := range got[:2] {
		t.Logf("change %d made under the wide identity: before=%v", i, ev.RowBefore)
		for c, v := range ev.RowBefore {
			if v == nil {
				t.Errorf("change %d: before[%s] is NULL: a value PostgreSQL did not send was stored as NULL", i, c)
			}
		}
		assertPartialBefore(t, ev, cols, map[string]string{"id": fmt.Sprint(i + 1)})
	}
	// The change made at FULL is whole.
	if nc := event.NotCapturedColumns(got[2].RowBefore); len(nc) > 0 || got[2].RowBefore["v"] != "a" || got[2].RowBefore["u"] != "30" {
		t.Errorf("the change made after the partition was put at FULL must be whole, got %v", got[2].RowBefore)
	}
}

// A partitioned table whose ROOT is at REPLICA IDENTITY NOTHING, published
// through that root, its partition at DEFAULT. PostgreSQL checks the replica
// identity in the partition, so UPDATE and DELETE are accepted, and the old
// row it sends holds the primary key. The root's description flags no
// identity column; capture takes the primary key as the identity and stores
// the changes under it. Without that every one of them was dropped, with a
// warning each.
func TestCapturer_RootAtNothing_Partitioned_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_nofull_rootnothing_it"
	const pub = "bintrail_nofull_rootnothing_pub"
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS nf_rootn, nf_rootn_end")
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec("CREATE TABLE nf_rootn (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)")
	mustExec("CREATE TABLE nf_rootn_1 PARTITION OF nf_rootn FOR VALUES FROM (0) TO (1000)")
	mustExec("ALTER TABLE nf_rootn REPLICA IDENTITY NOTHING") // the root only
	mustExec("CREATE TABLE nf_rootn_end (id int PRIMARY KEY)")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE nf_rootn, nf_rootn_end WITH (publish_via_partition_root = true)")

	r := startNofull(t, ctx, setup, baseDSN, slot, pub)
	mustExec("INSERT INTO nf_rootn VALUES (1, 'a')")
	mustExec("UPDATE nf_rootn SET v = 'b' WHERE id = 1")
	mustExec("UPDATE nf_rootn SET id = 2 WHERE id = 1")
	mustExec("DELETE FROM nf_rootn WHERE id = 2")
	mustExec("INSERT INTO nf_rootn_end VALUES (1)")
	evs := r.until(func(ev event.Event) bool { return ev.Table == "nf_rootn_end" })
	r.stop()
	t.Logf("capture log:\n%s", r.logText())
	got := of(evs, "nf_rootn")
	if rowKinds(got) != "IUUD" || r.skips.Total() != 0 {
		t.Fatalf("events %q (want IUUD), skips %d", rowKinds(got), r.skips.Total())
	}
	cols := []string{"id", "v"}
	assertPartialBefore(t, got[1], cols, map[string]string{"id": "1"})
	assertPartialBefore(t, got[2], cols, map[string]string{"id": "1"})
	if got[2].PKValues != "1" || got[2].RowAfter["id"] != "2" {
		t.Errorf("key-changing UPDATE: pk=%q after=%v", got[2].PKValues, got[2].RowAfter)
	}
	assertPartialBefore(t, got[3], cols, map[string]string{"id": "2"})
	if logs := r.logText(); !strings.Contains(logs, "partitioned table whose root has no replica identity") || strings.Contains(logs, "a change arrived without a value for a primary-key column") {
		t.Errorf("the log must say what PostgreSQL does for such a root, and warn of no dropped change")
	}
}

// No regression: a partitioned root at FULL with a primary key, every
// partition at FULL, published through the root. Every earlier row is whole,
// an unsent large value is taken from it, and each change can be undone.
func TestCapturer_PartitionedAllAtFull_WholeAndUndoable_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_nofull_allfull_it"
	const pub = "bintrail_nofull_allfull_pub"
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS nf_allfull, nf_allfull_end")
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	const bigVal = "(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 100) i)"
	var big string
	if err := setup.QueryRow(ctx, "SELECT "+bigVal).Scan(&big); err != nil {
		t.Fatal(err)
	}
	mustExec("CREATE TABLE nf_allfull (id int PRIMARY KEY, v text, big text) PARTITION BY RANGE (id)")
	mustExec("CREATE TABLE nf_allfull_1 PARTITION OF nf_allfull FOR VALUES FROM (0) TO (1000)")
	mustExec("CREATE TABLE nf_allfull_2 PARTITION OF nf_allfull FOR VALUES FROM (1000) TO (2000)")
	for _, tb := range []string{"nf_allfull", "nf_allfull_1", "nf_allfull_2"} {
		mustExec("ALTER TABLE " + tb + " REPLICA IDENTITY FULL")
	}
	for _, part := range []string{"nf_allfull_1", "nf_allfull_2"} {
		mustExec("ALTER TABLE " + part + " ALTER COLUMN big SET STORAGE EXTERNAL")
	}
	mustExec("CREATE TABLE nf_allfull_end (id int PRIMARY KEY)")
	mustExec("ALTER TABLE nf_allfull_end REPLICA IDENTITY FULL")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE nf_allfull, nf_allfull_end WITH (publish_via_partition_root = true)")

	r := startNofull(t, ctx, setup, baseDSN, slot, pub)
	for _, id := range []string{"1", "1001"} {
		mustExec("INSERT INTO nf_allfull VALUES (" + id + ", 'a', " + bigVal + ")")
		mustExec("UPDATE nf_allfull SET v = 'b' WHERE id = " + id)
		mustExec("UPDATE nf_allfull SET id = id + 1 WHERE id = " + id)
		mustExec("DELETE FROM nf_allfull WHERE id = " + id + " + 1")
	}
	mustExec("INSERT INTO nf_allfull_end VALUES (1)")
	evs := r.until(func(ev event.Event) bool { return ev.Table == "nf_allfull_end" })
	r.stop()
	got := of(evs, "nf_allfull")
	if rowKinds(got) != "IUUDIUUD" || r.skips.Total() != 0 {
		t.Fatalf("events %q (want IUUDIUUD), skips %d\nlog:\n%s", rowKinds(got), r.skips.Total(), r.logText())
	}
	for i, ev := range got {
		if ev.EventType == event.EventInsert {
			continue
		}
		if cols := event.NotCapturedColumns(ev.RowBefore); len(cols) > 0 || ev.RowBefore["big"] != big || ev.RowBefore["v"] == nil {
			t.Errorf("change %d: the earlier row must be whole; not captured %v, big real=%v, v=%v", i, cols, ev.RowBefore["big"] == big, ev.RowBefore["v"])
		}
		if ev.RowAfter != nil && ev.RowAfter["big"] != big {
			t.Errorf("change %d: after[big] = %T, want the value taken from the whole earlier row", i, ev.RowAfter["big"])
		}
		if err := event.CheckUnresolvedToast(ev.Schema, ev.Table, ev.PKValues, stored(t, ev.RowBefore), stored(t, ev.RowAfter)); err != nil {
			t.Errorf("change %d: it can be undone, got %v", i, err)
		}
	}
	if logs := r.logText(); strings.Contains(logs, "without undo") || strings.Contains(logs, "key only") {
		t.Errorf("nothing is to be said of a table whose partitions are all at FULL:\n%s", logs)
	}
}

// A table whose primary key is DEFERRABLE when capture starts: capture starts
// all the same, says so once, skips that table's changes under
// primary_key_deferrable, and captures the other tables. Refusing the start
// kept every table uncaptured, at every automatic restart.
func TestCapturer_DeferrableKeyAtStart_TableSkippedOthersCaptured(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_defstart_it"
	const pub = "bintrail_defstart_it_pub"
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS defstart_a, defstart_t")
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec("CREATE TABLE defstart_a (id int PRIMARY KEY, v text)")
	mustExec("CREATE TABLE defstart_t (id int, v text, CONSTRAINT defstart_t_pkey PRIMARY KEY (id) DEFERRABLE)")
	mustExec("ALTER TABLE defstart_t REPLICA IDENTITY FULL")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE defstart_a, defstart_t")

	// What the health panel is given: the table is not captured, and is not
	// listed as recorded.
	ids, err := pgcapture.QueryTableIdentities(ctx, setup, pub)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := pgcapture.QueryDeferrableKeys(ctx, setup, pub)
	if err != nil {
		t.Fatal(err)
	}
	undo, not := pgcapture.HealthTableLists(ids, keys, event.Filters{})
	if len(undo) != 0 || !slices.Equal(not, []string{"public.defstart_a (not at REPLICA IDENTITY FULL)", "public.defstart_t (primary key DEFERRABLE)"}) {
		t.Errorf("health lists: without undo %q, not captured %q", undo, not)
	}

	for run := 1; run <= 2; run++ { // a restart is not refused either
		r := startNofull(t, ctx, setup, baseDSN, slot, pub)
		mustExec(fmt.Sprintf("INSERT INTO defstart_t VALUES (%d, 'skipped')", run))
		mustExec(fmt.Sprintf("INSERT INTO defstart_a VALUES (%d, 'captured')", run))
		evs := r.until(func(ev event.Event) bool { return ev.Table == "defstart_a" && ev.PKValues == fmt.Sprint(run) })
		r.stop()
		if got := of(evs, "defstart_t"); len(got) != 0 {
			t.Errorf("run %d: changes of the table with a deferrable key were captured: %d", run, len(got))
		}
		const reason = pgcapture.SkipDeferrablePrimaryKey
		if r.skips.Count(reason) < 1 || r.skips.Total() != r.skips.Count(reason) {
			t.Errorf("run %d: want the change recorded under %q and nothing else, got %d of %d", run, reason, r.skips.Count(reason), r.skips.Total())
		}
		assertLedgerNames(t, r.skips, reason, "public.defstart_t")
		logs := r.logText()
		if run == 1 {
			t.Logf("startup log:\n%s", logs)
		}
		for _, want := range []string{"1 table of the publication has a primary key declared DEFERRABLE", "defstart_t", "the other tables are captured normally"} {
			if !strings.Contains(logs, want) {
				t.Errorf("run %d: the startup warning lacks %q", run, want)
			}
		}
	}
}

// TestCapturer_DeferrableKeyMidStream_Integration is what a live PostgreSQL
// does when a primary key becomes DEFERRABLE while capture runs, and what the
// check at each description of a relation makes of it.
//
// Observed on PostgreSQL 16 (the log of this test prints each fact):
//   - ALTER TABLE ... ALTER CONSTRAINT <primary key> DEFERRABLE is refused:
//     only a foreign key can be altered that way. A primary key becomes
//     deferrable by being dropped and added again.
//   - That DROP + ADD makes PostgreSQL describe the table again before its
//     next change, in the same streaming session, so the check sees it: the
//     table's changes from there on are skipped, the other tables go on.
//   - Rebuilt as not deferrable, the table is described again and captured.
func TestCapturer_DeferrableKeyMidStream_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_defmid_it"
	const pub = "bintrail_defmid_it_pub"
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS defmid_a, defmid_t, defmid_d")
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec("CREATE TABLE defmid_a (id int PRIMARY KEY, v text)")
	mustExec("CREATE TABLE defmid_t (id int, v text, CONSTRAINT defmid_t_pkey PRIMARY KEY (id))")
	mustExec("ALTER TABLE defmid_t REPLICA IDENTITY FULL")
	mustExec("CREATE TABLE defmid_d (id int, v text, CONSTRAINT defmid_d_pkey PRIMARY KEY (id))") // DEFAULT
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE defmid_a, defmid_t, defmid_d")

	// Fact 1: a primary key cannot be made deferrable in place.
	_, alterErr := setup.Exec(ctx, "ALTER TABLE defmid_t ALTER CONSTRAINT defmid_t_pkey DEFERRABLE")
	t.Logf("ALTER TABLE ... ALTER CONSTRAINT <primary key> DEFERRABLE -> %v", alterErr)
	if alterErr == nil {
		t.Fatal("PostgreSQL accepted ALTER CONSTRAINT on a primary key: the docs of this check must be reread")
	}

	r := startNofull(t, ctx, setup, baseDSN, slot, pub)
	const rebuildDeferrable = "ALTER TABLE %s DROP CONSTRAINT %s_pkey, ADD CONSTRAINT %s_pkey PRIMARY KEY (id) DEFERRABLE"
	const rebuildPlain = "ALTER TABLE %s DROP CONSTRAINT %s_pkey, ADD CONSTRAINT %s_pkey PRIMARY KEY (id)"
	rebuild := func(stmt, tb string) { mustExec(fmt.Sprintf(stmt, tb, tb, tb)) }

	// The check reads the catalog as it is when capture READS a change, so
	// each step waits for capture to have read the one before it. (Without the
	// wait, a change made before the key became deferrable and read after it is
	// skipped as well: the safe direction. The other direction is the check's
	// limit: a key that was deferrable and is no longer when capture reads the
	// changes made meanwhile is not seen.)
	mustExec("INSERT INTO defmid_t VALUES (1, 'key not deferrable')")
	mustExec("INSERT INTO defmid_d VALUES (1, 'key not deferrable')")
	evs := r.until(func(ev event.Event) bool { return ev.Table == "defmid_d" && ev.PKValues == "1" })
	rebuild(rebuildDeferrable, "defmid_t")
	rebuild(rebuildDeferrable, "defmid_d")
	mustExec("INSERT INTO defmid_t VALUES (2, 'key deferrable')")
	mustExec("UPDATE defmid_t SET v = 'key deferrable, updated' WHERE id = 2")
	mustExec("INSERT INTO defmid_d VALUES (2, 'key deferrable')")
	// Fact 2: at DEFAULT a deferrable key is no replica identity at all, so
	// PostgreSQL refuses the UPDATE itself.
	_, updErr := setup.Exec(ctx, "UPDATE defmid_d SET v = 'x' WHERE id = 2")
	t.Logf("UPDATE of a published table at REPLICA IDENTITY DEFAULT whose key is deferrable -> %v", updErr)
	if !refusedNoIdentity(updErr) {
		t.Errorf("want PostgreSQL's own refusal (55000, no replica identity), got %v", updErr)
	}
	mustExec("INSERT INTO defmid_a VALUES (1, 'the other table goes on')")
	evs = append(evs, r.until(func(ev event.Event) bool { return ev.Table == "defmid_a" && ev.PKValues == "1" })...)
	rebuild(rebuildPlain, "defmid_t")
	rebuild(rebuildPlain, "defmid_d")
	mustExec("INSERT INTO defmid_t VALUES (3, 'key not deferrable again')")
	mustExec("INSERT INTO defmid_d VALUES (3, 'key not deferrable again')")
	mustExec("INSERT INTO defmid_a VALUES (2, 'end')")

	evs = append(evs, r.until(func(ev event.Event) bool { return ev.Table == "defmid_a" && ev.PKValues == "2" })...)
	r.stop()
	logs := r.logText()
	t.Logf("capture log:\n%s", logs)

	keysOf := func(tb string) []string {
		var out []string
		for _, ev := range of(evs, tb) {
			if isRow(ev) {
				out = append(out, key(ev.Table, ev.EventType, ev.PKValues))
			}
		}
		return out
	}
	// Fact 3: the changes made while the key was deferrable are the ones
	// missing, which shows PostgreSQL described the table again after each
	// rebuild, inside one streaming session.
	for _, tb := range []string{"defmid_t", "defmid_d"} {
		want := []string{key(tb, event.EventInsert, "1"), key(tb, event.EventInsert, "3")}
		if got := keysOf(tb); !slices.Equal(got, want) {
			t.Errorf("%s: captured %v, want %v (the change made while the key was deferrable dropped, and only it)", tb, got, want)
		}
	}
	if got := keysOf("defmid_a"); len(got) != 2 {
		t.Errorf("defmid_a must keep flowing: %v", got)
	}
	const reason = pgcapture.SkipDeferrablePrimaryKey
	if n := r.skips.Count(reason); n != 3 || r.skips.Total() != 3 {
		t.Errorf("want 3 skips under %q (INSERT and UPDATE of defmid_t, INSERT of defmid_d), got %d of %d", reason, n, r.skips.Total())
	}
	assertLedgerNames(t, r.skips, reason, "public.defmid_t", "public.defmid_d") // in the order first skipped
	if n := strings.Count(logs, "primary key declared DEFERRABLE"); n != 2 {
		t.Errorf("want one warning per table described with a deferrable key, got %d", n)
	}
}
