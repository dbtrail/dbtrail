//go:build integration

package pgcapture

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

type slot3178 struct {
	dsn, slot, pub, tbl string
	conn, hold          *pgx.Conn
	holdPID             uint32
}

// newSlot3178 prepares a published table and leaves a transaction that has
// written open: PostgreSQL cannot end a slot's creation until it ends.
func newSlot3178(t *testing.T, name string) *slot3178 {
	t.Helper()
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	s := &slot3178{dsn: dsn, slot: "bintrail_3178_" + name, pub: "bintrail_3178_pub_" + name, tbl: "t3178_" + name}
	var err error
	if s.conn, err = pgx.Connect(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.conn.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		for range 100 {
			_, _ = s.conn.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", s.slot)
			var n int
			if err := s.conn.QueryRow(bg, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", s.slot).Scan(&n); err != nil || n == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = s.conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+s.pub)
		_, _ = s.conn.Exec(bg, "DROP TABLE IF EXISTS "+s.tbl)
	}
	drop()
	t.Cleanup(drop)
	for _, q := range []string{
		"CREATE TABLE " + s.tbl + " (id int PRIMARY KEY, v text)",
		"ALTER TABLE " + s.tbl + " REPLICA IDENTITY FULL",
		"CREATE PUBLICATION " + s.pub + " FOR TABLE " + s.tbl,
	} {
		if _, err := s.conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if s.hold, err = pgx.Connect(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.hold.Close(context.Background()) })
	s.holdPID = s.hold.PgConn().PID()
	if _, err := s.hold.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.hold.Exec(ctx, "INSERT INTO "+s.tbl+" VALUES (1, 'held')"); err != nil {
		t.Fatal(err)
	}
	return s
}

func (s *slot3178) capturer(onPhase, onDetail func(string), started func()) *Capturer {
	sep := "?"
	if strings.Contains(s.dsn, "?") {
		sep = "&"
	}
	return New(Config{
		ReplDSN: s.dsn + sep + "replication=database", QueryDSN: s.dsn,
		SlotName: s.slot, Publication: s.pub,
		OnPhase: onPhase, OnPhaseDetail: onDetail, OnStarted: started,
		StandbyInterval: 200 * time.Millisecond,
	})
}

func shrinkSlotWait(t *testing.T, budget time.Duration) {
	t.Helper()
	oldBudget, oldFirst, oldEvery, oldNotice := startupTimeout, slotBlockerFirstLook, slotBlockerEvery, slotNoticeEvery
	t.Cleanup(func() {
		startupTimeout, slotBlockerFirstLook, slotBlockerEvery, slotNoticeEvery = oldBudget, oldFirst, oldEvery, oldNotice
	})
	startupTimeout, slotBlockerFirstLook, slotBlockerEvery, slotNoticeEvery = budget, 100*time.Millisecond, 200*time.Millisecond, 100*time.Millisecond
}

// A source with a write transaction open for longer than the startup budget:
// capture waits, says which transaction, and starts when it ends, with the
// changes committed after that delivered.
func TestIntegrationCaptureSlotCreationOutlastsStartupBudget_3178(t *testing.T) {
	shrinkSlotWait(t, time.Second)
	s := newSlot3178(t, "wait")

	var mu sync.Mutex
	var phases, details []string
	started := make(chan struct{})
	c := s.capturer(
		func(p string) { mu.Lock(); phases = append(phases, p); mu.Unlock() },
		func(d string) { mu.Lock(); details = append(details, d); mu.Unlock() },
		func() { close(started) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan event.Event, 16)
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx, out) }()

	// Three times the budget.
	select {
	case err := <-runErr:
		t.Fatalf("Run ended while the slot was being created: %v", err)
	case <-started:
		t.Fatal("replication started with a transaction still open")
	case <-time.After(3 * time.Second):
	}
	want := fmt.Sprintf("session %d", s.holdPID)
	mu.Lock()
	gotPhases, gotDetails := append([]string(nil), phases...), append([]string(nil), details...)
	mu.Unlock()
	if len(gotPhases) != 1 || gotPhases[0] != PhaseCreatingSlot {
		t.Fatalf("phases while waiting: %q", gotPhases)
	}
	if len(gotDetails) == 0 || !strings.Contains(gotDetails[len(gotDetails)-1], want) {
		t.Fatalf("details while waiting: %q; want them to name %s", gotDetails, want)
	}
	t.Logf("status detail: %s", gotDetails[len(gotDetails)-1])

	if _, err := s.hold.Exec(context.Background(), "COMMIT"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case err := <-runErr:
		t.Fatalf("Run ended: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("capture did not start after the transaction ended")
	}
	mu.Lock()
	gotPhases = append([]string(nil), phases...)
	mu.Unlock()
	if len(gotPhases) != 2 || gotPhases[1] != "" {
		t.Fatalf("phases after the creation: %q; want it cleared", gotPhases)
	}
	if _, err := s.conn.Exec(context.Background(), "INSERT INTO "+s.tbl+" VALUES (2, 'after')"); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(20 * time.Second)
	for {
		select {
		case ev := <-out:
			if ev.EventType == event.EventInsert && ev.Table == s.tbl {
				cancel()
				if err := <-runErr; err != nil {
					t.Fatalf("Run: %v", err)
				}
				return
			}
		case err := <-runErr:
			t.Fatalf("Run ended: %v", err)
		case <-deadline:
			t.Fatal("the change committed after the slot was created was not delivered")
		}
	}
}

// Stop during the wait: Run returns at once and no half-made slot stays.
func TestIntegrationCaptureSlotCreationStopped_3178(t *testing.T) {
	shrinkSlotWait(t, 500*time.Millisecond)
	s := newSlot3178(t, "stop")
	var phaseMu sync.Mutex
	var last string
	c := s.capturer(func(p string) { phaseMu.Lock(); last = p; phaseMu.Unlock() }, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx, make(chan event.Event, 1)) }()
	time.Sleep(1500 * time.Millisecond)
	cancel()
	select {
	case <-runErr:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop during the slot's creation")
	}
	phaseMu.Lock()
	defer phaseMu.Unlock()
	if last != "" {
		t.Errorf("the phase stayed %q", last)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := s.conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", s.slot).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the unfinished slot stayed on the source")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// The blocker query against the real catalogs, a prepared transaction included
// when the server allows them.
func TestIntegrationReadSlotBlocker_3178(t *testing.T) {
	s := newSlot3178(t, "read")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, found, err := readSlotBlocker(ctx, s.conn, 0)
	if err != nil || !found {
		t.Fatalf("found %v, %v", found, err)
	}
	if b.pid != int32(s.holdPID) || b.age < 0 || b.user == "" || b.database == "" {
		t.Fatalf("blocker %+v; want session %d with its user, database and age", b, s.holdPID)
	}
	// A read with no write holds nothing, and is not named.
	if _, err := s.hold.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.hold.Exec(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.hold.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if b, found, err := readSlotBlocker(ctx, s.conn, 0); err != nil || (found && b.pid == int32(s.holdPID)) {
		t.Fatalf("a read-only transaction was named: %+v, %v, %v", b, found, err)
	}
	if _, err := s.hold.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// The name of a prepared transaction is chosen by the application and can
// carry data: it reaches neither the status nor the log. Needs a source with
// max_prepared_transactions above 0.
func TestIntegrationSlotWaitDoesNotShowPreparedName_3178(t *testing.T) {
	shrinkSlotWait(t, time.Second)
	s := newSlot3178(t, "gid")
	ctx := context.Background()
	var maxPrepared int
	if err := s.conn.QueryRow(ctx, "SELECT setting::int FROM pg_settings WHERE name = 'max_prepared_transactions'").Scan(&maxPrepared); err != nil {
		t.Fatal(err)
	}
	if maxPrepared == 0 {
		t.Skip("the source runs with max_prepared_transactions = 0")
	}
	const marker = "customer-card-4111111111111111"
	if _, err := s.hold.Exec(ctx, "PREPARE TRANSACTION '"+marker+"'"); err != nil {
		t.Fatal(err)
	}
	finished := false
	finish := func() {
		if !finished {
			finished = true
			_, _ = s.conn.Exec(context.Background(), "ROLLBACK PREPARED '"+marker+"'")
		}
	}
	t.Cleanup(finish)

	var logs lockedBuffer
	var mu sync.Mutex
	var details []string
	c := s.capturer(nil, func(d string) { mu.Lock(); details = append(details, d); mu.Unlock() }, nil)
	c.logger = slog.New(slog.NewTextHandler(&logs, nil))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(runCtx, make(chan event.Event, 16)) }()
	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		n := len(details)
		mu.Unlock()
		if n > 0 && strings.Contains(logs.String(), "still creating") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no detail and log line while waiting; log: %s", logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-runErr
	finish()
	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(details, "\n")
	t.Logf("status detail: %s", joined)
	if !strings.Contains(joined, "a prepared transaction") || !strings.Contains(joined, "pg_prepared_xacts") {
		t.Errorf("the detail does not say it waits for a prepared transaction: %q", joined)
	}
	if strings.Contains(joined, marker) {
		t.Errorf("the prepared transaction's name is in the status: %q", joined)
	}
	if strings.Contains(logs.String(), marker) {
		t.Errorf("the prepared transaction's name is in the log: %s", logs.String())
	}
}

// A transaction of another database of the server: its session number, and
// neither its user nor the name of that database.
func TestIntegrationSlotBlockerInAnotherDatabase_3178(t *testing.T) {
	s := newSlot3178(t, "otherdb")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := s.hold.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	const other = "bintrail_3178_other_db"
	_, _ = s.conn.Exec(ctx, "DROP DATABASE IF EXISTS "+other+" WITH (FORCE)")
	if _, err := s.conn.Exec(ctx, "CREATE DATABASE "+other); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+other+" WITH (FORCE)") })
	parsed, err := pgx.ParseConfig(s.dsn)
	if err != nil {
		t.Fatal(err)
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword(parsed.User, parsed.Password),
		Host: fmt.Sprintf("%s:%d", parsed.Host, parsed.Port), Path: "/" + other, RawQuery: "sslmode=disable"}
	oc, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer oc.Close(context.Background())
	for _, q := range []string{"CREATE TABLE w (id int)", "BEGIN", "INSERT INTO w VALUES (1)"} {
		if _, err := oc.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	b, found, err := readSlotBlocker(ctx, s.conn, 0)
	if err != nil || !found {
		t.Fatalf("found %v, %v", found, err)
	}
	if b.pid != int32(oc.PgConn().PID()) || !b.otherDatabase || b.user != "" || b.database != "" {
		t.Fatalf("blocker %+v; want session %d, marked as another database, with no user or database", b, oc.PgConn().PID())
	}
	got := b.describe()
	t.Log(got)
	if strings.Contains(got, other) || strings.Contains(got, "user ") || !strings.Contains(got, "in another database") {
		t.Errorf("detail %q", got)
	}
}
