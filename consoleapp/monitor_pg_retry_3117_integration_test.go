//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// pgOutageProxy is a TCP relay in front of a real PostgreSQL. down closes the
// listener and every open connection, so the source refuses connections as a
// stopped server does; up listens again on the same port.
type pgOutageProxy struct {
	target string
	addr   string

	mu    sync.Mutex
	ln    net.Listener
	conns map[net.Conn]struct{}
}

func newPGOutageProxy(t *testing.T, target string) *pgOutageProxy {
	t.Helper()
	p := &pgOutageProxy{target: target, conns: map[net.Conn]struct{}{}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.addr = ln.Addr().String()
	p.serve(ln)
	t.Cleanup(p.down)
	return p
}

func (p *pgOutageProxy) serve(ln net.Listener) {
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.relay(c)
		}
	}()
}

func (p *pgOutageProxy) relay(c net.Conn) {
	u, err := net.Dial("tcp", p.target)
	if err != nil {
		c.Close()
		return
	}
	p.mu.Lock()
	if p.ln == nil { // went down while this one was being accepted
		p.mu.Unlock()
		c.Close()
		u.Close()
		return
	}
	p.conns[c], p.conns[u] = struct{}{}, struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(u, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, u); done <- struct{}{} }()
	<-done
	c.Close()
	u.Close()
	p.mu.Lock()
	delete(p.conns, c)
	delete(p.conns, u)
	p.mu.Unlock()
}

func (p *pgOutageProxy) down() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		p.ln.Close()
		p.ln = nil
	}
	for c := range p.conns {
		c.Close()
	}
}

func (p *pgOutageProxy) up(t *testing.T) {
	t.Helper()
	var ln net.Listener
	var err error
	// The port was just released; the kernel can take a moment to give it back.
	for range 100 {
		if ln, err = net.Listen("tcp", p.addr); err == nil {
			p.serve(ln)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("could not listen again on %s: %v", p.addr, err)
}

// proxiedPGDSN is the DSN of cfg with its address replaced by addr.
func proxiedPGDSN(cfg *pgx.ConnConfig, addr string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     addr,
		Path:     "/" + cfg.Database,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

// TestIntegrationPGSourceOutageOutlastsGiveUp_3117 stops a real PostgreSQL
// source (its address starts refusing connections) for longer than the
// supervisor's give-up threshold. Capture must still be retrying, must say so,
// and when the source is back must index what changed meanwhile, once.
func TestIntegrationPGSourceOutageOutlastsGiveUp_3117(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	shrinkMonitorBackoff(t)
	monitorBackoffBase = 20 * time.Millisecond
	monitorBackoffCap = 100 * time.Millisecond
	monitorHealthyReset = time.Hour
	monitorGiveUpAfter = 400 * time.Millisecond

	const (
		slot = "bintrail_outage_3117"
		pub  = "bintrail_outage_3117_pub"
		tbl  = "outage_3117_t"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", slot)
	}
	dropAll()
	t.Cleanup(dropAll)
	for _, q := range []string{
		"CREATE TABLE " + tbl + " (id int PRIMARY KEY, v text)",
		"ALTER TABLE " + tbl + " REPLICA IDENTITY FULL",
		"CREATE PUBLICATION " + pub + " FOR TABLE " + tbl,
	} {
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	insert := func(id int) {
		t.Helper()
		if _, err := pg.Exec(ctx, "INSERT INTO "+tbl+" VALUES ($1, 'x')", id); err != nil {
			t.Fatal(err)
		}
	}
	count := func(id int) int {
		var n int
		_ = indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values = ?", tbl, fmt.Sprint(id)).Scan(&n)
		return n
	}
	waitUntil := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", what)
	}

	parsed, err := pgx.ParseConfig(pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	proxy := newPGOutageProxy(t, net.JoinHostPort(parsed.Host, fmt.Sprint(parsed.Port)))
	proxied := proxiedPGDSN(parsed, proxy.addr)

	m := &monitorSupervisor{baseCtx: ctx, jobs: map[string]*monitorJob{}}
	jobCtx, jobCancel := context.WithCancel(ctx)
	defer jobCancel()
	job := &monitorJob{cancel: jobCancel, done: make(chan struct{})}
	job.set("pending", "")
	cfg := pgstreamrun.Config{
		IndexDSN:    indexDSN,
		ReplDSN:     proxied + "&replication=database",
		QueryDSN:    proxied,
		SlotName:    slot,
		Publication: pub,
		ServerID:    3117,
		Tables:      "public." + tbl,
		BatchSize:   100,
		Checkpoint:  100 * time.Millisecond,
		Hooks:       job.pgStreamHooks(),
	}
	e := console.ServerEntry{ID: "e3117", Name: "pg-outage", SourceDSN: proxied, DSN: indexDSN}
	runOnce := pgRunThroughOutages(job, e, func(c context.Context) error { return pgstreamrun.One(c, cfg) })
	m.wg.Add(1)
	go m.run(jobCtx, job, e, console.FlavorPostgres, runOnce)

	waitUntil("replication to start", func() bool {
		var active bool
		_ = pg.QueryRow(ctx, "SELECT active AND confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&active)
		return active
	})
	insert(1)
	waitUntil("the row before the outage to be indexed", func() bool { return count(1) == 1 })
	waitUntil("capture to report running", func() bool { return job.snapshot().State == "running" })

	proxy.down()
	waitUntil("the outage to be reported", func() bool {
		st := job.snapshot()
		return st.State == "failed" && st.Retrying
	})
	insert(2) // committed while capture cannot reach the source

	// Well past the give-up threshold, counted from the first failure.
	time.Sleep(4 * monitorGiveUpAfter)
	select {
	case <-job.done:
		st := job.snapshot()
		t.Fatalf("the supervisor gave up on a source that is only unreachable: state=%q retrying=%v error=%q", st.State, st.Retrying, st.LastError)
	default:
	}
	var seenRetrying bool
	var last console.MonitorStatus
	for range 200 { // a reading can land between two attempts, in "pending"
		last = job.snapshot()
		if last.State == "failed" {
			seenRetrying = last.Retrying
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !seenRetrying {
		t.Fatalf("during the outage the state is %q retrying=%v; want failed and retrying", last.State, last.Retrying)
	}
	if !strings.Contains(last.LastError, "cannot be reached") {
		t.Errorf("the state does not say the source cannot be reached: %q", last.LastError)
	}
	if got := readCapture(last, time.Now()); got != captureRetrying {
		t.Errorf("capture_stopped reads this state as %v; want captureRetrying, so the alert fires", got)
	}

	proxy.up(t)
	waitUntil("the row committed during the outage to be indexed", func() bool { return count(2) >= 1 })
	insert(3)
	waitUntil("a row after the outage to be indexed", func() bool { return count(3) >= 1 })
	waitUntil("capture to report running again", func() bool { return job.snapshot().State == "running" })
	for id := 1; id <= 3; id++ {
		if n := count(id); n != 1 {
			t.Errorf("row %d is in the index %d times; want exactly once", id, n)
		}
	}

	jobCancel()
	select {
	case <-job.done:
	case <-time.After(15 * time.Second):
		t.Fatal("the job did not stop on cancel")
	}
}
