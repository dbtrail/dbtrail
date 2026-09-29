package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// ─── built-in rotation job provider (#420) ───────────────────────────────────

func TestActiveJobs(t *testing.T) {
	m := &monitorSupervisor{
		jobs: map[string]*monitorJob{
			"a": {indexDSN: "root@tcp(idx:3306)/bintrail_idx_a"},
			"b": {indexDSN: "root@tcp(idx:3306)/bintrail_idx_b"},
			"c": {indexDSN: ""}, // never published a DSN — skipped
		},
	}
	got := m.ActiveJobs()
	if len(got) != 2 {
		t.Fatalf("ActiveJobs returned %d jobs, want 2: %v", len(got), got)
	}
	seen := map[string]string{}
	for _, j := range got {
		seen[j.EntryID] = j.IndexDSN
	}
	if seen["a"] != "root@tcp(idx:3306)/bintrail_idx_a" || seen["b"] != "root@tcp(idx:3306)/bintrail_idx_b" {
		t.Errorf("ActiveJobs missing expected entry→DSN pairs: %v", got)
	}
	if _, ok := seen["c"]; ok {
		t.Error("ActiveJobs must skip a job with an empty index DSN")
	}
}

// TestSourceStreamConfig pins the registry-entry → supervised-stream config
// fan-out, with the source-TLS wiring (#879) as the load-bearing case: an entry
// with no ssl_* fields keeps the pre-#879 "preferred" default; an entry that
// sets them propagates all four to the stream so the supervised source no
// longer silently connects with an unauthenticated, unoverridable ssl-mode.
func TestSourceStreamConfig(t *testing.T) {
	// Default: no TLS config on the entry → "preferred", empty cert/key paths.
	def := sourceStreamConfig(console.ServerEntry{
		ID: "e1", DSN: "idx-dsn", SourceDSN: "src-dsn", Schemas: "shop",
	}, 42, 0)
	if def.SSLMode != "preferred" {
		t.Errorf("SSLMode = %q, want preferred (unset default)", def.SSLMode)
	}
	if def.SSLCA != "" || def.SSLCert != "" || def.SSLKey != "" {
		t.Errorf("unset TLS paths must stay empty, got CA=%q Cert=%q Key=%q", def.SSLCA, def.SSLCert, def.SSLKey)
	}
	if def.IndexDSN != "idx-dsn" || def.SourceDSN != "src-dsn" || def.MetricsSource != "e1" ||
		def.ServerID != 42 || def.Schemas != "shop" || def.BatchSize != 1000 || def.Checkpoint != 10 || def.GapTimeout != 30 {
		t.Errorf("base fields wrong: %+v", def)
	}
	// No Flavor on the entry declares nothing: the stream asks the server.
	if def.Flavor != "" {
		t.Errorf("Flavor = %q, want empty (detect) for an entry with no flavor", def.Flavor)
	}
	// A stored "mysql" is the form's default, not a declaration: detect too,
	// or a MariaDB added through Connect would refuse right after saving.
	my := sourceStreamConfig(console.ServerEntry{
		ID: "e0", DSN: "idx-dsn", SourceDSN: "src-dsn", Flavor: console.FlavorMySQL,
	}, 42, 0)
	if my.Flavor != "" {
		t.Errorf("Flavor = %q, want empty (detect) for a mysql entry", my.Flavor)
	}
	if def.Deps.ValidateBinlogFormat == nil {
		t.Error("Deps must be wired (streamdeps.Default())")
	}

	// A registry entry's ssl_* fields propagate verbatim (#879).
	got := sourceStreamConfig(console.ServerEntry{
		ID: "e2", DSN: "idx-dsn", SourceDSN: "src-dsn",
		SSLMode: "verify-ca", SSLCA: "/ca.pem", SSLCert: "/cert.pem", SSLKey: "/key.pem",
	}, 7, 0)
	if got.SSLMode != "verify-ca" || got.SSLCA != "/ca.pem" || got.SSLCert != "/cert.pem" || got.SSLKey != "/key.pem" {
		t.Errorf("registry TLS did not propagate to the stream config: %+v", got)
	}

	// Even a saved MariaDB Source type is only a hint: the stream detects.
	maria := sourceStreamConfig(console.ServerEntry{
		ID: "e3", DSN: "idx-dsn", SourceDSN: "src-dsn", Flavor: console.FlavorMariaDB,
	}, 9, 0)
	if maria.Flavor != "" {
		t.Errorf("Flavor = %q, want empty: a saved MariaDB Source type is a hint, the stream detects", maria.Flavor)
	}
}

// TestExplainRegistryFlavorError pins the fix a console-saved server shows when
// the server could not be asked what it is: it has no --source-flavor.
func TestExplainRegistryFlavorError(t *testing.T) {
	_, _, err := metadata.ResolveSourceFlavor("", "", "", errors.New("SELECT VERSION() failed: bad connection"))
	got := explainRegistryFlavorError(err).Error()
	t.Logf("registry undetected: %s", got)
	if strings.Contains(got, "--source-flavor") || !strings.Contains(got, "saved user and password") {
		t.Errorf("registry undetected message: %s", got)
	}
	other := errors.New("boom")
	if explainRegistryFlavorError(other) != other || explainRegistryFlavorError(nil) != nil {
		t.Error("other errors and nil must pass through unchanged")
	}
}

// TestRegistryFlavorHint: the Source type saved with a console server is a
// hint. The stream always detects. The hook saves what the server reported as
// the entry's Source type, so every screen reads it; only when that save
// fails does the server's status carry a warning, which names why.
func TestRegistryFlavorHint(t *testing.T) {
	refused := errors.New("server registry was written by a newer bintrail; upgrade bintrail to edit it")
	cases := []struct {
		hint, detected string
		correctErr     error
		wantWarn       string
	}{
		{console.FlavorMariaDB, console.FlavorMySQL, refused, "saved with Source type MariaDB, but the server reports MySQL"},
		{console.FlavorMySQL, console.FlavorMariaDB, refused, "saved with Source type MySQL, but the server reports MariaDB"},
		{console.FlavorMySQL, console.FlavorMariaDB, nil, ""},
		{console.FlavorMariaDB, console.FlavorMySQL, nil, ""},
		// An old entry saved blank reads as MySQL on every screen: a failed
		// save leaves that label wrong, so it warns too.
		{"", console.FlavorMariaDB, refused, "saved with Source type MySQL, but the server reports MariaDB"},
		{"", console.FlavorMySQL, refused, ""},
		{console.FlavorMariaDB, console.FlavorMariaDB, refused, ""},
		{console.FlavorMySQL, console.FlavorMySQL, nil, ""},
	}
	for _, tc := range cases {
		got := registryFlavorWarning(tc.hint, tc.detected, tc.correctErr)
		if tc.wantWarn == "" && got != "" || tc.wantWarn != "" && !strings.Contains(got, tc.wantWarn) {
			t.Errorf("hint %q detected %q err %v: warning %q, want containing %q", tc.hint, tc.detected, tc.correctErr, got, tc.wantWarn)
		}
		if tc.wantWarn != "" && !strings.Contains(got, "upgrade bintrail to edit it") {
			t.Errorf("warning %q does not say why the Source type was not changed", got)
		}
		if strings.Contains(got, "remove the server") {
			t.Errorf("warning %q advises removing the server, which leaves its history behind", got)
		}
		if strings.ContainsRune(got, '\u2014') {
			t.Errorf("em dash in operator text: %s", got)
		}
	}
	t.Logf("warning: %s", registryFlavorWarning(console.FlavorMariaDB, console.FlavorMySQL, refused))

	// The hook asks for the correction on every resolution, with what the
	// server reported, and the warning follows the latest one.
	job := &monitorJob{}
	job.set("pending", "")
	hooks := job.streamHooks()
	jobs := 0
	var asked []string
	correctErr := refused
	correct := func(d string) (bool, error) {
		asked = append(asked, d)
		return correctErr == nil, correctErr
	}
	hooks.OnFlavorResolved = registryFlavorHook(job, console.FlavorMariaDB, correct, func(string) { jobs++ })
	hooks.OnFlavorResolved(console.FlavorMySQL)
	if w := job.snapshot().FlavorWarning; !strings.Contains(w, "Source type MariaDB") {
		t.Errorf("status FlavorWarning = %q, want the contradiction", w)
	}
	if st := job.snapshot().State; st == "failed" {
		t.Error("a contradicted hint must not fail the job")
	}
	correctErr = nil
	hooks.OnFlavorResolved(console.FlavorMySQL) // a restart, and this time the save works
	if jobs != 1 {
		t.Errorf("source jobs started %d times, want 1", jobs)
	}
	if w := job.snapshot().FlavorWarning; w != "" {
		t.Errorf("warning kept after the Source type was corrected: %q", w)
	}
	if len(asked) != 2 || asked[0] != console.FlavorMySQL || asked[1] != console.FlavorMySQL {
		t.Errorf("correction asked with %q, want the detected flavor on each resolution", asked)
	}
	// The other direction: a server saved as MySQL that reports MariaDB is
	// corrected to MariaDB.
	asked = nil
	registryFlavorHook(&monitorJob{}, console.FlavorMySQL, correct, func(string) {})(console.FlavorMariaDB)
	if len(asked) != 1 || asked[0] != console.FlavorMariaDB {
		t.Errorf("correction asked with %q, want mariadb", asked)
	}

	// No registry to correct (nil): the contradiction stays visible.
	job2 := &monitorJob{}
	registryFlavorHook(job2, console.FlavorMySQL, nil, func(string) {})(console.FlavorMariaDB)
	if w := job2.snapshot().FlavorWarning; !strings.Contains(w, "saved with Source type MySQL, but the server reports MariaDB") {
		t.Errorf("no registry: warning %q, want the contradiction", w)
	}
}

// ─── derived monitor states (#402) ───────────────────────────────────────────

func TestMonitorJobSnapshot_stalledDerivation(t *testing.T) {
	job := &monitorJob{}
	job.set("running", "")

	// Fresh progress: plain running.
	job.progress()
	if st := job.snapshot(); st.State != "running" {
		t.Fatalf("state = %q, want running", st.State)
	}

	// Progress older than the threshold: derived stalled, stored state intact.
	job.mu.Lock()
	job.lastProgress = time.Now().UTC().Add(-monitorStalledAfter - time.Minute)
	job.mu.Unlock()
	st := job.snapshot()
	if st.State != "stalled" {
		t.Fatalf("state = %q, want stalled", st.State)
	}
	if !strings.Contains(st.LastError, "no progress") {
		t.Errorf("LastError = %q, want a no-progress explanation", st.LastError)
	}
	job.mu.Lock()
	if job.state != "running" {
		t.Errorf("stored state mutated to %q — derivation must be read-only", job.state)
	}
	job.mu.Unlock()
}

func TestMonitorJobSnapshot_lostPosition(t *testing.T) {
	job := &monitorJob{}
	job.set("running", "")
	job.progress()
	job.markLostPosition("binlog gap: events between A and B were purged")

	st := job.snapshot()
	if st.State != "lost_position" {
		t.Fatalf("state = %q, want lost_position", st.State)
	}
	if st.LastError != "binlog gap: events between A and B were purged" {
		t.Errorf("LastError = %q, want the gap detail", st.LastError)
	}

	// A wedged stream beats a historical data-loss note.
	job.mu.Lock()
	job.lastProgress = time.Now().UTC().Add(-monitorStalledAfter - time.Minute)
	job.mu.Unlock()
	if st := job.snapshot(); st.State != "stalled" {
		t.Errorf("state = %q, want stalled to take precedence over lost_position", st.State)
	}
}

func TestMonitorJobSnapshot_noDerivationOutsideRunning(t *testing.T) {
	for _, base := range []string{"pending", "failed", "stopped"} {
		job := &monitorJob{}
		job.set(base, "")
		job.markLostPosition("gap detail")
		job.mu.Lock()
		job.lastProgress = time.Now().UTC().Add(-time.Hour)
		job.mu.Unlock()
		if st := job.snapshot(); st.State != base {
			t.Errorf("state = %q, want %q (no derivation outside running)", st.State, base)
		}
	}

	// Running with zero lastProgress (not reachable via the normal flow, but
	// must not divide-by-zero into stalled).
	job := &monitorJob{}
	job.set("running", "")
	if st := job.snapshot(); st.State != "running" {
		t.Errorf("state = %q, want running when lastProgress is unset", st.State)
	}
}

func TestMonitorJobHooks_pendingFlipsToRunning(t *testing.T) {
	job := &monitorJob{}
	job.set("pending", "")
	hooks := job.streamHooks()

	if st := job.snapshot(); st.State != "pending" {
		t.Fatalf("state = %q, want pending before first checkpoint", st.State)
	}

	hooks.OnCheckpoint()
	if st := job.snapshot(); st.State != "running" {
		t.Fatalf("state = %q, want running after first checkpoint", st.State)
	}

	// OnIndexed is the equivalent attach signal.
	job2 := &monitorJob{}
	job2.set("pending", "")
	job2.streamHooks().OnIndexed(42)
	if st := job2.snapshot(); st.State != "running" {
		t.Fatalf("state = %q, want running after first indexed batch", st.State)
	}

	// OnSourceConnected marks this run as having reached the source without
	// flipping pending, and the next run starts unconnected again (#1606).
	job4 := &monitorJob{}
	job4.set("pending", "")
	job4.streamHooks().OnSourceConnected()
	if st := job4.snapshot(); st.State != "pending" || !st.SourceConnected {
		t.Fatalf("after OnSourceConnected: %+v, want pending and connected", st)
	}
	job4.set("failed", "boom (retrying)")
	if st := job4.snapshot(); !st.SourceConnected {
		t.Fatalf("a failure of the run that connected forgot it: %+v", st)
	}
	job4.set("pending", "")
	if st := job4.snapshot(); st.SourceConnected {
		t.Fatalf("a new run starts connected: %+v", st)
	}
	job5 := &monitorJob{}
	job5.set("pending", "")
	job5.pgStreamHooks().OnSourceConnected()
	if st := job5.snapshot(); !st.SourceConnected {
		t.Fatalf("the PostgreSQL hook does not mark the run connected: %+v", st)
	}

	// OnGapAutoAdvance alone must NOT flip pending (it fires during startup,
	// before the stream is attached).
	job3 := &monitorJob{}
	job3.set("pending", "")
	job3.streamHooks().OnGapAutoAdvance("gap")
	if st := job3.snapshot(); st.State != "pending" {
		t.Fatalf("state = %q, want pending after gap auto-advance only", st.State)
	}
}

// ─── circuit breaker (#402) ──────────────────────────────────────────────────

func TestMonitorRun_circuitBreakerGivesUp(t *testing.T) {
	old := monitorGiveUpAfter
	monitorGiveUpAfter = 0 // any continuous crash-looping trips it immediately
	defer func() { monitorGiveUpAfter = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := &monitorSupervisor{
		baseCtx: ctx,
		jobs:    map[string]*monitorJob{},
		streamFn: func(context.Context, streamrun.Config) error {
			return errors.New("boom: cannot connect")
		},
	}
	job := &monitorJob{cancel: cancel, done: make(chan struct{})}
	job.set("pending", "")
	m.jobs["e1"] = job

	m.wg.Add(1)
	m.run(ctx, job, console.ServerEntry{ID: "e1", Name: "prod"}, console.FlavorMySQL, func(c context.Context) error { return m.streamFn(c, streamrun.Config{}) })

	select {
	case <-job.done:
	default:
		t.Fatal("run returned but job.done is not closed")
	}
	st := job.snapshot()
	if st.State != "failed" {
		t.Fatalf("state = %q, want permanent failed", st.State)
	}
	if !strings.Contains(st.LastError, "gave up") {
		t.Errorf("LastError = %q, want a gave-up explanation", st.LastError)
	}
	if st.Retrying {
		t.Errorf("a failure the supervisor gave up on reports retrying: %+v", st)
	}
	if !strings.Contains(st.LastError, "boom") {
		t.Errorf("LastError = %q, want the underlying error preserved", st.LastError)
	}
}

// TestMonitorRun_retryingFailureSaysSo: a failure the loop will retry is
// reported as retrying while it waits, and a stop clears it (#1606).
func TestMonitorRun_retryingFailureSaysSo(t *testing.T) {
	oldBase, oldCap := monitorBackoffBase, monitorBackoffCap
	monitorBackoffBase, monitorBackoffCap = time.Hour, time.Hour
	defer func() { monitorBackoffBase, monitorBackoffCap = oldBase, oldCap }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &monitorSupervisor{
		baseCtx:  ctx,
		jobs:     map[string]*monitorJob{},
		streamFn: func(context.Context, streamrun.Config) error { return errors.New("boom: cannot connect") },
	}
	job := &monitorJob{cancel: cancel, done: make(chan struct{})}
	job.set("pending", "")
	m.wg.Add(1)
	go m.run(ctx, job, console.ServerEntry{ID: "e9", Name: "retry"}, console.FlavorMySQL, func(c context.Context) error { return m.streamFn(c, streamrun.Config{}) })

	deadline := time.Now().Add(5 * time.Second)
	for job.snapshot().State != "failed" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if st := job.snapshot(); st.State != "failed" || !st.Retrying {
		t.Fatalf("while waiting to retry: %+v, want failed and retrying", st)
	}
	cancel()
	<-job.done
	if st := job.snapshot(); st.State != "stopped" || st.Retrying {
		t.Fatalf("after stop: %+v, want stopped and not retrying", st)
	}
}

func TestMonitorRun_cleanStopBypassesBreaker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already-cancelled daemon: streamFn returns, run must report stopped

	m := &monitorSupervisor{
		baseCtx:  ctx,
		jobs:     map[string]*monitorJob{},
		streamFn: func(c context.Context, _ streamrun.Config) error { return c.Err() },
	}
	job := &monitorJob{cancel: func() {}, done: make(chan struct{})}
	job.set("pending", "")

	m.wg.Add(1)
	m.run(ctx, job, console.ServerEntry{ID: "e2", Name: "x"}, console.FlavorMySQL, func(c context.Context) error { return m.streamFn(c, streamrun.Config{}) })

	if st := job.snapshot(); st.State != "stopped" {
		t.Fatalf("state = %q, want stopped on cancellation", st.State)
	}
}

// ─── replica / duplicate detection (#402) ────────────────────────────────────

func TestGTIDSetContainsUUID(t *testing.T) {
	const (
		uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
		uuidB = "9f3829a2-3c4d-11ee-be56-0242ac120002"
	)
	set := uuidA + ":1-5,\n" + uuidB + ":1-30:40-44"

	tests := []struct {
		name string
		set  string
		uuid string
		want bool
	}{
		{"present", set, uuidA, true},
		{"present second entry", set, uuidB, true},
		{"case-insensitive (go-mysql lowercases)", set, strings.ToUpper(uuidA), true},
		{"absent", set, "00000000-0000-0000-0000-000000000000", false},
		{"empty set", "", uuidA, false},
		{"empty uuid", set, "", false},
		{"malformed set is never a match", "not-a-gtid-set", uuidA, false},
	}
	for _, tt := range tests {
		if got := gtidSetContainsUUID(tt.set, tt.uuid); got != tt.want {
			t.Errorf("%s: gtidSetContainsUUID(%q, %q) = %v, want %v", tt.name, tt.set, tt.uuid, got, tt.want)
		}
	}
}

func TestClassifyReplicaOverlap(t *testing.T) {
	const (
		primary = "3e11fa47-71ca-11e1-9e33-c80aa9429562" // monitored peer
		replica = "9f3829a2-3c4d-11ee-be56-0242ac120002" // candidate
		other   = "11111111-2222-3333-4444-555555555555"
	)

	// Candidate is a replica of the monitored peer: its executed set carries
	// transactions originated at the peer.
	rel := classifyReplicaOverlap(replica, primary+":1-100,"+replica+":1-5", primary, primary+":1-100")
	if !strings.Contains(rel, "replica of") {
		t.Errorf("replica direction: got %q", rel)
	}

	// Candidate is the primary of a monitored replica: the peer's
	// accumulated set carries the candidate's transactions.
	rel = classifyReplicaOverlap(primary, primary+":1-100", replica, primary+":1-90,"+replica+":1-5")
	if !strings.Contains(rel, "primary of") {
		t.Errorf("primary direction: got %q", rel)
	}

	// Same server added twice (case differs — go-mysql lowercases UUIDs).
	rel = classifyReplicaOverlap(strings.ToUpper(primary), primary+":1-10", primary, primary+":1-10")
	if !strings.Contains(rel, "same server") {
		t.Errorf("same-server: got %q", rel)
	}

	// Unrelated servers: no finding.
	if rel = classifyReplicaOverlap(other, other+":1-3", primary, primary+":1-100"); rel != "" {
		t.Errorf("unrelated: got %q, want empty", rel)
	}

	// Peer with no recorded executed set (position mode / never streamed):
	// only the replica direction can fire.
	if rel = classifyReplicaOverlap(other, other+":1-3", primary, ""); rel != "" {
		t.Errorf("no peer set, unrelated: got %q, want empty", rel)
	}
}

func TestEvaluateReplicaOverlap_cardAssembly(t *testing.T) {
	const (
		primary = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
		cand    = "9f3829a2-3c4d-11ee-be56-0242ac120002"
		other   = "11111111-2222-3333-4444-555555555555"
	)

	// Two findings + one clean peer → warn card naming both, no unverified.
	card := evaluateReplicaOverlap(cand, primary+":1-100,"+cand+":1-5", []peerIdentity{
		{name: "prod", uuid: primary, executed: primary + ":1-100"},
		{name: "same", uuid: cand, executed: cand + ":1-5"},
		{name: "unrelated", uuid: other, executed: other + ":1-3"},
	})
	if card.Status != "warn" {
		t.Fatalf("status = %q, want warn", card.Status)
	}
	if !strings.Contains(card.Detail, `replica of already-monitored "prod"`) ||
		!strings.Contains(card.Detail, `same server as already-monitored "same"`) {
		t.Errorf("Detail = %q, want both findings named", card.Detail)
	}
	if !strings.Contains(card.Remediation, "monitoring has already started") {
		t.Errorf("Remediation = %q, must reflect that warns never block", card.Remediation)
	}

	// No findings, one unreadable peer + one unparseable peer set → pass
	// card with an honest unverified count.
	card = evaluateReplicaOverlap(cand, cand+":1-5", []peerIdentity{
		{name: "down", unreadable: true},
		{name: "corrupt", uuid: other, executed: "not-a-gtid-set"},
		{name: "clean", uuid: primary, executed: primary + ":1-100"},
	})
	if card.Status != "pass" {
		t.Fatalf("status = %q, want pass", card.Status)
	}
	if !strings.Contains(card.Detail, "3 monitored source(s)") ||
		!strings.Contains(card.Detail, "(2 could not be verified)") {
		t.Errorf("Detail = %q, want 3 peers with 2 unverified", card.Detail)
	}

	// A peer found via the replica direction does NOT count as unverified
	// even if its own set is unparseable — the relationship WAS detected.
	card = evaluateReplicaOverlap(cand, primary+":1-100", []peerIdentity{
		{name: "prod", uuid: primary, executed: "garbage"},
	})
	if card.Status != "warn" || strings.Contains(card.Detail, "could not be verified") {
		t.Errorf("card = %+v, want warn without unverified", *card)
	}

	// Unparseable candidate set → explicit skip, never a silent pass.
	card = evaluateReplicaOverlap(cand, "garbage", []peerIdentity{{name: "p", uuid: primary}})
	if card.Status != "skip" || !strings.Contains(card.Detail, "could not parse") {
		t.Errorf("card = %+v, want skip on unparseable candidate set", *card)
	}
}

func TestMonitorRun_healthyRunResetsBreaker(t *testing.T) {
	oldGiveUp, oldBase, oldCap, oldHealthy := monitorGiveUpAfter, monitorBackoffBase, monitorBackoffCap, monitorHealthyReset
	monitorGiveUpAfter = 60 * time.Millisecond
	monitorBackoffBase = time.Millisecond
	monitorBackoffCap = 2 * time.Millisecond
	monitorHealthyReset = 5 * time.Millisecond
	defer func() {
		monitorGiveUpAfter, monitorBackoffBase, monitorBackoffCap, monitorHealthyReset = oldGiveUp, oldBase, oldCap, oldHealthy
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Every run is "healthy" (outlives monitorHealthyReset) before failing —
	// the breaker clock must keep resetting and never trip, no matter how
	// long the flapping goes on in total.
	m := &monitorSupervisor{
		baseCtx: ctx,
		jobs:    map[string]*monitorJob{},
		streamFn: func(c context.Context, _ streamrun.Config) error {
			select {
			case <-time.After(10 * time.Millisecond): // > monitorHealthyReset
				return errors.New("flap")
			case <-c.Done():
				return c.Err()
			}
		},
	}
	job := &monitorJob{cancel: cancel, done: make(chan struct{})}
	job.set("pending", "")

	m.wg.Add(1)
	go m.run(ctx, job, console.ServerEntry{ID: "e3", Name: "flappy"}, console.FlavorMySQL, func(c context.Context) error { return m.streamFn(c, streamrun.Config{}) })

	// Let it flap well past monitorGiveUpAfter in wall-clock time.
	time.Sleep(150 * time.Millisecond)
	if st := job.snapshot(); strings.Contains(st.LastError, "gave up") {
		t.Fatalf("breaker tripped despite healthy runs in between: %+v", st)
	}
	cancel()
	<-job.done
	if st := job.snapshot(); st.State != "stopped" {
		t.Fatalf("state = %q, want stopped after cancel", st.State)
	}
}

// A stream can sit in the resume-time dedup for minutes before it captures
// anything (#1690). The supervisor carries that step as a PHASE next to the
// unchanged "pending" state, so the row can say what is happening without
// moving any state every caller already switches on — notably
// isLiveMonitorState, which keeps the Stop button on the row.
func TestMonitorJobPhase(t *testing.T) {
	job := &monitorJob{}
	job.set("pending", "")
	hooks := job.streamHooks()

	hooks.OnPhase(streamrun.PhaseResumeCleanup)
	st := job.snapshot()
	if st.State != "pending" || st.Phase != streamrun.PhaseResumeCleanup {
		t.Fatalf("during the cleanup: %+v, want pending with the resume_cleanup phase", st)
	}

	hooks.OnPhase("")
	if st := job.snapshot(); st.Phase != "" {
		t.Fatalf("the phase outlived the step it names: %+v", st)
	}

	// Every state transition clears it too. A phase describes the run that is
	// executing, so a crash mid-cleanup must not leave the row claiming the
	// daemon is still cleaning up while it waits out a retry backoff — which
	// is exactly the "looks alive but isn't" reading #1690 set out to remove.
	for _, tc := range []struct {
		name string
		move func()
	}{
		{"a failure the supervisor will retry", func() { job.setRetrying("boom (retrying)") }},
		{"a terminal failure", func() { job.set("failed", "gave up") }},
		{"a stop", func() { job.set("stopped", "") }},
		{"the next run starting", func() { job.set("pending", "") }},
		// The fifth transition, and the one set()/setRetrying() do not cover:
		// pending→running happens inside progress(), not set().
		{"capture producing its first batch", func() { job.set("pending", ""); job.setPhase(streamrun.PhaseResumeCleanup); job.progress() }},
	} {
		job.setPhase(streamrun.PhaseResumeCleanup)
		tc.move()
		if st := job.snapshot(); st.Phase != "" {
			t.Errorf("%s left a stale phase: %+v", tc.name, st)
		}
	}
}

// #1747: the daemon's --batch-size has to reach the sources added from the
// interface. It used to reach ONLY the --source-dsn stream typed on the
// command line, which is not the deployment the product documents: a daemon
// with no source, every source added from the web interface. The documented
// remedy for replication lag is to raise this flag, and for those sources it
// could not be followed at all.
func TestSourceStreamConfig_carriesTheDaemonBatchSize(t *testing.T) {
	e := console.ServerEntry{DSN: "u:p@tcp(i)/idx", SourceDSN: "u:p@tcp(s)/db"}

	if got := sourceStreamConfig(e, 1, 2500).BatchSize; got != 2500 {
		t.Errorf("BatchSize = %d, want the daemon's 2500 — the flag does not reach a supervised "+
			"source, so the documented fix for replication lag cannot be applied there", got)
	}
	// Zero is not a batch of zero: it means the caller never parsed the flag.
	if got := sourceStreamConfig(e, 1, 0).BatchSize; got != defaultStreamBatchSize {
		t.Errorf("BatchSize = %d with no flag, want the built-in %d", got, defaultStreamBatchSize)
	}
	if got := sourceStreamConfig(e, 1, -5).BatchSize; got != defaultStreamBatchSize {
		t.Errorf("BatchSize = %d for a negative flag, want the built-in %d", got, defaultStreamBatchSize)
	}
}

// The same for a PostgreSQL source: one flag, both source families.
func TestSourcePGStreamConfig_carriesTheDaemonBatchSize(t *testing.T) {
	e := console.ServerEntry{
		DSN: "u:p@tcp(i)/idx", SourceDSN: "postgres://u:p@h:5432/db",
		SourceSlot: "s", SourcePublication: "p",
	}
	cfg, err := sourcePGStreamConfig(e, 1, 2500)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BatchSize != 2500 {
		t.Errorf("BatchSize = %d, want the daemon's 2500", cfg.BatchSize)
	}
}

// The ceiling is named where the operator reads the flag. It used to be
// nowhere: the indexer clamped anything larger and logged a warning, while
// the flag help, the spec and the troubleshooting table all said "raise it".
func TestBatchSizeHelpNamesTheCeiling(t *testing.T) {
	help := indexer.BatchSizeHelp()
	if !strings.Contains(help, fmt.Sprint(indexer.MaxBatchSize)) {
		t.Errorf("the --batch-size help does not name the ceiling %d: %q", indexer.MaxBatchSize, help)
	}
}
