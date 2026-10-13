package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// clearEnv neutralises every environment control so a test starts from a known
// state. Without this the suite's own result would depend on whether it runs
// on a laptop or in CI.
func clearEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DO_NOT_TRACK", "")
	t.Setenv(EnvVar, "")
	t.Setenv(DirEnvVar, "")
	for _, v := range ciEnvVars {
		t.Setenv(v, "")
	}
}

func boolPtr(b bool) *bool { return &b }

// ── The wire contract ────────────────────────────────────────

// TestAllowlistMatchesStruct is the field allowlist guarantee: a field added to
// Event without being added to AllowedFields (or vice versa) fails here. It is
// the reason a reviewer can read AllowedFields and know it is the whole story.
func TestAllowlistMatchesStruct(t *testing.T) {
	typ := reflect.TypeOf(Event{})
	var got []string
	for i := range typ.NumField() {
		tag := typ.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			t.Fatalf("field %s has no json tag; every wire field must be explicit", typ.Field(i).Name)
		}
		got = append(got, strings.Split(tag, ",")[0])
	}
	if !reflect.DeepEqual(got, AllowedFields) {
		t.Errorf("Event JSON keys and AllowedFields disagree:\n  struct:    %v\n  allowlist: %v", got, AllowedFields)
	}
}

// TestMarshalledEventHasOnlyAllowedKeys checks the actual serialized bytes,
// not just the struct definition — an embedded type or a custom marshaller
// could add keys the reflection test above would miss.
func TestMarshalledEventHasOnlyAllowedKeys(t *testing.T) {
	clearEnv(t)
	data, err := json.Marshal(SampleEvent())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	allowed := make(map[string]bool, len(AllowedFields))
	for _, f := range AllowedFields {
		allowed[f] = true
	}
	for k := range m {
		if !allowed[k] {
			t.Errorf("serialized event carries key %q, which is not on the allowlist", k)
		}
	}
}

// TestRequestCarriesNoCredential is the no-credential guarantee. Telemetry
// posting an Authorization header would make the metadata-only claim false and
// would let usage data be joined to a customer account.
func TestRequestCarriesNoCredential(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := postNDJSON(context.Background(), srv.Client(), srv.URL, []byte("{}\n")); err != nil {
		t.Fatalf("postNDJSON: %v", err)
	}
	for _, banned := range []string{"Authorization", "Cookie", "X-Api-Key", "X-Bintrail-Server-Uuid"} {
		if v := got.Get(banned); v != "" {
			t.Errorf("telemetry request carried %s: %q", banned, v)
		}
	}
	if ct := got.Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want application/x-ndjson", ct)
	}
}

// ── Field coarsening ─────────────────────────────────────────

func TestDurationBucket(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{0, "<100ms"},
		{99 * time.Millisecond, "<100ms"},
		{100 * time.Millisecond, "100ms-1s"},
		{999 * time.Millisecond, "100ms-1s"},
		{time.Second, "1s-10s"},
		{9 * time.Second, "1s-10s"},
		{10 * time.Second, "10s-1m"},
		{59 * time.Second, "10s-1m"},
		{time.Minute, "1m-10m"},
		{9 * time.Minute, "1m-10m"},
		{10 * time.Minute, ">10m"},
		{48 * time.Hour, ">10m"}, // tail collapses: real durations leak data volume
	}
	for _, c := range cases {
		if got := durationBucket(c.d); got != c.want {
			t.Errorf("durationBucket(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestMinorVersionAndIsRelease(t *testing.T) {
	cases := []struct {
		in        string
		want      string
		isRelease bool
	}{
		{"0.40.0", "0.40", true},
		{"v1.2.3", "1.2", true},
		{"10.11.12", "10.11", true},
		{"dev", "unknown", false},
		{"", "unknown", false},
		// A custom -ldflags string can be near-unique, so it must never pass
		// through verbatim.
		{"0.40.0-acme-internal-build-7", "unknown", false},
		{"0.40", "unknown", false},
	}
	for _, c := range cases {
		if got := minorVersion(c.in); got != c.want {
			t.Errorf("minorVersion(%q) = %q, want %q", c.in, got, c.want)
		}
		if got := isReleaseVersion(c.in); got != c.isRelease {
			t.Errorf("isReleaseVersion(%q) = %v, want %v", c.in, got, c.isRelease)
		}
	}
}

func TestCoarseArch(t *testing.T) {
	for in, want := range map[string]string{
		"amd64": "amd64", "arm64": "arm64",
		"riscv64": "other", "386": "other", "": "other",
	} {
		if got := coarseArch(in); got != want {
			t.Errorf("coarseArch(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSanitizeCommand(t *testing.T) {
	for in, want := range map[string]string{
		"status": "status", "recover-cascade": "recover-cascade", "UP": "up",
		"": "other",
		// Anything that is not a plain command name is refused rather than
		// forwarded — the field must never become a channel for free text.
		"query --index-dsn=user:pass@tcp(h)/db": "other",
		"/etc/passwd":                           "other",
		strings.Repeat("a", 33):                 "other",
	} {
		if got := sanitizeCommand(in); got != want {
			t.Errorf("sanitizeCommand(%q) = %q, want %q", in, got, want)
		}
	}
}

// ── Error classification ─────────────────────────────────────

func TestClassifyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"access denied", &mysql.MySQLError{Number: 1045}, ClassDBPermission},
		{"table denied", &mysql.MySQLError{Number: 1142}, ClassDBPermission},
		{"unknown database", &mysql.MySQLError{Number: 1049}, ClassNotFound},
		{"no such table", &mysql.MySQLError{Number: 1146}, ClassNotFound},
		{"syntax error", &mysql.MySQLError{Number: 1064}, ClassUnknown},
		{"wrapped mysql", fmt.Errorf("query failed: %w", &mysql.MySQLError{Number: 1045}), ClassDBPermission},
		{"missing file", fmt.Errorf("open: %w", fs.ErrNotExist), ClassNotFound},
		{"permission", fmt.Errorf("open: %w", fs.ErrPermission), ClassStorageIO},
		{"deadline", context.DeadlineExceeded, ClassDBConnection},
		{"plain", errors.New("something went wrong"), ClassUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ClassifyError(c.err); got != c.want {
				t.Errorf("ClassifyError = %q, want %q", got, c.want)
			}
		})
	}
}

// TestClassifyErrorNeverLeaksMessage is the load-bearing property: bintrail
// error strings routinely carry DSNs and table names, and none of that may
// reach the wire under any input.
func TestClassifyErrorNeverLeaksMessage(t *testing.T) {
	secret := "root:hunter2@tcp(db.internal:3306)/customer_orders"
	err := fmt.Errorf("connect %s: %w", secret, &mysql.MySQLError{Number: 1045, Message: secret})
	got := ClassifyError(err)
	if strings.Contains(got, "hunter2") || strings.Contains(got, "customer_orders") || strings.Contains(got, "db.internal") {
		t.Fatalf("ClassifyError leaked the error message: %q", got)
	}
	if !classes[got] {
		t.Fatalf("ClassifyError returned %q, which is outside the taxonomy", got)
	}
}

func TestNormalizeClassRejectsFreeText(t *testing.T) {
	if got := normalizeClass("db_connection"); got != ClassDBConnection {
		t.Errorf("known class was altered: %q", got)
	}
	if got := normalizeClass("failed to open /var/lib/mysql/binlog.000042"); got != ClassUnknown {
		t.Errorf("free text was not coerced: %q", got)
	}
}

// ── Consent resolution ───────────────────────────────────────

func TestResolvePrecedence(t *testing.T) {
	t.Run("default is on", func(t *testing.T) {
		clearEnv(t)
		got := Resolve("", t.TempDir())
		if !got.Enabled || got.Source != SourceDefault {
			t.Errorf("got %+v, want enabled via default", got)
		}
	})

	t.Run("config file overrides default", func(t *testing.T) {
		clearEnv(t)
		dir := t.TempDir()
		if err := SetEnabled(dir, false); err != nil {
			t.Fatalf("SetEnabled: %v", err)
		}
		got := Resolve("", dir)
		if got.Enabled || got.Source != SourceConfig {
			t.Errorf("got %+v, want disabled via config file", got)
		}
	})

	t.Run("env overrides config file", func(t *testing.T) {
		clearEnv(t)
		dir := t.TempDir()
		if err := SetEnabled(dir, false); err != nil {
			t.Fatalf("SetEnabled: %v", err)
		}
		t.Setenv(EnvVar, "on")
		got := Resolve("", dir)
		if !got.Enabled || got.Source != SourceEnv {
			t.Errorf("got %+v, want enabled via env", got)
		}
	})

	t.Run("flag overrides env", func(t *testing.T) {
		clearEnv(t)
		t.Setenv(EnvVar, "on")
		got := Resolve("off", t.TempDir())
		if got.Enabled || got.Source != SourceFlag {
			t.Errorf("got %+v, want disabled via flag", got)
		}
	})

	t.Run("DO_NOT_TRACK beats everything", func(t *testing.T) {
		clearEnv(t)
		dir := t.TempDir()
		if err := SetEnabled(dir, true); err != nil {
			t.Fatalf("SetEnabled: %v", err)
		}
		t.Setenv(EnvVar, "on")
		t.Setenv("DO_NOT_TRACK", "1")
		got := Resolve("on", dir)
		if got.Enabled || got.Source != SourceDoNotTrack {
			t.Errorf("got %+v, want disabled via DO_NOT_TRACK", got)
		}
	})

	t.Run("DO_NOT_TRACK=0 does not disable", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("DO_NOT_TRACK", "0")
		if got := Resolve("", t.TempDir()); !got.Enabled {
			t.Errorf("got %+v, want the 0 value ignored", got)
		}
	})

	t.Run("unparseable values fall through", func(t *testing.T) {
		clearEnv(t)
		dir := t.TempDir()
		if err := SetEnabled(dir, false); err != nil {
			t.Fatalf("SetEnabled: %v", err)
		}
		t.Setenv(EnvVar, "maybe")
		// A typo must not silently mean "off" — it falls through to the config
		// file, which here says off, via SourceConfig not SourceEnv.
		if got := Resolve("", dir); got.Source != SourceConfig {
			t.Errorf("got %+v, want fall-through to the config file", got)
		}
	})
}

// TestCorruptStateFileFallsBackToDefault: a hand-edited or truncated consent
// file must degrade to the default, never fail a command the operator asked
// for.
func TestCorruptStateFileFallsBackToDefault(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	if err := os.WriteFile(StatePath(dir), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := Resolve("", dir); !got.Enabled || got.Source != SourceDefault {
		t.Errorf("got %+v, want the default", got)
	}
}

func TestStateFilePermissions(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	if err := SetEnabled(dir, false); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	fi, err := os.Stat(StatePath(dir))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("consent file mode = %o, want 600", perm)
	}
}

func TestIsCI(t *testing.T) {
	clearEnv(t)
	if IsCI() {
		t.Fatal("IsCI true with every marker cleared")
	}
	t.Setenv("GITHUB_ACTIONS", "true")
	if !IsCI() {
		t.Error("IsCI false with GITHUB_ACTIONS=true")
	}
}

// ── Spool ────────────────────────────────────────────────────

func TestAppendAndDrain(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 7, 21, 10, 0, 0, 0, time.UTC)
	for i := range 3 {
		e := Event{SchemaVersion: SchemaVersion, EventType: EventCommandRun, Command: fmt.Sprintf("cmd%d", i)}
		if err := appendEvent(dir, e, now); err != nil {
			t.Fatalf("appendEvent: %v", err)
		}
	}

	var received [][]byte
	drain(dir, now, func(b []byte) error {
		received = append(received, b)
		return nil
	})
	if len(received) != 1 {
		t.Fatalf("got %d batches, want 1", len(received))
	}
	if n := strings.Count(strings.TrimSpace(string(received[0])), "\n") + 1; n != 3 {
		t.Errorf("batch has %d lines, want 3", n)
	}
	left, _ := os.ReadDir(dir)
	if len(left) != 0 {
		t.Errorf("spool not emptied after a successful drain: %v", left)
	}
}

func TestSpoolFilePermissions(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	if err := appendEvent(dir, Event{}, now); err != nil {
		t.Fatalf("appendEvent: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("want 1 spool file, got %d", len(entries))
	}
	fi, err := entries[0].Info()
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("spool file mode = %o, want 600", perm)
	}
}

// TestSpoolCapDropsRatherThanGrows: a box that never reaches the network must
// not fill its disk with telemetry.
func TestSpoolCapDropsRatherThanGrows(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	path := filepath.Join(dir, now.UTC().Format("2006-01-02")+spoolSuffix)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), maxSpoolFileBytes+1), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	before, _ := os.Stat(path)
	if err := appendEvent(dir, Event{Command: "status"}, now); err != nil {
		t.Fatalf("appendEvent: %v", err)
	}
	after, _ := os.Stat(path)
	if after.Size() != before.Size() {
		t.Errorf("spool grew past the cap: %d -> %d", before.Size(), after.Size())
	}
}

// TestDrainReclaimsAbandonedClaim covers the common case, not an exotic one: a
// fast command's process exits while its detached drain goroutine is still in
// its HTTP call, leaving a claimed file behind. A later run must pick it up
// rather than let the events evaporate.
func TestDrainReclaimsAbandonedClaim(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	abandoned := writeClaim(t, dir, "2026-07-20", time.Now().Add(-10*time.Minute))

	var received [][]byte
	drain(dir, time.Now(), func(b []byte) error {
		received = append(received, b)
		return nil
	})
	if len(received) != 1 {
		t.Fatalf("abandoned claim was not reclaimed (got %d batches)", len(received))
	}
	if _, err := os.Stat(abandoned); !os.IsNotExist(err) {
		t.Error("reclaimed file still on disk")
	}
}

// TestDrainLeavesFreshClaimAlone: a claim younger than claimReclaimAfter may
// still belong to a live drainer, so stealing it would send the batch twice.
//
// The file's mtime is deliberately set an hour into the past. A real claim
// always looks like this — os.Rename preserves mtime, so a claim of yesterday's
// spool file carries yesterday's timestamp — and an implementation that judges
// claim age by mtime instead of by the stamp in the claim's name passes every
// other test in this file while failing this one.
func TestDrainLeavesFreshClaimAlone(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	fresh := writeClaim(t, dir, "2026-07-20", time.Now())
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(fresh, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	called := 0
	drain(dir, time.Now(), func([]byte) error { called++; return nil })
	if called != 0 {
		t.Errorf("a fresh claim was stolen from a possibly-live drainer (%d sends)", called)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh claim was removed: %v", err)
	}
}

func TestClaimStamp(t *testing.T) {
	want := time.Unix(0, 1753084800123456789)
	name := "2026-07-21" + spoolSuffix + claimedMark + "4211-" + strconv.FormatInt(want.UnixNano(), 10)
	got, ok := claimStamp(name)
	if !ok || !got.Equal(want) {
		t.Errorf("claimStamp(%q) = %v, %v; want %v, true", name, got, ok, want)
	}
	if _, ok := claimStamp("2026-07-21" + spoolSuffix); ok {
		t.Error("a plain spool name should not yield a claim stamp")
	}
	if _, ok := claimStamp("2026-07-21" + spoolSuffix + claimedMark + "garbage"); ok {
		t.Error("an unparseable claim suffix should not yield a stamp")
	}
}

// TestDrainDropsBatchOnSendFailure covers the documented drop-on-fail
// behaviour AND that a refusing endpoint is not hammered: the loop stops after
// the first failure rather than working through every remaining file.
func TestDrainDropsBatchOnSendFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for _, day := range []string{"2026-07-18", "2026-07-19"} {
		if err := os.WriteFile(filepath.Join(dir, day+spoolSuffix), []byte("{}\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	calls := 0
	drain(dir, time.Now(), func([]byte) error {
		calls++
		return errors.New("endpoint refused")
	})
	if calls != 1 {
		t.Errorf("kept sending to a refusing endpoint: %d calls, want 1", calls)
	}
	// The attempted batch is dropped rather than requeued (no backlog to flush
	// later), and the file the loop never reached is still there for a later
	// run — so exactly one of the two remains.
	left, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	if len(left) != 1 {
		t.Fatalf("want 1 file left (the unattempted one), got %d: %v", len(left), left)
	}
	if left[0].Name() != "2026-07-19"+spoolSuffix {
		t.Errorf("the failed batch was requeued instead of dropped: %s remains", left[0].Name())
	}
}

func TestPurgeSpool(t *testing.T) {
	dir := t.TempDir()
	if err := appendEvent(SpoolDir(dir), Event{Command: "status"}, time.Now()); err != nil {
		t.Fatalf("appendEvent: %v", err)
	}
	if err := PurgeSpool(dir); err != nil {
		t.Fatalf("PurgeSpool: %v", err)
	}
	if _, err := os.Stat(SpoolDir(dir)); !os.IsNotExist(err) {
		t.Error("spool directory survived the purge")
	}
	// Purging an already-absent spool is not an error — `telemetry off` must
	// work on a machine that has never spooled anything.
	if err := PurgeSpool(dir); err != nil {
		t.Errorf("purging an absent spool errored: %v", err)
	}
	if err := PurgeSpool(""); err != nil {
		t.Errorf("purging with no config dir errored: %v", err)
	}
}

// initDrained builds a client whose startup drain has already run to
// completion.
//
// Necessary for any test that writes events and then inspects the spool: the
// drain is a background goroutine, and if it scans AFTER the test has appended,
// it legitimately claims that file, fails to deliver it to the dead endpoint,
// and drops it — leaving the test reading an empty spool. Waiting costs nothing
// here, since a drain over an empty spool returns immediately. Wait without a
// deadline (not Shutdown, which abandons the drain after shutdownGrace) so the
// race stays impossible rather than merely rare under scheduler load.
func initDrained(t *testing.T, cfg Config) *Client {
	t.Helper()
	c := Init(cfg)
	c.WaitStartupDrain()
	return c
}

func TestShutdownIsSafeOnEveryClient(t *testing.T) {
	clearEnv(t)
	var nilClient *Client
	nilClient.Shutdown() // must not panic

	// Disabled client: no goroutine was ever started, so there is nothing to
	// wait for and Shutdown must return at once rather than block for the grace
	// period.
	disabled := Init(Config{Dir: t.TempDir(), Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})
	start := time.Now()
	disabled.Shutdown()
	disabled.Shutdown() // idempotent
	if elapsed := time.Since(start); elapsed > shutdownGrace {
		t.Errorf("Shutdown on a disabled client blocked for %v", elapsed)
	}

	enabled := Init(Config{
		Dir: t.TempDir(), Endpoint: "http://127.0.0.1:1",
		Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
	})
	enabled.Shutdown()
	enabled.Shutdown()
}

// TestWaitStartupDrainIsSafeOnEveryClient mirrors TestShutdownIsSafeOnEveryClient
// for the sync primitive #1084's fix added, and pins its DELIBERATE guard
// divergence from Shutdown: WaitStartupDrain gates on c.drained (was a drain
// goroutine ever started?), NOT on Enabled() (runtime consent). Harmonizing it
// to Shutdown's Enabled() guard would return early on a client opted out at
// runtime while its startup drain is still live — reintroducing the spool race
// as "rare" for exactly the tests the method exists to serve.
func TestWaitStartupDrainIsSafeOnEveryClient(t *testing.T) {
	clearEnv(t)
	var nilClient *Client
	nilClient.WaitStartupDrain() // must not panic

	// Disabled client: no drain goroutine was ever started; must return at once.
	disabled := Init(Config{Dir: t.TempDir(), Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})
	start := time.Now()
	disabled.WaitStartupDrain()
	disabled.WaitStartupDrain() // idempotent
	if elapsed := time.Since(start); elapsed > shutdownGrace {
		t.Errorf("WaitStartupDrain on a disabled client blocked for %v", elapsed)
	}

	// Opt-out AFTER Init: the drain goroutine is already live, so the wait must
	// still wait on it rather than early-return through an Enabled() check.
	enabled := Init(Config{
		Dir: t.TempDir(), Endpoint: "http://127.0.0.1:1",
		Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
	})
	enabled.SetRuntimeConsent(false)
	enabled.WaitStartupDrain()
	enabled.WaitStartupDrain() // idempotent after completion
}

// writeClaim creates a claim file whose name carries stamp, the way drain
// names its own claims.
func writeClaim(t *testing.T, dir, day string, stamp time.Time) string {
	t.Helper()
	path := filepath.Join(dir, day+spoolSuffix+claimedMark+"999-"+strconv.FormatInt(stamp.UnixNano(), 10))
	if err := os.WriteFile(path, []byte(`{"command":"status"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write claim: %v", err)
	}
	return path
}

// TestDrainExpiresAncientSpool: undelivered events must not linger forever.
func TestDrainExpiresAncientSpool(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	stale := filepath.Join(dir, "2020-01-01"+spoolSuffix)
	if err := os.WriteFile(stale, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	old := time.Now().Add(-maxSpoolAge - time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	called := 0
	drain(dir, time.Now(), func([]byte) error { called++; return nil })
	if called != 0 {
		t.Errorf("expired events were sent (%d batches)", called)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Error("expired spool file was not removed")
	}
}

// TestConcurrentDrainSendsEachBatchOnce is the claim-by-rename guarantee: a
// cron run and a human run sharing $HOME must not double-report.
func TestConcurrentDrainSendsEachBatchOnce(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	for i := range 5 {
		if err := appendEvent(dir, Event{Command: fmt.Sprintf("c%d", i)}, now); err != nil {
			t.Fatalf("appendEvent: %v", err)
		}
	}
	// Age the spool file before the drainers run. This is what an ordinary
	// prior-day spool file looks like, and it is what makes this test capable
	// of failing: rename preserves mtime, so an implementation that ages claims
	// by mtime sees every claim of this file as instantly abandoned and lets a
	// second drainer adopt and re-send a batch that is still in flight.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	old := now.Add(-30 * time.Minute)
	for _, e := range entries {
		if err := os.Chtimes(filepath.Join(dir, e.Name()), old, old); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	var mu sync.Mutex
	var lines int
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Stagger, so later drainers scan while an earlier claim is live
			// rather than all racing on the pre-claim name.
			time.Sleep(time.Duration(i) * 50 * time.Millisecond)
			drain(dir, time.Now(), func(b []byte) error {
				time.Sleep(150 * time.Millisecond) // hold the claim, like a real POST
				mu.Lock()
				defer mu.Unlock()
				lines += strings.Count(string(b), "\n")
				return nil
			})
		}(i)
	}
	wg.Wait()
	if lines != 5 {
		t.Errorf("delivered %d lines, want exactly 5 (duplicated or lost)", lines)
	}
}

func TestDrainMissingDirIsNotAnError(t *testing.T) {
	drain(filepath.Join(t.TempDir(), "does-not-exist"), time.Now(), func([]byte) error {
		t.Fatal("send called with no spool present")
		return nil
	})
}

// ── Client lifecycle ─────────────────────────────────────────

// TestInertWithoutEndpoint is the build-time kill switch: no endpoint compiled
// in means no spool, no network, nothing. This is what makes `go build` and the
// whole test suite incapable of emitting telemetry.
func TestInertWithoutEndpoint(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	c := Init(Config{Dir: dir, Interactive: boolPtr(true), Stderr: &bytes.Buffer{}})
	if c.Enabled() {
		t.Fatal("client enabled with no endpoint compiled in")
	}
	c.RecordCommand("status").Finish()
	c.Beacon("stream")
	if entries, _ := os.ReadDir(SpoolDir(dir)); len(entries) != 0 {
		t.Errorf("inert client wrote to the spool: %v", entries)
	}
}

func TestDisabledByConsentWritesNothing(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvVar, "off")
	dir := t.TempDir()
	c := Init(Config{Dir: dir, Endpoint: "http://127.0.0.1:1", Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})
	if c.Enabled() {
		t.Fatal("client enabled while consent says off")
	}
	c.RecordCommand("status").Finish()
	if entries, _ := os.ReadDir(SpoolDir(dir)); len(entries) != 0 {
		t.Errorf("disabled client wrote to the spool: %v", entries)
	}
}

func TestCISuppressesReporting(t *testing.T) {
	clearEnv(t)
	t.Setenv("CI", "true")
	dir := t.TempDir()
	c := Init(Config{Dir: dir, Endpoint: "http://127.0.0.1:1", Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})
	if c.Enabled() {
		t.Fatal("telemetry active in CI")
	}
	if d := c.Decision(); !d.Enabled {
		t.Error("CI detection must suppress reporting without rewriting the consent decision")
	}
}

func TestSpanRecordsToSpool(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	c := initDrained(t, Config{
		Dir: dir, Endpoint: "http://127.0.0.1:1", Version: "0.40.0",
		Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
	})
	if !c.Enabled() {
		t.Fatal("client not enabled")
	}
	s := c.RecordCommand("query")
	s.SetError(ClassDBPermission)
	s.Finish()

	e := readOneSpooledEvent(t, SpoolDir(dir))
	if e.Command != "query" || e.EventType != EventCommandError || e.Outcome != OutcomeError {
		t.Errorf("unexpected event: %+v", e)
	}
	if e.ErrorClass != ClassDBPermission {
		t.Errorf("error_class = %q, want %q", e.ErrorClass, ClassDBPermission)
	}
	if e.Version != "0.40" || !e.IsRelease {
		t.Errorf("version = %q is_release = %v, want 0.40/true", e.Version, e.IsRelease)
	}
	if e.RunID == "" {
		t.Error("command events should carry a run_id for ingestion-side dedup")
	}
}

// TestBeaconCarriesNoRunID: a daemon lives for months, so its run_id would be a
// longitudinal identifier — precisely what this design refuses to create.
func TestBeaconCarriesNoRunID(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	c := initDrained(t, Config{Dir: dir, Endpoint: "http://127.0.0.1:1", Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)})
	c.Beacon("stream")

	e := readOneSpooledEvent(t, SpoolDir(dir))
	if e.EventType != EventDaemonBeacon || e.Command != "stream" {
		t.Errorf("unexpected beacon: %+v", e)
	}
	if e.RunID != "" {
		t.Errorf("beacon carries run_id %q; it must not", e.RunID)
	}
}

// TestRecordDaemonCommandOmitsRunID: an action inside a long-running daemon (a
// console request under `watch`) shares the daemon's months-lived run_id, so
// stamping it on every action would reconstruct a per-install activity timeline.
// The daemon-command span must drop run_id exactly like a beacon, while still
// recording the command name, outcome, error class, and duration bucket.
func TestRecordDaemonCommandOmitsRunID(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	c := initDrained(t, Config{
		Dir: dir, Endpoint: "http://127.0.0.1:1", Version: "0.42.0",
		Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
	})
	if !c.Enabled() {
		t.Fatal("client not enabled")
	}
	s := c.RecordDaemonCommand("console-recover")
	s.SetError(ClassInternal)
	s.Finish()

	e := readOneSpooledEvent(t, SpoolDir(dir))
	if e.Command != "console-recover" {
		t.Errorf("command = %q, want console-recover", e.Command)
	}
	if e.RunID != "" {
		t.Errorf("daemon command carries run_id %q; a months-lived run_id is a longitudinal key", e.RunID)
	}
	if e.EventType != EventCommandError || e.Outcome != OutcomeError || e.ErrorClass != ClassInternal {
		t.Errorf("unexpected event: %+v", e)
	}
}

// TestBeaconAtMostOncePerUTCDay: a finer beat would reconstruct a daemon's
// uptime and maintenance windows.
func TestBeaconAtMostOncePerUTCDay(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	base := time.Date(2026, 7, 21, 8, 0, 0, 0, time.UTC)

	// The clock is atomic because Config.Now is called from the background drain
	// goroutine as well as from this test — a plain variable is a data race that
	// only -race reports, and CI runs with it.
	var clock atomic.Int64
	clock.Store(base.UnixNano())
	c := initDrained(t, Config{
		Dir: dir, Endpoint: "http://127.0.0.1:1", Stderr: &bytes.Buffer{},
		Interactive: boolPtr(false),
		Now:         func() time.Time { return time.Unix(0, clock.Load()).UTC() },
	})
	for range 50 {
		c.Beacon("stream")
	}
	if n := countSpooledEvents(t, SpoolDir(dir)); n != 1 {
		t.Fatalf("same-day beacons recorded %d events, want 1", n)
	}

	clock.Store(base.Add(24 * time.Hour).UnixNano())
	c.Beacon("stream")
	if n := countSpooledEvents(t, SpoolDir(dir)); n != 2 {
		t.Errorf("next-day beacon recorded %d events total, want 2", n)
	}
}

func TestNoticeShownOnceOnDefault(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	cfg := func(w *bytes.Buffer) Config {
		return Config{Dir: dir, Endpoint: "http://127.0.0.1:1", Stderr: w, Interactive: boolPtr(true)}
	}
	var first bytes.Buffer
	Init(cfg(&first))
	if !strings.Contains(first.String(), "Telemetry is currently ON") {
		t.Fatalf("first run did not disclose the default:\n%s", first.String())
	}
	if !strings.Contains(first.String(), "telemetry off") {
		t.Error("notice does not tell the operator how to turn it off")
	}
	var second bytes.Buffer
	Init(cfg(&second))
	if second.Len() != 0 {
		t.Errorf("notice repeated on the second run:\n%s", second.String())
	}
}

func TestNoticeSuppressedWhenNotInteractive(t *testing.T) {
	clearEnv(t)
	var out bytes.Buffer
	Init(Config{Dir: t.TempDir(), Endpoint: "http://127.0.0.1:1", Stderr: &out, Interactive: boolPtr(false)})
	if out.Len() != 0 {
		t.Errorf("notice printed into non-interactive output:\n%s", out.String())
	}
}

// TestNoticeSuppressedAfterExplicitChoice: an operator who already chose has
// nothing to be told.
func TestNoticeSuppressedAfterExplicitChoice(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	if err := SetEnabled(dir, true); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	var out bytes.Buffer
	Init(Config{Dir: dir, Endpoint: "http://127.0.0.1:1", Stderr: &out, Interactive: boolPtr(true)})
	if out.Len() != 0 {
		t.Errorf("notice shown despite an explicit choice:\n%s", out.String())
	}
}

// TestNilSafety: every entry point tolerates a nil client or span, so callers
// never need a guard and a telemetry mistake can never panic a command.
func TestNilSafety(t *testing.T) {
	var c *Client
	c.RecordCommand("status").Finish()
	c.Beacon("stream")
	if c.Enabled() {
		t.Error("nil client reported enabled")
	}
	var s *Span
	s.SetError(ClassInternal)
	s.Finish()
}

// deliveryCeiling bounds how long TestEndToEndDelivery waits for the background
// drain to reach the endpoint. It is deliberately generous: the test waits on
// the delivery itself, so on a loaded runner this costs latency, never a
// failure, and only a genuine hang trips it (#1502). It is not a performance
// assertion; the drain's own budget is drainDeadline, and a local round trip
// that outruns THAT is what the failure message points at.
const deliveryCeiling = 30 * time.Second

// TestEndToEndDelivery exercises the whole path: a first run spools, a second
// run drains and delivers over HTTP.
func TestEndToEndDelivery(t *testing.T) {
	clearEnv(t)
	// The handler hands the body over on a channel so the test blocks on the
	// delivery rather than polling the clock. Buffered and non-blocking: claim-
	// by-rename delivers each batch once, but the handler must never stall a
	// drainer if a second POST ever arrives after the test has stopped reading.
	delivered := make(chan []byte, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := readAll(r)
		select {
		case delivered <- b:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfg := Config{Dir: dir, Endpoint: srv.URL, Version: "0.40.0", Stderr: &bytes.Buffer{}, Interactive: boolPtr(false)}

	// Drained before it spools: appendEvent creates the file and then writes
	// it, and a bare Init's startup drain could scan in that gap, claim the
	// still-empty file, read nothing and remove it, so the write would land on
	// an unlinked inode and nothing would ever POST. With the first drain over
	// before Finish appends, the second Init below is provably the deliverer.
	c := initDrained(t, cfg)
	c.RecordCommand("status").Finish()

	// A later invocation drains what the first one spooled.
	Init(cfg)
	var body []byte
	select {
	case body = <-delivered:
	case <-time.After(deliveryCeiling):
		t.Fatalf("nothing delivered to the endpoint within %v: the startup drain never reached it (a hang, or the POST outran drainDeadline=%v and was abandoned)", deliveryCeiling, drainDeadline)
	}
	if len(body) == 0 {
		t.Fatal("endpoint received an empty body")
	}
	var e Event
	if err := json.Unmarshal([]byte(strings.SplitN(strings.TrimSpace(string(body)), "\n", 2)[0]), &e); err != nil {
		t.Fatalf("delivered payload is not NDJSON: %v", err)
	}
	if e.Command != "status" {
		t.Errorf("delivered command = %q, want status", e.Command)
	}
}

// ── helpers ──────────────────────────────────────────────────

func readAll(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r.Body)
	return buf.Bytes(), err
}

func spooledLines(t *testing.T, spoolDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(spoolDir)
	if os.IsNotExist(err) {
		return nil // never spooled anything: zero events, not a failure
	}
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}
	var lines []string
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(spoolDir, entry.Name()))
		if err != nil {
			t.Fatalf("read spool file: %v", err)
		}
		for _, l := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if l != "" {
				lines = append(lines, l)
			}
		}
	}
	return lines
}

func countSpooledEvents(t *testing.T, spoolDir string) int {
	t.Helper()
	return len(spooledLines(t, spoolDir))
}

func readOneSpooledEvent(t *testing.T, spoolDir string) Event {
	t.Helper()
	lines := spooledLines(t, spoolDir)
	if len(lines) != 1 {
		t.Fatalf("want exactly 1 spooled event, got %d", len(lines))
	}
	var e Event
	if err := json.Unmarshal([]byte(lines[0]), &e); err != nil {
		t.Fatalf("unmarshal spooled event: %v", err)
	}
	return e
}
