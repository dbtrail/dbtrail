package doctor

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// #1527: "not measurable from here" used to be a dead end that also asserted a
// topology the check had only inferred ("the index runs on another host or
// container"), which reads as plainly false on a single-machine install whose
// index data directory is simply not mounted into this process. Every branch
// that gives up now names itself, so the surface reporting it can say what
// would make the volume measurable.
//
// The invariant these cases exist to protect (#948) is unchanged and is what
// makes the new wording safe: the ONLY directory this check ever stats is one
// the operator declared, or the server's own datadir behind the locality gate.
// Nothing here selects a path to measure.
func TestIndexDatadirFree_namesTheBranchItLandedOn(t *testing.T) {
	ctx := context.Background()

	t.Run("declared mount, readable: measured through the mount", func(t *testing.T) {
		t.Setenv(datadirMountEnv, t.TempDir())
		// nil db: the declaration short-circuits before any query, exactly as
		// the bundled stack needs (it reaches the index over TCP only).
		free, ok, reason := indexDatadirFree(ctx, nil, "root:x@tcp(index-mysql:3306)/bintrail_index")
		if !ok || free == 0 {
			t.Fatalf("free=%d known=%v, want a real measurement through the declared mount", free, ok)
		}
		if reason != CapacityFreeFromMount {
			t.Errorf("reason = %q, want %q", reason, CapacityFreeFromMount)
		}
	})

	// The demanded proof: a declaration that points somewhere unrelated to any
	// index is refused, and refused AS a broken declaration. It never silently
	// degrades into a topology guess, and the check never goes looking for
	// another directory to stat instead.
	t.Run("declared mount points somewhere unrelated: refused, and named as the declaration", func(t *testing.T) {
		unrelated := filepath.Join(t.TempDir(), "not-the-index-datadir")
		// A loopback DSN still tries the local path after the declaration
		// fails, so that case gets a server that answers from another host;
		// the other two return before any query (nil db proves it). The
		// loopback row doubles as the precedence proof: a broken declaration
		// must be named ahead of host_unconfirmed too.
		local, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer local.Close()
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("SELECT @@hostname").
			WillReturnRows(sqlmock.NewRows([]string{"@@hostname"}).AddRow("some-other-machine"))
		for dsn, db := range map[string]*sql.DB{
			"root:x@tcp(127.0.0.1:3306)/bintrail_index":   local, // a layout that otherwise looks local
			"root:x@tcp(index-mysql:3306)/bintrail_index": nil,   // and the bundled one
			"root:x@tcp(10.0.0.5:3306)/bintrail_index":    nil,   // and a plainly remote one
		} {
			t.Setenv(datadirMountEnv, unrelated)
			free, ok, reason := indexDatadirFree(ctx, db, dsn)
			if ok || free != 0 {
				t.Fatalf("dsn %s: free=%d known=%v, want nothing measured from an unresolvable declaration", dsn, free, ok)
			}
			if reason != CapacityFreeMountUnusable {
				t.Errorf("dsn %s: reason = %q, want %q (the declaration is what is broken)", dsn, reason, CapacityFreeMountUnusable)
			}
		}
	})

	t.Run("declared mount is a file, not a directory: same refusal", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "not-a-dir")
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv(datadirMountEnv, f)
		if _, ok, reason := indexDatadirFree(ctx, nil, "root:x@tcp(index-mysql:3306)/db"); ok || reason != CapacityFreeMountUnusable {
			t.Errorf("known=%v reason=%q, want an unusable declaration", ok, reason)
		}
	})

	// The bundled stack without the mount: the layout the issue reports. The
	// reason must invite the mount, and the check must still measure NOTHING.
	// nil db proves it: any query would panic, and nothing is stat'd because
	// no path was ever chosen.
	t.Run("bundled index host, no mount declared: invites the mount, measures nothing", func(t *testing.T) {
		t.Setenv(datadirMountEnv, "")
		free, ok, reason := indexDatadirFree(ctx, nil, "root:x@tcp(index-mysql:3306)/bintrail_index")
		if ok || free != 0 {
			t.Fatalf("free=%d known=%v, want nothing measured without a declared mount", free, ok)
		}
		if reason != CapacityFreeMountUnset {
			t.Errorf("reason = %q, want %q", reason, CapacityFreeMountUnset)
		}
	})

	t.Run("index at another address: says so, and invites no mount", func(t *testing.T) {
		t.Setenv(datadirMountEnv, "")
		_, ok, reason := indexDatadirFree(ctx, nil, "root:x@tcp(db.example.com:3306)/bintrail_index")
		if ok {
			t.Fatal("a remote index must never report a local measurement")
		}
		if reason != CapacityFreeIndexNotLocal {
			t.Errorf("reason = %q, want %q", reason, CapacityFreeIndexNotLocal)
		}
	})

	t.Run("unreadable DSN: says unknown rather than guessing", func(t *testing.T) {
		t.Setenv(datadirMountEnv, "")
		if _, ok, reason := indexDatadirFree(ctx, nil, "not a dsn"); ok || reason != CapacityFreeReasonUnknown {
			t.Errorf("known=%v reason=%q, want %q", ok, reason, CapacityFreeReasonUnknown)
		}
	})

	// The tunnel shape: a local ADDRESS whose server is not this machine. The
	// mount suggestion still applies (the operator may well have the index's
	// datadir here), but only with its precondition attached, so this state is
	// its own reason and not folded into mount_unset. A host running a local
	// mysqld AND reaching the real index through a port-forward would
	// otherwise be steered at /var/lib/mysql, and the card would then show a
	// measured number, with live thresholds, for a volume that is not the
	// index's.
	t.Run("loopback DSN, server is not this host: says so, and qualifies the mount", func(t *testing.T) {
		t.Setenv(datadirMountEnv, "")
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT @@hostname").
			WillReturnRows(sqlmock.NewRows([]string{"@@hostname"}).AddRow("some-other-machine"))
		_, ok, reason := indexDatadirFree(ctx, db, "root:x@tcp(127.0.0.1:3306)/bintrail_index")
		if ok {
			t.Fatal("a hostname mismatch must not produce a measurement")
		}
		// The datadir was never stat'd either: the SHOW VARIABLES query is not
		// even expected below.
		if reason != CapacityFreeHostUnconfirmed {
			t.Errorf("reason = %q, want %q", reason, CapacityFreeHostUnconfirmed)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	// The same shape, reached through the OTHER exits that run BEFORE locality
	// is confirmed. A slow or dead link is exactly what a port-forward or a
	// tunnel gives you, and checkIndexCapacity is the last check inside
	// doctor.Build's shared 30s budget, so this is where the deadline lands.
	// Falling back to mount_unset here would hand the unqualified mount advice
	// to the one topology it must never be unqualified for.
	t.Run("loopback DSN, the hostname probe fails: still says the host is unconfirmed", func(t *testing.T) {
		t.Setenv(datadirMountEnv, "")
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT @@hostname").WillReturnError(errors.New("driver: bad connection"))
		if _, ok, reason := indexDatadirFree(ctx, db, "root:x@tcp(127.0.0.1:3306)/bintrail_index"); ok || reason != CapacityFreeHostUnconfirmed {
			t.Errorf("known=%v reason=%q, want %q: locality was never confirmed", ok, reason, CapacityFreeHostUnconfirmed)
		}
	})

	t.Run("loopback DSN, the probe deadline expired: still says the host is unconfirmed", func(t *testing.T) {
		t.Setenv(datadirMountEnv, "")
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		dead, cancel := context.WithCancel(context.Background())
		cancel()
		if _, ok, reason := indexDatadirFree(dead, db, "root:x@tcp(127.0.0.1:3306)/bintrail_index"); ok || reason != CapacityFreeHostUnconfirmed {
			t.Errorf("known=%v reason=%q, want %q: a timed-out probe confirmed nothing", ok, reason, CapacityFreeHostUnconfirmed)
		}
	})

	// A declaration that is broken still outranks the unconfirmed host on
	// those exits too.
	t.Run("declared mount unusable and the hostname probe fails: the declaration is named", func(t *testing.T) {
		t.Setenv(datadirMountEnv, filepath.Join(t.TempDir(), "gone"))
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT @@hostname").WillReturnError(errors.New("driver: bad connection"))
		if _, ok, reason := indexDatadirFree(ctx, db, "root:x@tcp(127.0.0.1:3306)/bintrail_index"); ok || reason != CapacityFreeMountUnusable {
			t.Errorf("known=%v reason=%q, want %q", ok, reason, CapacityFreeMountUnusable)
		}
	})

	t.Run("loopback DSN, same host, datadir unreadable: invites the mount", func(t *testing.T) {
		t.Setenv(datadirMountEnv, "")
		host, err := os.Hostname()
		if err != nil {
			t.Skip("no hostname on this machine")
		}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT @@hostname").
			WillReturnRows(sqlmock.NewRows([]string{"@@hostname"}).AddRow(host))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'datadir'").
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
				AddRow("datadir", filepath.Join(t.TempDir(), "gone")))
		if _, ok, reason := indexDatadirFree(ctx, db, "root:x@tcp(127.0.0.1:3306)/bintrail_index"); ok || reason != CapacityFreeMountUnset {
			t.Errorf("known=%v reason=%q, want %q", ok, reason, CapacityFreeMountUnset)
		}
	})

	t.Run("loopback DSN, same host, datadir readable: measured through the datadir", func(t *testing.T) {
		t.Setenv(datadirMountEnv, "")
		host, err := os.Hostname()
		if err != nil {
			t.Skip("no hostname on this machine")
		}
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("SELECT @@hostname").
			WillReturnRows(sqlmock.NewRows([]string{"@@hostname"}).AddRow(host))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'datadir'").
			WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("datadir", t.TempDir()))
		free, ok, reason := indexDatadirFree(ctx, db, "root:x@tcp(127.0.0.1:3306)/bintrail_index")
		if !ok || free == 0 {
			t.Fatalf("free=%d known=%v, want the local datadir measured", free, ok)
		}
		if reason != CapacityFreeFromDatadir {
			t.Errorf("reason = %q, want %q", reason, CapacityFreeFromDatadir)
		}
	})
}

// The capacity check is advisory: it must never turn a working install into a
// failing one. The free-space REASON is reported and never graded on, so a
// missing mount cannot become a WARN.
func TestClassifyCapacity_freeReasonNeverMovesTheGrade(t *testing.T) {
	p := capacityProjection{eventsPerDay: 24000, bytesPerEvent: 1000, projectedBytes: 720_000_000, currentBytes: 6_000_000, sampleHours: 6}
	retain := 30 * 24 * time.Hour

	for _, r := range []CapacityFreeReason{CapacityFreeMountUnset, CapacityFreeMountUnusable, CapacityFreeIndexNotLocal, CapacityFreeHostUnconfirmed, CapacityFreeReasonUnknown, ""} {
		m := classifyCapacity(p, true, retain, true, 0, false, r)
		if m.Status != StatusSkip || m.Reason != CapacityFreeUnknown {
			t.Errorf("reason %q graded %s/%s, want skip/free_unknown", r, m.Status, m.Reason)
		}
		if m.FreeReason != r {
			t.Errorf("FreeReason = %q, want %q carried through untouched", m.FreeReason, r)
		}
	}
	for _, r := range []CapacityFreeReason{CapacityFreeFromMount, CapacityFreeFromDatadir} {
		m := classifyCapacity(p, true, retain, true, 2_000_000_000, true, r)
		if m.Status != StatusPass || m.Reason != CapacityOK {
			t.Errorf("reason %q graded %s/%s, want pass/ok whichever path measured it", r, m.Status, m.Reason)
		}
	}
}

// The CLI check's own text: what it could not see and what would fix it,
// never where it guessed the index runs.
func TestCapacityCheckResult_freeUnknownNamesTheFixNotATopology(t *testing.T) {
	p := capacityProjection{eventsPerDay: 24000, bytesPerEvent: 1000, projectedBytes: 720_000_000, currentBytes: 6_000_000, sampleHours: 6}

	cases := []struct {
		reason   CapacityFreeReason
		wantAll  []string
		wantNone []string
	}{
		{CapacityFreeMountUnset,
			[]string{"not measurable from here", datadirMountEnv, "docker-compose.yml", "read-only"},
			[]string{"another host", "separate host"}},
		{CapacityFreeMountUnusable,
			[]string{datadirMountEnv, "cannot read"},
			[]string{"another host", "separate host"}},
		{CapacityFreeIndexNotLocal,
			[]string{"another address", "wrong volume"},
			// No mount suggestion here: a mount that is not the index's would
			// measure the wrong filesystem, which is worse than measuring
			// nothing.
			[]string{datadirMountEnv, "another host", "separate host"}},
		{CapacityFreeHostUnconfirmed,
			// Names the fix AND its precondition: this is the one state where
			// following the advice blindly could measure the wrong volume.
			[]string{"cannot confirm", datadirMountEnv, "OWN data directory"},
			[]string{"another host", "separate host"}},
		{CapacityFreeReasonUnknown,
			[]string{"cannot see the index volume"},
			[]string{datadirMountEnv, "another host", "separate host"}},
	}
	for _, tc := range cases {
		r := capacityVerdict(p, 30*24*time.Hour, 0, false, tc.reason)
		if r.Status != StatusSkip {
			t.Errorf("reason %q: status = %s, want skip", tc.reason, r.Status)
		}
		if !strings.Contains(r.Detail, "projected steady-state") {
			t.Errorf("reason %q: detail dropped the projection: %s", tc.reason, r.Detail)
		}
		for _, want := range tc.wantAll {
			if !strings.Contains(r.Detail, want) {
				t.Errorf("reason %q: detail is missing %q: %s", tc.reason, want, r.Detail)
			}
		}
		for _, bad := range tc.wantNone {
			if strings.Contains(r.Detail, bad) {
				t.Errorf("reason %q: detail must not say %q: %s", tc.reason, bad, r.Detail)
			}
		}
	}
}

// bundledIndexHost only ever words a message, but it words it for the exact
// population the issue is about, so a compose rename must not silently drop
// them back to the generic text.
func TestBundledIndexHostMatchesCompose(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	compose := string(b)
	if !strings.Contains(compose, "tcp("+bundledIndexHost+":3306)") {
		t.Errorf("docker-compose.yml no longer builds a tcp(%s:3306) index DSN: the bundled stack would fall back to the generic reason", bundledIndexHost)
	}
	if !strings.Contains(compose, datadirMountEnv) {
		t.Errorf("docker-compose.yml no longer sets %s: the guidance names a variable the shipped stack does not wire", datadirMountEnv)
	}
}

// #2263, the reported case: 63.7 GB a day under a 6 h window, the index flat
// at its steady size, 84.7 GB free. Free space is five times the index and
// still under three days of gross writes, so the floor warns (a stalled
// rotation fills the volume in 1.3 days). What it must not do is call that a
// date the disk fills: the index is not growing.
func TestClassifyCapacity_shortWindowAtSteadyState(t *testing.T) {
	const gb = 1_000_000_000.0
	p := capacityProjection{eventsPerDay: 63_700_000, bytesPerEvent: 1000, projectedBytes: 15.9 * gb, currentBytes: uint64(16.5 * gb), sampleHours: 6}
	free := uint64(84.7 * gb)

	m := classifyCapacity(p, true, 6*time.Hour, true, free, true, CapacityFreeFromMount)
	if m.Status != StatusWarn || m.Reason != CapacityFreeUnderFloor {
		t.Fatalf("graded %s/%s, want warn/free_under_floor: the floor does not scale with the window", m.Status, m.Reason)
	}
	if !m.FreeDaysKnown || m.FreeDays < 1.3 || m.FreeDays > 1.4 {
		t.Fatalf("FreeDays = %v known=%v, want ≈1.33", m.FreeDays, m.FreeDaysKnown)
	}
	if d, ok := m.DaysUntilFull(); ok {
		t.Fatalf("DaysUntilFull = %v under a 6 h window, want not reported", d)
	}
	if detail := capacityCheckResult(m, "binlog_index", "").Detail; strings.Contains(detail, "until the volume fills") {
		t.Fatalf("doctor text forecasts a full disk at steady state:\n%s", detail)
	}

	// The three retention cases. Unknown is not "none": the window may be set.
	if _, ok := classifyCapacity(p, true, 0, false, free, true, CapacityFreeFromMount).DaysUntilFull(); ok {
		t.Error("retention unknown: DaysUntilFull reported, want not reported")
	}
	none := classifyCapacity(p, true, 0, true, free, true, CapacityFreeFromMount)
	if d, ok := none.DaysUntilFull(); !ok || d < 1.3 || d > 1.4 {
		t.Errorf("no retention: DaysUntilFull = %v ok=%v, want ≈1.33 reported", d, ok)
	}
	if detail := capacityCheckResult(none, "binlog_index", "").Detail; !strings.Contains(detail, "until the volume fills") {
		t.Errorf("no retention: doctor text lost the forecast:\n%s", detail)
	}
}
