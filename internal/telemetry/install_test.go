package telemetry

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// beaconClient builds a client on dir at a given version and clock. Each call
// is a fresh PROCESS as far as the once-a-day cap is concerned, which is what
// an upgrade is.
func beaconClient(t *testing.T, dir, version string, clock *atomic.Int64) *Client {
	t.Helper()
	return initDrained(t, Config{
		Dir: dir, Endpoint: "http://127.0.0.1:1", Version: version,
		Stderr: &bytes.Buffer{}, Interactive: boolPtr(false),
		Now: func() time.Time { return time.Unix(0, clock.Load()).UTC() },
	})
}

// spooledEvents reads every spooled event, oldest file first.
func spooledEvents(t *testing.T, spoolDir string) []Event {
	t.Helper()
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	var out []Event
	for _, ent := range entries {
		f, err := os.Open(filepath.Join(spoolDir, ent.Name()))
		if err != nil {
			t.Fatalf("open spool file: %v", err)
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			var e Event
			if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
				t.Fatalf("spooled line is not an event: %v", err)
			}
			out = append(out, e)
		}
		f.Close()
	}
	return out
}

func day(n int) int64 {
	return time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC).Add(time.Duration(n) * 24 * time.Hour).UnixNano()
}

func TestInstallAgeBucket(t *testing.T) {
	first := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		days int
		want string
	}{
		{0, AgeUnder7d}, {6, AgeUnder7d},
		{7, Age7to30d}, {29, Age7to30d},
		{30, Age30to90d}, {89, Age30to90d},
		{90, AgeOver90d}, {4000, AgeOver90d},
		// A first-seen date in the future is a clock that moved backwards.
		// It is not an age, so it reads as the youngest bucket rather than
		// as a negative number of days.
		{-1, AgeUnder7d}, {-400, AgeUnder7d},
	} {
		if got := installAgeBucket(first, first.AddDate(0, 0, tc.days)); got != tc.want {
			t.Errorf("age after %d days = %q, want %q", tc.days, got, tc.want)
		}
	}
}

// TestBeaconReportsInstallAge: the first beacon starts the clock, and later
// beacons from later PROCESSES (the daemon restarted, the container recreated)
// keep counting from that same day.
func TestBeaconReportsInstallAge(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64

	for _, step := range []struct {
		day  int
		want string
	}{
		{0, AgeUnder7d}, {8, Age7to30d}, {45, Age30to90d}, {120, AgeOver90d},
	} {
		clock.Store(day(step.day))
		beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")
		evs := spooledEvents(t, SpoolDir(dir))
		if got := evs[len(evs)-1].InstallAge; got != step.want {
			t.Errorf("day %d: install_age = %q, want %q", step.day, got, step.want)
		}
	}
}

// TestBeaconReportsPreviousVersionOnce: the first beacon on a new version names
// the version it came from, and no later beacon repeats it.
func TestBeaconReportsPreviousVersionOnce(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64

	type step struct {
		day     int
		version string
		want    string
	}
	for _, s := range []step{
		{0, "v0.102.0", ""},      // first ever: nothing came before
		{1, "v0.102.0", ""},      // same version, another day
		{2, "v0.102.3", ""},      // a patch release: same major.minor, not a move
		{3, "v0.104.0", "0.102"}, // the upgrade
		{4, "v0.104.0", ""},      // reported once, never again
		{5, "v0.103.0", "0.104"}, // a downgrade is a move too
		// A source build has no version worth naming and must not erase the
		// one this install really came from.
		{6, "dev-abc123", ""},
		{7, "v0.105.0", "0.103"},
	} {
		clock.Store(day(s.day))
		beaconClient(t, dir, s.version, &clock).Beacon("watch")
		evs := spooledEvents(t, SpoolDir(dir))
		last := evs[len(evs)-1]
		if last.PreviousVersion != s.want {
			t.Errorf("day %d on %s: previous_version = %q, want %q", s.day, s.version, last.PreviousVersion, s.want)
		}
	}
}

// TestInstallFieldsOnlyOnBeacons: the two fields describe the install, and a
// command event carrying them next to its run_id would tie a command sequence
// to an install's age.
func TestInstallFieldsOnlyOnBeacons(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	beaconClient(t, dir, "v0.102.0", &clock).Beacon("watch")
	clock.Store(day(1))
	c := beaconClient(t, dir, "v0.104.0", &clock)
	c.RecordCommand("status").Finish()
	c.RecordDaemonCommand("console-sql").Finish()

	for _, e := range spooledEvents(t, SpoolDir(dir)) {
		if e.EventType == EventDaemonBeacon {
			continue
		}
		if e.InstallAge != "" || e.PreviousVersion != "" {
			t.Errorf("%s event carries install fields: %+v", e.EventType, e)
		}
	}
}

// TestBeaconNeverRewritesTheConsentFile: the install record is its own file so
// that the daily beacon can never write a stale copy of the consent file over
// an opt-out recorded a moment earlier by another process.
func TestBeaconNeverRewritesTheConsentFile(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	if err := SetEnabled(dir, true); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	before, err := os.ReadFile(StatePath(dir))
	if err != nil {
		t.Fatalf("read consent file: %v", err)
	}
	var clock atomic.Int64
	clock.Store(day(0))
	beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")

	after, err := os.ReadFile(StatePath(dir))
	if err != nil {
		t.Fatalf("read consent file after the beacon: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("the beacon rewrote the consent file:\nbefore: %s\nafter:  %s", before, after)
	}
	if _, err := os.Stat(InstallPath(dir)); err != nil {
		t.Errorf("the beacon did not write the install record: %v", err)
	}
}

// TestDisabledBeaconWritesNoInstallRecord: with telemetry off nothing at all is
// written, the install record included.
func TestDisabledBeaconWritesNoInstallRecord(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvVar, "off")
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")
	if _, err := os.Stat(InstallPath(dir)); !os.IsNotExist(err) {
		t.Errorf("a disabled client wrote the install record (stat err = %v)", err)
	}
}

// TestOptOutDeletesTheInstallRecord: opting out leaves nothing of telemetry's
// on disk except the choice itself.
func TestOptOutDeletesTheInstallRecord(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")
	if _, err := os.Stat(InstallPath(dir)); err != nil {
		t.Fatalf("precondition: no install record: %v", err)
	}
	if err := SetEnabled(dir, false); err != nil {
		t.Fatalf("SetEnabled(false): %v", err)
	}
	if _, err := os.Stat(InstallPath(dir)); !os.IsNotExist(err) {
		t.Errorf("the install record survived an opt-out (stat err = %v)", err)
	}
}

// TestUnspooledBeaconDoesNotConsumeTheUpgrade: previous_version is reported on
// ONE beacon. If that beacon could not be spooled, recording the new version
// anyway would spend the signal on an event that never existed.
func TestUnspooledBeaconDoesNotConsumeTheUpgrade(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	beaconClient(t, dir, "v0.102.0", &clock).Beacon("watch")

	// Day 1, upgraded, but the spool cannot be written: a file sits where
	// the day's spool directory entry has to be created.
	clock.Store(day(1))
	spool := SpoolDir(dir)
	if err := os.RemoveAll(spool); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(spool, []byte("in the way"), 0o600); err != nil {
		t.Fatal(err)
	}
	beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")

	// Day 2, the spool works again: this is the first beacon that exists.
	clock.Store(day(2))
	if err := os.Remove(spool); err != nil {
		t.Fatal(err)
	}
	beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")
	evs := spooledEvents(t, spool)
	if len(evs) != 1 {
		t.Fatalf("spooled %d events, want the one from day 2", len(evs))
	}
	if evs[0].PreviousVersion != "0.102" {
		t.Errorf("previous_version = %q, want 0.102: the failed beacon consumed the upgrade", evs[0].PreviousVersion)
	}
}

// TestTamperedInstallRecord: the file is on the operator's disk and its
// contents go on the wire. Each row is a file, the age its first_seen may
// produce, and the previous_version it may produce (the build is 0.104).
func TestTamperedInstallRecord(t *testing.T) {
	const good = `"first_seen":"2026-07-01"` // 40 days before the beacon below
	for name, tc := range map[string]struct {
		content  string
		wantAge  string
		wantPrev string
	}{
		"intact":                      {`{` + good + `,"last_versions":{"watch":"0.102"}}`, Age30to90d, "0.102"},
		"not json":                    {"{{{", AgeUnder7d, ""},
		"empty":                       {"", AgeUnder7d, ""},
		"a version with text":         {`{` + good + `,"last_versions":{"watch":"0.102; DROP"}}`, Age30to90d, ""},
		"a full triple":               {`{` + good + `,"last_versions":{"watch":"0.102.3"}}`, Age30to90d, ""},
		"a long number":               {`{` + good + `,"last_versions":{"watch":"84729135550123.99887766"}}`, Age30to90d, ""},
		"major one digit too long":    {`{` + good + `,"last_versions":{"watch":"1234.1"}}`, Age30to90d, ""},
		"minor one digit too long":    {`{` + good + `,"last_versions":{"watch":"1.12345"}}`, Age30to90d, ""},
		"the longest that is allowed": {`{` + good + `,"last_versions":{"watch":"999.9999"}}`, Age30to90d, "999.9999"},
		"digits of another script":    {`{` + good + `,"last_versions":{"watch":"٠.١٢"}}`, Age30to90d, ""},
		"another daemon's version":    {`{` + good + `,"last_versions":{"shim":"0.102"}}`, Age30to90d, ""},
		"a daemon name that is text":  {`{` + good + `,"last_versions":{"Watch; x":"0.102"}}`, Age30to90d, ""},
		// One field of the wrong type must not take the other with it.
		"versions of the wrong type": {`{` + good + `,"last_versions":104}`, Age30to90d, ""},
		"a date of the wrong type":   {`{"first_seen":20260701,"last_versions":{"watch":"0.102"}}`, AgeUnder7d, "0.102"},
		"a date with a time":         {`{"first_seen":"2026-07-01T10:00:00Z","last_versions":{"watch":"0.102"}}`, AgeUnder7d, "0.102"},
		"a date that is text":        {`{"first_seen":"yesterday","last_versions":{"watch":"0.102"}}`, AgeUnder7d, "0.102"},
	} {
		t.Run(name, func(t *testing.T) {
			clearEnv(t)
			dir := t.TempDir()
			if err := os.WriteFile(InstallPath(dir), []byte(tc.content), 0o600); err != nil {
				t.Fatal(err)
			}
			var clock atomic.Int64
			clock.Store(day(40))
			beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")
			evs := spooledEvents(t, SpoolDir(dir))
			if len(evs) != 1 {
				t.Fatalf("spooled %d events, want 1", len(evs))
			}
			if got := evs[0].InstallAge; got != tc.wantAge {
				t.Errorf("install_age = %q, want %q", got, tc.wantAge)
			}
			if got := evs[0].PreviousVersion; got != tc.wantPrev {
				t.Errorf("previous_version = %q, want %q", got, tc.wantPrev)
			}
		})
	}
}

// TestTooManyDaemonNamesDropsTheVersions: there are fewer than ten daemons, so
// a file naming more than the bound was not written by this program.
func TestTooManyDaemonNamesDropsTheVersions(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	versions := map[string]string{"watch": "0.102"}
	for i := range maxInstallDaemons {
		versions[fmt.Sprintf("d%d", i)] = "0.1"
	}
	data, _ := json.Marshal(installRecord{FirstSeen: "2026-07-01", Versions: versions})
	if err := os.WriteFile(InstallPath(dir), data, 0o600); err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(day(40))
	beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")
	if e := spooledEvents(t, SpoolDir(dir))[0]; e.PreviousVersion != "" || e.InstallAge != Age30to90d {
		t.Errorf("beacon from an over-full record: %+v", e)
	}
}

// TestUnreadableInstallRecordIsLeftAlone: a file this process may not read is
// not "no record". Starting over would reset the install's clock and overwrite
// a file it was not allowed to see.
func TestUnreadableInstallRecordIsLeftAlone(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-000 file")
	}
	clearEnv(t)
	dir := t.TempDir()
	content := []byte(`{"first_seen":"2026-01-01","last_versions":{"watch":"0.102"}}`)
	if err := os.WriteFile(InstallPath(dir), content, 0o000); err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(day(40))
	beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")

	e := spooledEvents(t, SpoolDir(dir))[0]
	if e.InstallAge != "" || e.PreviousVersion != "" {
		t.Errorf("beacon carries install fields from a record that could not be read: %+v", e)
	}
	if err := os.Chmod(InstallPath(dir), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(InstallPath(dir)); !bytes.Equal(got, content) {
		t.Errorf("the unreadable record was overwritten: %s", got)
	}
}

// TestUnsavableRecordSendsNoInstallFields: the fields are only worth sending
// when the process can remember that it sent them. With a folder it cannot
// write and a spool it can, saving nothing and sending anyway would repeat the
// same previous_version every day and hold install_age at its youngest value.
func TestUnsavableRecordSendsNoInstallFields(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes into a mode-500 directory")
	}
	clearEnv(t)
	dir := t.TempDir()
	if err := os.WriteFile(InstallPath(dir), []byte(`{"first_seen":"2026-07-01","last_versions":{"watch":"0.102"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(SpoolDir(dir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	var clock atomic.Int64
	for d := 40; d < 43; d++ {
		clock.Store(day(d))
		beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")
		evs := spooledEvents(t, SpoolDir(dir))
		e := evs[len(evs)-1]
		if e.EventType != EventDaemonBeacon {
			t.Fatalf("day %d: no beacon spooled: %+v", d, e)
		}
		if e.InstallAge != "" || e.PreviousVersion != "" {
			t.Errorf("day %d: beacon carries install fields it could not record as sent: %+v", d, e)
		}
	}
}

// TestBeaconDroppedAtTheSpoolCapDoesNotConsumeTheUpgrade: the day's spool file
// is full, so the beacon is dropped. The record must not say it was sent.
func TestBeaconDroppedAtTheSpoolCapDoesNotConsumeTheUpgrade(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	beaconClient(t, dir, "v0.102.0", &clock).Beacon("watch")

	clock.Store(day(1))
	c := beaconClient(t, dir, "v0.104.0", &clock) // built first: its start drains the spool
	full := filepath.Join(SpoolDir(dir), time.Unix(0, day(1)).UTC().Format("2006-01-02")+spoolSuffix)
	if err := os.MkdirAll(SpoolDir(dir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, bytes.Repeat([]byte("x"), maxSpoolFileBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	c.Beacon("watch")
	if err := os.Remove(full); err != nil {
		t.Fatal(err)
	}

	clock.Store(day(2))
	beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")
	evs := spooledEvents(t, SpoolDir(dir))
	if len(evs) != 1 || evs[0].PreviousVersion != "0.102" {
		t.Errorf("after a beacon dropped at the cap, the next one is %+v; want previous_version 0.102", evs)
	}
}

// TestTwoDaemonsAtDifferentVersionsDoNotReportEachOther: `watch` and `shim`
// can share one folder and be upgraded at different times. With one shared
// version each would see the other's as "previous", every day.
func TestTwoDaemonsAtDifferentVersionsDoNotReportEachOther(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64
	last := func() Event { evs := spooledEvents(t, SpoolDir(dir)); return evs[len(evs)-1] }

	for d := range 4 {
		clock.Store(day(d))
		beaconClient(t, dir, "v0.105.0", &clock).Beacon("watch")
		if e := last(); e.PreviousVersion != "" {
			t.Errorf("day %d: watch reports previous_version %q with no change of its own", d, e.PreviousVersion)
		}
		beaconClient(t, dir, "v0.104.0", &clock).Beacon("shim")
		if e := last(); e.PreviousVersion != "" {
			t.Errorf("day %d: shim reports previous_version %q with no change of its own", d, e.PreviousVersion)
		}
	}
	// And each still reports its OWN change, once.
	clock.Store(day(5))
	beaconClient(t, dir, "v0.105.0", &clock).Beacon("shim")
	if e := last(); e.PreviousVersion != "0.104" {
		t.Errorf("shim's own upgrade: previous_version = %q, want 0.104", e.PreviousVersion)
	}
}

// TestRunningDaemonFollowsARecordedOptOut: `telemetry off` typed next to a
// daemon that has been up for weeks. The daemon resolved consent at start; it
// has to notice the file, stop, and leave nothing behind.
func TestRunningDaemonFollowsARecordedOptOut(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	c := beaconClient(t, dir, "v0.104.0", &clock)
	c.Beacon("watch")

	// Another process records the opt-out, exactly as the CLI does.
	if err := SetEnabled(dir, false); err != nil {
		t.Fatal(err)
	}
	if err := PurgeSpool(dir); err != nil {
		t.Fatal(err)
	}
	// The daemon, unaware, records one more action before its next tick.
	c.RecordDaemonCommand("console-sql").Finish()

	clock.Store(day(30))
	c.followRecordedOptOut() // what every tick does before its beacon
	c.Beacon("watch")

	if c.Enabled() {
		t.Error("the daemon is still reporting after an opt-out was recorded")
	}
	if _, err := os.Stat(InstallPath(dir)); !os.IsNotExist(err) {
		t.Errorf("the install record is back after the opt-out (stat err = %v)", err)
	}
	if entries, _ := os.ReadDir(SpoolDir(dir)); len(entries) != 0 {
		t.Errorf("events recorded after the opt-out were left in the spool: %v", entries)
	}
}

// TestRecordedOptOutDoesNotOverrideAHigherControl: BINTRAIL_TELEMETRY=on in
// the daemon's environment outranks the file, at start and afterwards.
func TestRecordedOptOutDoesNotOverrideAHigherControl(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvVar, "on")
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	c := beaconClient(t, dir, "v0.104.0", &clock)
	if err := SetEnabled(dir, false); err != nil {
		t.Fatal(err)
	}
	c.followRecordedOptOut()
	if !c.Enabled() {
		t.Error("the consent file overrode BINTRAIL_TELEMETRY=on in a running daemon")
	}
}

// TestOptOutWaitsForABeaconInFlight: SetRuntimeConsent(false) must not return
// while a beacon is between its consent check and its last write, or the
// caller would delete the install record and the beacon would write it back.
func TestOptOutWaitsForABeaconInFlight(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	for range 200 {
		c := beaconClient(t, dir, "v0.104.0", &clock)
		done := make(chan struct{})
		go func() { defer close(done); c.Beacon("watch") }()
		// The web interface's order: stop the process, then clean the disk.
		c.SetRuntimeConsent(false)
		if err := SetEnabled(dir, false); err != nil {
			t.Fatal(err)
		}
		if err := PurgeSpool(dir); err != nil {
			t.Fatal(err)
		}
		<-done
		if _, err := os.Stat(InstallPath(dir)); !os.IsNotExist(err) {
			t.Fatalf("a beacon wrote the install record back after the opt-out (stat err = %v)", err)
		}
		if entries, _ := os.ReadDir(SpoolDir(dir)); len(entries) != 0 {
			t.Fatalf("a beacon was spooled after the opt-out: %v", entries)
		}
		if err := os.Remove(StatePath(dir)); err != nil { // next round starts enabled
			t.Fatal(err)
		}
	}
}

func TestInstallRecordPermissions(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	beaconClient(t, dir, "v0.104.0", &clock).Beacon("watch")
	info, err := os.Stat(InstallPath(dir))
	if err != nil {
		t.Fatalf("stat install record: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("install record mode = %o, want 600", perm)
	}
}

// TestSampleBeaconShowsWhatTheNextBeaconWouldCarry: `telemetry show` reads the
// real record, so a pending version change is visible before it is sent, and
// rendering it neither starts the install's clock nor spends the change.
func TestSampleBeaconShowsWhatTheNextBeaconWouldCarry(t *testing.T) {
	clearEnv(t)
	prevBuild := buildVersion
	buildVersion = "v0.104.0"
	t.Cleanup(func() { buildVersion = prevBuild })

	dir := t.TempDir()
	if e := SampleBeacon(dir); e.EventType != EventDaemonBeacon || e.RunID != "" || e.InstallAge != AgeUnder7d || e.PreviousVersion != "" {
		t.Errorf("sample with no record: %+v", e)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("rendering the sample wrote to the config directory: %v", entries)
	}

	old := time.Now().UTC().AddDate(0, 0, -200).Format(firstSeenLayout)
	content := []byte(`{"first_seen":"` + old + `","last_versions":{"watch":"0.102"}}`)
	if err := os.WriteFile(InstallPath(dir), content, 0o600); err != nil {
		t.Fatal(err)
	}
	if e := SampleBeacon(dir); e.InstallAge != AgeOver90d || e.PreviousVersion != "0.102" {
		t.Errorf("sample did not read the record: %+v", e)
	}
	if got, _ := os.ReadFile(InstallPath(dir)); !bytes.Equal(got, content) {
		t.Errorf("rendering the sample rewrote the record: %s", got)
	}
}

// TestSampleBeaconWithNoDirectoryReadsNothing: with no config directory the
// record's path would be relative, and must not be looked up in whatever
// directory the command was started from.
func TestSampleBeaconWithNoDirectoryReadsNothing(t *testing.T) {
	clearEnv(t)
	prevBuild := buildVersion
	buildVersion = "v0.104.0"
	t.Cleanup(func() { buildVersion = prevBuild })

	cwd := t.TempDir()
	old := time.Now().UTC().AddDate(0, 0, -200).Format(firstSeenLayout)
	if err := os.WriteFile(filepath.Join(cwd, "telemetry-install.json"),
		[]byte(`{"first_seen":"`+old+`","last_versions":{"watch":"9.9"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(cwd)
	if e := SampleBeacon(""); e.InstallAge != AgeUnder7d || e.PreviousVersion != "" {
		t.Errorf("the sample read a record from the current directory: %+v", e)
	}
}

// TestBeaconChecksConsentAgainUnderTheLock: a beacon that passed its first
// consent check and is waiting for the lock while an opt-out takes effect must
// find it there and write nothing. The test holds the lock itself, so the
// order is fixed rather than left to the scheduler.
func TestBeaconChecksConsentAgainUnderTheLock(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	var clock atomic.Int64
	clock.Store(day(0))
	c := beaconClient(t, dir, "v0.104.0", &clock)

	c.mu.Lock()
	done := make(chan struct{})
	go func() { defer close(done); c.Beacon("watch") }()
	time.Sleep(100 * time.Millisecond) // the beacon is past its first check, parked on c.mu
	c.enabled.Store(false)             // the opt-out, exactly as SetRuntimeConsent applies it
	c.mu.Unlock()
	<-done

	if _, err := os.Stat(InstallPath(dir)); !os.IsNotExist(err) {
		t.Errorf("a beacon parked across an opt-out wrote the install record (stat err = %v)", err)
	}
	if entries, _ := os.ReadDir(SpoolDir(dir)); len(entries) != 0 {
		t.Errorf("a beacon parked across an opt-out was spooled: %v", entries)
	}
}
