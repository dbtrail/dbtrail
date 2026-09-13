package console

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stubRestorer records the last request and returns a scripted error.
type stubRestorer struct {
	err  error
	last *BaselineRestoreRequest
	st   BaselineStatus
}

func (s *stubRestorer) TriggerRestore(req BaselineRestoreRequest) error {
	s.last = &req
	return s.err
}
func (s *stubRestorer) RestoreStatus(string) BaselineStatus {
	if s.st.State == "" {
		return BaselineStatus{State: "idle"}
	}
	return s.st
}

func newRestoreServer(t *testing.T, restorer BaselineRestorer) *Server {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, BaselineRestore: restorer,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func addRestoreEntry(t *testing.T, srv *Server, baselineDir string) string {
	t.Helper()
	e, err := srv.cm.reg.Add(ServerEntry{
		Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/binlog_index",
		SourceDSN: "src:pw@tcp(127.0.0.1:3306)/", BaselineDir: baselineDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e.ID
}

func TestBaselineRestore_gates(t *testing.T) {
	// No restorer wired: the standalone console refuses.
	srvOff := newRestoreServer(t, nil)
	idOff := addRestoreEntry(t, srvOff, t.TempDir())
	rec, body := doServersReq(t, srvOff, "POST", "/api/servers/"+idOff+"/baseline/restore", `{"at":"2026-06-10 12:00:00"}`)
	if rec.Code != 403 {
		t.Fatalf("no restorer: code=%d body=%s, want 403", rec.Code, body)
	}

	stub := &stubRestorer{}
	srv := newRestoreServer(t, stub)

	// S3-only entry: the fold needs a local directory.
	idS3, err := srv.cm.reg.Add(ServerEntry{Name: "s3only", DSN: "i:p@tcp(h:3306)/idx",
		SourceDSN: "s:p@tcp(h:3306)/", BaselineS3: "s3://b/baselines"})
	if err != nil {
		t.Fatal(err)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+idS3.ID+"/baseline/restore", `{"at":"2026-06-10 12:00:00"}`)
	if rec.Code != 400 {
		t.Fatalf("s3-only: code=%d body=%s, want 400", rec.Code, body)
	}

	dir := t.TempDir()
	id := addRestoreEntry(t, srv, dir)

	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline/restore", `{"at":"lunes"}`)
	if rec.Code != 400 {
		t.Fatalf("bad at: code=%d body=%s, want 400", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline/restore", `{"at":"2999-01-01 00:00:00"}`)
	if rec.Code != 400 {
		t.Fatalf("future at: code=%d body=%s, want 400", rec.Code, body)
	}

	// A snapshot already at exactly that instant: refuse rather than collide.
	// (A bare directory has neither marker, which is legacy-complete — the
	// same rule SnapshotComplete applies everywhere.)
	if err := os.MkdirAll(filepath.Join(dir, "2026-06-10T12-00-00Z"), 0o755); err != nil {
		t.Fatal(err)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline/restore", `{"at":"2026-06-10 12:00:00"}`)
	if rec.Code != 409 {
		t.Fatalf("collision: code=%d body=%s, want 409", rec.Code, body)
	}

	// An _INCOMPLETE leftover from a failed fold is the retry-the-same-instant
	// case the engine supports on purpose; refusing it would strand the
	// operator on an instant the listing does not even show.
	leftover := filepath.Join(dir, "2026-06-10T10-00-00Z")
	if err := os.MkdirAll(leftover, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(leftover, "_INCOMPLETE"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline/restore", `{"at":"2026-06-10 10:00:00"}`)
	if rec.Code != 202 {
		t.Fatalf("incomplete leftover: code=%d body=%s, want 202 (retry allowed)", rec.Code, body)
	}

	// A failed fold that ALSO left converted tables behind is the shape the
	// engine refuses (its retry rule tolerates only the marker): a 202 here
	// would promise work that cannot happen.
	dirty := filepath.Join(dir, "2026-06-10T09-00-00Z")
	if err := os.MkdirAll(filepath.Join(dirty, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"_INCOMPLETE": "", "shop/orders.parquet": "x"} {
		if err := os.WriteFile(filepath.Join(dirty, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline/restore", `{"at":"2026-06-10 09:00:00"}`)
	if rec.Code != 409 || !strings.Contains(string(body), "left files behind") {
		t.Fatalf("dirty leftover: code=%d body=%s, want 409 naming the leftover", rec.Code, body)
	}

	// Accepted: the request reaches the restorer with the parsed instant.
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline/restore", `{"at":"2026-06-10 11:30:00"}`)
	if rec.Code != 202 {
		t.Fatalf("accept: code=%d body=%s, want 202", rec.Code, body)
	}
	want := time.Date(2026, 6, 10, 11, 30, 0, 0, time.UTC)
	if stub.last == nil || !stub.last.At.Equal(want) || stub.last.BaselineDir != dir {
		t.Fatalf("restorer got %+v, want At=%s dir=%s", stub.last, want, dir)
	}

	// Another job in flight maps to 409.
	stub.err = ErrBaselineRunning
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline/restore", `{"at":"2026-06-10 11:00:00"}`)
	if rec.Code != 409 {
		t.Fatalf("busy: code=%d body=%s, want 409", rec.Code, body)
	}
}

func TestBaselineRestore_statusAndCapability(t *testing.T) {
	stub := &stubRestorer{st: BaselineStatus{State: "running", At: "2026-06-10T11:30:00Z"}}
	srv := newRestoreServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())

	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/baseline/restore", "")
	if rec.Code != 200 {
		t.Fatalf("status: code=%d body=%s", rec.Code, body)
	}
	var got struct {
		Restore BaselineStatus `json:"restore"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Restore.State != "running" || got.Restore.At != "2026-06-10T11:30:00Z" {
		t.Fatalf("status = %+v", got.Restore)
	}

	rec, body = doServersReq(t, srv, "GET", "/api/capabilities", "")
	if rec.Code != 200 {
		t.Fatal("capabilities")
	}
	var caps capabilitiesResponse
	if err := json.Unmarshal(body, &caps); err != nil {
		t.Fatal(err)
	}
	if !caps.BaselineRestore {
		t.Fatal("baseline_restore capability must be true with a restorer wired")
	}
	srvOff := newRestoreServer(t, nil)
	srvOff.cm.boot = &bundle{}
	rec, body = doServersReq(t, srvOff, "GET", "/api/capabilities", "")
	if err := json.Unmarshal(body, &caps); err != nil {
		t.Fatal(err)
	}
	if caps.BaselineRestore {
		t.Fatal("baseline_restore capability must be false without a restorer")
	}
}

func TestBaselineRunHistory_roundTripAndCap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-baseline-history.json")
	h, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < BaselineRunHistoryCap+5; i++ {
		rec := BaselineRunRecord{ServerID: "srv1", Kind: BaselineRunRefresh,
			StartedAt: "2026-06-10T11:00:00Z", FinishedAt: "2026-06-10T11:02:30Z",
			SnapshotTime: time.Date(2026, 6, 10, 11, 0, i, 0, time.UTC).Format(time.RFC3339), Tables: i}
		if err := h.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	// Reload from disk: capped, newest survive, lookup by snapshot works.
	h2, err := OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	last := time.Date(2026, 6, 10, 11, 0, BaselineRunHistoryCap+4, 0, time.UTC).Format(time.RFC3339)
	rec := h2.FindBySnapshot("srv1", last)
	if rec == nil || rec.Tables != BaselineRunHistoryCap+4 {
		t.Fatalf("newest record = %+v", rec)
	}
	first := time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC).Format(time.RFC3339)
	if h2.FindBySnapshot("srv1", first) != nil {
		t.Fatal("oldest record should have fallen off the cap")
	}
	if h2.FindBySnapshot("srv1", "") != nil {
		t.Fatal("empty snapshot time must never match")
	}
	if err := errors.Join(); err != nil {
		t.Fatal(err)
	}
}

func TestBaselineFiles_joinsRunHistory(t *testing.T) {
	dir := newDetailFixture(t)
	srv := newBaselineServer(t, dir, true)
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	srv.baselineHistory = h
	// The boot bundle serves with no selection header → server id "default".
	if err := h.Append(BaselineRunRecord{ServerID: "default", Kind: BaselineRunRestore,
		SnapshotTime: "2026-06-10T12:00:00Z",
		StartedAt:    "2026-06-10T12:00:00Z", FinishedAt: "2026-06-10T12:03:30Z",
		Tables: 2, Rows: 12}); err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(detailSnapAt), "")
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Run == nil || got.Run.Kind != BaselineRunRestore || got.Run.Seconds != 210 || got.Run.Rows != 12 {
		t.Fatalf("run = %+v, want restore/210s/12 rows", got.Run)
	}
}

// TestBaselineRestore_carriesTheEffectiveReuseSetting: the console resolves the
// setting at request time and puts it in the request.
//
// Resolving at request time is deliberate. A restore runs asynchronously, so
// binding the value the operator was looking at is more honest than re-reading
// it whenever the fold happens to start. Both branches are asserted because the
// override is the whole point: a daemon default of off with a saved override of
// on must reach the restore as on.
func TestBaselineRestore_carriesTheEffectiveReuseSetting(t *testing.T) {
	for _, tc := range []struct {
		name     string
		daemon   bool
		override *bool
		want     bool
	}{
		{"daemon flag off, nothing saved", false, nil, false},
		{"daemon flag on, nothing saved", true, nil, true},
		{"override on beats a flag saying off", false, boolPtrRestore(true), true},
		{"override off beats a flag saying on", true, boolPtrRestore(false), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubRestorer{}
			srv := newRestoreServer(t, stub)
			srv.baselineRefreshDefaults = BaselineRefreshDefaults{CarryForwardUnchanged: tc.daemon, Enabled: true}
			if tc.override != nil {
				if err := srv.cm.reg.SetBaselineRefresh(&BaselineRefreshConfig{CarryForwardUnchanged: *tc.override}); err != nil {
					t.Fatal(err)
				}
			}
			id := addRestoreEntry(t, srv, t.TempDir())

			rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline/restore",
				`{"at":"2026-01-02 03:04:05"}`)
			if rec.Code != 202 {
				t.Fatalf("POST code=%d body=%s", rec.Code, body)
			}
			if stub.last == nil {
				t.Fatal("no restore request reached the restorer, so the assertion below checks nothing")
			}
			if stub.last.CarryForwardUnchanged != tc.want {
				t.Errorf("CarryForwardUnchanged = %v, want %v: the restore does not honour the reuse setting",
					stub.last.CarryForwardUnchanged, tc.want)
			}
		})
	}
}

func boolPtrRestore(b bool) *bool { return &b }

// TestBaselineRestore_carriesTheServersS3Destination: the handler puts the
// server's OWN S3 destination in the request, so the restore looks for the
// backup to fold from where the scheduled update looks (#1541). Both shapes
// are asserted: a dir-only server must reach the restorer with an empty S3,
// or a local-only restore would start uploading.
func TestBaselineRestore_carriesTheServersS3Destination(t *testing.T) {
	for _, tc := range []struct {
		name string
		s3   string
	}{
		{"backups go to S3: the request carries the bucket", "s3://bucket/backups/"},
		{"local only: the request carries no bucket", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubRestorer{}
			srv := newRestoreServer(t, stub)
			dir := t.TempDir()
			e, err := srv.cm.reg.Add(ServerEntry{
				Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/binlog_index",
				SourceDSN: "src:pw@tcp(127.0.0.1:3306)/", BaselineDir: dir, BaselineS3: tc.s3,
			})
			if err != nil {
				t.Fatal(err)
			}

			rec, body := doServersReq(t, srv, "POST", "/api/servers/"+e.ID+"/baseline/restore",
				`{"at":"2026-01-02 03:04:05"}`)
			if rec.Code != 202 {
				t.Fatalf("POST code=%d body=%s", rec.Code, body)
			}
			if stub.last == nil {
				t.Fatal("no restore request reached the restorer, so the assertion below checks nothing")
			}
			if stub.last.BaselineS3 != tc.s3 || stub.last.BaselineDir != dir {
				t.Errorf("request carried dir=%q s3=%q, want dir=%q s3=%q",
					stub.last.BaselineDir, stub.last.BaselineS3, dir, tc.s3)
			}
		})
	}
}

// A fractional second in `at` used to slip past every "already exists at
// exactly" guard: Go's parser accepts "10:00:00.5" against a layout with no
// fraction, the snapshot at 10:00:00 is at-or-before it, and the fold names
// its output by the whole second, so it would overwrite that snapshot in
// place (#1541). The instant is truncated at the boundary so every consumer
// agrees on the second.
func TestParseSnapshotAt_truncatesToTheSecond(t *testing.T) {
	for _, raw := range []string{"2026-09-07 10:00:00.5", "2026-09-07T10:00:00.5Z"} {
		got, ok := parseSnapshotAt(raw)
		if !ok {
			t.Fatalf("%q did not parse", raw)
		}
		if got.Nanosecond() != 0 || !got.Equal(time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)) {
			t.Errorf("%q -> %s, want the whole second", raw, got.Format(time.RFC3339Nano))
		}
	}
}
