package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// captureStatusStub answers from a table keyed by server id, and records
// which entries it was asked about.
type captureStatusStub struct {
	answers map[string]CaptureStatus
	asked   []ServerEntry
}

func (c *captureStatusStub) CaptureStatus(_ context.Context, e ServerEntry) CaptureStatus {
	c.asked = append(c.asked, e)
	return c.answers[e.ID]
}

func captureStatusServer(t *testing.T, rep CaptureStatusReporter) *Server {
	t.Helper()
	clearStores(t)
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, CaptureStatus: rep})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// captureStatusGet asks through the real handler chain, token and route
// included. id "" sends no server header.
func captureStatusGet(t *testing.T, srv *Server, id string) (int, CaptureStatus) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/capture-status", nil)
	req.Header.Set("Authorization", "Bearer t")
	if id != "" {
		req.Header.Set(serverHeader, id)
	}
	srv.Handler().ServeHTTP(rec, req)
	var v CaptureStatus
	if rec.Code == 200 {
		if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
			t.Fatalf("decode %q: %v", rec.Body.String(), err)
		}
	}
	return rec.Code, v
}

// The read-only web interface is connected to no source: unknown, for every
// server, and it says why.
func TestCaptureStatus_readOnlyIsUnknown(t *testing.T) {
	srv := captureStatusServer(t, nil)
	e, err := srv.cm.reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx", SourceDSN: "u:p@tcp(src:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	code, got := captureStatusGet(t, srv, e.ID)
	if code != 200 || got.State != CaptureStateUnknown || got.Detail == "" || got.ServerID != e.ID {
		t.Fatalf("code %d, got %+v; want unknown with a reason, for %q", code, got, e.ID)
	}
}

// The answer is the selected server's: the header's, or the default's when
// there is none. Never another server's.
func TestCaptureStatus_answersForTheSelectedServer(t *testing.T) {
	stub := &captureStatusStub{answers: map[string]CaptureStatus{}}
	srv := captureStatusServer(t, stub)
	a, err := srv.cm.reg.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/a", SourceDSN: "u:p@tcp(src-a:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := srv.cm.reg.Add(ServerEntry{Name: "b", DSN: "u:p@tcp(h:3306)/b", SourceDSN: "u:p@tcp(src-b:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	stub.answers[a.ID] = CaptureStatus{State: CaptureStateUpToDate}
	// The reporter's own id is not trusted either.
	stub.answers[b.ID] = CaptureStatus{ServerID: a.ID, State: CaptureStateBehind, Detail: "the source reports transactions"}

	if code, got := captureStatusGet(t, srv, b.ID); code != 200 || got.State != CaptureStateBehind || got.ServerID != b.ID {
		t.Errorf("b: code %d, got %+v", code, got)
	}
	if code, got := captureStatusGet(t, srv, a.ID); code != 200 || got.State != CaptureStateUpToDate || got.ServerID != a.ID {
		t.Errorf("a: code %d, got %+v", code, got)
	}
	def := srv.cm.defaultID()
	if code, got := captureStatusGet(t, srv, ""); code != 200 || got.ServerID != def || got.State != stub.answers[def].State {
		t.Errorf("no header: code %d, got %+v, want the default server %q", code, got, def)
	}
	if len(stub.asked) != 3 || stub.asked[0].ID != b.ID || stub.asked[0].SourceDSN != b.SourceDSN || stub.asked[1].ID != a.ID || stub.asked[1].SourceDSN != a.SourceDSN {
		t.Errorf("asked about %+v", stub.asked)
	}
	if code, _ := captureStatusGet(t, srv, "nope"); code != 404 {
		t.Errorf("unknown server: code %d, want 404", code)
	}
	if len(stub.asked) != 3 {
		t.Errorf("an unknown server reached the reporter: %+v", stub.asked)
	}
}

// A state this build does not know is unknown on the wire, and only unknown
// carries a time to ask again.
func TestCaptureStatus_onlyTheThreeStatesReachTheWire(t *testing.T) {
	stub := &captureStatusStub{answers: map[string]CaptureStatus{}}
	srv := captureStatusServer(t, stub)
	e, err := srv.cm.reg.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/a", SourceDSN: "u:p@tcp(src-a:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	for in, want := range map[CaptureStatus]CaptureStatus{
		{}:                     {State: CaptureStateUnknown},
		{State: "caught_up"}:   {State: CaptureStateUnknown},
		{State: "UP_TO_DATE"}:  {State: CaptureStateUnknown},
		{State: "up_to_date "}: {State: CaptureStateUnknown},
		{State: CaptureStateUpToDate, RetryInSeconds: 25}: {State: CaptureStateUpToDate},
		{State: CaptureStateBehind, RetryInSeconds: 25}:   {State: CaptureStateBehind},
		{State: CaptureStateUnknown, RetryInSeconds: 25}:  {State: CaptureStateUnknown, RetryInSeconds: 25},
	} {
		stub.answers[e.ID] = in
		want.ServerID = e.ID
		if code, got := captureStatusGet(t, srv, e.ID); code != 200 || got != want {
			t.Errorf("%+v: code %d, got %+v, want %+v", in, code, got, want)
		}
	}
}

func TestCaptureStatus_noServersIsNotFound(t *testing.T) {
	stub := &captureStatusStub{}
	srv := captureStatusServer(t, stub)
	if code, _ := captureStatusGet(t, srv, ""); code != 404 {
		t.Errorf("no servers: code %d, want 404", code)
	}
	if code, _ := captureStatusGet(t, srv, bootServerID); code != 404 {
		t.Errorf("no boot entry: code %d, want 404", code)
	}
	if len(stub.asked) != 0 {
		t.Errorf("the reporter was asked about %+v", stub.asked)
	}
}

func TestCaptureStatus_routeIsClassified(t *testing.T) {
	if p, ok := permForRoute("GET", "/api/capture-status"); !ok || p != ext.PermStatusRead {
		t.Errorf("permForRoute = (%q,%v), want (%q,true)", p, ok, ext.PermStatusRead)
	}
}
