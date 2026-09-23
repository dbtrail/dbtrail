package console

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1681: GET /api/baselines carries the local retention and the last prune at
// the top level, OMITTED (never null or zero) when there is nothing to say.
// The Snapshots page reads their PRESENCE to write one sentence:
//
//	local_retention only        → "Keeps the newest N snapshots on this machine."
//	plus last_prune             → "... Removed M older ones on <date>."
//	neither                     → nothing.

// retentionServer loads a registry from path (a fresh process each call, which
// is what makes the restart case a restart) and opens the entry's bundle the
// way the manager publishes it.
func retentionServer(t *testing.T, path string, pruneLoop bool) *Server {
	t.Helper()
	clearStores(t)
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, LocalPruneLoop: pruneLoop})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range reg.List() {
		srv.cm.bundles[e.ID] = &bundle{}
		srv.cm.rebuildDerived(e)
	}
	return srv
}

// retentionFields GETs the listing for id and returns the two fields RAW, so
// an absent field and a null one are told apart.
func retentionFields(t *testing.T, srv *Server, id string) (map[string]json.RawMessage, string) {
	t.Helper()
	rec, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if rec.Code != 200 {
		t.Fatalf("GET /api/baselines: %d %s", rec.Code, body)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	return raw, string(body)
}

func writeSnapshot(t *testing.T, dir string, ts time.Time) {
	t.Helper()
	snap := filepath.Join(dir, strings.ReplaceAll(ts.UTC().Format(time.RFC3339), ":", "-"))
	writeBaselineFixture(t, snap, "shop", "orders.parquet")
	if err := baseline.WriteSuccessMarker(snap); err != nil {
		t.Fatal(err)
	}
}

func TestBaselinesRetention_shape(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	dir := filepath.Join(state, "baselines", "srv")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	counted, err := reg.Add(ServerEntry{Name: "counted", DSN: "u:p@tcp(h:3306)/a", BaselineDir: dir, LocalKeepNewest: 2})
	if err != nil {
		t.Fatal(err)
	}
	keepsAll, err := reg.Add(ServerEntry{Name: "keeps-all", DSN: "u:p@tcp(h:3306)/b", BaselineDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	for i := 4; i >= 1; i-- {
		writeSnapshot(t, dir, now.Add(-time.Duration(i)*48*time.Hour))
	}

	// 1. A server that keeps everything: neither field.
	srv := retentionServer(t, path, true)
	raw, body := retentionFields(t, srv, keepsAll.ID)
	if _, ok := raw["local_retention"]; ok {
		t.Errorf("keeps-all server carries local_retention: %s", body)
	}
	if _, ok := raw["last_prune"]; ok {
		t.Errorf("never-pruned server carries last_prune: %s", body)
	}

	// 2. A policy, no prune yet: local_retention only.
	raw, body = retentionFields(t, srv, counted.ID)
	if got := string(raw["local_retention"]); got != `{"keep_newest":2}` {
		t.Errorf("local_retention = %s, want {\"keep_newest\":2}; body %s", got, body)
	}
	if _, ok := raw["last_prune"]; ok {
		t.Errorf("last_prune before any prune: %s", body)
	}

	// 3. After a prune, and across a restart: both, the removal read back
	// from disk by a process that did not do it.
	res, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: dir, KeepNewest: 2})
	if err != nil || len(res.Pruned) != 2 {
		t.Fatalf("prune: %+v %v", res, err)
	}
	restarted := retentionServer(t, path, true)
	raw, body = retentionFields(t, restarted, counted.ID)
	var lp struct {
		At      string `json:"at"`
		Removed int    `json:"removed"`
	}
	if err := json.Unmarshal(raw["last_prune"], &lp); err != nil {
		t.Fatalf("last_prune missing after restart: %s", body)
	}
	if lp.Removed != 2 {
		t.Errorf("removed = %d, want 2", lp.Removed)
	}
	at, err := time.Parse(time.RFC3339, lp.At)
	if err != nil || !strings.HasSuffix(lp.At, "Z") {
		t.Errorf("at = %q, want RFC3339 UTC (%v)", lp.At, err)
	}
	if time.Since(at) > time.Hour || time.Since(at) < -time.Minute {
		t.Errorf("at = %s is not the prune's time", lp.At)
	}
	if _, ok := raw["local_retention"]; !ok {
		t.Errorf("local_retention lost after restart: %s", body)
	}
}

// Where this process runs no prune loop (the read-only serve), no retention is
// announced: nothing here removes anything. A prune that did happen is still
// reported, because the copies are gone either way.
func TestBaselinesRetention_noLoopNoPolicyButThePruneStillShows(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	dir := filepath.Join(state, "d")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	reg, _ := LoadRegistry(path)
	e, err := reg.Add(ServerEntry{Name: "s", DSN: "u:p@tcp(h:3306)/a", BaselineDir: dir, LocalKeepNewest: 1})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	writeSnapshot(t, dir, now.Add(-96*time.Hour))
	writeSnapshot(t, dir, now.Add(-48*time.Hour))
	if _, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: dir, KeepNewest: 1}); err != nil {
		t.Fatal(err)
	}
	raw, body := retentionFields(t, retentionServer(t, path, false), e.ID)
	if _, ok := raw["local_retention"]; ok {
		t.Errorf("serve announced a retention it does not run: %s", body)
	}
	if _, ok := raw["last_prune"]; !ok {
		t.Errorf("a prune that happened is not reported: %s", body)
	}
}

// With an external destination, the count does nothing, so it is not
// announced; the same server without one announces it. Asserted on the helper
// the handler calls, because the handler's S3 listing cannot run offline and a
// 502 body would pass a "field absent" check for the wrong reason.
func TestBaselinesRetention_destinationMeansNoCountAnnounced(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	reg, _ := LoadRegistry(path)
	e, err := reg.Add(ServerEntry{Name: "s", DSN: "u:p@tcp(h:3306)/a", BaselineDir: t.TempDir(), BaselineS3: "s3://b/p/", LocalKeepNewest: 3})
	if err != nil {
		t.Fatal(err)
	}
	srv := retentionServer(t, path, true)
	if got := srv.localRetentionOf(e.ID); got != nil {
		t.Errorf("a server with a destination announced %+v", *got)
	}
	e.BaselineS3 = ""
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if got := srv.localRetentionOf(e.ID); got == nil || got.KeepNewest != 3 {
		t.Errorf("without a destination: %+v, want keep 3", got)
	}
	// The daemon's own folder is the global rule's, never a count's.
	g := t.TempDir()
	clearStores(t)
	reg2, _ := LoadRegistry(filepath.Join(t.TempDir(), "r.yaml"))
	e2, _ := reg2.Add(ServerEntry{Name: "g", DSN: "u:p@tcp(h:3306)/a", BaselineDir: g, LocalKeepNewest: 3})
	srv2, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg2, LocalPruneLoop: true, BaselineDir: g})
	if err != nil {
		t.Fatal(err)
	}
	if got := srv2.localRetentionOf(e2.ID); got != nil {
		t.Errorf("the daemon's own folder announced %+v", *got)
	}
}
