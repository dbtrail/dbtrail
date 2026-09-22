package console

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v2"
)

func tmpRegistry(t *testing.T) (*Registry, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nested", "console-servers.yaml")
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	return r, path
}

func TestRegistryRoundTrip(t *testing.T) {
	r, path := tmpRegistry(t)

	added, err := r.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(10.0.0.5:3306)/binlog_index", BaselineDir: "/b"})
	if err != nil {
		t.Fatal(err)
	}
	if added.ID == "" || len(added.ID) != 16 {
		t.Errorf("Add must mint a 16-hex-char id, got %q", added.ID)
	}

	r2, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := r2.Get(added.ID)
	if !ok {
		t.Fatal("entry lost across save/load")
	}
	if got.Name != "prod" || got.DSN != "u:p@tcp(10.0.0.5:3306)/binlog_index" || got.BaselineDir != "/b" {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}

// TestRegistryForwardCompatRoundTrip is the load-bearing forward-compat
// invariant: fields written by a NEWER bintrail (the phase-2 control plane's
// source_dsn / server_id / monitor_state) must survive a load → edit → save
// cycle on THIS binary. Non-strict parsing alone only tolerates them on read;
// the yaml:",inline" Extra catch-all is what re-emits them on save.
func TestRegistryForwardCompatRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	newer := `version: 1
future_top_level: ignored-not-fatal
servers:
  - id: a1b2c3d4e5f60718
    name: staging
    index_dsn: ro:pw@tcp(127.0.0.1:3306)/staging_index
    source_dsn: monitor:secret@tcp(10.0.1.9:3306)/
    server_id: 42
    monitor_state: stopped
`
	if err := os.WriteFile(path, []byte(newer), 0o600); err != nil {
		t.Fatal(err)
	}

	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("non-strict load must tolerate unknown fields: %v", err)
	}
	e, ok := r.Get("a1b2c3d4e5f60718")
	if !ok {
		t.Fatal("entry not loaded")
	}
	// Edit something this binary DOES model, then save.
	e.Name = "staging-renamed"
	if err := r.Update(e); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"source_dsn", "monitor:secret@tcp(10.0.1.9:3306)/", "server_id", "monitor_state", "stopped"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("phase-2 field %q dropped on save:\n%s", want, raw)
		}
	}
	if !strings.Contains(string(raw), "staging-renamed") {
		t.Errorf("edit not persisted:\n%s", raw)
	}
}

// TestRegistryRefusesDowngradeWrite: a file with a NEWER version loads
// read-only — list/get work, every mutation is refused, and the file is never
// rewritten through this binary's narrower schema.
func TestRegistryRefusesDowngradeWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	content := "version: 99\nservers:\n  - id: aaaa\n    name: future\n    index_dsn: u:p@tcp(h:3306)/db\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if !r.ReadOnly() {
		t.Fatal("version 99 file must load read-only")
	}
	if _, ok := r.Get("aaaa"); !ok {
		t.Error("read-only registry must still serve entries")
	}
	if _, err := r.Add(ServerEntry{Name: "x", DSN: "u:p@tcp(h:3306)/db"}); !errors.Is(err, ErrRegistryReadOnly) {
		t.Errorf("Add on read-only registry: err=%v, want ErrRegistryReadOnly", err)
	}
	if err := r.Delete("aaaa"); !errors.Is(err, ErrRegistryReadOnly) {
		t.Errorf("Delete on read-only registry: err=%v, want ErrRegistryReadOnly", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != content {
		t.Error("read-only registry rewrote the file")
	}
}

func TestRegistryUniqueNames(t *testing.T) {
	r, _ := tmpRegistry(t)
	a, err := r.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/db"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h2:3306)/db"}); !errors.Is(err, ErrDuplicateName) {
		t.Errorf("duplicate name on Add: err=%v, want ErrDuplicateName", err)
	}
	b, err := r.Add(ServerEntry{Name: "staging", DSN: "u:p@tcp(h2:3306)/db"})
	if err != nil {
		t.Fatal(err)
	}
	// Renaming b onto a's name must fail; renaming b onto its own name is fine.
	b.Name = "prod"
	if err := r.Update(b); !errors.Is(err, ErrDuplicateName) {
		t.Errorf("duplicate name on Update: err=%v, want ErrDuplicateName", err)
	}
	b.Name = "staging"
	if err := r.Update(b); err != nil {
		t.Errorf("self-rename must not collide: %v", err)
	}
	// The boot id doubles as a reserved name.
	if _, err := r.Add(ServerEntry{Name: bootServerID, DSN: "u:p@tcp(h3:3306)/db"}); err == nil {
		t.Error("the reserved boot name must be rejected")
	}
	if _, err := r.Add(ServerEntry{Name: "", DSN: "u:p@tcp(h3:3306)/db"}); err == nil {
		t.Error("empty name must be rejected")
	}
	_ = a
}

func TestRegistryFilePerms(t *testing.T) {
	r, path := tmpRegistry(t)
	if _, err := r.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/db"}); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("registry file perms = %o, want 600 (it holds DSN passwords)", perm)
	}
	di, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if perm := di.Mode().Perm(); perm != 0o700 {
		t.Errorf("registry dir perms = %o, want 700", perm)
	}
	// Atomic write leaves no temp files behind.
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".console-servers-") {
			t.Errorf("leftover temp file: %s", e.Name())
		}
	}
}

// TestRegistrySourceFieldsRoundTrip: the control-plane fields (source DSN,
// server id, schemas, monitor intent) persist and reload like every other
// typed field.
func TestRegistrySourceFieldsRoundTrip(t *testing.T) {
	r, path := tmpRegistry(t)
	added, err := r.Add(ServerEntry{
		Name:           "prod",
		DSN:            "u:p@tcp(h:3306)/binlog_index",
		SourceDSN:      "repl:" + secretPW + "@tcp(db.prod:3306)/",
		SourceServerID: 4242,
		Schemas:        "shop,billing",
		MonitorDesired: true,
		SSLMode:        "verify-ca",
		SSLCA:          "/etc/ssl/ca.pem",
		SSLCert:        "/etc/ssl/client-cert.pem",
		SSLKey:         "/etc/ssl/client-key.pem",
	})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := r2.Get(added.ID)
	if !ok {
		t.Fatal("entry lost")
	}
	if got.SourceDSN != "repl:"+secretPW+"@tcp(db.prod:3306)/" ||
		got.SourceServerID != 4242 || got.Schemas != "shop,billing" || !got.MonitorDesired {
		t.Errorf("source fields round-trip mismatch: %+v", got)
	}
	// Source-TLS fields (#879) persist and reload like the other typed fields.
	if got.SSLMode != "verify-ca" || got.SSLCA != "/etc/ssl/ca.pem" ||
		got.SSLCert != "/etc/ssl/client-cert.pem" || got.SSLKey != "/etc/ssl/client-key.pem" {
		t.Errorf("source TLS fields round-trip mismatch: %+v", got)
	}
}

// TestRegistryCorruptFileFailsLoud: the cmd layer's fail-loud stance rests on
// LoadRegistry erroring for garbage — silently starting with zero servers
// would look like data loss.
func TestRegistryCorruptFileFailsLoud(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	if err := os.WriteFile(path, []byte("{{{ not yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRegistry(path); err == nil {
		t.Fatal("corrupt registry file must fail loud, not load empty")
	}
}

func TestRegistryMissingFileIsEmpty(t *testing.T) {
	r, err := LoadRegistry(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("missing file must be an empty registry, not an error: %v", err)
	}
	if r.Len() != 0 || r.ReadOnly() {
		t.Errorf("missing file: len=%d readOnly=%v, want 0/false", r.Len(), r.ReadOnly())
	}
}

func TestRegistryDeleteRoundTrip(t *testing.T) {
	r, path := tmpRegistry(t)
	a, _ := r.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/db"})
	b, _ := r.Add(ServerEntry{Name: "b", DSN: "u:p@tcp(h2:3306)/db"})
	if err := r.Delete(a.ID); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete("nope"); !errors.Is(err, ErrUnknownServer) {
		t.Errorf("unknown delete: err=%v, want ErrUnknownServer", err)
	}
	r2, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r2.Get(a.ID); ok {
		t.Error("deleted entry survived reload")
	}
	if _, ok := r2.Get(b.ID); !ok {
		t.Error("sibling entry lost on delete")
	}
}

// TestRegistryVersionNormalized: a hand-written file without a version field
// is stamped with the current version on first write.
func TestRegistryVersionNormalized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	handWritten := "servers:\n  - id: abcd\n    name: hand\n    index_dsn: u:p@tcp(h:3306)/db\n"
	if err := os.WriteFile(path, []byte(handWritten), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Add(ServerEntry{Name: "new", DSN: "u:p@tcp(h2:3306)/db"}); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	var f registryFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if f.Version != registryVersion {
		t.Errorf("version = %d after write, want %d", f.Version, registryVersion)
	}
}

// TestRegistryRotationRoundTrip: the global rotation override persists across a
// reload and coexists with server entries — no version bump needed (it lives in
// the same additive envelope).
func TestRegistryRotationRoundTrip(t *testing.T) {
	r, path := tmpRegistry(t)
	if _, ok := r.Rotation(); ok {
		t.Fatal("a fresh registry must have no rotation policy")
	}
	if err := r.SetRotation(RotationConfig{Retain: "7d", Interval: "30m", AddFuture: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx"}); err != nil {
		t.Fatal(err)
	}

	r2, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	rc, ok := r2.Rotation()
	if !ok || rc.Retain != "7d" || rc.Interval != "30m" || rc.AddFuture != 5 {
		t.Fatalf("rotation policy did not round-trip: %+v ok=%v", rc, ok)
	}
	if r2.Len() != 1 {
		t.Errorf("server entry lost alongside the rotation block: len=%d", r2.Len())
	}
}

// TestRegistrySetRotationRefusedReadOnly: a newer-version file loads read-only,
// so SetRotation is refused like every other mutation.
func TestRegistrySetRotationRefusedReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	if err := os.WriteFile(path, []byte("version: 999\nservers: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if !r.ReadOnly() {
		t.Fatal("a version-999 file must load read-only")
	}
	if err := r.SetRotation(RotationConfig{Retain: "7d", Interval: "1h", AddFuture: 3}); !errors.Is(err, ErrRegistryReadOnly) {
		t.Fatalf("SetRotation on a read-only registry = %v, want ErrRegistryReadOnly", err)
	}
}

// TestRegistryOldBaselineRefreshKeySaysItIsIgnored (#1681): an operator who
// turned reuse off in the old console has that choice in this file, and this
// build does not read it. Loading says so once, with the flag that still
// turns reuse off: otherwise the daemon reuses files that operator asked it
// not to, the card says reuse is always on, and nothing connects the two.
func TestRegistryOldBaselineRefreshKeySaysItIsIgnored(t *testing.T) {
	load := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "console-servers.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		if _, err := LoadRegistry(path); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	out := load(t, "version: 1\nbaseline_refresh:\n  carry_forward_unchanged: false\nservers: []\n")
	for _, want := range []string{"no longer read", "--baseline-carry-forward-unchanged=false", "left in the file untouched"} {
		if !strings.Contains(out, want) {
			t.Errorf("loading a registry with the old block did not warn about %q; the operator's saved choice is dropped in silence:\n%s", want, out)
		}
	}
	// And a file without it says nothing: a warning on every start would be
	// noise nobody can act on.
	if out := load(t, "version: 1\nservers: []\n"); strings.Contains(out, "no longer read") {
		t.Errorf("a registry without the old block warned anyway:\n%s", out)
	}
}

// TestRegistryOldBaselineRefreshKeyIsIgnoredAndKept (#1681): the console's
// saved "reuse unchanged tables" override is gone. A registry written by an
// older binary still carries the block, and two things must hold: this build
// ignores it (reuse is always on, decided by the daemon flag alone), and a
// save does not silently drop it — the envelope's Extra catch-all carries it,
// so an operator who goes back to an older binary finds their value intact.
func TestRegistryOldBaselineRefreshKeyIsIgnoredAndKept(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	old := "version: 1\nbaseline_refresh:\n  carry_forward_unchanged: false\nservers: []\n"
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("a registry with the old block must still load: %v", err)
	}
	if r.ReadOnly() {
		t.Fatal("the old block made the file read-only; it is an ordinary version-1 file")
	}
	// A write of something else must carry the unknown block through.
	if _, err := r.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx"}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "carry_forward_unchanged: false") {
		t.Fatalf("saving dropped the old baseline_refresh block, so a downgrade loses the operator's value:\n%s", raw)
	}
	r2, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Len() != 1 {
		t.Errorf("server entry lost alongside the old block: len=%d", r2.Len())
	}
}
