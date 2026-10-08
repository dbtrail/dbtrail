package console

import (
	"encoding/json"
	"fmt"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2210: the memory SQL on the copy runs with is a setting the web interface
// saves and applies at once. A value given where DBTrail starts wins and the
// page shows it read-only; a saved file that does not load is reported and
// refuses edits instead of reading as the default.

func TestParseSQLMemory_2210(t *testing.T) {
	for _, c := range []struct {
		raw     string
		wantMiB int64 // 0 = refused
	}{
		{"4GB", 4096},
		{"4gb", 4096},
		{" 8GB ", 8192},
		{"512MB", 512},
		{"1536MiB", 1536},
		{"2GiB", 2048},
		{"2gib", 2048},
		{"1048576MB", 1048576}, // very large: accepted, the warning says the rest
		{"600000KB", 585},      // rounds down to whole MiB, still over the floor
		{"", 0},
		{"   ", 0},
		{"0", 0},
		{"4 GB", 0},
		{"511MB", 0},
		{"256MB", 0},
		{"-1GB", 0},
		{"1.5GB", 0},
		{"1TB", 0},
		{"lots", 0},
		{"99999999999999999999GB", 0},
		{"9223372036854775807GB", 0},
	} {
		mib, err := ParseSQLMemory(c.raw)
		if c.wantMiB == 0 {
			if err == nil {
				t.Errorf("ParseSQLMemory(%q) = %d MiB, want a refusal", c.raw, mib)
			}
			continue
		}
		if err != nil || mib != c.wantMiB {
			t.Errorf("ParseSQLMemory(%q) = %d, %v; want %d MiB", c.raw, mib, err, c.wantMiB)
		}
	}
	// The refusal says what is wanted, with an example a reader can copy.
	if _, err := ParseSQLMemory("4 GB"); err == nil || !strings.Contains(err.Error(), "512MB") || !strings.Contains(err.Error(), "4GB") {
		t.Errorf("refusal: %v", err)
	}
}

// Words a person reads: one decimal only where it is exact.
func TestSQLMemoryWords_2210(t *testing.T) {
	for in, want := range map[string]string{
		"": "2 GB", "2048MiB": "2 GB", "4GB": "4 GB", "1536MiB": "1.5 GB", "1030MiB": "1030 MB",
		"512MiB": "512 MB", "2560MiB": "2.5 GB", "1048576MiB": "1024 GB", "8796093022207MiB": "8796093022207 MB",
	} {
		if got := sqlMemoryWords(in); got != want {
			t.Errorf("sqlMemoryWords(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSQLMemoryOverHost_2210(t *testing.T) {
	const host = 8 << 30
	for _, c := range []struct {
		limit string
		n     int
		host  uint64
		over  bool
	}{
		{"2048MiB", 2, host, false},
		{"4096MiB", 2, host, false},
		{"4097MiB", 2, host, true},
		{"6144MiB", 1, host, false},
		{"6144MiB", 2, 0, false},
		{"6144MiB", 0, host, false},
		{"not a size", 2, host, false},
		// Huge values and counts must not wrap around to "fits".
		{"8796093022207MiB", 4, host, true},
		{"1048576MiB", 1 << 40, host, true},
	} {
		_, over := SQLMemoryOverHost(c.limit, c.n, c.host)
		if over != c.over {
			t.Errorf("SQLMemoryOverHost(%q, %d, %d) over = %v, want %v", c.limit, c.n, c.host, over, c.over)
		}
	}
}

func TestSQLSettingsFile_2210(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "nope.json")
	f, err := loadSQLSettingsFile(missing)
	if err != nil || f.MemoryMiB != 0 {
		t.Fatalf("a missing file is no setting: %+v, %v", f, err)
	}
	p := filepath.Join(dir, "sub", SQLSettingsFileName)
	if err := saveSQLSettingsFile(p, sqlSettingsFile{MemoryMiB: 4096}); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(p)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("saved file mode: %v, %v", fi, err)
	}
	if f, err := loadSQLSettingsFile(p); err != nil || f.MemoryMiB != 4096 || f.Version != sqlSettingsFileVersion {
		t.Fatalf("round trip: %+v, %v", f, err)
	}
	entries, _ := os.ReadDir(filepath.Dir(p))
	if len(entries) != 1 {
		t.Errorf("a temp file was left beside the setting: %v", entries)
	}
	for name, body := range map[string]string{
		"not json":        "{memory: 4GB",
		"below the floor": `{"version":1,"memory_mib":100}`,
		"negative":        `{"version":1,"memory_mib":-4096}`,
		"newer version":   `{"version":2,"memory_mib":4096}`,
		"null":            `null`,
		"empty object":    `{}`,
		"no version":      `{"memory_mib":4096}`,
		"misspelled key":  `{"version":1,"memory_mb":4096}`,
		"too large":       `{"version":1,"memory_mib":10000000000000}`,
		"a string":        `{"version":1,"memory_mib":"4GB"}`,
		"two objects":     `{"version":1,"memory_mib":4096}{"version":1,"memory_mib":8192}`,
		"trailing text":   `{"version":1,"memory_mib":4096} oops`,
	} {
		bad := filepath.Join(dir, strings.ReplaceAll(name, " ", "-")+".json")
		if err := os.WriteFile(bad, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadSQLSettingsFile(bad); err == nil {
			t.Errorf("%s: loaded without an error", name)
		}
	}
}

// sqlSettingsServer is a real New server with the settings file at path and,
// when startup is not "", a value given where DBTrail starts.
func sqlSettingsServer(t *testing.T, path, startup string) *Server {
	t.Helper()
	s, err := New(Config{Listen: "127.0.0.1:8090", Token: "tok", SQLSettingsPath: path,
		SQLLimits: sqlsandbox.Limits{MemoryLimit: startup}, SQLMaxInFlight: 2})
	if err != nil {
		t.Fatal(err)
	}
	s.hostMemory = func() uint64 { return 8 << 30 }
	if s.sqlSpillState == nil {
		t.Fatal("New left the runner's spill state unwired: the settings panel would never say how much disk a statement may use")
	}
	// The runner's answer depends on the free space of the machine running
	// the test; the panel's sentences are pinned against a fixed one, four
	// times the memory (sqlsandbox tests the real computation).
	s.sqlSpillState = func(memory string) (int64, error) {
		n, err := cliutil.ParseByteSize(memory)
		return 4 * n, err
	}
	return s
}

func sqlSettingsDo(t *testing.T, s *Server, method, body string) (*httptest.ResponseRecorder, sqlSettingsDTO) {
	t.Helper()
	req := httptest.NewRequest(method, "http://127.0.0.1:8090/api/sql-settings", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+s.token)
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	var dto sqlSettingsDTO
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
			t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
	}
	return rec, dto
}

func TestSQLSettingsAPI_saveApplyAndRestart_2210(t *testing.T) {
	path := filepath.Join(t.TempDir(), SQLSettingsFileName)
	s := sqlSettingsServer(t, path, "")

	rec, st := sqlSettingsDo(t, s, "GET", "")
	if rec.Code != 200 || st.Source != "default" || st.Memory != "2 GB" || st.MemoryBytes != 2<<30 || st.Value != "2GB" ||
		!st.CanManage || st.MaxUnmergedMB != 48 || st.FloorMB != 512 || st.Default != "2 GB" || st.Warning != "" ||
		st.HostMemoryBytes != 8<<30 || st.MaxInFlight != 2 || st.Error != "" || st.Locked != "" {
		t.Fatalf("default: %d %+v", rec.Code, st)
	}

	rec, st = sqlSettingsDo(t, s, "PUT", `{"memory":"4gb"}`)
	if rec.Code != 200 || st.Source != "saved" || st.Memory != "4 GB" || st.Value != "4GB" || st.MaxUnmergedMB != 96 {
		t.Fatalf("PUT 4gb: %d %s", rec.Code, rec.Body.String())
	}
	if lim, src := s.sqlMemoryNow(); lim != "4096MiB" || src != "saved" {
		t.Errorf("live value after PUT: %q %q", lim, src)
	}
	if f, err := loadSQLSettingsFile(path); err != nil || f.MemoryMiB != 4096 {
		t.Errorf("file after PUT: %+v, %v", f, err)
	}
	// GET /api/sql reads the same live value.
	if got := sqlMemoryWords(mustLimit(s)); got != "4 GB" {
		t.Errorf("words: %q", got)
	}

	// A restart reads it back.
	s2 := sqlSettingsServer(t, path, "")
	if _, st := sqlSettingsDo(t, s2, "GET", ""); st.Source != "saved" || st.Memory != "4 GB" {
		t.Errorf("after restart: %+v", st)
	}

	// 1536MB is not whole GB: words and value keep the MB.
	if rec, st = sqlSettingsDo(t, s, "PUT", `{"memory":"1536MiB"}`); rec.Code != 200 || st.Memory != "1.5 GB" || st.Value != "1536MB" || st.MaxUnmergedMB != 36 {
		t.Errorf("PUT 1536MiB: %d %+v", rec.Code, st)
	}

	// Very large: accepted, and the warning says it does not fit this host.
	rec, st = sqlSettingsDo(t, s, "PUT", `{"memory":"1048576MB"}`)
	if rec.Code != 200 || st.Warning == "" || !strings.Contains(st.Warning, "8 GB") || strings.Contains(st.Warning, "—") || strings.Contains(st.Warning, "--") {
		t.Errorf("PUT huge: %d %+v", rec.Code, st)
	}

	// "" and whitespace go back to the default, and say so in the file.
	for _, body := range []string{`{"memory":""}`, `{"memory":"   "}`} {
		rec, st = sqlSettingsDo(t, s, "PUT", body)
		if rec.Code != 200 || st.Source != "default" || st.Memory != "2 GB" {
			t.Errorf("PUT %s: %d %+v", body, rec.Code, st)
		}
		if f, err := loadSQLSettingsFile(path); err != nil || f.MemoryMiB != 0 {
			t.Errorf("file after PUT %s: %+v, %v", body, f, err)
		}
	}
}

func mustLimit(s *Server) string { l, _ := s.sqlMemoryNow(); return l }

func TestSQLSettingsAPI_refusals_2210(t *testing.T) {
	path := filepath.Join(t.TempDir(), SQLSettingsFileName)
	s := sqlSettingsServer(t, path, "")
	for _, body := range []string{`{"memory":"4 GB"}`, `{"memory":"100MB"}`, `{"memory":"0"}`, `{"memory":"lots"}`,
		`{}`, `{"memory":null}`, `{"memory":4}`, `not json`} {
		rec, _ := sqlSettingsDo(t, s, "PUT", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("PUT %s: %d %s, want 400", body, rec.Code, rec.Body.String())
		}
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("a refused PUT wrote the file")
	}
	if l, src := s.sqlMemoryNow(); l != "2048MiB" || src != "default" {
		t.Errorf("a refused PUT moved the live value: %q %q", l, src)
	}
}

func TestSQLSettingsAPI_startupWins_2210(t *testing.T) {
	path := filepath.Join(t.TempDir(), SQLSettingsFileName)
	if err := saveSQLSettingsFile(path, sqlSettingsFile{MemoryMiB: 8192}); err != nil {
		t.Fatal(err)
	}
	s := sqlSettingsServer(t, path, "3072MiB")
	rec, st := sqlSettingsDo(t, s, "GET", "")
	if rec.Code != 200 || st.Source != "startup" || st.Memory != "3 GB" || st.CanManage || st.Saved != "8 GB" ||
		!strings.Contains(st.Locked, "--sql-memory") || !strings.Contains(st.Locked, "BINTRAIL_CONSOLE_SQL_MEMORY") {
		t.Fatalf("startup: %d %+v", rec.Code, st)
	}
	rec, _ = sqlSettingsDo(t, s, "PUT", `{"memory":"4GB"}`)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "--sql-memory") || !strings.Contains(rec.Body.String(), "BINTRAIL_CONSOLE_SQL_MEMORY") {
		t.Fatalf("PUT under a startup value: %d %s", rec.Code, rec.Body.String())
	}
	if l, src := s.sqlMemoryNow(); l != "3072MiB" || src != "startup" {
		t.Errorf("live: %q %q", l, src)
	}
	if f, _ := loadSQLSettingsFile(path); f.MemoryMiB != 8192 {
		t.Errorf("the refused PUT changed the file: %+v", f)
	}
}

func TestSQLSettingsAPI_corruptFile_2210(t *testing.T) {
	for _, startup := range []string{"", "3072MiB"} {
		t.Run("startup="+startup, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), SQLSettingsFileName)
			if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
				t.Fatal(err)
			}
			s := sqlSettingsServer(t, path, startup)
			rec, st := sqlSettingsDo(t, s, "GET", "")
			wantSrc, wantMem := "default", "2 GB"
			if startup != "" {
				wantSrc, wantMem = "startup", "3 GB"
			}
			if rec.Code != 200 || st.Error == "" || st.CanManage || st.Source != wantSrc || st.Memory != wantMem || st.Locked == "" {
				t.Fatalf("GET: %d %+v", rec.Code, st)
			}
			rec, _ = sqlSettingsDo(t, s, "PUT", `{"memory":"4GB"}`)
			if rec.Code != http.StatusConflict {
				t.Fatalf("PUT over an unreadable file: %d %s", rec.Code, rec.Body.String())
			}
			if b, _ := os.ReadFile(path); string(b) != "{not json" {
				t.Errorf("the file was replaced: %q", b)
			}
		})
	}
}

func TestSQLSettingsAPI_noFile_2210(t *testing.T) {
	s := sqlSettingsServer(t, "", "")
	_, st := sqlSettingsDo(t, s, "GET", "")
	if st.CanManage || st.Source != "default" || st.Locked == "" {
		t.Errorf("no path: %+v", st)
	}
	if rec, _ := sqlSettingsDo(t, s, "PUT", `{"memory":"4GB"}`); rec.Code != http.StatusConflict {
		t.Errorf("PUT with no file: %d", rec.Code)
	}
}

// Many PUTs at once: every one answers, and the file and the live value end
// on the same input.
func TestSQLSettingsAPI_concurrentPUTs_2210(t *testing.T) {
	path := filepath.Join(t.TempDir(), SQLSettingsFileName)
	s := sqlSettingsServer(t, path, "")
	var wg sync.WaitGroup
	codes := make([]int, 24)
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest("PUT", "http://127.0.0.1:8090/api/sql-settings", strings.NewReader(fmt.Sprintf(`{"memory":"%dMB"}`, 1024+i)))
			req.Header.Set("Authorization", "Bearer "+s.token)
			rec := httptest.NewRecorder()
			s.mux.ServeHTTP(rec, req)
			codes[i] = rec.Code
			// Readers run beside the writers.
			s.sqlMemoryNow()
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c != 200 {
			t.Errorf("PUT %d: %d", i, c)
		}
	}
	f, err := loadSQLSettingsFile(path)
	if err != nil {
		t.Fatal(err)
	}
	live, _ := s.sqlMemoryNow()
	if live != fmt.Sprintf("%dMiB", f.MemoryMiB) || f.MemoryMiB < 1024 || f.MemoryMiB >= 1024+int64(len(codes)) {
		t.Errorf("file %d MiB, live %q", f.MemoryMiB, live)
	}
}

// The value saved in the web interface is what the next statement's worker
// gets, and what GET /api/sql reports.
func TestSQLSettings_reachesTheNextStatement_2210(t *testing.T) {
	f := newSQLFixture(t, &fakeSQLRunner{}, false)
	if w := f.post(t, `{"sql":"SELECT 1 AS one"}`); w.Code != http.StatusOK {
		t.Fatalf("first: %d %s", w.Code, w.Body.String())
	}
	if got := f.runner.last(t).Limits.MemoryLimit; got != "2048MiB" {
		t.Errorf("default Job memory = %q", got)
	}
	f.s.sqlMem.path = filepath.Join(t.TempDir(), SQLSettingsFileName)
	rec, st := sqlSettingsDo(t, f.s, "PUT", `{"memory":"6GB"}`)
	if rec.Code != 200 || st.Source != "saved" {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}
	if w := f.post(t, `{"sql":"SELECT 1 AS one"}`); w.Code != http.StatusOK {
		t.Fatalf("second: %d %s", w.Code, w.Body.String())
	}
	if got := f.runner.last(t).Limits.MemoryLimit; got != "6144MiB" {
		t.Errorf("Job memory after the PUT = %q, want 6144MiB", got)
	}
	req := httptest.NewRequest("GET", "/api/sql", nil)
	w := httptest.NewRecorder()
	f.s.handleSQLInfo(w, req)
	if !strings.Contains(w.Body.String(), `"memory":"6 GB"`) || !strings.Contains(w.Body.String(), `"max_unmerged_mb":144`) {
		t.Errorf("GET /api/sql after the PUT: %s", w.Body.String())
	}
}
