package console

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The memory SQL on the copy runs with (#2210) can be set in the web
// interface, not only with --sql-memory. This file is that setting: how a
// value is read, where it is saved, and the two routes that show and change
// it. Precedence, as for the MySQL port (#2101): a value given where DBTrail
// starts wins and the page shows it read-only; else the saved value; else the
// default. Every statement reads the value in force when it starts.

// SQLMemoryFloor is the least memory a statement may be given: below it a
// worker cannot open the views of a real copy, and every statement would
// fail late.
const SQLMemoryFloor = 512 << 20

// SQLSettingsFileName is the saved setting's file name, beside the servers
// file (consoleapp names the path).
const SQLSettingsFileName = "console-sql-settings.json"

// sqlSettingsFileVersion is the file schema this binary writes. A newer one
// does not load: its fields may mean something this build cannot apply.
const sqlSettingsFileVersion = 1

// ParseSQLMemory reads a memory size as --sql-memory, its environment
// variable and the web interface take it: a whole number with KB/MB/GB (or
// KiB/MiB/GiB), any case, binary units, at least SQLMemoryFloor. It returns
// whole MiB, which is how the value reaches DuckDB (whose own "GB" is
// decimal).
func ParseSQLMemory(raw string) (int64, error) {
	n, err := cliutil.ParseByteSize(raw)
	if err != nil || n < SQLMemoryFloor {
		return 0, fmt.Errorf("%q is not a size of at least 512MB; write it like 4GB or 1536MB, with no space", strings.TrimSpace(raw))
	}
	return n >> 20, nil
}

// SQLMemoryOverHost says whether maxInFlight statements, each allowed limit
// (a DuckDB memory string), can together take more than hostBytes, and how
// many MiB that is (saturated). Never over when anything is unknown.
func SQLMemoryOverHost(limit string, maxInFlight int, hostBytes uint64) (totalMiB int64, over bool) {
	each, err := cliutil.ParseByteSize(limit)
	if err != nil || each <= 0 || hostBytes == 0 || maxInFlight < 1 {
		return 0, false
	}
	n := uint64(maxInFlight)
	// each*n > host, written so neither side can wrap.
	over = uint64(each) > hostBytes/n
	eachMiB := each >> 20
	if eachMiB > math.MaxInt64/int64(maxInFlight) {
		return math.MaxInt64, over
	}
	return eachMiB * int64(maxInFlight), over
}

// HostMemoryBytes is MemTotal from /proc/meminfo, 0 when it cannot be read
// (outside Linux). The machine's, not a container's: the warning it feeds is
// about the memory capture shares.
func HostMemoryBytes() uint64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == "MemTotal:" {
			kb, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				return 0
			}
			return kb * 1024
		}
	}
	return 0
}

// sqlSettingsFile is the saved setting. MemoryMiB 0 = none saved (the
// default applies).
type sqlSettingsFile struct {
	Version   int   `json:"version"`
	MemoryMiB int64 `json:"memory_mib,omitempty"`
}

// loadSQLSettingsFile reads the saved setting. A missing file is no setting.
// Anything else that cannot be applied as written is an error, never the
// default: a hand edit that went wrong must be seen.
func loadSQLSettingsFile(path string) (sqlSettingsFile, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return sqlSettingsFile{Version: sqlSettingsFileVersion}, nil
	}
	if err != nil {
		return sqlSettingsFile{}, fmt.Errorf("read %s: %w", path, err)
	}
	// Strict: a misspelled key or a bare null would otherwise load as "no
	// setting", and a hand edit gone wrong would read as the default.
	var f *sqlSettingsFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return sqlSettingsFile{}, fmt.Errorf("parse %s: %w", path, err)
	}
	// Decode stops after the first object: a second one left over from a
	// hand edit would otherwise be dropped and the first loaded as if valid.
	if dec.More() {
		return sqlSettingsFile{}, fmt.Errorf("parse %s: unexpected data after the settings object", path)
	}
	if f == nil || f.Version < 1 {
		return sqlSettingsFile{}, fmt.Errorf("parse %s: no \"version\"; this file is written by DBTrail", path)
	}
	if f.Version > sqlSettingsFileVersion {
		return sqlSettingsFile{}, fmt.Errorf("%s was written by a newer DBTrail (version %d)", path, f.Version)
	}
	if f.MemoryMiB != 0 && f.MemoryMiB < SQLMemoryFloor>>20 {
		return sqlSettingsFile{}, fmt.Errorf("%s: memory_mib %d is under the 512 MiB floor", path, f.MemoryMiB)
	}
	// What ParseSQLMemory can return at most: past it the size no longer
	// fits in bytes, and every limit derived from it would fall back.
	if f.MemoryMiB > math.MaxInt64>>20 {
		return sqlSettingsFile{}, fmt.Errorf("%s: memory_mib %d is too large", path, f.MemoryMiB)
	}
	return *f, nil
}

// saveSQLSettingsFile writes atomically: temp file in the same directory,
// fsync, rename. 0600 in a 0700 directory, like the files beside it.
func saveSQLSettingsFile(path string, f sqlSettingsFile) error {
	f.Version = sqlSettingsFileVersion
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal SQL settings: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create SQL settings directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".console-sql-settings-*.json")
	if err != nil {
		return fmt.Errorf("create temp SQL settings file in %s: %w", dir, err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp SQL settings file for %s: %w", path, err)
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("write SQL settings %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync SQL settings %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp SQL settings file for %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace SQL settings %s: %w", path, err)
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// sqlMemoryState is the server's live view of the setting.
type sqlMemoryState struct {
	// mutate serializes PUTs (save, then apply); readers take mu only.
	mutate sync.Mutex

	mu sync.Mutex
	// startup: Config.SQLLimits.MemoryLimit was given (--sql-memory or its
	// variable). It decides; the saved value is reported but not used.
	startup bool
	// path is the saved setting's file; "" = this console keeps none.
	path string
	// savedMiB is the saved value, 0 = none.
	savedMiB int64
	// loadErr: the file is there and did not load. Nothing is saved over it
	// until it is fixed or removed.
	loadErr string
}

// initSQLMemory loads the saved setting at startup. It never fails: a file
// that does not load is reported in the web interface and in the log, and
// statements keep the startup value or the default.
func (s *Server) initSQLMemory(startup bool, path string) {
	m := &s.sqlMem
	m.startup, m.path = startup, path
	if path == "" {
		return
	}
	f, err := loadSQLSettingsFile(path)
	if err != nil {
		m.loadErr = err.Error()
		lim, _ := s.sqlMemoryNow()
		slog.Warn("console: the saved SQL memory setting could not be read; SQL on the copy keeps "+sqlMemoryWords(lim)+
			", and the setting cannot be changed in the web interface until the file is fixed or removed", "path", path, "error", err)
		return
	}
	m.savedMiB = f.MemoryMiB
	if startup && f.MemoryMiB > 0 {
		slog.Info("console: the SQL memory saved in the web interface is not used: the one given at startup wins",
			"saved", sqlMemoryWords(fmt.Sprintf("%dMiB", f.MemoryMiB)), "startup", sqlMemoryWords(s.sqlLimits.MemoryLimit))
	}
	if startup || f.MemoryMiB == 0 {
		return
	}
	lim, _ := s.sqlMemoryNow()
	slog.Info("console: SQL on the copy runs with the memory saved in the web interface", "memory", sqlMemoryWords(lim))
	if total, over := SQLMemoryOverHost(lim, s.sqlMaxInFlight, s.hostMemoryBytes()); over {
		slog.Warn(fmt.Sprintf("console: saved SQL memory %d MiB: %d statements at once can take %d MiB, more than this host's %d MiB, which capture shares; a statement may be killed by the kernel instead of refused",
			sqlMiB(lim), s.sqlMaxInFlight, total, s.hostMemoryBytes()>>20))
	}
}

// sqlMemoryNow is the memory a statement starting now runs with, as DuckDB
// takes it, and where it comes from: "startup", "saved" or "default".
func (s *Server) sqlMemoryNow() (limit, source string) {
	return s.sqlMemoryFrom(s.sqlMem.snapshot())
}

// sqlMemorySnap is sqlMemoryState read under one lock, so what a response
// reports comes from one moment.
type sqlMemorySnap struct {
	startup       bool
	path, loadErr string
	savedMiB      int64
}

func (m *sqlMemoryState) snapshot() sqlMemorySnap {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sqlMemorySnap{startup: m.startup, path: m.path, loadErr: m.loadErr, savedMiB: m.savedMiB}
}

func (s *Server) sqlMemoryFrom(sn sqlMemorySnap) (limit, source string) {
	switch {
	case sn.startup:
		return s.sqlLimits.MemoryLimit, "startup"
	case sn.savedMiB > 0:
		return fmt.Sprintf("%dMiB", sn.savedMiB), "saved"
	}
	return s.sqlLimits.MemoryLimit, "default"
}

func (s *Server) hostMemoryBytes() uint64 {
	if s.hostMemory != nil {
		return s.hostMemory()
	}
	return HostMemoryBytes()
}

func sqlMiB(limit string) int64 {
	n, _ := cliutil.ParseByteSize(limit)
	return n >> 20
}

// sqlMemoryValue is a memory as the form takes it back: "4GB", "1536MB".
func sqlMemoryValue(limit string) string {
	mib := sqlMiB(limit)
	if mib > 0 && mib%1024 == 0 {
		return fmt.Sprintf("%dGB", mib/1024)
	}
	return fmt.Sprintf("%dMB", mib)
}

// sqlMemoryLocked is why the setting cannot be changed here, "" when it can.
// Callers that act on it hold sqlMem.mutate.
func (s *Server) sqlMemoryLocked() string { return sqlMemoryLockedReason(s.sqlMem.snapshot()) }

func sqlMemoryLockedReason(m sqlMemorySnap) string {
	switch {
	case m.startup:
		return "It is set where DBTrail starts (CLI: --sql-memory, or the environment variable BINTRAIL_CONSOLE_SQL_MEMORY), so it is changed there. Remove that setting and restart DBTrail to change it here."
	case m.path == "":
		return "This DBTrail keeps no settings file, so the memory is set where it starts (CLI: --sql-memory, or the environment variable BINTRAIL_CONSOLE_SQL_MEMORY)."
	case m.loadErr != "":
		return "The saved setting could not be read, so it is not changed here: " + m.loadErr + ". Fix or remove the file and restart DBTrail."
	}
	return ""
}

// sqlSettingsDTO is GET (and PUT) /api/sql-settings.
type sqlSettingsDTO struct {
	// Memory is the value in force as a person reads it ("4 GB"),
	// MemoryBytes the same in bytes, Value as the form takes it ("4GB").
	Memory      string `json:"memory"`
	MemoryBytes int64  `json:"memory_bytes"`
	Value       string `json:"value"`
	// Source: "default", "saved" or "startup".
	Source string `json:"source"`
	// Saved is a saved value a startup value overrides, as words.
	Saved string `json:"saved,omitempty"`
	// Default and FloorMB bound the form: what "" gives back, the least
	// accepted.
	Default string `json:"default"`
	FloorMB int64  `json:"floor_mb"`
	// MaxUnmergedMB is the changes not yet merged into a statement's tables
	// this memory takes before refusing (sqlChainLimit).
	MaxUnmergedMB int64 `json:"max_unmerged_mb"`
	// Disk is what a statement may also spill to disk past its memory, as
	// words (#2210, the runner's SpillState); NoDisk, set instead, is why a
	// statement cannot spill at all.
	Disk   string `json:"disk,omitempty"`
	NoDisk string `json:"no_disk,omitempty"`
	// MaxInFlight is how many statements run at once; HostMemoryBytes the
	// machine's memory, 0 (omitted) when not known; Warning is set when the
	// first times the memory is more than the second.
	MaxInFlight     int    `json:"max_in_flight"`
	HostMemoryBytes uint64 `json:"host_memory_bytes,omitempty"`
	Warning         string `json:"warning,omitempty"`
	// CanManage: a PUT can change it. Locked is why not, when it cannot.
	CanManage bool   `json:"can_manage"`
	Locked    string `json:"locked,omitempty"`
	// Error is why the saved file did not load.
	Error string `json:"error,omitempty"`
}

func (s *Server) sqlSettings() sqlSettingsDTO {
	sn := s.sqlMem.snapshot()
	lim, src := s.sqlMemoryFrom(sn)
	saved, loadErr := sn.savedMiB, sn.loadErr
	nbytes, _ := cliutil.ParseByteSize(lim)
	locked := sqlMemoryLockedReason(sn)
	dto := sqlSettingsDTO{
		Memory:          sqlMemoryWords(lim),
		MemoryBytes:     nbytes,
		Value:           sqlMemoryValue(lim),
		Source:          src,
		Default:         sqlMemoryWords(sqlsandbox.DefaultLimits().MemoryLimit),
		FloorMB:         SQLMemoryFloor >> 20,
		MaxUnmergedMB:   sqlChainLimit(lim) >> 20,
		MaxInFlight:     s.sqlMaxInFlight,
		HostMemoryBytes: s.hostMemoryBytes(),
		CanManage:       locked == "",
		Locked:          locked,
		Error:           loadErr,
	}
	if s.sqlSpillState != nil {
		if n, err := s.sqlSpillState(lim); err != nil {
			dto.NoDisk = err.Error()
		} else {
			dto.Disk = sqlMemoryWords(fmt.Sprintf("%dMiB", n>>20))
		}
	}
	if src == "startup" && saved > 0 {
		dto.Saved = sqlMemoryWords(fmt.Sprintf("%dMiB", saved))
	}
	if total, over := SQLMemoryOverHost(lim, s.sqlMaxInFlight, dto.HostMemoryBytes); over {
		dto.Warning = fmt.Sprintf("Together, the statements that can run at once can take %s, more than the %s of memory this machine has. If memory runs out, the system may stop a statement instead of DBTrail refusing it.",
			sqlMemoryWords(fmt.Sprintf("%dMiB", total)), sqlMemoryWords(fmt.Sprintf("%dMiB", dto.HostMemoryBytes>>20)))
	}
	return dto
}

// handleSQLSettingsGet is GET /api/sql-settings (settings:read).
func (s *Server) handleSQLSettingsGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.sqlSettings())
}

// handleSQLSettingsPut is PUT /api/sql-settings (settings:write):
// {"memory":"4GB"} saves and applies it, {"memory":""} goes back to the
// default. Saved first, applied second: a save that fails changes nothing.
func (s *Server) handleSQLSettingsPut(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Memory *string `json:"memory"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Memory == nil {
		writeJSONError(w, http.StatusBadRequest, `"memory" is required: a size such as 4GB, or "" for the default`)
		return
	}
	m := &s.sqlMem
	m.mutate.Lock()
	defer m.mutate.Unlock()
	if why := s.sqlMemoryLocked(); why != "" {
		writeJSONError(w, http.StatusConflict, why)
		return
	}
	var mib int64
	if raw := strings.TrimSpace(*req.Memory); raw != "" {
		var err error
		if mib, err = ParseSQLMemory(raw); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	before, _ := s.sqlMemoryNow()
	m.mu.Lock()
	path := m.path
	m.mu.Unlock()
	if err := saveSQLSettingsFile(path, sqlSettingsFile{MemoryMiB: mib}); err != nil {
		slog.Warn("console: SQL memory not saved; it stays as it was", "actor", consoleActor(r), "path", path, "error", err)
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	m.mu.Lock()
	m.savedMiB = mib
	m.mu.Unlock()
	after, src := s.sqlMemoryNow()
	slog.Info("console: SQL memory changed from the web interface", "actor", consoleActor(r),
		"from", sqlMemoryWords(before), "to", sqlMemoryWords(after), "source", src)
	if total, over := SQLMemoryOverHost(after, s.sqlMaxInFlight, s.hostMemoryBytes()); over {
		slog.Warn(fmt.Sprintf("console: SQL memory saved from the web interface, %d MiB: %d statements at once can take %d MiB, more than this host's %d MiB, which capture shares; a statement may be killed by the kernel instead of refused",
			sqlMiB(after), s.sqlMaxInFlight, total, s.hostMemoryBytes()>>20))
	}
	writeJSON(w, http.StatusOK, s.sqlSettings())
}

// SQLChainLimit is the line SQL on the copy answers from an earlier copy
// past, for the memory in force now (#2210).
func (s *Server) SQLChainLimit() int64 {
	m, _ := s.sqlMemoryNow()
	return sqlChainLimit(m)
}

// SQLFoldLine is the size the daemon's refresh keeps a table's changes
// under, for the memory in force now (sqlFoldChainBytes): it reads it each
// cycle and ends a table's chain at half of it, so a change saved in the web
// interface moves it on the next cycle.
func (s *Server) SQLFoldLine() int64 {
	m, _ := s.sqlMemoryNow()
	return sqlScaled(sqlFoldChainBytes, m)
}
