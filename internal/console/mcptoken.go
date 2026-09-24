package console

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/ext"
)

// mcpTokenFileVersion is the managed-MCP-token file schema version this binary
// writes and fully understands. A file with a HIGHER version loads read-only
// (generate/rotate/revoke refuse; the token authenticates only if its digest
// field is still readable) — same contract as the auth file and the registry.
const mcpTokenFileVersion = 1

// mcpTokenPrefix makes a managed token recognizable in configs and logs
// ("bmt_" = bintrail managed token) without revealing anything — the shape is
// not a secret, the value is.
const mcpTokenPrefix = "bmt_"

// ErrMCPTokenFileReadOnly is returned by write paths when the on-disk file was
// written by a newer bintrail (version > mcpTokenFileVersion).
var ErrMCPTokenFileReadOnly = errors.New("MCP token file was written by a newer bintrail; changes are refused")

// errMCPTokenFileMalformed marks load failures that mean the CONTENT is junk
// (unparseable YAML, bad digest in a version we own) — the class the
// generate/revoke self-heal is allowed to replace or remove. Read errors
// (EACCES, EIO) deliberately do NOT carry this mark: an unreadable file might
// be a newer binary's perfectly valid credential, and destroying what we
// could not inspect would bypass the read-only contract.
var errMCPTokenFileMalformed = errors.New("malformed MCP token file")

// MCPTokenFile is the on-disk managed MCP token: only the SHA-256 of the
// token is persisted (API-key pattern — the plaintext is shown once at
// generation and never stored), so a read of the file does not yield a usable
// credential. The versioned envelope and Extra leave room for additive fields
// without a format break (authfile.go precedent). Construct only via
// LoadMCPTokenFile / GenerateMCPToken — readOnly is set at load time.
type MCPTokenFile struct {
	Version     int    `yaml:"version"`
	TokenSHA256 string `yaml:"token_sha256"`
	CreatedAt   string `yaml:"created_at"`
	// Permissions is the grant set recorded at mint time (#1124): the minting
	// session's permissions, so a token can never exceed what its minter could
	// do through the browser API. A nil (absent) field means the FULL read
	// surface — both the file a full-access minter writes (every OSS session,
	// the static token, a password login) and every file minted before grants
	// were recorded, which were all minted by full-access sessions in OSS
	// builds. A present-but-empty list grants nothing; the pointer keeps
	// "absent" and "empty" distinguishable across the YAML round trip.
	Permissions *[]string `yaml:"permissions,omitempty"`
	// Extra preserves unknown top-level fields a newer binary wrote, across
	// this binary's load→save cycle.
	Extra map[string]any `yaml:",inline"`

	readOnly bool
}

// mcpTokenFileMu serializes writers within this process. Cross-process
// collisions are last-writer-wins on the atomic rename — acceptable for a
// single-credential file.
var mcpTokenFileMu sync.Mutex

// DefaultMCPTokenPath returns ~/.config/bintrail/console-mcp-token.yaml, with
// the same homeless fallback as DefaultAuthPath (see configPath).
func DefaultMCPTokenPath() string {
	return configPath("console-mcp-token.yaml")
}

// LoadMCPTokenFile reads the managed-token file at path. A missing file
// returns (nil, nil): no managed token configured. An unparseable file, or a
// malformed digest in a file THIS version owns, errors with
// errMCPTokenFileMalformed. A NEWER-version file is never an error for its
// payload: it loads read-only, and a digest this binary can't read simply
// never authenticates — a newer format must degrade, not brick startup or
// demand deletion (the readOnly contract).
func LoadMCPTokenFile(path string) (*MCPTokenFile, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read console MCP token file %s: %w", path, err)
	}
	var f MCPTokenFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("%w: parse %s: %v", errMCPTokenFileMalformed, path, err)
	}
	if f.Version > mcpTokenFileVersion {
		f.readOnly = true
		return &f, nil
	}
	if _, err := f.digest(); err != nil {
		return nil, fmt.Errorf("%w: %s has a malformed token_sha256; delete it or generate a new token from Settings → MCP Server", errMCPTokenFileMalformed, path)
	}
	return &f, nil
}

// ReadOnly reports whether write paths must refuse (newer-version file).
func (f *MCPTokenFile) ReadOnly() bool { return f != nil && f.readOnly }

// digest returns the decoded SHA-256, or an error when the field is not a
// 32-byte hex string.
func (f *MCPTokenFile) digest() ([]byte, error) {
	raw, err := hex.DecodeString(f.TokenSHA256)
	if err != nil {
		return nil, err
	}
	if len(raw) != sha256.Size {
		return nil, fmt.Errorf("digest is %d bytes, want %d", len(raw), sha256.Size)
	}
	return raw, nil
}

// GenerateMCPToken mints a new managed token, persists its SHA-256 at path
// (atomic, 0600), and returns the plaintext and the stored record. A
// pre-existing file is replaced (rotate); a MALFORMED file is replaced too
// (the UI's Generate button is the documented self-heal); an unreadable file
// or a newer-version file refuses — what could not be inspected must not be
// destroyed.
//
// grants is the permission set the token will carry on /mcp tool dispatch
// (#1124): nil records no permissions field — the full read surface, the
// full-access-minter case — while a non-nil slice (even empty) is recorded
// verbatim and caps the token at exactly those permissions. A rotate always
// re-records from the CURRENT minter, never from the replaced file.
func GenerateMCPToken(path string, grants []ext.Permission) (string, *MCPTokenFile, error) {
	mcpTokenFileMu.Lock()
	defer mcpTokenFileMu.Unlock()

	existing, err := LoadMCPTokenFile(path)
	if err != nil {
		if !errors.Is(err, errMCPTokenFileMalformed) {
			return "", nil, err
		}
		slog.Warn("console: replacing malformed MCP token file", "path", path, "error", err)
		existing = nil
	}
	if existing.ReadOnly() {
		return "", nil, fmt.Errorf("%s: %w", path, ErrMCPTokenFileReadOnly)
	}

	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("generate MCP token: %w", err)
	}
	token := mcpTokenPrefix + hex.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))

	f := MCPTokenFile{
		Version:     mcpTokenFileVersion,
		TokenSHA256: hex.EncodeToString(sum[:]),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	if grants != nil {
		gs := make([]string, 0, len(grants))
		for _, g := range grants {
			gs = append(gs, string(g))
		}
		f.Permissions = &gs
	}
	if existing != nil {
		f.Extra = existing.Extra
	}
	if err := saveMCPTokenFile(path, &f); err != nil {
		return "", nil, err
	}
	return token, &f, nil
}

// RevokeMCPToken removes the managed-token file. A missing file is a no-op, a
// MALFORMED file is removed with a warning (revoking junk is still revoking),
// an unreadable file or a newer-version file refuses — deleting requires only
// directory permission, so an unreadable file could otherwise be destroyed
// without ever checking the read-only contract.
func RevokeMCPToken(path string) error {
	mcpTokenFileMu.Lock()
	defer mcpTokenFileMu.Unlock()

	existing, err := LoadMCPTokenFile(path)
	switch {
	case err != nil && errors.Is(err, errMCPTokenFileMalformed):
		slog.Warn("console: revoking malformed MCP token file", "path", path, "error", err)
	case err != nil:
		return fmt.Errorf("revoke MCP token: %w", err)
	case existing == nil:
		return nil
	case existing.ReadOnly():
		return fmt.Errorf("%s: %w", path, ErrMCPTokenFileReadOnly)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove console MCP token file %s: %w", path, err)
	}
	return nil
}

// saveMCPTokenFile writes atomically: marshal → temp file in the same
// directory → fsync → rename. File 0600, directory 0700 — it holds a
// credential hash (same class as the auth file). Callers hold mcpTokenFileMu.
func saveMCPTokenFile(path string, f *MCPTokenFile) error {
	data, err := yaml.Marshal(f)
	if err != nil {
		return fmt.Errorf("marshal console MCP token file %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create MCP token directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".console-mcp-token-*.yaml")
	if err != nil {
		return fmt.Errorf("create temp MCP token file in %s: %w", dir, err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp MCP token file for %s: %w", path, err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write console MCP token file %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync console MCP token file %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp MCP token file for %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace console MCP token file %s: %w", path, err)
	}
	return nil
}

// managedMCPToken is the server's live view of the managed token. Every check
// re-reads the ~100-byte file, so a rotate or revoke performed by ANOTHER
// console process sharing the path takes effect here immediately — a revoke
// that reports success must actually revoke everywhere, with no
// mtime-granularity window (the same staleness discipline as
// LoadAuthFile-per-login). The read runs on /mcp authentication, the Connect
// AI status endpoint, and the capabilities probe — never on the general /api
// data path, which does not accept this credential; at that request rate a
// tiny file read is noise.
type managedMCPToken struct {
	mu   sync.Mutex
	path string

	digest    []byte // raw SHA-256; nil = no managed token usable
	createdAt string
	readOnly  bool
	// perms is the token's recorded grant set (#1124); nil means the full
	// read surface (a full-access mint, or a file from before grants were
	// recorded). Projected from MCPTokenFile.Permissions on every refresh, so
	// a rotate by another process swaps digest and grants atomically here.
	perms []ext.Permission

	// loadWarned de-duplicates the unreadable-file warning: once per broken
	// state, not once per request (re-armed by a successful load).
	loadWarned bool
}

// initFromDisk seeds the state at New() time. Load errors degrade to
// not-configured (already logged by the caller) — an auxiliary credential
// file must never block console startup.
func (m *managedMCPToken) initFromDisk(path string, f *MCPTokenFile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.path = path
	m.applyLocked(f)
}

// applyLocked projects a loaded file (or nil) into the live fields. Callers
// hold mu.
func (m *managedMCPToken) applyLocked(f *MCPTokenFile) {
	if f == nil {
		m.digest, m.createdAt, m.readOnly, m.perms = nil, "", false, nil
		return
	}
	d, err := f.digest()
	if err != nil {
		// Only reachable for newer-version files (v1 digests are validated at
		// load): the file exists but this binary cannot authenticate against
		// it. readOnly is still reported so the UI can explain.
		d = nil
	}
	var perms []ext.Permission
	if f.Permissions != nil {
		perms = make([]ext.Permission, 0, len(*f.Permissions))
		for _, p := range *f.Permissions {
			perms = append(perms, ext.Permission(p))
		}
	}
	m.digest, m.createdAt, m.readOnly, m.perms = d, f.CreatedAt, f.readOnly, perms
}

// refreshLocked re-reads the file. A file that became unreadable or malformed
// degrades to not-configured (deny, warned once per broken state) — never a
// panic, never a stale accept. Callers hold mu.
func (m *managedMCPToken) refreshLocked() {
	if m.path == "" {
		return
	}
	f, err := LoadMCPTokenFile(m.path)
	if err != nil {
		if !m.loadWarned {
			slog.Warn("console: MCP token file unreadable; managed token disabled until regenerated", "path", m.path, "error", err)
			m.loadWarned = true
		}
		m.applyLocked(nil)
		return
	}
	m.loadWarned = false
	m.applyLocked(f)
}

// reload force-applies freshly persisted state (the generate/revoke handlers
// call it right after writing).
func (m *managedMCPToken) reload(f *MCPTokenFile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadWarned = false
	m.applyLocked(f)
}

func (m *managedMCPToken) configured() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshLocked()
	return m.digest != nil
}

func (m *managedMCPToken) info() (createdAt string, readOnly bool, configured bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshLocked()
	return m.createdAt, m.readOnly, m.digest != nil
}

// matches reports whether got is the managed token, comparing raw SHA-256
// digests in constant time (hashing the presented value also equalizes the
// compared length, so nothing about the stored credential's shape leaks).
// On a match it also returns the token's recorded grants as an access policy
// (#1124): nil for the full read surface, a policy capping tool dispatch at
// the minter's permissions otherwise. Match and grants come from the same
// locked refresh, so a concurrent rotate can never pair the old digest with
// the new grant set.
func (m *managedMCPToken) matches(got string) (bool, *ext.AccessPolicy) {
	if got == "" {
		return false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshLocked()
	if m.digest == nil {
		return false, nil
	}
	sum := sha256.Sum256([]byte(got))
	if subtle.ConstantTimeCompare(sum[:], m.digest) != 1 {
		return false, nil
	}
	if m.perms == nil {
		return true, nil
	}
	return true, &ext.AccessPolicy{Permissions: slices.Clone(m.perms)}
}
