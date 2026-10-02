package console

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.yaml.in/yaml/v2"
	"golang.org/x/crypto/bcrypt"
)

// authFileVersion is the auth file schema version this binary writes and fully
// understands. A file with a HIGHER version loads read-only (logins still
// verify; every write path refuses) — same contract as the server registry.
const authFileVersion = 1

// consoleBcryptCost is the bcrypt work factor for the console password
// (~250ms/verify). The stored "$2a$12$..." string is self-describing, so a
// future cost bump keeps verifying old hashes; verifyAndMaybeRehash upgrades
// them opportunistically at login, the one moment the plaintext is in hand.
const consoleBcryptCost = 12

// bcryptCost is the cost SetAuthPassword hashes at and the cost
// verifyAndMaybeRehash upgrades to. It equals consoleBcryptCost in every
// shipped binary; only test binaries lower it, through the hook in testing.go,
// so their logins do not pay ~250ms (several seconds under -race) per hash.
var bcryptCost = consoleBcryptCost

// minPasswordChars is the floor enforced when a password is SET (never at
// verify time — an existing shorter password must keep logging in after a
// policy bump). The ceiling is bcrypt's own 72-byte limit, rejected with a
// clear message rather than silently truncated.
const minPasswordChars = 8

// dummyBcryptHash is a fixed cost-12 hash of a throwaway string. Login
// attempts with an unknown username still run a full bcrypt compare against
// it, so response timing cannot distinguish "wrong user" from "wrong
// password". Pinned by TestLoginUnknownUserRunsBcrypt — do not "optimize" the
// compare away.
const dummyBcryptHash = "$2a$12$wQ5gZchl8aI7U.iiz1zPZ.GTvhtnntAjYuvdkvRrtj9MxOAvrVA86"

// ErrAuthFileReadOnly is returned by write paths when the on-disk file was
// written by a newer bintrail (version > authFileVersion).
var ErrAuthFileReadOnly = errors.New("auth file was written by a newer bintrail; logins work but changes are refused")

// bcryptCompare is bcrypt.CompareHashAndPassword, injectable so tests can
// pin that the timing-equalizing compare fires on unknown-username attempts
// without resorting to flaky wall-clock assertions.
var bcryptCompare = bcrypt.CompareHashAndPassword

// AuthFile is the on-disk console credential: one username and one bcrypt
// hash. The console is single-user by design (multi-user/RBAC is dbtrail
// territory); the versioned envelope and Extra leave room for additive fields
// without a format break.
type AuthFile struct {
	Version        int    `yaml:"version"`
	Username       string `yaml:"username"`
	PasswordBcrypt string `yaml:"password_bcrypt"`
	UpdatedAt      string `yaml:"updated_at"`
	// Extra preserves unknown top-level fields a newer binary wrote, across
	// this binary's load→save cycle (registry.go precedent; inline requires
	// map[string]any).
	Extra map[string]any `yaml:",inline"`

	readOnly bool
}

// authFileMu serializes writers within this process (the change-password
// handler). Cross-process collisions (CLI vs server) are last-writer-wins on
// the atomic rename — acceptable for a single-user file.
var authFileMu sync.Mutex

// DefaultAuthPath returns ~/.config/bintrail/console-auth.yaml, with the same
// homeless fallback as DefaultRegistryPath (see configPath).
func DefaultAuthPath() string {
	return configPath("console-auth.yaml")
}

// LoadAuthFile reads the credential file at path. A missing file returns
// (nil, nil): password login not configured. A present-but-unreadable or
// unparseable file is a loud error — silently degrading to token-only when
// the operator configured a password would be an auth downgrade.
func LoadAuthFile(path string) (*AuthFile, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read console auth file %s: %w", path, err)
	}
	var a AuthFile
	if err := yaml.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("parse console auth file %s: %w", path, err)
	}
	if a.Version > authFileVersion {
		a.readOnly = true
	}
	if a.Username == "" || a.PasswordBcrypt == "" {
		return nil, fmt.Errorf("console auth file %s is missing username or password_bcrypt; re-create it with `bintrail-console user set-password`", path)
	}
	// Reject a structurally-broken hash at load time, so login, `user status`,
	// and change-password all fail loud together. Without this a truncated /
	// hand-edited password_bcrypt would parse here (non-empty), then silently
	// fail every bcrypt compare (login looks like "wrong password" forever)
	// while `user status` reports the file as healthy. bcrypt.Cost validates
	// the hash structure; any cost is accepted (rehash-on-login upgrades it).
	if _, err := bcrypt.Cost([]byte(a.PasswordBcrypt)); err != nil {
		return nil, fmt.Errorf("console auth file %s has a malformed password hash (%v); re-create it with `bintrail-console user set-password`", path, err)
	}
	return &a, nil
}

// ReadOnly reports whether write paths must refuse (newer-version file).
func (a *AuthFile) ReadOnly() bool { return a != nil && a.readOnly }

// VerifyPassword checks username+password against the stored credential in
// constant time: the username compare is subtle.ConstantTimeCompare and a
// username mismatch STILL runs a full bcrypt compare (against the dummy hash)
// so timing cannot enumerate the username. Safe on a nil receiver (false).
func (a *AuthFile) VerifyPassword(username, password string) bool {
	if a == nil {
		_ = bcryptCompare([]byte(dummyBcryptHash), []byte(password))
		return false
	}
	userOK := subtle.ConstantTimeCompare([]byte(username), []byte(a.Username)) == 1
	hash := a.PasswordBcrypt
	if !userOK {
		hash = dummyBcryptHash
	}
	passOK := bcryptCompare([]byte(hash), []byte(password)) == nil
	return userOK && passOK
}

// ValidateNewPassword enforces the set-time policy: at least minPasswordChars
// characters and at most 72 bytes (bcrypt's hard limit — reject, never
// truncate).
func ValidateNewPassword(password string) error {
	if len([]rune(password)) < minPasswordChars {
		return fmt.Errorf("password must be at least %d characters", minPasswordChars)
	}
	if len(password) > 72 {
		return errors.New("password exceeds bcrypt's 72-byte limit; choose a shorter one")
	}
	return nil
}

// SetAuthPassword hashes password (bcrypt, cost consoleBcryptCost) and writes
// the credential file atomically, preserving any Extra fields a newer binary
// stored. username == "" keeps the existing username (default "admin" on
// first set). Refuses a newer-version file with ErrAuthFileReadOnly.
func SetAuthPassword(path, username, password string) error {
	if err := ValidateNewPassword(password); err != nil {
		return err
	}
	authFileMu.Lock()
	defer authFileMu.Unlock()

	existing, err := LoadAuthFile(path)
	if err != nil {
		return err
	}
	if existing.ReadOnly() {
		return fmt.Errorf("%s: %w", path, ErrAuthFileReadOnly)
	}

	a := AuthFile{Version: authFileVersion, Username: username}
	if existing != nil {
		a.Extra = existing.Extra
		if a.Username == "" {
			a.Username = existing.Username
		}
	}
	if a.Username == "" {
		a.Username = "admin"
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
	if err != nil {
		// ErrPasswordTooLong is pre-empted by ValidateNewPassword; anything
		// else here is an internal bcrypt failure.
		return fmt.Errorf("hash console password for %s: %w", path, err)
	}
	a.PasswordBcrypt = string(hash)
	a.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return saveAuthFile(path, &a)
}

// verifyAndMaybeRehash is the login-path verify: on success, if the stored
// hash uses a cost below consoleBcryptCost, it re-hashes at the current cost
// and rewrites the file (best-effort — a failed rewrite never fails the
// login).
func verifyAndMaybeRehash(path string, a *AuthFile, username, password string) bool {
	if !a.VerifyPassword(username, password) {
		return false
	}
	if cost, err := bcrypt.Cost([]byte(a.PasswordBcrypt)); err == nil && cost < bcryptCost && !a.ReadOnly() {
		_ = SetAuthPassword(path, a.Username, password)
	}
	return true
}

// saveAuthFile writes atomically: marshal → temp file in the same directory →
// fsync → rename. File 0600, directory 0700 — it holds a credential hash
// (same class as the server registry). Callers hold authFileMu.
func saveAuthFile(path string, a *AuthFile) error {
	// Every error wraps the resolved path: in a homeless container the default
	// resolves outside the operator's home and the path is the only clue
	// (e.g. a full-disk write needs to say WHERE).
	data, err := yaml.Marshal(a)
	if err != nil {
		return fmt.Errorf("marshal console auth file %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create auth directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".console-auth-*.yaml")
	if err != nil {
		return fmt.Errorf("create temp auth file in %s: %w", dir, err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp auth file for %s: %w", path, err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write console auth file %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync console auth file %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp auth file for %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace console auth file %s: %w", path, err)
	}
	return nil
}
