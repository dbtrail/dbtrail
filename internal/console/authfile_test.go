package console

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestAuthFileMissingIsNotConfigured(t *testing.T) {
	a, err := LoadAuthFile(filepath.Join(t.TempDir(), "nope.yaml"))
	if err != nil || a != nil {
		t.Fatalf("missing file: got (%v, %v), want (nil, nil)", a, err)
	}
}

func TestAuthFileCorruptFailsLoud(t *testing.T) {
	p := filepath.Join(t.TempDir(), "auth.yaml")
	os.WriteFile(p, []byte("{not yaml::"), 0o600)
	if _, err := LoadAuthFile(p); err == nil {
		t.Fatal("corrupt file should fail loud, not degrade to token-only")
	}
	// Parseable but missing fields is also corrupt — a hand-edited file that
	// lost its hash must not load as a verifiable credential.
	os.WriteFile(p, []byte("version: 1\nusername: admin\n"), 0o600)
	if _, err := LoadAuthFile(p); err == nil {
		t.Fatal("file without password_bcrypt should fail loud")
	}
}

func TestSetAuthPasswordRoundTrip(t *testing.T) {
	useShippedBcryptCost(t)
	p := filepath.Join(t.TempDir(), "auth.yaml")
	if err := SetAuthPassword(p, "", "hunter22hunter"); err != nil {
		t.Fatal(err)
	}
	a, err := LoadAuthFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if a.Username != "admin" {
		t.Errorf("default username = %q, want admin", a.Username)
	}
	if !a.VerifyPassword("admin", "hunter22hunter") {
		t.Error("round-trip verify failed")
	}
	if a.VerifyPassword("admin", "wrong-password") {
		t.Error("wrong password verified")
	}
	if a.VerifyPassword("administrator", "hunter22hunter") {
		t.Error("wrong username verified")
	}
	if cost, _ := bcrypt.Cost([]byte(a.PasswordBcrypt)); cost != consoleBcryptCost {
		t.Errorf("stored cost = %d, want %d", cost, consoleBcryptCost)
	}
	if a.UpdatedAt == "" {
		t.Error("updated_at not stamped")
	}
}

func TestSetAuthPasswordFilePerms(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub")
	p := filepath.Join(dir, "auth.yaml")
	if err := SetAuthPassword(p, "ops", "longenoughpw"); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("file perm = %o, want 600", perm)
	}
	di, _ := os.Stat(dir)
	if perm := di.Mode().Perm(); perm != 0o700 {
		t.Errorf("dir perm = %o, want 700", perm)
	}
	// Atomic write leaves no temp droppings.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("dir has %d entries, want 1 (no temp files)", len(entries))
	}
}

func TestSetAuthPasswordPolicy(t *testing.T) {
	p := filepath.Join(t.TempDir(), "auth.yaml")
	if err := SetAuthPassword(p, "", "short77"); err == nil {
		t.Error("7-char password accepted")
	}
	if err := SetAuthPassword(p, "", strings.Repeat("x", 73)); err == nil {
		t.Error("73-byte password accepted (bcrypt truncates silently past 72)")
	}
	if err := SetAuthPassword(p, "", strings.Repeat("x", 72)); err != nil {
		t.Errorf("72-byte password rejected: %v", err)
	}
}

func TestSetAuthPasswordPreservesUsernameAndExtra(t *testing.T) {
	p := filepath.Join(t.TempDir(), "auth.yaml")
	// Simulate a future binary having written an extra top-level field.
	hash, _ := bcrypt.GenerateFromPassword([]byte("originalpass"), bcrypt.MinCost)
	os.WriteFile(p, []byte("version: 1\nusername: ops\npassword_bcrypt: "+string(hash)+"\nfuture_field: keep-me\n"), 0o600)

	if err := SetAuthPassword(p, "", "rotatedpassword"); err != nil {
		t.Fatal(err)
	}
	a, err := LoadAuthFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if a.Username != "ops" {
		t.Errorf("rotation changed username to %q, want ops preserved", a.Username)
	}
	data, _ := os.ReadFile(p)
	if !strings.Contains(string(data), "future_field: keep-me") {
		t.Error("Extra inline field lost across load→save (forward-compat broken)")
	}
}

func TestAuthFileNewerVersionLoadsReadOnly(t *testing.T) {
	p := filepath.Join(t.TempDir(), "auth.yaml")
	hash, _ := bcrypt.GenerateFromPassword([]byte("futurepass99"), bcrypt.MinCost)
	os.WriteFile(p, []byte("version: 99\nusername: admin\npassword_bcrypt: "+string(hash)+"\n"), 0o600)

	a, err := LoadAuthFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !a.ReadOnly() {
		t.Error("version 99 file should load read-only")
	}
	if !a.VerifyPassword("admin", "futurepass99") {
		t.Error("read-only file must still verify logins")
	}
	if err := SetAuthPassword(p, "", "newpassword1"); !errors.Is(err, ErrAuthFileReadOnly) {
		t.Errorf("write to newer-version file: err = %v, want ErrAuthFileReadOnly", err)
	}
}

func TestVerifyNilAuthFileRunsDummyCompare(t *testing.T) {
	// nil receiver must deny — and still burn a bcrypt compare against the
	// dummy hash so a missing file is not timing-distinguishable at verify.
	var gotHash []byte
	orig := bcryptCompare
	bcryptCompare = func(hash, pw []byte) error { gotHash = hash; return orig(hash, pw) }
	t.Cleanup(func() { bcryptCompare = orig })

	var a *AuthFile
	if a.VerifyPassword("admin", "whatever-pass") {
		t.Error("nil AuthFile verified a password")
	}
	if string(gotHash) != dummyBcryptHash {
		t.Errorf("nil-receiver compare ran against %q, want the dummy hash", gotHash)
	}
}

// TestDummyHashCostMatchesReal pins the load-bearing timing-equalization
// invariant: the dummy hash run on an unknown username must cost the SAME as a
// real verify, or response time leaks whether the username exists. A future
// edit dropping consoleBcryptCost or the dummy to a cheaper cost would reopen
// the enumeration oracle while every other test stays green.
func TestDummyHashCostMatchesReal(t *testing.T) {
	cost, err := bcrypt.Cost([]byte(dummyBcryptHash))
	if err != nil {
		t.Fatalf("dummy hash is not a valid bcrypt hash: %v", err)
	}
	if cost != consoleBcryptCost {
		t.Errorf("dummy hash cost = %d, want %d (timing oracle)", cost, consoleBcryptCost)
	}
}

func TestAuthFileMalformedHashFailsLoud(t *testing.T) {
	p := filepath.Join(t.TempDir(), "auth.yaml")
	// Present but structurally broken hash: must fail loud at load, so login,
	// `user status`, and change-password all agree the file is corrupt rather
	// than login silently treating it as a permanent wrong password.
	os.WriteFile(p, []byte("version: 1\nusername: admin\npassword_bcrypt: $2a$12$truncated\n"), 0o600)
	if _, err := LoadAuthFile(p); err == nil {
		t.Fatal("malformed password_bcrypt loaded as a valid credential")
	}
}

func TestVerifyAndMaybeRehashUpgradesCost(t *testing.T) {
	useShippedBcryptCost(t)
	p := filepath.Join(t.TempDir(), "auth.yaml")
	hash, _ := bcrypt.GenerateFromPassword([]byte("upgrademe123"), bcrypt.MinCost)
	os.WriteFile(p, []byte("version: 1\nusername: admin\npassword_bcrypt: "+string(hash)+"\n"), 0o600)

	a, _ := LoadAuthFile(p)
	if !verifyAndMaybeRehash(p, a, "admin", "upgrademe123") {
		t.Fatal("verify failed")
	}
	a2, err := LoadAuthFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if cost, _ := bcrypt.Cost([]byte(a2.PasswordBcrypt)); cost != consoleBcryptCost {
		t.Errorf("post-login cost = %d, want opportunistic rehash to %d", cost, consoleBcryptCost)
	}
	if !a2.VerifyPassword("admin", "upgrademe123") {
		t.Error("rehashed credential no longer verifies")
	}
}
