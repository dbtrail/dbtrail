package installid

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/serverid"
)

const (
	testSource = "user:pass@tcp(source.example.com:3306)/mydb"
	testIndex  = "root:pw@tcp(index-mysql:3306)/bintrail_idx_ab12"
)

func indexUUID(t *testing.T, id string, err error) {
	t.Helper()
	t.Cleanup(SetIndexServerUUIDForTest(func(context.Context, string) (string, error) { return id, err }))
}

// TestDeriveForInstall_TwoInstallationsGetTwoIDs is the point of it: the same
// source, through the same connection, from two installations.
func TestDeriveForInstall_TwoInstallationsGetTwoIDs(t *testing.T) {
	ctx := context.Background()
	indexUUID(t, "8b0c2f3e-7a11-4d5e-9c1a-000000000001", nil)
	a1, sourceOnly, err := DeriveForInstall(ctx, testSource, testIndex)
	if err != nil || sourceOnly != nil {
		t.Fatalf("first installation: %d, source-only because %v, %v", a1, sourceOnly, err)
	}
	a2, _, _ := DeriveForInstall(ctx, testSource, testIndex)
	if a1 != a2 {
		t.Fatalf("one installation derived %d then %d; the id must survive a restart", a1, a2)
	}
	legacy, _ := serverid.DeriveServerID(testSource)
	if a1 == legacy {
		t.Fatalf("the per-installation id equals the source-only one (%d)", a1)
	}

	// The same compose stack installed elsewhere: same index address, same
	// database name, another MySQL data directory.
	seen := map[uint32]string{a1: "first"}
	for i := range 100 {
		id := fmt.Sprintf("8b0c2f3e-7a11-4d5e-9c1a-1%011d", i)
		indexUUID(t, id, nil)
		got, sourceOnly, err := DeriveForInstall(ctx, testSource, testIndex)
		if err != nil || sourceOnly != nil {
			t.Fatalf("installation %s: source-only because %v, %v", id, sourceOnly, err)
		}
		if got < 100000000 {
			t.Errorf("installation %s: id %d is below the floor", id, got)
		}
		if prev, dup := seen[got]; dup {
			t.Fatalf("installations %s and %s both derive %d", prev, id, got)
		}
		seen[got] = id
	}

	// One index server holding two installations' databases.
	indexUUID(t, "8b0c2f3e-7a11-4d5e-9c1a-000000000001", nil)
	other, _, _ := DeriveForInstall(ctx, testSource, "root:pw@tcp(index-mysql:3306)/another_index")
	if other == a1 {
		t.Fatalf("two index databases on one server derive the same id (%d)", other)
	}
}

// TestDeriveForInstall_FallsBackAndNeverFails: an index that cannot say who
// it is costs the per-installation part, not capture.
func TestDeriveForInstall_FallsBackAndNeverFails(t *testing.T) {
	ctx := context.Background()
	legacy, _ := serverid.DeriveServerID(testSource)
	called := 0
	t.Cleanup(SetInstallSaltRetryWaitForTest(0))
	t.Cleanup(SetIndexServerUUIDForTest(func(context.Context, string) (string, error) {
		called++
		return "", errors.New("dial tcp: connection refused")
	}))
	for name, index := range map[string]string{
		"unreachable index": testIndex,
		"no index DSN":      "",
		"blank index DSN":   "   ",
		"unparseable index": "not a dsn",
	} {
		got, sourceOnly, err := DeriveForInstall(ctx, testSource, index)
		if err != nil || sourceOnly == nil || got != legacy {
			t.Errorf("%s: %d, source-only because %v, %v; want the source-only id %d, a reason and no error", name, got, sourceOnly, err, legacy)
		}
	}
	if called != installSaltAttempts {
		t.Errorf("the index was asked %d times; want %d tries for the one parseable DSN and none for the rest", called, installSaltAttempts)
	}
	indexUUID(t, "  ", nil)
	if got, sourceOnly, err := DeriveForInstall(ctx, testSource, testIndex); err != nil || sourceOnly == nil || got != legacy {
		t.Errorf("an index with no @@server_uuid: %d, source-only because %v, %v", got, sourceOnly, err)
	}

	// The source DSN is still the caller's to get right, either way.
	indexUUID(t, "8b0c2f3e-7a11-4d5e-9c1a-000000000001", nil)
	if _, _, err := DeriveForInstall(ctx, "not-a-dsn", testIndex); err == nil {
		t.Error("an unparseable source DSN derived an id with a readable index")
	}
	indexUUID(t, "", errors.New("down"))
	if _, _, err := DeriveForInstall(ctx, "not-a-dsn", testIndex); err == nil {
		t.Error("an unparseable source DSN derived an id with an unreadable index")
	}
}

// TestInstallSalt: the same server spelled differently is the same server.
func TestInstallSalt(t *testing.T) {
	ctx := context.Background()
	indexUUID(t, " 8B0C2F3E-7A11-4D5E-9C1A-000000000001\n", nil)
	got, err := InstallSalt(ctx, testIndex)
	if err != nil || got != "8b0c2f3e-7a11-4d5e-9c1a-000000000001|bintrail_idx_ab12" {
		t.Fatalf("InstallSalt = %q, %v", got, err)
	}
	// No database in the DSN is still an installation (the server's own id).
	if got, err := InstallSalt(ctx, "root:pw@tcp(index-mysql:3306)/"); err != nil || got != "8b0c2f3e-7a11-4d5e-9c1a-000000000001|" {
		t.Fatalf("InstallSalt with no database = %q, %v", got, err)
	}
}

// TestInstallSalt_OneDroppedConnectionDoesNotDecideTheID: the id chosen at
// startup lasts for the life of the process, so a read that fails once (a
// MySQL still finishing its first start) is tried again.
func TestInstallSalt_OneDroppedConnectionDoesNotDecideTheID(t *testing.T) {
	t.Cleanup(SetInstallSaltRetryWaitForTest(0))
	calls := 0
	t.Cleanup(SetIndexServerUUIDForTest(func(context.Context, string) (string, error) {
		if calls++; calls == 1 {
			return "", errors.New("invalid connection")
		}
		return "8b0c2f3e-7a11-4d5e-9c1a-000000000001", nil
	}))
	id, sourceOnly, err := DeriveForInstall(context.Background(), testSource, testIndex)
	legacy, _ := serverid.DeriveServerID(testSource)
	if err != nil || sourceOnly != nil || id == legacy || calls != 2 {
		t.Fatalf("id %d, source-only because %v, %v, after %d reads; want the per-installation id on the second read", id, sourceOnly, err, calls)
	}

	// A cancelled context ends the wait instead of sitting through it.
	t.Cleanup(SetInstallSaltRetryWaitForTest(time.Hour))
	t.Cleanup(SetIndexServerUUIDForTest(func(context.Context, string) (string, error) { return "", errors.New("down") }))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { _, _, _ = DeriveForInstall(ctx, testSource, testIndex); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a cancelled context did not end the retry wait")
	}
}

// TestAutoDerive: what a command with no --server-id prints, and that it
// reads the index it was given.
func TestAutoDerive(t *testing.T) {
	var asked string
	t.Cleanup(SetIndexServerUUIDForTest(func(_ context.Context, dsn string) (string, error) {
		asked = dsn
		return "8b0c2f3e-7a11-4d5e-9c1a-000000000001", nil
	}))
	var out strings.Builder
	id, err := AutoDerive(context.Background(), &out, testSource, testIndex)
	want, _, _ := DeriveForInstall(context.Background(), testSource, testIndex)
	if err != nil || id != want || asked != testIndex {
		t.Fatalf("AutoDerive = %d, %v (asked %q); want %d from %q", id, err, asked, want, testIndex)
	}
	if got := out.String(); got != fmt.Sprintf("Auto-derived server-id from the source connection and this installation's index: %d\n", id) {
		t.Errorf("printed %q", got)
	}

	out.Reset()
	legacy, _ := serverid.DeriveServerID(testSource)
	if id, err := AutoDerive(context.Background(), &out, testSource, ""); err != nil || id != legacy ||
		out.String() != fmt.Sprintf("Auto-derived server-id from source DSN: %d\n", legacy) {
		t.Errorf("with no index: %d, %v, printed %q", id, err, out.String())
	}
	if _, err := AutoDerive(context.Background(), &out, "not-a-dsn", testIndex); err == nil || !strings.Contains(err.Error(), "--server-id") {
		t.Errorf("an unparseable source: %v, want an error naming --server-id", err)
	}
}
