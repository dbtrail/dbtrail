package serverid

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

const (
	testSource = "user:pass@tcp(source.example.com:3306)/mydb"
	testIndex  = "root:pw@tcp(index-mysql:3306)/bintrail_idx_ab12"
)

func indexUUID(t *testing.T, id string, err error) {
	t.Helper()
	t.Cleanup(SetIndexServerUUIDForTest(func(context.Context, string) (string, error) { return id, err }))
}

// TestDeriveServerID_SourceOnlyValueIsUnchanged pins the id an installation
// used before the index was part of it. It is still what capture falls back
// to, and the number an operator may have seen in a source's replica list.
func TestDeriveServerID_SourceOnlyValueIsUnchanged(t *testing.T) {
	got, err := DeriveServerID(testSource)
	if err != nil || got != 3762492331 {
		t.Fatalf("DeriveServerID = %d, %v; want 3762492331", got, err)
	}
}

// TestDeriveForInstall_TwoInstallationsGetTwoIDs is the point of it: the same
// source, through the same connection, from two installations.
func TestDeriveForInstall_TwoInstallationsGetTwoIDs(t *testing.T) {
	ctx := context.Background()
	indexUUID(t, "8b0c2f3e-7a11-4d5e-9c1a-000000000001", nil)
	a1, per, err := DeriveForInstall(ctx, testSource, testIndex)
	if err != nil || !per {
		t.Fatalf("first installation: %d, perInstall %v, %v", a1, per, err)
	}
	a2, _, _ := DeriveForInstall(ctx, testSource, testIndex)
	if a1 != a2 {
		t.Fatalf("one installation derived %d then %d; the id must survive a restart", a1, a2)
	}
	legacy, _ := DeriveServerID(testSource)
	if a1 == legacy {
		t.Fatalf("the per-installation id equals the source-only one (%d)", a1)
	}

	// The same compose stack installed elsewhere: same index address, same
	// database name, another MySQL data directory.
	seen := map[uint32]string{a1: "first"}
	for i := range 100 {
		id := fmt.Sprintf("8b0c2f3e-7a11-4d5e-9c1a-1%011d", i)
		indexUUID(t, id, nil)
		got, per, err := DeriveForInstall(ctx, testSource, testIndex)
		if err != nil || !per {
			t.Fatalf("installation %s: perInstall %v, %v", id, per, err)
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
	legacy, _ := DeriveServerID(testSource)
	called := 0
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
		got, per, err := DeriveForInstall(ctx, testSource, index)
		if err != nil || per || got != legacy {
			t.Errorf("%s: %d, perInstall %v, %v; want the source-only id %d and no error", name, got, per, err, legacy)
		}
	}
	if called != 1 {
		t.Errorf("the index was asked %d times; only a parseable DSN is worth a connection", called)
	}
	indexUUID(t, "  ", nil)
	if got, per, err := DeriveForInstall(ctx, testSource, testIndex); err != nil || per || got != legacy {
		t.Errorf("an index with no @@server_uuid: %d, perInstall %v, %v", got, per, err)
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
