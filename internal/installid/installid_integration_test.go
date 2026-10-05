//go:build integration

package installid_test

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/installid"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationInstallSalt_AsksARealIndexServer runs the one query the unit
// tests replace: the index server's own identity, read through a DSN whose
// database does not exist yet (the id is derived before the index is sure to).
func TestIntegrationInstallSalt_AsksARealIndexServer(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	var want string
	if err := db.QueryRow("SELECT @@server_uuid").Scan(&want); err != nil {
		t.Fatalf("read @@server_uuid: %v", err)
	}
	if !regexp.MustCompile(`^[0-9a-f-]{36}$`).MatchString(strings.ToLower(want)) {
		t.Fatalf("setup: @@server_uuid = %q", want)
	}

	const noSuchDB = "bintrail_install_salt_no_such_database"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	salt, err := installid.InstallSalt(ctx, testutil.IntegrationDSN(noSuchDB))
	if err != nil || salt != strings.ToLower(want)+"|"+noSuchDB {
		t.Fatalf("InstallSalt = %q, %v; want the server's own id and the database named, with the database absent", salt, err)
	}

	const source = "u:p@tcp(source.example.com:3306)/"
	id, sourceOnly, err := installid.DeriveForInstall(ctx, source, testutil.IntegrationDSN(noSuchDB))
	shared, _ := serverid.DeriveServerID(source)
	if err != nil || sourceOnly != nil || id == shared {
		t.Fatalf("DeriveForInstall = %d (source-only because %v), %v; the shared id is %d", id, sourceOnly, err, shared)
	}
}
