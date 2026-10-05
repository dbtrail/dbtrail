// Package installid derives the replication server-id capture uses: the
// source's own triple (internal/serverid) and what tells this installation
// apart from another one capturing the same source. It is its own package
// because it opens a connection (internal/config), and internal/serverid is
// imported by the test helpers that package's own tests use.
package installid

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/serverid"
)

// readIndexServerUUID asks the index's MySQL server for its @@server_uuid.
// It connects to the server, not to the index database: the id is needed
// before that database is sure to exist. A variable for tests.
var readIndexServerUUID = func(ctx context.Context, indexDSN string) (string, error) {
	cfg, err := mysql.ParseDSN(indexDSN)
	if err != nil {
		return "", fmt.Errorf("parse index DSN: %w", err)
	}
	cfg.DBName = ""
	db, err := config.Connect(cfg.FormatDSN())
	if err != nil {
		return "", err
	}
	defer db.Close()
	var id sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&id); err != nil {
		return "", err
	}
	return id.String, nil
}

// InstallSalt is what tells one installation apart from another capturing the
// same source: the @@server_uuid of the MySQL server its index lives on, which
// MySQL generates once and keeps with the data, and the index database's
// name. Two installations have two index servers, or two databases on one.
// It is stable across restarts, so the id derived with it is too.
//
// Not told apart: an installation cloned WITH its index's data directory (a
// machine image, a volume snapshot) carries the same @@server_uuid, and the
// two then derive the same id, as every installation did before.
//
// The read is tried a few times: the id chosen at startup is kept for the
// life of the process, and one dropped connection (a MySQL still finishing
// its first start) must not decide it.
func InstallSalt(ctx context.Context, indexDSN string) (string, error) {
	if strings.TrimSpace(indexDSN) == "" {
		return "", errors.New("no index DSN")
	}
	cfg, err := mysql.ParseDSN(indexDSN)
	if err != nil {
		return "", fmt.Errorf("parse index DSN: %w", err)
	}
	var id string
	for attempt := 1; ; attempt++ {
		if id, err = readIndexServerUUID(ctx, indexDSN); err == nil || attempt == installSaltAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(installSaltRetryWait):
		}
	}
	if err != nil {
		return "", err
	}
	id = strings.ToLower(strings.TrimSpace(id))
	if id == "" {
		return "", errors.New("the index server reports no @@server_uuid")
	}
	return id + "|" + cfg.DBName, nil
}

const installSaltAttempts = 3

// installSaltRetryWait is a variable so tests do not wait.
var installSaltRetryWait = time.Second

// DeriveForInstall returns the replication server-id for capturing sourceDSN
// from the installation whose index is indexDSN: stable across restarts, and
// different from the one another installation derives for the same source.
//
// sourceOnly is nil when the index said who it is. Otherwise it is why not
// (no index DSN, an index server that does not answer or has no
// @@server_uuid), and the id is serverid.DeriveServerID's: it works, and every
// installation capturing this source through the same connection shares it.
// That is never an error, because capture must not wait on this; a caller
// that is about to capture logs it (WarnSourceOnly), and one that only
// reports (doctor) says it.
func DeriveForInstall(ctx context.Context, sourceDSN, indexDSN string) (id uint32, sourceOnly error, err error) {
	salt, saltErr := InstallSalt(ctx, indexDSN)
	if saltErr != nil {
		id, err = serverid.DeriveServerIDSalted(sourceDSN, "")
		return id, saltErr, err
	}
	id, err = serverid.DeriveServerIDSalted(sourceDSN, salt)
	return id, nil, err
}

// WarnSourceOnly logs that capture is about to use the id every installation
// shares, and why. server names the source when the caller has a name for it.
func WarnSourceOnly(id uint32, server string, why error) {
	slog.Warn("replication server-id derived from the source connection alone; another DBTrail capturing this source through the same connection would use the same id and the two would interrupt each other. It is chosen again at the next start",
		"server_id", id, "server", server, "reason", why.Error())
}

// AutoDerive is the derivation for a command that was given no --server-id:
// it derives, says on w which id it chose and from what, and warns when the
// id is the shared one.
func AutoDerive(ctx context.Context, w io.Writer, sourceDSN, indexDSN string) (uint32, error) {
	id, sourceOnly, err := DeriveForInstall(ctx, sourceDSN, indexDSN)
	if err != nil {
		return 0, fmt.Errorf("cannot auto-derive --server-id from --source-dsn: %w (pass --server-id explicitly to bypass)", err)
	}
	if sourceOnly != nil {
		WarnSourceOnly(id, "", sourceOnly)
		fmt.Fprintf(w, "Auto-derived server-id from source DSN: %d\n", id)
		return id, nil
	}
	fmt.Fprintf(w, "Auto-derived server-id from the source connection and this installation's index: %d\n", id)
	return id, nil
}

// SetIndexServerUUIDForTest replaces the query that reads the index server's
// @@server_uuid, so a test can derive ids without a MySQL to ask.
func SetIndexServerUUIDForTest(read func(ctx context.Context, indexDSN string) (string, error)) (restore func()) {
	prev := readIndexServerUUID
	readIndexServerUUID = read
	return func() { readIndexServerUUID = prev }
}

// SetInstallSaltRetryWaitForTest shortens the wait between reads of the index
// server's identity.
func SetInstallSaltRetryWaitForTest(d time.Duration) (restore func()) {
	prev := installSaltRetryWait
	installSaltRetryWait = d
	return func() { installSaltRetryWait = prev }
}
