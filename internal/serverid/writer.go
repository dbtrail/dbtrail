package serverid

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// SnapshotWriterID returns the identity a snapshot written from this index
// is signed with (#1762), "" when the index names none.
//
// It is the source's bintrail_id, read where the rest of the product reads
// it: the stream's own row in stream_state first (the id archives are
// partitioned under), and, for an index no stream ever wrote to, the one
// source registered in bintrail_servers. An index serving several sources
// with no stream names no single writer, so it signs nothing: an unsigned
// snapshot takes no part in the shared-location warning, and a guessed
// identity would raise that warning falsely.
//
// A missing table is "names none", not an error: an index created before the
// table existed is not broken. Any other failure is returned, and the caller
// publishes unsigned.
func SnapshotWriterID(ctx context.Context, db *sql.DB) (string, error) {
	var id sql.NullString
	err := db.QueryRowContext(ctx, "SELECT bintrail_id FROM stream_state WHERE id = 1").Scan(&id)
	switch {
	case err == nil:
		if s := strings.TrimSpace(id.String); s != "" {
			return s, nil
		}
	case errors.Is(err, sql.ErrNoRows), noSuchTableOrColumn(err):
	default:
		return "", fmt.Errorf("read the stream's bintrail_id: %w", err)
	}

	var n int
	var only sql.NullString
	err = db.QueryRowContext(ctx, "SELECT COUNT(*), MIN(bintrail_id) FROM bintrail_servers").Scan(&n, &only)
	switch {
	case err == nil:
	case noSuchTableOrColumn(err):
		return "", nil
	default:
		return "", fmt.Errorf("read the registered sources: %w", err)
	}
	if n != 1 {
		return "", nil
	}
	return strings.TrimSpace(only.String), nil
}

// noSuchTableOrColumn reports MySQL's "table doesn't exist" (1146) and
// "unknown column" (1054): an index from before the table or the column.
func noSuchTableOrColumn(err error) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && (myErr.Number == 1146 || myErr.Number == 1054)
}
