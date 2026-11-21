package pgcapture

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// A publication created WITH (publish = 'insert, update, delete') leaves
// TRUNCATE out (pg_publication.pubtruncate = false), and pgoutput then sends
// nothing for one: the decoder never sees the TruncateMessage it records as
// a marker (#827), so the rows a TRUNCATE removed stay in the copy and come
// back in every rebuild, silently (#3108).
//
// This is a warning and never a refusal, unlike a missing insert, update or
// delete (validatePublication). Without TRUNCATE every row change is still
// captured, and only the record of a TRUNCATE is lost; refusing would stop
// every other function over the one that is missing. It is said at the start
// of capture (warnCaptureCoverage), in the doctor, and on each health probe,
// with the statement that fixes it. DBTrail never changes the publication.

// QueryPublishesTruncate reads whether publication publishes TRUNCATE. A
// publication that does not exist returns exists=false and no error: that is
// validatePublication's to report.
func QueryPublishesTruncate(ctx context.Context, conn *pgx.Conn, publication string) (exists, publishes bool, err error) {
	err = conn.QueryRow(ctx, `SELECT pubtruncate FROM pg_publication WHERE pubname = $1`, publication).Scan(&publishes)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("pgcapture: reading whether publication %q publishes TRUNCATE: %w", publication, err)
	}
	return true, publishes, nil
}

// TruncateFixStatement is the statement that makes publication publish
// TRUNCATE. SET (publish = ...) changes that one option and keeps the
// publication's tables and its other options (publish_via_partition_root,
// publish_generated_columns). It names all four operations because the
// value replaces the list, and capture refuses to start without the other
// three, so a publication this is shown for already publishes them.
func TruncateFixStatement(publication string) string {
	return "ALTER PUBLICATION " + printable(quoteIdent(publication)) + " SET (publish = 'insert, update, delete, truncate');"
}

// TruncateNotRecorded is what a publication without TRUNCATE means for the
// data.
const TruncateNotRecorded = "a TRUNCATE on the source is not recorded: " +
	"the rows it removed stay in the copy and come back in every rebuild of the table, with nothing to say they are gone"

// TruncateNotPublishedProblem says it for publication, on one line.
func TruncateNotPublishedProblem(publication string) string {
	return "publication " + printable(quoteIdent(publication)) + " does not publish TRUNCATE, so " + TruncateNotRecorded
}
