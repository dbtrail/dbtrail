package consoleapp

import (
	"context"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/status"
	drivermysql "github.com/go-sql-driver/mysql"
)

// The newest change a published copy holds (#2201). A copy's files can be a
// minute old while its data is an hour old: an update folds what the index
// holds, and when capture is an hour behind the source, so is everything the
// update wrote. The Overview used to show the files' age and read fresh over
// exactly that case. The run record now carries the data's own instant.

// newestIndexedTimeout bounds the read below. It runs on the refresh's own
// goroutine, before the fold, and a slow answer only costs this record.
const newestIndexedTimeout = 10 * time.Second

// readNewestIndexed is the index's newest change: the newest row by primary
// key (status.NewestIndexedRow, #2509), at most the exact newest event the
// coverage window's upper edge reads and never past it, so the page compares
// the two on one clock and one rule. known is false when the index did not
// answer; a zero time with known true is an empty index. A seam for tests.
var readNewestIndexed = readNewestIndexedFromDB

// newestIndexedRead is the read behind readNewestIndexedFromDB: the newest
// row by key, not the exact MAX over the newest partition, which read every
// row of that partition once per update (#2509). A seam so a test can pin
// which one it is.
var newestIndexedRead = status.NewestIndexedRow

func readNewestIndexedFromDB(ctx context.Context, dsn string) (time.Time, bool) {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil || cfg.DBName == "" {
		slog.Debug("snapshot update: the index DSN names no database; the copy's data instant is not recorded", "error", err)
		return time.Time{}, false
	}
	db, err := config.Connect(dsn)
	if err != nil {
		slog.Debug("snapshot update: could not open the index to read its newest change", "error", err)
		return time.Time{}, false
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, newestIndexedTimeout)
	defer cancel()
	t, err := newestIndexedRead(ctx, db)
	if err != nil {
		slog.Debug("snapshot update: could not read the index's newest change", "error", err)
		return time.Time{}, false
	}
	return t.UTC(), true
}

// foldDataAsOf is the newest change an update targeting at holds: the later of
// what its ancestor snapshot held and the newest change the index had when the
// fold started, the latter capped at at, because the fold leaves out an event
// stamped after the instant it targets (a source clock running ahead). Read
// before the fold, so an event indexed while it ran and folded anyway makes
// the record older than the copy, never newer; so does the newest row by
// key (#2509): an older binlog indexed after live capture makes it read
// older than the index's newest change until the next live batch, and the
// Overview then shows the copy's data older than it is for that long, never
// newer. Unknown (false) only when neither half is known: an empty index
// with no ancestor on record dates nothing.
func foldDataAsOf(ancestor time.Time, ancestorKnown bool, newest time.Time, newestKnown bool, at time.Time) (time.Time, bool) {
	var folded time.Time
	if newestKnown && !newest.IsZero() {
		folded = newest
		if folded.After(at) {
			folded = at
		}
	}
	if ancestorKnown && !ancestor.IsZero() && ancestor.After(folded) {
		return ancestor, true
	}
	return folded, !folded.IsZero()
}
