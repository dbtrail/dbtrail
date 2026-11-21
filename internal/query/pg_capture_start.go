package query

import "database/sql"

// PGCaptureStartLSN is where the PostgreSQL capture writing this index started
// reading its source from (stream_state.pg_capture_from_lsn, #2843), and
// whether the index records one: false for a MySQL or MariaDB index, a
// capture that has not saved a position yet or predates the column, and a
// read that failed.
//
// A snapshot whose floor is before it has a known gap (#3078): the changes
// committed between the two were never delivered to this index.
func PGCaptureStartLSN(db *sql.DB) (lsn uint64, ok bool) {
	if db == nil {
		return 0, false
	}
	from, ok := pgCaptureStart(db)
	return from.StartPos, ok
}
