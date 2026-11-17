package cliapp

import (
	"path/filepath"

	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// uploadablePG reports whether path is one of the two files of a PostgreSQL
// table delta (#2757): "<table>.<six digits>.pgdead" or ".pgnew", beside the
// table's file.
//
// It travels with its table for the reason a MySQL delta does (uploadable):
// the table file alone is the table as it was at an earlier snapshot update,
// under a directory that says otherwise.
//
// The name is taken as written. A file still being written, a name with
// another count of digits, and a suffix in another case are not a delta's
// files: no reader looks for them, so none is sent.
func uploadablePG(path string) bool {
	_, ok := pgdelta.ParseName(filepath.Base(path))
	return ok
}
