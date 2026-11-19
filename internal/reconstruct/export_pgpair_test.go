package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// SetPGTableDeltas sets the PostgreSQL table delta switch for one test.
func SetPGTableDeltas(t *testing.T, on bool) {
	t.Helper()
	prev := pgTableDeltas
	pgTableDeltas = func() bool { return on }
	t.Cleanup(func() { pgTableDeltas = prev })
}

// SetPGFoldByEngine turns off, for one test, the engine's fold of a table's
// delta into its file, so the table is written the row-by-row way.
func SetPGFoldByEngine(t *testing.T, on bool) {
	t.Helper()
	prev := pgFoldPair
	if !on {
		pgFoldPair = func(context.Context, *sql.DB, string, string, string, string, map[string]string) (int64, error) {
			return 0, errors.New("the engine's fold is turned off for this test")
		}
	}
	t.Cleanup(func() { pgFoldPair = prev })
}
