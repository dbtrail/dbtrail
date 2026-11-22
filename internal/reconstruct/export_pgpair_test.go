package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// SetPGTableDeltas sets the PostgreSQL table delta switch for one test.
func SetPGTableDeltas(t *testing.T, on bool) {
	t.Helper()
	prev := pgTableDeltas
	pgTableDeltas = func() bool { return on }
	t.Cleanup(func() { pgTableDeltas = prev })
}

// SetPGChainMaxAge sets, for one test, how old a chain beside a PostgreSQL
// table file may be before it is folded into a new file.
func SetPGChainMaxAge(t *testing.T, d time.Duration) {
	t.Helper()
	prev := pgChainMaxAge
	pgChainMaxAge = d
	t.Cleanup(func() { pgChainMaxAge = prev })
}

// CountPGFoldsByEngine counts, for one test, the tables the engine wrote in
// full from a file and its delta; with on false the engine's fold fails
// instead, so the table is written the row-by-row way.
func CountPGFoldsByEngine(t *testing.T, on bool) *int {
	t.Helper()
	prev, n := pgFoldPair, new(int)
	pgFoldPair = func(ctx context.Context, duck *sql.DB, base string, dead, rows []string, dst string, md map[string]string) (string, int64, error) {
		if !on {
			return "", 0, errors.New("the engine's fold is turned off for this test")
		}
		folded, total, err := prev(ctx, duck, base, dead, rows, dst, md)
		if err == nil {
			*n++
		}
		return folded, total, err
	}
	t.Cleanup(func() { pgFoldPair = prev })
	return n
}
