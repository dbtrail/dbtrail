package reconstruct

import "testing"

// SetPGTableDeltas sets the PostgreSQL table delta switch for one test.
func SetPGTableDeltas(t *testing.T, on bool) {
	t.Helper()
	prev := pgTableDeltas
	pgTableDeltas = func() bool { return on }
	t.Cleanup(func() { pgTableDeltas = prev })
}
