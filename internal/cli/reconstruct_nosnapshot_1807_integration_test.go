//go:build integration

package cli

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The three no-snapshot cases of reconstruct_nosnapshot_1807_test.go, run
// against a real, initialized history database named by --index-dsn: the
// refusal must be the same one a person gets on a working install, not one
// that only holds when the database is unreachable.
func TestIntegrationReconstruct1807_noSnapshotCasesWithARealIndex(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	for _, tc := range noSnapshotCases {
		for _, baselineOnly := range []bool{false, true} {
			name := tc.name
			if baselineOnly {
				name += ", --baseline-only"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				tc.setup(t, dir)
				setSingleRow(t, dir, baselineOnly)
				recIndexDSN = testutil.IntegrationDSN(dbName)
				err := runReconstruct(reconstructCmd, nil)
				checkNoSnapshotCase(t, err, dir, baselineOnly, tc.want, tc.mustNot)
			})
		}
	}
}
