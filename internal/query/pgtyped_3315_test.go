package query_test

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The footer key a snapshot read looks for is the one the writer writes.
func TestPGTypedColumnsKeyIsBaselines(t *testing.T) {
	if query.PGTypedColumnsKey != baseline.MetaKeyPGTypedColumns {
		t.Fatalf("%q is not %q", query.PGTypedColumnsKey, baseline.MetaKeyPGTypedColumns)
	}
}
