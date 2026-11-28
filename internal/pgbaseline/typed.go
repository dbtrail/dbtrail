package pgbaseline

import (
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// typedColumnsOf are the columns of t its snapshot file stores as their type
// (#3315; baseline.PGTypedColumns): none when BINTRAIL_PG_TYPED_COPY turns
// it off, which writes every column as text, as before.
func typedColumnsOf(t tableInfo) map[string]string {
	if !typedCopyOn(os.Getenv(baseline.PGTypedDisableEnv)) {
		return nil
	}
	return baseline.PGTypedColumns(baseline.PGRelation{OID: t.OID, Columns: t.Shape, PrimaryKey: t.PrimaryKey})
}

var typedCopyWarn sync.Once

// typedCopyOn reads BINTRAIL_PG_TYPED_COPY: on unless 0, false, no or off.
// Another value keeps it on, with one warning.
func typedCopyOn(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false", "no", "off":
		return false
	case "", "1", "true", "yes", "on":
		return true
	}
	typedCopyWarn.Do(func() {
		slog.Warn("pgbaseline: "+baseline.PGTypedDisableEnv+" is not one of 0, false, no, off, 1, true, yes or on; snapshots store columns as their type, as by default",
			"value", raw)
	})
	return true
}
