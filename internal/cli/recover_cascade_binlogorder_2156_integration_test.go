//go:build integration

package cli

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// addReorderedParents2156 adds two parent DELETEs to the cascade fixture
// whose binary log order (2 at 500, then 3 at 600) is the reverse of their
// statement times (3 started first, then waited): #2156's shape.
func addReorderedParents2156(t *testing.T, db *sql.DB, dbName string) {
	t.Helper()
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	const layout = "2006-01-02 15:04:05"
	testutil.InsertEvent(t, db, "binlog.000001", 500, 550, h.Add(20*time.Minute+2*time.Second).Format(layout), nil,
		dbName, "parent", 3 /* DELETE */, "2", nil, []byte(`{"id":2}`), nil)
	testutil.InsertEvent(t, db, "binlog.000001", 600, 650, h.Add(20*time.Minute+time.Second).Format(layout), nil,
		dbName, "parent", 3 /* DELETE */, "3", nil, []byte(`{"id":3}`), nil)
}

// checkReorderedParents2156: the script undoes parent 3 (last in the binary
// log) before parent 2, and its header and warnings say why.
func checkReorderedParents2156(t *testing.T, script string, warnings []string) {
	t.Helper()
	p3 := strings.Index(script, ".parent pk=3 ")
	p2 := strings.Index(script, ".parent pk=2 ")
	if p3 < 0 || p2 < 0 || p3 > p2 {
		t.Fatalf("parent 3 is not undone before parent 2 (%d, %d):\n%s", p3, p2, script)
	}
	const note = "Order of the changes: 2 of 3 changes were written to the binary log in a different order"
	if !strings.Contains(script, "--   - "+note) {
		t.Fatalf("the script header does not carry the order note:\n%s", script)
	}
	found := false
	for _, w := range warnings {
		found = found || strings.HasPrefix(w, note)
	}
	if !found {
		t.Fatalf("the order note is not among the warnings: %q", warnings)
	}
}

func TestRecoverCascade_parentsInBinlogOrder2156(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName, dsn := seedCascadeIndex(t)
	addReorderedParents2156(t, db, dbName)
	out := t.TempDir() + "/cascade.sql"
	rcIndexDSN, rcSchema, rcTable = dsn, dbName, "parent"
	rcPK, rcPKs, rcSince, rcUntil = "", nil, "", ""
	rcOutput, rcDryRun, rcFormat = out, false, "text"
	rcLookback, rcMaxDepth, rcLimit, rcAllowIncomplete = "30d", 5, 1000, false
	if err := runCascadeCmd(t); err != nil {
		t.Fatalf("runRecoverCascade: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	// The text output has no warnings list of its own: the header is it.
	script := string(b)
	checkReorderedParents2156(t, script, []string{strings.TrimPrefix(lineWith(script, "Order of the changes"), "--   - ")})
}

func lineWith(s, needle string) string {
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, needle) {
			return l
		}
	}
	return ""
}
