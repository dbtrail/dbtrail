package shim

import (
	"encoding/json"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// notingFreeSQL is a fakeFreeSQL that keeps the answers the port reports.
type notingFreeSQL struct {
	*fakeFreeSQL
	noted []string
}

func (n *notingFreeSQL) NoteRoute(side string) { n.noted = append(n.noted, side) }

// #2369: where the copy answers alone, the port says what answered each
// statement, after its own last word: a result it refuses once the copy ran
// it (over the row cap) is a refusal, not the copy's answer.
func TestFreeSQL_unroutedNotesItsAnswer(t *testing.T) {
	one := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "id", Type: "INTEGER"}}, Rows: [][]any{{json.Number("1")}}}
	cut := one
	cut.Truncated = true

	f := &notingFreeSQL{fakeFreeSQL: &fakeFreeSQL{res: one}}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	h.BindConn(&warningsConn{})
	if _, err := h.HandleQuery("SELECT id FROM orders"); err != nil {
		t.Fatal(err)
	}
	h.ReleaseResult()
	f.res = cut
	if _, err := h.HandleQuery("SELECT id FROM orders"); err == nil {
		t.Fatal("a cut result was served")
	}
	if len(f.noted) != 2 || f.noted[0] != "copy" || f.noted[1] != "refused" {
		t.Fatalf("noted %v, want [copy refused]", f.noted)
	}
}
