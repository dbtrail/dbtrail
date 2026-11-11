package shim

import (
	"encoding/json"
	"testing"
	"time"

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

// countingFreeSQL also counts the statements, as the copy of a connection
// that does not route does for the status log.
type countingFreeSQL struct {
	*fakeFreeSQL
	refused []bool
	took    []time.Duration
}

func (c *countingFreeSQL) NoteAnswered(took time.Duration, refused bool) {
	c.refused = append(c.refused, refused)
	c.took = append(c.took, took)
}

// With Engine Routing off, which is how the port starts, no routing hook
// fires. Each statement is still counted, once, with whether the client got
// an error: an uncounted one is a port that answers and a status line that
// says it saw nothing.
func TestFreeSQL_unroutedCountsEachStatement(t *testing.T) {
	one := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "id", Type: "INTEGER"}}, Rows: [][]any{{json.Number("1")}}}
	cut := one
	cut.Truncated = true

	f := &countingFreeSQL{fakeFreeSQL: &fakeFreeSQL{res: one}}
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
	if len(f.refused) != 2 || f.refused[0] || !f.refused[1] {
		t.Fatalf("counted %v, want one answered and one refused", f.refused)
	}
	for i, d := range f.took {
		if d < 0 {
			t.Errorf("statement %d took %v", i, d)
		}
	}
}
