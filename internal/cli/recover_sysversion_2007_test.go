package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

func TestNoStatementsMessage_2007(t *testing.T) {
	if msg, hint := noStatementsMessage(0); !hint || !strings.Contains(msg, "No events matched") {
		t.Fatalf("nothing matched: %q hint=%v", msg, hint)
	}
	msg, hint := noStatementsMessage(3)
	if hint || strings.Contains(msg, "No events matched") || !strings.Contains(msg, "3 event(s) matched") ||
		!strings.Contains(msg, "system-versioned") {
		t.Fatalf("only skipped history versions: %q hint=%v, want the count, the reason and no sibling hint", msg, hint)
	}
}

// query --pks on a system-versioned table: the rows found under an added
// spelling (key plus current marker, #2007), possibly re-spelled for a UUID
// key, belong to the group of the value the operator typed.
func TestWriteGroupedJSON_sysVersionedAliases_2007(t *testing.T) {
	const m = "2106-02-07 06:28:15.999999"
	rows := []query.ResultRow{
		{EventID: 1, PKValues: "2|" + m},
		{EventID: 2, PKValues: "3"},
		{EventID: 3, PKValues: "BYTES|" + m},
	}
	aliases := map[string]string{"2|" + m: "2", "u|" + m: "u"}
	spelled := map[string]string{"u|" + m: "BYTES|" + m}
	var buf bytes.Buffer
	n, err := writeGroupedJSON([]string{"2", "3", "u"}, spelled, aliases, rows, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("grouped %d events, want all 3:\n%s", n, buf.String())
	}
	var out struct {
		Results []struct {
			PK     string `json:"pk"`
			Events []struct {
				EventID uint64 `json:"event_id"`
			} `json:"events"`
		} `json:"results"`
	}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	for _, g := range out.Results {
		if len(g.Events) != 1 {
			t.Fatalf("group %q holds %d events, want 1:\n%s", g.PK, len(g.Events), buf.String())
		}
	}
}
