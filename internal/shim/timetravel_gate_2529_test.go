package shim

import (
	"encoding/json"
	"strings"
	"testing"
)

// A server with hidden rules refuses the time-travel schemas on the port,
// asked per statement, and leaves every other statement alone (#2529).
func TestTimeTravelGate(t *testing.T) {
	const why = "this server hides some tables and columns"
	refuse := true
	h := NewHandler(nil, nil)
	h.BindFreeSQL(&fakeFreeSQL{res: oneCell("id", "INTEGER", json.Number("7"))})
	h.BindTimeTravelGate(func() string {
		if refuse {
			return why
		}
		return ""
	})
	if err := h.UseDB("shop"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00' WHERE id = 1",
		"SELECT * FROM _snapshot.orders AS OF '2026-01-01 00:00:00' WHERE id = 1",
		"SELECT * FROM _diff.orders BETWEEN '2026-01-01 00:00:00' AND '2026-01-02 00:00:00' WHERE id = 1",
		"SHOW TABLES FROM _flashback",
	} {
		_, err := h.HandleQuery(q)
		if err == nil || !strings.Contains(err.Error(), why) {
			t.Errorf("%s: err = %v, want the gate's refusal", q, err)
		}
	}
	if _, err := h.HandleQuery("SELECT id FROM orders"); err != nil {
		t.Errorf("an ordinary statement was refused: %v", err)
	}
	// The gate is asked per statement: opened, it refuses nothing. (Not run
	// through HandleQuery: past the gate a time-travel read needs an index.)
	refuse = false
	if err := h.timeTravelRefused(); err != nil {
		t.Errorf("an open gate still refused: %v", err)
	}
}
