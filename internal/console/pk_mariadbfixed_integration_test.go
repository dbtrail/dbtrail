//go:build integration

package console

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A MariaDB UUID key is stored in the index as its bytes (pk_values in hex),
// while the operator types the text the application shows. The Events search
// and Restore answered "no events" / an empty script for the text form. Both
// must find the row, and a key whose column type the snapshot cannot tell is
// refused (422) instead of answering empty.
func TestIntegrationMariaDBUUIDKeyTextSpelling(t *testing.T) {
	const (
		text   = "cc5c4e6e-bc5a-11f1-9a0c-0affd251cac9"
		stored = "0xCC5C4E6EBC5A11F19A0C0AFFD251CAC9"
	)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 11:00:00", "app", "sessions", "sid", 1, "PRI", "uuid", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 11:00:00", "app", "sessions", "payload", 2, "", "varchar", "YES")
	raw, err := hex.DecodeString(strings.TrimPrefix(stored, "0x"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(map[string]any{"sid": base64.StdEncoding.EncodeToString(raw), "payload": "s2"})
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"sessions", "ghost"} {
		testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
			"app", table, 3 /*DELETE*/, stored, nil, before, nil)
	}

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}

	events := func(table, pk string) (int, string) {
		t.Helper()
		q := url.Values{"schema": {"app"}, "table": {table}, "pk": {pk}}
		r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/events?"+q.Encode(), nil)
		r.Host = "127.0.0.1:8090"
		w := httptest.NewRecorder()
		srv.handleEvents(w, r)
		return w.Code, w.Body.String()
	}
	for _, pk := range []string{text, stored} {
		code, body := events("sessions", pk)
		if code != 200 {
			t.Fatalf("events pk=%s: code %d, body %s", pk, code, body)
		}
		var resp struct {
			Count int `json:"count"`
		}
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatalf("decode: %v\n%s", err, body)
		}
		if resp.Count != 1 {
			t.Errorf("events pk=%s: count %d, want 1 (body %s)", pk, resp.Count, body)
		}
	}
	if code, body := events("ghost", text); code != 422 {
		t.Errorf("events on a table no snapshot describes: code %d (body %s), want 422", code, body)
	}

	body, err := json.Marshal(map[string]string{"schema": "app", "table": "sessions", "pk": text})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/recover", strings.NewReader(string(body)))
	r.Host = "127.0.0.1:8090"
	w := httptest.NewRecorder()
	srv.handleRecover(w, r)
	if w.Code != 200 {
		t.Fatalf("recover code = %d, body = %s", w.Code, w.Body.String())
	}
	var rec struct {
		StatementCount int `json:"statement_count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &rec); err != nil {
		t.Fatalf("decode: %v\n%s", err, w.Body.String())
	}
	if rec.StatementCount != 1 {
		t.Errorf("recover with the text key: %d statements, want 1 (body %s)", rec.StatementCount, w.Body.String())
	}
}
