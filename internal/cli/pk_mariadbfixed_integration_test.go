//go:build integration

package cli

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The key from the RDS for MariaDB test. Its bytes are not valid UTF-8, so the
// index spells it in hex (event.BuildPKValues).
const (
	mariaUUIDText   = "cc5c4e6e-bc5a-11f1-9a0c-0affd251cac9"
	mariaUUIDStored = "0xCC5C4E6EBC5A11F19A0C0AFFD251CAC9"
)

// seedMariaDBUUIDKey builds an index that holds what capture writes for a
// MariaDB table keyed by a UUID: the snapshot types the key `uuid`, and the
// event's pk_values is the bytes in hex. No MariaDB is needed, only the index,
// so this runs in every integration job. `ghost` has events and no snapshot
// row, the case where the key type cannot be told.
func seedMariaDBUUIDKey(t *testing.T) (dbName, dsn string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	snapTS := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, dbName, "sessions", "sid", 1, "PRI", "uuid", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, dbName, "sessions", "payload", 2, "", "varchar", "YES")

	raw, err := hex.DecodeString(strings.TrimPrefix(mariaUUIDStored, "0x"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := json.Marshal(map[string]any{"sid": base64.StdEncoding.EncodeToString(raw), "payload": "s2"})
	if err != nil {
		t.Fatal(err)
	}
	ts := h.Add(time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 150, ts, nil,
		dbName, "sessions", 3 /* DELETE */, mariaUUIDStored, nil, before, nil)
	testutil.InsertEvent(t, db, "binlog.000001", 200, 250, ts, nil,
		dbName, "ghost", 3 /* DELETE */, mariaUUIDStored, nil, before, nil)
	return dbName, testutil.IntegrationDSN(dbName)
}

// `query --pk` with the UUID as the application shows it returned 0 rows and
// no error; only the hex form worked. Both must find the row.
func TestQuery_MariaDBUUIDKeyEitherSpelling(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	dbName, dsn := seedMariaDBUUIDKey(t)
	for _, pk := range []string{mariaUUIDText, strings.ToUpper(mariaUUIDText), mariaUUIDStored} {
		resetQueryGlobals(t)
		qIndexDSN, qSchema, qTable, qPK, qFormat = dsn, dbName, "sessions", pk, "json"
		var runErr error
		out := captureStdout(t, func() { runErr = runQuery(newQueryTestCmd(), nil) })
		if runErr != nil {
			t.Fatalf("--pk %s: %v", pk, runErr)
		}
		var rows []struct {
			PKValues string `json:"pk_values"`
		}
		if err := json.Unmarshal([]byte(out), &rows); err != nil {
			t.Fatalf("--pk %s: unmarshal %q: %v", pk, out, err)
		}
		if len(rows) != 1 || rows[0].PKValues != mariaUUIDStored {
			t.Errorf("--pk %s: %d rows (%s), want the one event", pk, len(rows), out)
		}
	}

	// --pks with grouped JSON: the group is the typed label's, with its row.
	resetQueryGlobals(t)
	qIndexDSN, qSchema, qTable, qPKs, qFormat = dsn, dbName, "sessions", []string{mariaUUIDText}, "json"
	var runErr error
	out := captureStdout(t, func() { runErr = runQuery(newQueryTestCmd(), nil) })
	if runErr != nil {
		t.Fatalf("--pks: %v", runErr)
	}
	if !strings.Contains(out, `"pk": "`+mariaUUIDText+`"`) && !strings.Contains(out, `"pk":"`+mariaUUIDText+`"`) ||
		!strings.Contains(out, mariaUUIDStored) {
		t.Errorf("--pks grouped output lost the row under the typed label:\n%s", out)
	}
}

// With no snapshot row to type the key, a key that looks like a UUID is
// refused instead of answering "no history".
func TestQuery_MariaDBUUIDKeyWithoutSnapshotRefuses(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	dbName, dsn := seedMariaDBUUIDKey(t)
	resetQueryGlobals(t)
	qIndexDSN, qSchema, qTable, qPK, qFormat = dsn, dbName, "ghost", mariaUUIDText, "json"
	var runErr error
	captureStdout(t, func() { runErr = runQuery(newQueryTestCmd(), nil) })
	if !errors.Is(runErr, reconstruct.ErrPKTypeUnknown) {
		t.Errorf("err = %v, want a refusal (ErrPKTypeUnknown)", runErr)
	}
}

// `recover --pk` with the text form generated 0 statements and no error.
func TestRecover_MariaDBUUIDKeyTextSpelling(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	dbName, dsn := seedMariaDBUUIDKey(t)
	resetRecoverGlobals(t)
	out := t.TempDir() + "/recovery.sql"
	rIndexDSN, rSchema, rTable, rPK, rOutput = dsn, dbName, "sessions", mariaUUIDText, out
	if err := runRecover(newRecoverTestCmd(), nil); err != nil {
		t.Fatalf("runRecover: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	script := string(b)
	if !strings.Contains(script, "INSERT INTO") || !strings.Contains(strings.ToLower(script), "cc5c4e6ebc5a11f19a0c0affd251cac9") {
		t.Errorf("the script does not re-insert the deleted row:\n%s", script)
	}
}
