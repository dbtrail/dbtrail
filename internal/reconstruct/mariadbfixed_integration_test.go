//go:build integration

package reconstruct

import (
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestDecodeEventBinaries_MariaDBFixedTypesRenderText: a MariaDB UUID, INET4
// or INET6 value is stored in the event image as base64 of its bytes (#1944),
// while the source, mydumper and so the baseline all give its text form. The
// decode pass must hand back that text, or single-row reconstruct returns
// base64 and the full-table fold mixes two spellings of one column. A value
// that is not the stored form of a full-width value (an event captured before
// #1944) stays as it is. VECTOR decodes to bytes, like MySQL 9's.
func TestDecodeEventBinaries_MariaDBFixedTypesRenderText(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Hour)
	ts := now.Add(time.Minute).Format("2006-01-02 15:04:05")
	for i, c := range []struct{ name, typ string }{
		{"id", "int"}, {"u", "uuid"}, {"i4", "inet4"}, {"i6", "inet6"}, {"v", "vector"},
	} {
		key := ""
		if i == 0 {
			key = "PRI"
		}
		testutil.InsertSnapshot(t, db, 1, ts, "mdb", "t", c.name, i+1, key, c.typ, "YES")
	}
	b := func(h string) string {
		raw, err := hex.DecodeString(h)
		if err != nil {
			t.Fatal(err)
		}
		return base64.StdEncoding.EncodeToString(raw)
	}
	events := []query.ResultRow{{
		SchemaName: "mdb", TableName: "t", EventTimestamp: now.Add(5 * time.Minute),
		RowBefore: map[string]any{"id": float64(1), "u": b("00000000000000000000000000000000"),
			"i4": b("0A000000"), "i6": b("00000000000000000000FFFF01020304"), "v": nil},
		RowAfter: map[string]any{"id": float64(1), "u": b("123E4567E89B12D3A456426614174000"),
			"i4": "\n", "i6": nil, "v": b("0000803F00002040000040C0")},
	}}
	if !DecodeEventBinaries(db, "mdb", "t", events) {
		t.Fatal("DecodeEventBinaries reported the epoch as untyped")
	}
	for _, c := range []struct {
		img  map[string]any
		col  string
		want any
	}{
		{events[0].RowBefore, "u", "00000000-0000-0000-0000-000000000000"},
		{events[0].RowBefore, "i4", "10.0.0.0"},
		{events[0].RowBefore, "i6", "::ffff:1.2.3.4"},
		{events[0].RowAfter, "u", "123e4567-e89b-12d3-a456-426614174000"},
		{events[0].RowAfter, "i4", "\n"}, // pre-#1944 raw byte: no rendering exists
		{events[0].RowAfter, "i6", nil},
	} {
		if got := c.img[c.col]; got != c.want {
			t.Errorf("%s = %#v, want %#v", c.col, got, c.want)
		}
	}
	if got, ok := events[0].RowAfter["v"].([]byte); !ok || hex.EncodeToString(got) != "0000803f00002040000040c0" {
		t.Errorf("v = %#v, want the packed floats as []byte", events[0].RowAfter["v"])
	}
}
