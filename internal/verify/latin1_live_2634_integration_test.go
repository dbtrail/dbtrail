//go:build integration

package verify

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestLatin1LiveDigestMatchesUTF8Copy is the live-source half of #2634 on a
// real MySQL: the source checksum reads a latin1 table's stored bytes
// (character_set_results = binary), while the copy and the index hold its
// text as UTF-8. A digest of the UTF-8 rows through the renderer VerifyTable
// uses must equal the live one, or every non-ASCII row of a latin1 table is a
// false mismatch.
func TestLatin1LiveDigestMatchesUTF8Copy(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	ctx := context.Background()
	for _, q := range []string{
		"SET NAMES utf8mb4",
		"CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20), x TEXT, e ENUM('sí','no'), u VARCHAR(20) CHARACTER SET utf8mb4) DEFAULT CHARSET=latin1",
		"INSERT INTO t VALUES (1,'café','naïve € ×','sí','café'),(2,'plain','Ž ‰ ÿ','no',NULL),(3,'Ã©',NULL,NULL,'x')",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	live, err := consistency.ConsistentTableChecksumNormalized(ctx, db, name, "t", normalizeRenderedBytes)
	if err != nil {
		t.Fatal(err)
	}

	cols := []metadata.ColumnMeta{
		{Name: "id", DataType: "int"},
		{Name: "v", DataType: "varchar", CharacterSet: "latin1"},
		{Name: "x", DataType: "text", CharacterSet: "latin1"},
		{Name: "e", DataType: "enum", CharacterSet: "latin1"},
		{Name: "u", DataType: "varchar", CharacterSet: "utf8mb4"},
	}
	copyRows := [][]any{
		{int64(1), "café", "naïve € ×", "sí", "café"},
		{int64(2), "plain", "Ž ‰ ÿ", "no", nil},
		{int64(3), "Ã©", nil, nil, "x"},
	}
	h := consistency.NewHasher()
	for _, r := range copyRows {
		cells := make([][]byte, len(cols))
		for i, c := range cols {
			cells[i] = renderCellNormalized(r[i], c)
		}
		h.AddBytes(cells)
	}
	if got := h.Digest(); got != live.Digest {
		t.Fatalf("copy digest %s != live digest %s: a latin1 table would verify as a mismatch", got, live.Digest)
	}
}
