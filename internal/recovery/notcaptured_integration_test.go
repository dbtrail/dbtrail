//go:build integration

package recovery

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The not-captured marker as it is really stored: a JSON object in
// binlog_events.row_before, read back by the real query engine. The refusal
// depends on the read side seeing it as a one-key map holding the bool true;
// this pins that the MySQL JSON round trip keeps that shape.
func TestGenerateSQL_notCapturedMarkerFromTheIndex(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertEvent(t, db, "0/16B3748", 100, 200, "2026-02-19 14:00:00", nil,
		"public", "orders", 1 /*INSERT*/, "1", nil,
		nil, []byte(`{"id":"1","note":"a"}`))
	testutil.InsertEvent(t, db, "0/16B3800", 100, 200, "2026-02-19 14:00:01", nil,
		"public", "orders", 3 /*DELETE*/, "2", nil,
		[]byte(`{"id":"2","note":{"__bintrail_not_captured__":true}}`), nil)

	gen := NewForDialect(db, nil, PostgresDialect)

	// The INSERT alone undoes: it needs only row_after and the key.
	var ok bytes.Buffer
	if n, err := gen.GenerateSQL(context.Background(), query.Options{Schema: "public", Table: "orders", PKValues: "1", Limit: 10}, &ok); err != nil || n != 1 {
		t.Fatalf("INSERT-only window: n=%d err=%v", n, err)
	}

	// The whole table's window holds the partial DELETE: refused, nothing written.
	var buf bytes.Buffer
	n, err := gen.GenerateSQL(context.Background(), query.Options{Schema: "public", Table: "orders", Limit: 10}, &buf)
	var nc *event.NotCapturedError
	if !errors.As(err, &nc) {
		t.Fatalf("want the not-captured refusal, got n=%d err=%v\n%s", n, err, buf.String())
	}
	if nc.PKValues != "2" || len(nc.Columns) != 1 || nc.Columns[0] != "note" {
		t.Errorf("refusal = %+v, want pk 2 column note", nc)
	}
	if buf.Len() != 0 {
		t.Errorf("a refusal must write nothing, got %d bytes", buf.Len())
	}
}
