package recovery

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestGenerateSQLFromRows_refusesDriftedEventAndWritesNothing drives the #601
// branch of GenerateSQLFromRows itself (#1633): driftedColumns and
// schemaDriftError are each unit-tested, but the wiring between them (resolve
// the event's own snapshot, compare the emitted columns against the latest,
// refuse BEFORE a byte reaches the writer) ran only under the integration
// tag. A regression there (wrong resolver picked, the check moved after
// emission) would emit SQL that does not apply, with the suite green.
func TestGenerateSQLFromRows_refusesDriftedEventAndWritesNothing(t *testing.T) {
	evt, cur := driftResolvers()
	g := New(new(sql.DB), cur)
	g.cache = map[uint32]*metadata.Resolver{10: evt, 20: cur}

	// A DELETE captured under snapshot 10: its reversal is an INSERT of every
	// before-image column, `legacy` included, which snapshot 20 no longer has.
	rows := []query.ResultRow{{
		EventID: 1, SchemaName: "shop", TableName: "orders",
		EventType: parser.EventDelete, SchemaVersion: 10, PKValues: "1",
		RowBefore: map[string]any{"id": 1, "status": "paid", "legacy": "x"},
	}}
	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(rows, &buf)
	if err == nil {
		t.Fatalf("expected the schema-drift refusal, got %d statement(s):\n%s", n, buf.String())
	}
	if !strings.Contains(err.Error(), "shop.orders (legacy)") {
		t.Errorf("the refusal must name the table and the drifted column, got: %v", err)
	}
	if n != 0 || buf.Len() != 0 {
		t.Errorf("a refusal must write nothing: n=%d, %d byte(s):\n%s", n, buf.Len(), buf.String())
	}
}

// The UPDATE reversal computes its emitted set differently (SET from the
// before-image, WHERE from the PK), so it is driven too; the integration
// test that used to be its only cover skips without Docker.
func TestGenerateSQLFromRows_refusesDriftedUpdate(t *testing.T) {
	evt, cur := driftResolvers()
	g := New(new(sql.DB), cur)
	g.cache = map[uint32]*metadata.Resolver{10: evt, 20: cur}
	rows := []query.ResultRow{{
		EventID: 3, SchemaName: "shop", TableName: "orders",
		EventType: parser.EventUpdate, SchemaVersion: 10, PKValues: "3",
		RowBefore: map[string]any{"id": 3, "status": "paid", "legacy": "x"},
		RowAfter:  map[string]any{"id": 3, "status": "paid", "legacy": "y"},
	}}
	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(rows, &buf)
	if err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("expected the drift refusal naming legacy, got n=%d err=%v:\n%s", n, err, buf.String())
	}
	if n != 0 || buf.Len() != 0 {
		t.Errorf("a refusal must write nothing: n=%d, %d byte(s)", n, buf.Len())
	}
}

// The mirror: the same table under the current shape emits normally, so the
// guard sees both colours and a refusal that fires on every input would fail.
func TestGenerateSQLFromRows_currentShapeEmits(t *testing.T) {
	evt, cur := driftResolvers()
	g := New(new(sql.DB), cur)
	g.cache = map[uint32]*metadata.Resolver{10: evt, 20: cur}

	rows := []query.ResultRow{{
		EventID: 2, SchemaName: "shop", TableName: "orders",
		EventType: parser.EventDelete, SchemaVersion: 20, PKValues: "2",
		RowBefore: map[string]any{"id": 2, "status": "paid"},
	}}
	var buf bytes.Buffer
	n, err := g.GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("a current-shape event must emit, got: %v", err)
	}
	if n != 1 || !strings.Contains(buf.String(), "INSERT INTO `shop`.`orders`") {
		t.Errorf("n=%d, output:\n%s", n, buf.String())
	}
}
