package console

import (
	"context"
	"testing"
	"time"
)

// The list of columns takes no tails but stays under the server's lag limit.
func TestPGCatalogContextKeepsTheLagLimit(t *testing.T) {
	reg, ids := freshTestRegistry(t, "prod")
	limit := int64(30)
	if _, err := reg.SetSQLMode(ids[0], nil, nil, &limit); err != nil {
		t.Fatal(err)
	}
	q := &SQLOnCopy{s: &Server{cm: &connManager{reg: reg}}, id: ids[0]}
	ctx := q.pgCatalogContext(context.Background())
	l, ok := behindLimitFrom(ctx)
	if !ok || l.limit != 30*time.Second || l.id != ids[0] {
		t.Errorf("limit %+v (%v), want 30 s of %s", l, ok, ids[0])
	}
	if _, tails := tailsFor(ctx); tails {
		t.Error("the context names a server's tails")
	}
}
