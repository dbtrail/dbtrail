//go:build integration

package pgstreamrun

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A table taken out of the publication (or dropped) is recorded once, by
// comparing readings of the publication's tables, and an update of its
// snapshot is refused from then on: its file is no longer the table. A
// renamed table is still in. The reading survives a failed probe and a
// capture that starts again, which is how a table that left while capture
// was down is noticed.
func TestIntegrationPGTableLeavingThePublicationIsRecorded(t *testing.T) {
	ctx := context.Background()
	indexDB, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, indexDB, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	a := pgcapture.PublishedTable{OID: 16401, Schema: "public", Table: "orders"}
	b := pgcapture.PublishedTable{OID: 16402, Schema: "public", Table: "items"}
	bRenamed := pgcapture.PublishedTable{OID: 16402, Schema: "public", Table: "things"}

	// run drives one capture loop through the readings given, one per poll,
	// and stops once the last was saved.
	type reading struct {
		tables []pgcapture.PublishedTable
		err    error // the probe failing
		pubErr error // the publication alone not read
	}
	run := func(readings ...reading) {
		t.Helper()
		loopCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		polls := make(chan int, 64)
		n := 0
		probe := func(context.Context) (pgcapture.HealthSnapshot, error) {
			i := min(n, len(readings)-1)
			n++
			polls <- n
			r := readings[i]
			if r.err != nil {
				return pgcapture.HealthSnapshot{}, r.err
			}
			return pgcapture.HealthSnapshot{FlushedLSN: uint64(0x1000 + n), Published: r.tables, PublishedErr: r.pubErr}, nil
		}
		done := make(chan error, 1)
		go func() {
			done <- streamLoopPG(loopCtx, make(chan event.Event), indexer.New(indexDB, 100), indexDB, nil, time.Hour, probe,
				10*time.Millisecond, &pgStreamState{serverID: 88}, slog.New(slog.DiscardHandler), nil)
		}()
		// Two polls past the last reading: the one after it has then run to
		// its end, save included.
		deadline := time.After(10 * time.Second)
		for got := 0; got < len(readings)+2; {
			select {
			case got = <-polls:
			case <-deadline:
				t.Fatal("the health poll stopped")
			}
		}
		cancel()
		<-done
	}
	markers := func(table string) []string {
		t.Helper()
		rs, err := indexDB.Query(`SELECT ddl_query FROM schema_changes WHERE schema_name = 'public' AND table_name = ? ORDER BY id`, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		var out []string
		for rs.Next() {
			var q string
			if err := rs.Scan(&q); err != nil {
				t.Fatal(err)
			}
			out = append(out, q)
		}
		return out
	}

	// The first reading ever is only kept: nothing to compare it with.
	run(reading{tables: []pgcapture.PublishedTable{a, b}})
	if got := append(markers("orders"), markers("items")...); len(got) != 0 {
		t.Fatalf("the first reading recorded %v", got)
	}
	// Same tables, a rename, a failed probe, the publication not read: none
	// is a table leaving.
	before := time.Now().UTC().Add(-time.Second)
	run(reading{tables: []pgcapture.PublishedTable{a, b}}, reading{tables: []pgcapture.PublishedTable{a, bRenamed}},
		reading{err: errors.New("the source is away")}, reading{pubErr: errors.New("no")}, reading{tables: []pgcapture.PublishedTable{a, bRenamed}})
	if got := append(append(markers("orders"), markers("items")...), markers("things")...); len(got) != 0 {
		t.Fatalf("readings with no table gone recorded %v", got)
	}

	// The table leaves while capture is down: the next run's first reading
	// finds it gone, once, under the name it had last.
	run(reading{tables: []pgcapture.PublishedTable{a}}, reading{tables: []pgcapture.PublishedTable{a}})
	got := markers("things")
	if len(got) != 1 || !strings.HasPrefix(got[0], event.InferredDDLPrefix) || !strings.Contains(got[0], "oid=16402") ||
		!strings.Contains(got[0], "gap="+event.InferredGapUncaptured) || !strings.Contains(got[0], "DROP TABLE") {
		t.Fatalf("the table that left was recorded as %q; want one row, inferred, with its OID and the gap", got)
	}
	if len(markers("orders")) != 0 {
		t.Fatal("the table that stayed was recorded as gone")
	}
	// And again in a later run: still the one row.
	run(reading{tables: []pgcapture.PublishedTable{a}})
	if got := markers("things"); len(got) != 1 {
		t.Fatalf("a run after it recorded the table again: %d rows", len(got))
	}

	// An update of a snapshot from before it left is refused; one from a
	// full read after it is not, and neither is the table that stayed.
	after := time.Now().UTC().Add(time.Second)
	err := reconstruct.CheckDestructiveDDL(ctx, indexDB, "public", "things", reconstruct.DDLWindow{Since: before, Until: after})
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("an update across the table leaving the publication: %v; want a refusal", err)
	}
	if err := reconstruct.CheckDestructiveDDL(ctx, indexDB, "public", "things", reconstruct.DDLWindow{Since: after, Until: after.Add(time.Hour)}); err != nil {
		t.Fatalf("an update from a snapshot taken after it left: %v", err)
	}
	if err := reconstruct.CheckDestructiveDDL(ctx, indexDB, "public", "orders", reconstruct.DDLWindow{Since: before, Until: after}); err != nil {
		t.Fatalf("an update of the table that stayed: %v", err)
	}

	// Back in, and out again: a second row.
	run(reading{tables: []pgcapture.PublishedTable{a, bRenamed}}, reading{tables: []pgcapture.PublishedTable{a}})
	if got := markers("things"); len(got) != 2 {
		t.Fatalf("out, in and out again: %d rows, want 2", len(got))
	}
}
