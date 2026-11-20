package reconstruct

import (
	"errors"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// The shapes the stream recorded are read once for each floor a run's tables
// start from, however many tables ask and at once; a failed read is not kept;
// and a run with no cache reads for every table, as before.
func TestPGRunShapes(t *testing.T) {
	var reads int
	var mu sync.Mutex
	loadFor := func(oid uint32) func() ([]metadata.PGShape, error) {
		return func() ([]metadata.PGShape, error) {
			mu.Lock()
			reads++
			mu.Unlock()
			return []metadata.PGShape{{Relation: metadata.PGRelationSchema{OID: oid}}}, nil
		}
	}
	c := &pgRunShapes{}
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := c.from(0x500, loadFor(7))
			if err != nil || len(got) != 1 || got[0].Relation.OID != 7 {
				t.Errorf("shapes from 500: %v, %v", got, err)
			}
		}()
	}
	wg.Wait()
	if reads != 1 {
		t.Fatalf("50 tables starting from one floor read the shapes %d times, want once", reads)
	}
	// Another floor is another read, and keeps its own answer.
	if got, err := c.from(0x900, loadFor(9)); err != nil || got[0].Relation.OID != 9 || reads != 2 {
		t.Fatalf("another floor: %v, %v, %d reads", got, err, reads)
	}
	if got, _ := c.from(0x500, loadFor(99)); got[0].Relation.OID != 7 || reads != 2 {
		t.Fatalf("the first floor again: OID %d, %d reads; want its own answer and no read", got[0].Relation.OID, reads)
	}

	// A read that fails is tried again by the next table.
	failing := true
	flaky := func() ([]metadata.PGShape, error) {
		reads++
		if failing {
			return nil, errors.New("the index is away")
		}
		return nil, nil
	}
	if _, err := c.from(0xa00, flaky); err == nil {
		t.Fatal("a failed read answered")
	}
	failing = false
	before := reads
	if got, err := c.from(0xa00, flaky); err != nil || got != nil || reads != before+1 {
		t.Fatalf("after a failed read: %v, %v, %d more reads; want a new read, of no shape", got, err, reads-before)
	}
	// No shape at all is an answer too, and is kept.
	if _, err := c.from(0xa00, flaky); err != nil || reads != before+1 {
		t.Fatalf("an answer of no shape was read again (%d more reads)", reads-before)
	}

	// No cache: every table reads.
	var none *pgRunShapes
	reads = 0
	for range 3 {
		if _, err := none.from(0x500, loadFor(1)); err != nil {
			t.Fatal(err)
		}
	}
	if reads != 3 {
		t.Fatalf("a run with no cache read %d times for 3 tables", reads)
	}
}
