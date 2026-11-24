package pgshim

import (
	"context"
	"errors"
	"testing"
	"time"
)

// stampFake is a copy whose snapshot can change between statements.
type stampFake struct {
	stamp    string
	stampErr error
	reads    int
	// trimmed is what the snapshot of each stamp says of column code.
	trimmed map[string]bool
	// onRead, when set, runs as the catalog is read: a new snapshot
	// arriving between the stamp and the catalog.
	onRead func()
}

func (f *stampFake) session(withStamp bool) *session {
	s := &session{user: "app"}
	s.initSettings("public", "app")
	s.reported = map[string]string{}
	s.database = "orders"
	s.bound = &Binding{Catalog: func(context.Context) ([]CatalogTable, error) {
		f.reads++
		at := f.stamp
		if f.onRead != nil {
			f.onRead()
		}
		return []CatalogTable{{Schema: "public", Name: "t", Columns: []CatalogColumn{
			{Name: "code", Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: 9, TrimmedOnCopy: f.trimmed[at]}}}}, nil
	}}
	if withStamp {
		s.bound.CatalogStamp = func(context.Context) (string, error) { return f.stamp, f.stampErr }
	}
	return s
}

func trimmedCode(t *testing.T, s *session) bool {
	t.Helper()
	v, perr := s.catalog()
	if perr != nil {
		t.Fatalf("catalog: %v", perr)
	}
	return v.tables[0].Columns[0].TrimmedOnCopy
}

func TestCatalogStamp_sameSnapshotKeepsTheCatalog(t *testing.T) {
	f := &stampFake{stamp: "a"}
	s := f.session(true)
	for range 3 {
		trimmedCode(t, s)
	}
	if f.reads != 1 {
		t.Fatalf("catalog read %d times under one snapshot, want 1", f.reads)
	}
}

func TestCatalogStamp_newSnapshotReadsItAgain(t *testing.T) {
	// The column was a varchar in snapshot a and is a char(n) the view
	// trims in snapshot b: the screen must see b's at once, not ten seconds
	// later.
	f := &stampFake{stamp: "a", trimmed: map[string]bool{"b": true}}
	s := f.session(true)
	if trimmedCode(t, s) {
		t.Fatal("snapshot a: code trimmed")
	}
	f.stamp = "b"
	if !trimmedCode(t, s) {
		t.Fatal("snapshot b: the kept catalog of snapshot a was used")
	}
	trimmedCode(t, s)
	if f.reads != 2 {
		t.Fatalf("catalog read %d times over two snapshots, want 2", f.reads)
	}
}

func TestCatalogStamp_anErrorReadsItAgain(t *testing.T) {
	f := &stampFake{stamp: "a"}
	s := f.session(true)
	trimmedCode(t, s)
	f.stampErr = errors.New("snapshot listing failed")
	trimmedCode(t, s)
	trimmedCode(t, s)
	if f.reads != 3 {
		t.Fatalf("catalog read %d times with the stamp failing, want 3 (never kept in silence)", f.reads)
	}
}

func TestCatalogStamp_unreadAtFirstIsNotKept(t *testing.T) {
	// The stamp failed when the catalog was read: no stamp is kept, so the
	// next statement reads it again even though the stamp works by then.
	f := &stampFake{stamp: "a", stampErr: errors.New("busy")}
	s := f.session(true)
	trimmedCode(t, s)
	f.stampErr = nil
	trimmedCode(t, s)
	trimmedCode(t, s)
	if f.reads != 2 {
		t.Fatalf("catalog read %d times, want 2", f.reads)
	}
}

func TestCatalogStamp_snapshotBetweenStampAndCatalog(t *testing.T) {
	// The stamp says a, the catalog read already sees b: what is kept is
	// tied to a, so the next statement (stamp b) reads it again.
	f := &stampFake{stamp: "a", trimmed: map[string]bool{"b": true}}
	f.onRead = func() { f.stamp = "b"; f.onRead = nil }
	s := f.session(true)
	trimmedCode(t, s)
	if !trimmedCode(t, s) || f.reads != 2 {
		t.Fatalf("reads %d: the catalog kept under the older stamp was reused", f.reads)
	}
}

func TestCatalogStamp_withoutOneKeepsTenSeconds(t *testing.T) {
	f := &stampFake{stamp: "a"}
	s := f.session(false)
	trimmedCode(t, s)
	f.stamp = "b"
	trimmedCode(t, s)
	if f.reads != 1 {
		t.Fatalf("catalog read %d times with no stamp inside ten seconds, want 1", f.reads)
	}
	s.catalogAt = time.Now().Add(-catalogCacheFor - time.Second)
	trimmedCode(t, s)
	if f.reads != 2 {
		t.Fatalf("catalog read %d times after ten seconds, want 2", f.reads)
	}
}
