package shim

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestRunSnapshotFullTable_pgShapedSnapshotGetsWrongPathVerdict drives a
// PG-shaped schema snapshot (empty DATA_TYPE — the #533 invariant: only
// WritePGSnapshot writes "") through full-table _snapshot's real PK-type gate
// with an unresolved source flavor (nil index DB reads as ""). Before #1198
// the refusal blamed the PK type (`has type , which the baseline merge cannot
// canonicalize`); it must now name the wrong-path cause. The gate fires before
// any baseline or index read, so a nil DB and an empty baseline dir suffice —
// same DB-free pattern as TestRunSnapshotFullTable_BaselineConfiguredRefusesWireError.
func TestRunSnapshotFullTable_pgShapedSnapshotGetsWrongPathVerdict(t *testing.T) {
	h := &Handler{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg:    Config{BaselineDir: t.TempDir()},
		resolverFn: func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"app.orders": {Schema: "app", Table: "orders", PKColumns: []string{"id"},
					Columns: []metadata.ColumnMeta{
						{Name: "id", OrdinalPosition: 1, DataType: "", IsPK: true},
					}},
			}), nil
		},
	}
	_, err := h.runSnapshotFullTable(TimeTravelQuery{
		Type: TypeSnapshot, Schema: "app", Table: "orders", AsOf: time.Now(),
	})
	if err == nil {
		t.Fatal("PG-shaped PK on the full-table MySQL path must refuse, got nil")
	}
	var myErr *gomysql.MyError
	if !errors.As(err, &myErr) {
		t.Fatalf("error = %T %v, want *mysql.MyError", err, err)
	}
	if myErr.Code != gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE {
		t.Errorf("wire code = %d, want %d (ER_NO_PARTITION_FOR_GIVEN_VALUE)", myErr.Code, gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE)
	}
	// Anchored on the sentence a PG-shaped column must NOT get. It used to be
	// anchored on the typed branch's old private wording, which #1461 deleted
	// from the tree: an assertion against a string that exists nowhere passes
	// for an unrelated reason and guards nothing.
	if strings.Contains(myErr.Message, "unsupported by the baseline canonicalizer") {
		t.Errorf("empty DATA_TYPE must not get the misleading PK-type blame: %q", myErr.Message)
	}
	for _, want := range []string{"PG snapshot shape", "not yet supported for PostgreSQL sources (#1596)", "app.orders", "_flashback"} {
		if !strings.Contains(myErr.Message, want) {
			t.Errorf("wrong-path verdict lacks %q: %q", want, myErr.Message)
		}
	}
}

// TestRunSnapshotFullTable_realUnsupportedTypeKeepsCanonicalizeMessage pins the
// other half of the discrimination: a REAL MySQL type the canonicalizer does
// not handle keeps the accurate per-type refusal — #1198 must not blur it into
// the PostgreSQL verdict. Since #1461 that refusal is the SHARED
// reconstruct.PKTypeGateReason sentence rather than a third private phrasing
// of the same limit; the _flashback steer this wire surface owns stays in the
// frame around it.
func TestRunSnapshotFullTable_realUnsupportedTypeKeepsCanonicalizeMessage(t *testing.T) {
	h := &Handler{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg:    Config{BaselineDir: t.TempDir()},
		resolverFn: func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"app.readings": {Schema: "app", Table: "readings", PKColumns: []string{"k"},
					Columns: []metadata.ColumnMeta{
						{Name: "k", OrdinalPosition: 1, DataType: "float", IsPK: true},
					}},
			}), nil
		},
	}
	_, err := h.runSnapshotFullTable(TimeTravelQuery{
		Type: TypeSnapshot, Schema: "app", Table: "readings", AsOf: time.Now(),
	})
	if err == nil {
		t.Fatal("unsupported PK type with a baseline configured must refuse, got nil")
	}
	var myErr *gomysql.MyError
	if !errors.As(err, &myErr) {
		t.Fatalf("error = %T %v, want *mysql.MyError", err, err)
	}
	want := reconstruct.PKTypeGateReason(
		metadata.ColumnMeta{Name: "k", IsPK: true, DataType: "float"}, "full-table _snapshot", "materialize")
	if !strings.Contains(myErr.Message, want) {
		t.Errorf("a real unsupported MySQL type must render the shared gate reason %q, got: %q", want, myErr.Message)
	}
	// The frame is this surface's own and must survive the unification: the
	// operator needs the table named and the _flashback fallback offered.
	for _, w := range []string{"app.readings", "_snapshot cannot return a complete table", "_flashback"} {
		if !strings.Contains(myErr.Message, w) {
			t.Errorf("refusal lacks %q: %q", w, myErr.Message)
		}
	}
	if strings.Contains(myErr.Message, "PostgreSQL") {
		t.Errorf("a real MySQL type must not get the PostgreSQL verdict: %q", myErr.Message)
	}
}
