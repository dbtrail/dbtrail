package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

func TestDrillTargetIsPostgres(t *testing.T) {
	for dsn, want := range map[string]bool{
		"postgres://u:p@127.0.0.1:15433/postgres": true,
		"postgresql://u@h/db?sslmode=disable":     true,
		"POSTGRES://u@h/db":                       true,
		"  postgresql://u@h/db":                   true,
		"root:drill@tcp(127.0.0.1:13307)/":        false,
		"host=127.0.0.1 user=postgres dbname=x":   false,
		"postgres:pw@tcp(127.0.0.1:3306)/":        false, // a MySQL user named postgres
		"":                                        false,
	} {
		if got := drillTargetIsPostgres(dsn); got != want {
			t.Errorf("drillTargetIsPostgres(%q) = %v, want %v", dsn, got, want)
		}
	}
}

// A binary with no PostgreSQL loader refuses a postgres:// target and names
// the binary that has one, before connecting anywhere.
func TestOpenDrillTarget_postgresNeedsTheLoader(t *testing.T) {
	prev := postgresDrillTarget
	postgresDrillTarget = nil
	t.Cleanup(func() { postgresDrillTarget = prev })
	_, err := openDrillTarget(context.Background(), "postgres://u@127.0.0.1:1/db")
	if err == nil || !strings.Contains(err.Error(), "bintrail-pg") {
		t.Fatalf("want a refusal naming bintrail-pg, got %v", err)
	}
}

func TestOpenDrillTarget_postgresUsesTheLoader(t *testing.T) {
	prev := postgresDrillTarget
	t.Cleanup(func() { postgresDrillTarget = prev })
	var gotDSN string
	fake := &fakeDrillTarget{}
	SetPostgresDrillTarget(func(_ context.Context, dsn string) (DrillTarget, error) {
		gotDSN = dsn
		return fake, nil
	})
	got, err := openDrillTarget(context.Background(), " postgresql://u@h/db")
	if err != nil || got != fake || gotDSN != "postgresql://u@h/db" {
		t.Fatalf("got %v %v dsn=%q", got, err, gotDSN)
	}
}

// A keyword-form PostgreSQL DSN is not detected (only the URL form is), so it
// reaches the MySQL parser; the refusal says how to write a PostgreSQL target.
func TestOpenDrillTarget_loaderOpensNothing(t *testing.T) {
	prev := postgresDrillTarget
	t.Cleanup(func() { postgresDrillTarget = prev })
	SetPostgresDrillTarget(func(context.Context, string) (DrillTarget, error) { return nil, nil })
	if got, err := openDrillTarget(context.Background(), "postgres://u@h/db"); err == nil {
		t.Fatalf("a nil target was accepted: %v", got)
	}
}

func TestOpenDrillTarget_keywordDSNHint(t *testing.T) {
	_, err := openDrillTarget(context.Background(), "host=127.0.0.1 user=postgres dbname=x")
	if err == nil || !strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("want a hint naming the postgres:// form, got %v", err)
	}
}

// The reconstruct refusals name --output-format, a flag drill does not have;
// drill says what to change instead: the kind of server --target-dsn names.
func TestDrillReconstructError(t *testing.T) {
	err := drillReconstructError(fmt.Errorf("x: %w", reconstruct.ErrPGFullTableMydumper))
	if !errors.Is(err, reconstruct.ErrPGFullTableMydumper) ||
		!strings.Contains(err.Error(), "PostgreSQL") || !strings.Contains(err.Error(), "postgres://") ||
		strings.Contains(err.Error(), "--output-format") {
		t.Fatalf("PostgreSQL source into a MySQL target: %v", err)
	}
	err = drillReconstructError(reconstruct.ErrPsqlNeedsPostgres)
	if !errors.Is(err, reconstruct.ErrPsqlNeedsPostgres) ||
		!strings.Contains(err.Error(), "scratch MySQL") || strings.Contains(err.Error(), "--output-format") {
		t.Fatalf("MySQL source into a PostgreSQL target: %v", err)
	}
	// Joined with a real failure of another table, the refusal is not
	// rewritten: the other error must stay visible.
	mixed := errors.Join(fmt.Errorf("shop.a: %w", reconstruct.ErrPGFullTableMydumper), errors.New("shop.b: disk full"))
	if drillReconstructError(mixed) != nil || drillReconstructError(fmt.Errorf("run: %w", mixed)) != nil {
		t.Fatal("a refusal beside another failure was rewritten, hiding it")
	}
	if drillReconstructError(emptyJoin{}) != nil {
		t.Fatal("an error joining nothing is not a refusal")
	}
	both := errors.Join(fmt.Errorf("shop.a: %w", reconstruct.ErrPGFullTableMydumper), fmt.Errorf("shop.b: %w", reconstruct.ErrPGFullTableMydumper))
	if drillReconstructError(fmt.Errorf("run: %w", both)) == nil {
		t.Fatal("every table refused the same way is rewritten")
	}
	other := errors.New("boom")
	if drillReconstructError(other) != nil {
		t.Fatal("other errors are not translated")
	}
}

type fakeDrillTarget struct {
	loadErr error
	count   int64
	loaded  bool
}

func (f *fakeDrillTarget) OutputFormat() string                                 { return reconstruct.OutputFormatPsql }
func (f *fakeDrillTarget) Empty(context.Context, []string) error                { return nil }
func (f *fakeDrillTarget) Count(context.Context, string, string) (int64, error) { return f.count, nil }
func (f *fakeDrillTarget) Close() error                                         { return nil }
func (f *fakeDrillTarget) Load(context.Context, string, *reconstruct.TableReport) error {
	f.loaded = true
	return f.loadErr
}

// drillTable reaches the target only through the seam: the count it compares
// is the target's, a load failure fails the table, binlog-only never loads.
func TestDrillTable_throughTheSeam(t *testing.T) {
	rep := func() *reconstruct.TableReport {
		return &reconstruct.TableReport{Schema: "shop", Table: "Orders", RowsWritten: 3}
	}
	if got := drillTable(context.Background(), &fakeDrillTarget{count: 3}, "", rep()); got.Status != "pass" {
		t.Fatalf("matching count: %+v", got)
	}
	if got := drillTable(context.Background(), &fakeDrillTarget{count: 2}, "", rep()); got.Status != "fail" ||
		!strings.Contains(got.Error, "loaded 2 rows, dump wrote 3") {
		t.Fatalf("count mismatch: %+v", got)
	}
	if got := drillTable(context.Background(), &fakeDrillTarget{count: 3, loadErr: errors.New("copy failed")}, "", rep()); got.Status != "fail" ||
		got.Error != "copy failed" {
		t.Fatalf("load failure: %+v", got)
	}
	f := &fakeDrillTarget{count: 0}
	r := rep()
	r.BinlogOnly = true
	if got := drillTable(context.Background(), f, "", r); got.Status != "fail" || f.loaded {
		t.Fatalf("binlog-only must fail without loading: %+v loaded=%v", got, f.loaded)
	}
}

type emptyJoin struct{}

func (emptyJoin) Error() string   { return "nothing" }
func (emptyJoin) Unwrap() []error { return nil }
