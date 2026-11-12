package reconstruct

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A table with no snapshot at or before the moment is refused unless the
// caller asked for changes-only output. No index is needed to reach the
// refusal: it comes before anything is fetched.
func TestReconstructTable_noSnapshotIsRefusedWithoutTheOptIn(t *testing.T) {
	at := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, format := range []string{OutputFormatMydumper, ""} {
		cfg := FullTableConfig{BaselineSrc: t.TempDir(), At: at, OutputDir: t.TempDir(), OutputFormat: format}
		rep, err := ReconstructTable(context.Background(), cfg, "shop", "orders", nil, nil, nil, nil, "")
		if rep != nil {
			t.Fatalf("format %q: a report came back for a refused table: %+v", format, rep)
		}
		var refusal *ChangesOnlyError
		if !errors.As(err, &refusal) || !errors.Is(err, ErrChangesOnly) {
			t.Fatalf("format %q: err = %v, want a ChangesOnlyError", format, err)
		}
		if errors.Is(err, ErrNoBaseline) {
			t.Errorf("the refusal reads as ErrNoBaseline, which callers answer by looking in another location")
		}
		if refusal.Schema != "shop" || refusal.Table != "orders" || !refusal.At.Equal(at) {
			t.Errorf("refusal = %+v", refusal)
		}
		t.Logf("rendered: %s", err)
		if err.Error() != "there is no snapshot at or before 2026-06-01T00:00:00Z to start it from. "+
			"Its recorded changes alone give only the rows they touched, with every other row missing, "+
			"so nothing was written for it" {
			t.Errorf("the refusal's wording changed: %s", err)
		}
		// The run puts "schema.table: " in front of every table's failure,
		// so the refusal itself must not name the table again.
		if strings.Contains(err.Error(), "orders") {
			t.Errorf("the refusal names the table, which the run already puts in front of it: %s", err)
		}
		// Each surface has its own way to opt in, or none: an MCP client or a
		// console user handed a CLI flag cannot pass it.
		if strings.Contains(err.Error(), "--") || strings.Contains(err.Error(), "—") {
			t.Errorf("the engine's refusal names a flag or holds a dash: %s", err)
		}
	}
}

// Parquet output keeps its own refusal, with or without the opt-in: a
// snapshot is never written from changes alone.
func TestReconstructTable_parquetRefusalIgnoresTheOptIn(t *testing.T) {
	for _, allow := range []bool{false, true} {
		cfg := FullTableConfig{BaselineSrc: t.TempDir(), At: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			OutputFormat: OutputFormatParquet, AllowChangesOnly: allow}
		_, err := ReconstructTable(context.Background(), cfg, "shop", "orders", nil, nil, nil, nil, "")
		if err == nil || !strings.Contains(err.Error(), "cannot be re-emitted as one") || errors.Is(err, ErrChangesOnly) {
			t.Fatalf("AllowChangesOnly=%v: err = %v, want the Parquet refusal unchanged", allow, err)
		}
	}
}

// Both schema files a changes-only table can get open with the marker, and
// the marker is comment lines only whatever the names hold.
func TestChangesOnlyMarker_opensTheSchemaFile(t *testing.T) {
	placeholder := binlogOnlySchemaPlaceholder("shop", "orders")
	t.Logf("rendered:\n%s", placeholder)
	if !strings.HasPrefix(placeholder, "-- bintrail: CHANGES ONLY. The data file(s) of shop.orders hold only the rows") {
		t.Errorf("the placeholder schema file does not open with the marker:\n%s", placeholder)
	}
	if !strings.Contains(placeholder, "A row that no recorded change touched is missing") {
		t.Errorf("the marker does not say what is missing:\n%s", placeholder)
	}
	for _, line := range strings.Split(strings.TrimRight(changesOnlyMarker("my\ndb", "or\nders"), "\n"), "\n") {
		if !strings.HasPrefix(line, "--") {
			t.Errorf("marker line is not a comment and would execute: %q", line)
		}
	}
}

// SplitChangesOnly reads a run's joined error: nested joins, the run's
// "schema.table: " wrapping, and errors of other kinds, which it must keep.
func TestSplitChangesOnly(t *testing.T) {
	a := &ChangesOnlyError{Schema: "shop", Table: "a"}
	b := &ChangesOnlyError{Schema: "shop", Table: "b"}
	other, cancel := errors.New("shop.c: no primary key"), context.Canceled
	run := errors.Join(cancel, errors.Join(fmt.Errorf("shop.a: %w", a), other, fmt.Errorf("shop.b: %w", b)))

	refused, rest := SplitChangesOnly(run)
	if len(refused) != 2 || refused[0] != a || refused[1] != b {
		t.Errorf("refused = %v", refused)
	}
	if len(rest) != 2 || rest[0] != cancel || rest[1] != other {
		t.Errorf("rest = %v", rest)
	}
	if refused, rest := SplitChangesOnly(nil); refused != nil || rest != nil {
		t.Errorf("nil error: %v, %v", refused, rest)
	}
	if refused, rest := SplitChangesOnly(other); len(refused) != 0 || len(rest) != 1 {
		t.Errorf("a single other error: %v, %v", refused, rest)
	}
}

// createdByStatement decides, one recorded statement at a time from the
// newest, whether the table was new after it. Only a CREATE TABLE or CREATE
// OR REPLACE TABLE without IF NOT EXISTS says yes; an ALTER TABLE that moves
// no rows leaves the question to the statement before it; everything else
// says no.
func TestCreatedByStatement(t *testing.T) {
	cases := []struct {
		kind, text       string
		created, decided bool
	}{
		{"CREATE TABLE", "CREATE TABLE orders (id int primary key)", true, true},
		{"CREATE TABLE", "CREATE TABLE IF NOT EXISTS orders (id int primary key)", false, true},
		{"CREATE TABLE", "create table /* app */ if\n not   exists `orders` (id int)", false, true},
		{"CREATE TABLE", "CREATE TABLE /*!32312 IF NOT EXISTS*/ `orders` (id int)", false, true},
		{"CREATE TABLE", "CREATE TABLE orders LIKE orders_template", true, true},
		{"CREATE TABLE", "CREATE TABLE orders AS SELECT * FROM old_orders", true, true},
		{"CREATE TABLE", "", false, true},
		{"CREATE TABLE", "(same CREATE TABLE statement as the row for shop.a)", false, true},
		{"CREATE TABLE", "CREATE TABLE orders (id int) /* cut", false, true},
		{"CREATE OR REPLACE TABLE", "CREATE OR REPLACE TABLE orders (id int primary key)", true, true},
		{"CREATE OR REPLACE TABLE", "CREATE OR REPLACE TABLE IF NOT EXISTS orders (id int)", false, true},
		{"RENAME TABLE", "RENAME TABLE orders TO _orders_old, _orders_new TO orders", false, true},
		{"RENAME TABLE", "(same RENAME TABLE statement as the row for shop.orders)", false, true},
		{"DROP TABLE", "DROP TABLE orders", false, true},
		{"TRUNCATE TABLE", "TRUNCATE TABLE orders", false, true},
		{"ALTER TABLE", "ALTER TABLE orders ADD COLUMN note varchar(20)", false, false},
		{"ALTER TABLE", "ALTER TABLE orders EXCHANGE PARTITION p1 WITH TABLE orders_p1", false, true},
		{"ALTER TABLE", "", false, true},
		{"ALTER PUBLICATION", "ALTER PUBLICATION p ADD TABLE orders", false, true},
		{"CREATE TABLE", "/* dbtrail: inferred from relation shape; low=none */ CREATE TABLE orders (id int)", false, true},
	}
	for _, tc := range cases {
		created, decided := createdByStatement(tc.kind, tc.text)
		if created != tc.created || decided != tc.decided {
			t.Errorf("%s %q: created=%v decided=%v, want %v %v", tc.kind, tc.text, created, decided, tc.created, tc.decided)
		}
	}
}
