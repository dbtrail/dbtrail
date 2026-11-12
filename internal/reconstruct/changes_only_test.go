package reconstruct

import (
	"context"
	"errors"
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
		for _, want := range []string{
			"shop.orders has no snapshot at or before 2026-06-01T00:00:00Z to start from",
			"only the rows they touched, with every other row missing",
			"nothing was written for it",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal does not say %q", want)
			}
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
