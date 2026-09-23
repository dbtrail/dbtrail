package consoleapp

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/status"
)

// TestBootCaptureFilterCarriesTheDaemonsOwnScope (#1802): the web interface
// counts tables only where it knows what capture watches, and the ONLY place
// that scope exists is this process's own flags — the schema snapshot records
// the schemas, nothing records --tables, and the parser drops a filtered-out
// table's events with no counter and no log line. Without this wiring a
// daemon started with --tables would report full coverage over tables nobody
// is watching.
func TestBootCaptureFilterCarriesTheDaemonsOwnScope(t *testing.T) {
	saved := struct{ source, schemas, tables string }{upSourceDSN, upSchemas, upTables}
	t.Cleanup(func() { upSourceDSN, upSchemas, upTables = saved.source, saved.schemas, saved.tables })

	// Source-less: this daemon captures nothing of its own, so it knows no
	// scope and must claim none.
	upSourceDSN, upSchemas, upTables = "", "shop", "shop.orders"
	if f := bootCaptureFilter(); f != nil {
		t.Errorf("a source-less daemon claims a scope it does not run: %+v", f)
	}

	upSourceDSN = "root:pw@tcp(127.0.0.1:3306)/"
	upSchemas, upTables = "shop, crm", "shop.orders,shop.customers"
	f := bootCaptureFilter()
	if f == nil || !f.Known {
		t.Fatalf("a daemon that runs the boot capture must report its scope: %+v", f)
	}
	if len(f.Schemas) != 2 || f.Schemas[0] != "shop" || f.Schemas[1] != "crm" {
		t.Errorf("schemas = %q, want the flag's own list", f.Schemas)
	}
	if len(f.Tables) != 2 || f.Tables[0] != "shop.orders" {
		t.Errorf("tables = %q, want the flag's own list", f.Tables)
	}
	// The scope is parsed exactly as the stream parses it, so the report and
	// capture cannot disagree about what is watched.
	if got := cliutil.ParseSchemaList(upTables); len(got) != len(f.Tables) {
		t.Errorf("the filter is not parsed the way the stream parses it: %q vs %q", got, f.Tables)
	}

	// And the console the daemon builds carries it: without this the web
	// interface would count the snapshot and claim coverage over tables the
	// --tables filter leaves unwatched.
	cfg, err := upConsoleConfig(nil, "root:pw@tcp(127.0.0.1:3306)/binlog_index", upConsoleOpts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BootCaptureFilter == nil || !cfg.BootCaptureFilter.Known ||
		len(cfg.BootCaptureFilter.Tables) != 2 || cfg.BootCaptureFilter.Tables[0] != "shop.orders" {
		t.Errorf("the console config does not carry this daemon's own scope: %+v", cfg.BootCaptureFilter)
	}

	// No filters at all: still known (every table of every schema).
	upSchemas, upTables = "", ""
	if f := bootCaptureFilter(); f == nil || !f.Known || len(f.Schemas) != 0 || len(f.Tables) != 0 {
		t.Errorf("an unfiltered daemon must claim the full scope it runs: %+v", f)
	}
	var _ status.CaptureFilter
}
