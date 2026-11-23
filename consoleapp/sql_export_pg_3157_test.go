package consoleapp

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A PostgreSQL server's .sql build asks the engine for the files psql loads;
// every other server's, and one whose flavor is not said, for a mydumper
// dump (#3157).
func TestSQLExportFormatFollowsTheServer(t *testing.T) {
	for flavor, want := range map[string]string{
		console.FlavorPostgres: reconstruct.OutputFormatPsql,
		console.FlavorMySQL:    reconstruct.OutputFormatMydumper,
		console.FlavorMariaDB:  reconstruct.OutputFormatMydumper,
		"":                     reconstruct.OutputFormatMydumper,
		"Postgres":             reconstruct.OutputFormatMydumper,
	} {
		cfg := sqlExportFoldConfig(console.SQLExportRequest{ServerID: "s1", IndexDSN: "dsn", At: time.Now(), Flavor: flavor},
			"/build", []string{"public.orders"})
		if cfg.OutputFormat != want {
			t.Errorf("flavor %q: the build asks for %q, want %q", flavor, cfg.OutputFormat, want)
		}
	}
}

// The engine's refusals about the output format name the terminal command's
// options. What the web interface shows names none, says each once, and
// keeps the run's other errors.
func TestSQLExportFormatRefusalsInWebWords(t *testing.T) {
	for _, refusal := range []error{reconstruct.ErrPGFullTableMydumper, reconstruct.ErrPsqlNeedsPostgres} {
		if !strings.Contains(refusal.Error(), "--output-format") {
			t.Fatalf("the engine's refusal no longer names an option; this test checks nothing: %v", refusal)
		}
		engine := errors.Join(
			fmt.Errorf("public.orders: %w", refusal),
			errors.New("public.items: no primary key"),
			fmt.Errorf("public.users: %w", refusal),
		)
		err := sqlExportRunError(nil, engine)
		if err == nil {
			t.Fatal("the refusal did not fail the build")
		}
		got := err.Error()
		t.Logf("rendered: %s", got)
		if strings.Contains(got, "--") || strings.Contains(got, "`") || strings.Contains(got, "output-format") {
			t.Errorf("the build's error names a terminal option or command: %s", got)
		}
		lines := strings.Split(got, "\n")
		if len(lines) != 2 || lines[1] != "public.items: no primary key" {
			t.Errorf("want the refusal once and the other error kept, got %q", lines)
		}
		if !strings.Contains(lines[0], "PostgreSQL") {
			t.Errorf("the refusal does not say what is wrong: %s", lines[0])
		}
		// Returned bare, as ErrPsqlNeedsPostgres is.
		if alone := sqlExportRunError(nil, refusal); alone == nil || strings.Contains(alone.Error(), "--") {
			t.Errorf("the refusal alone: %v", alone)
		}
	}
}
