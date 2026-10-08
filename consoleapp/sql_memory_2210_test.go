package consoleapp

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// #2210: the memory SQL on the copy runs with, from --sql-memory or
// BINTRAIL_CONSOLE_SQL_MEMORY. The flag wins over the variable. A value that
// is not a size, or one under 512 MiB, stops the daemon at startup with the
// name to fix: a worker that small cannot open the views, and falling back to
// the default would hide the typo. The value reaches DuckDB in MiB, so its
// decimal "GB" never reads 4GB as 4,000,000,000 bytes.
func TestSQLMemory_2210(t *testing.T) {
	newCmd := func() *cobra.Command {
		c := &cobra.Command{}
		c.Flags().StringVar(&upSQLMemory, "sql-memory", "", "")
		return c
	}
	defer func() { upSQLMemory, upSQLMemoryLimit = "", "" }()
	cases := []struct {
		name    string
		env     string
		flag    string // "" = not given
		want    string // the DuckDB memory_limit, "" = the sandbox default
		wantErr string
	}{
		{name: "default", want: ""},
		{name: "env", env: "4GB", want: "4096MiB"},
		{name: "env lower case, spaces", env: " 8gb ", want: "8192MiB"},
		{name: "env MiB", env: "1536MiB", want: "1536MiB"},
		{name: "env not a size", env: "lots", wantErr: "BINTRAIL_CONSOLE_SQL_MEMORY"},
		{name: "env with a space inside", env: "4 GB", wantErr: "BINTRAIL_CONSOLE_SQL_MEMORY"},
		{name: "env too small", env: "256MB", wantErr: "BINTRAIL_CONSOLE_SQL_MEMORY"},
		{name: "env zero", env: "0", wantErr: "BINTRAIL_CONSOLE_SQL_MEMORY"},
		{name: "flag", flag: "3GB", want: "3072MiB"},
		{name: "flag at the floor", flag: "512MB", want: "512MiB"},
		{name: "flag wins over env", env: "8GB", flag: "1GB", want: "1024MiB"},
		{name: "flag too small beats a good env", env: "8GB", flag: "100MB", wantErr: "--sql-memory"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			upSQLMemory, upSQLMemoryLimit = "", ""
			t.Setenv("BINTRAIL_CONSOLE_SQL_MEMORY", c.env)
			cmd := newCmd()
			if c.flag != "" {
				if err := cmd.Flags().Set("sql-memory", c.flag); err != nil {
					t.Fatal(err)
				}
			}
			err := resolveSQLMemory(cmd)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if upSQLMemoryLimit != c.want {
				t.Errorf("memory_limit = %q, want %q", upSQLMemoryLimit, c.want)
			}
		})
	}
}

// The resolved memory reaches the console's configuration, where the SQL
// worker and the unmerged-changes limit both read it.
func TestUpConsoleConfig_sqlMemoryReachesTheConsole_2210(t *testing.T) {
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/idx",
		consoleOpts{Listen: "127.0.0.1:8090", Token: "tok", SQLMemoryLimit: "4096MiB"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SQLLimits.MemoryLimit != "4096MiB" {
		t.Errorf("Config.SQLLimits.MemoryLimit = %q, want 4096MiB", cfg.SQLLimits.MemoryLimit)
	}
	opts := upConsoleOpts()
	if opts.SQLMemoryLimit != upSQLMemoryLimit {
		t.Errorf("upConsoleOpts().SQLMemoryLimit = %q, want the resolved %q", opts.SQLMemoryLimit, upSQLMemoryLimit)
	}
}

// serve runs SQL on the copy too, so it reads the same flag and variable.
func TestSQLMemoryFrom_serve_2210(t *testing.T) {
	t.Setenv("BINTRAIL_CONSOLE_SQL_MEMORY", "6GB")
	c := &cobra.Command{}
	var v string
	c.Flags().StringVar(&v, "sql-memory", "", "")
	if got, err := sqlMemoryFrom(c); err != nil || got != "6144MiB" {
		t.Fatalf("env: %q, %v", got, err)
	}
	if err := c.Flags().Set("sql-memory", "nope"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlMemoryFrom(c); err == nil || !strings.Contains(err.Error(), "--sql-memory") {
		t.Fatalf("a bad flag must stop serve too: %v", err)
	}
	if f := serveCmd.Flags().Lookup("sql-memory"); f == nil {
		t.Fatal("serve has no --sql-memory flag")
	}
}

// Memory times statements at once past the host's memory is said at startup,
// only for a memory the operator set.
func TestSQLMemoryWarning_2210(t *testing.T) {
	const host = 8 << 30
	for _, c := range []struct {
		limit string
		n     int
		host  uint64
		warn  bool
	}{
		{"", 2, host, false},
		{"2048MiB", 2, host, false},
		{"4096MiB", 2, host, false},
		{"4097MiB", 2, host, true},
		{"6144MiB", 2, host, true},
		{"6144MiB", 1, host, false},
		{"6144MiB", 2, 0, false},
	} {
		got := sqlMemoryWarning(c.limit, c.n, c.host)
		if (got != "") != c.warn {
			t.Errorf("sqlMemoryWarning(%q, %d, %d) = %q, want warn=%v", c.limit, c.n, c.host, got, c.warn)
		}
		if c.warn && !strings.Contains(got, "--sql-memory") {
			t.Errorf("warning must name the flag: %q", got)
		}
	}
}
