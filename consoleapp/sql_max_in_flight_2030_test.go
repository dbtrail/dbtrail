package consoleapp

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2030: the in-flight cap for SQL on the copy, from --sql-max-in-flight or
// BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT. The flag wins over the variable, and a
// value below 1, or a variable that is not a whole number, stops the daemon
// at startup with the name to fix: a cap of 0 would refuse every statement,
// and silently falling back to 2 would hide the typo.
func TestSQLMaxInFlight_2030(t *testing.T) {
	newCmd := func() *cobra.Command {
		c := &cobra.Command{}
		c.Flags().IntVar(&upSQLMaxInFlight, "sql-max-in-flight", sqlsandbox.DefaultMaxInFlight, "")
		return c
	}
	defer func() { upSQLMaxInFlight = sqlsandbox.DefaultMaxInFlight }()
	cases := []struct {
		name    string
		env     string
		flag    string // "" = not given
		want    int
		wantErr string
	}{
		{name: "default", want: 2},
		{name: "env", env: "4", want: 4},
		{name: "env with spaces", env: " 3 ", want: 3},
		{name: "env not a number", env: "four", wantErr: "BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT"},
		{name: "env zero", env: "0", wantErr: "BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT"},
		{name: "env negative", env: "-1", wantErr: "BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT"},
		{name: "flag", flag: "6", want: 6},
		{name: "flag wins over env", env: "4", flag: "1", want: 1},
		{name: "flag zero", flag: "0", wantErr: "--sql-max-in-flight"},
		{name: "flag zero beats a good env", env: "4", flag: "0", wantErr: "--sql-max-in-flight"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT", c.env)
			cmd := newCmd()
			if c.flag != "" {
				if err := cmd.Flags().Set("sql-max-in-flight", c.flag); err != nil {
					t.Fatal(err)
				}
			}
			err := resolveSQLMaxInFlight(cmd)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if upSQLMaxInFlight != c.want {
				t.Errorf("cap = %d, want %d", upSQLMaxInFlight, c.want)
			}
		})
	}
}

// The resolved cap reaches the console's configuration.
func TestUpConsoleConfig_sqlMaxInFlightReachesTheConsole_2030(t *testing.T) {
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/idx",
		consoleOpts{Listen: "127.0.0.1:8090", Token: "tok", SQLMaxInFlight: 5}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SQLMaxInFlight != 5 {
		t.Errorf("Config.SQLMaxInFlight = %d, want 5", cfg.SQLMaxInFlight)
	}
}

// More statements than the host has cores for is allowed (the operator may
// know better), but said once at startup, with the arithmetic.
func TestSQLMaxInFlightWarning_2030(t *testing.T) {
	if w := sqlMaxInFlightWarning(2, 2, 8); w != "" {
		t.Errorf("2 x 2 threads on 8 cores warned: %q", w)
	}
	if w := sqlMaxInFlightWarning(4, 2, 8); w != "" {
		t.Errorf("4 x 2 threads on 8 cores (exactly full) warned: %q", w)
	}
	w := sqlMaxInFlightWarning(6, 2, 8)
	for _, want := range []string{"6", "12", "8", "capture"} {
		if !strings.Contains(w, want) {
			t.Errorf("warning %q lacks %q", w, want)
		}
	}
}

// The resolved global reaches the options both watch entry points build.
func TestUpConsoleOpts_carriesSQLMaxInFlight_2030(t *testing.T) {
	defer func() { upSQLMaxInFlight = sqlsandbox.DefaultMaxInFlight }()
	upSQLMaxInFlight = 7
	if got := upConsoleOpts().SQLMaxInFlight; got != 7 {
		t.Errorf("upConsoleOpts().SQLMaxInFlight = %d, want 7", got)
	}
}
