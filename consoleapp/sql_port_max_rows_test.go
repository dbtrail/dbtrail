package consoleapp

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
)

// --sql-port-max-rows: the flag, its environment variable, and the values
// that fail at startup instead of refusing every result later.
func TestResolveSQLPortMaxRows(t *testing.T) {
	cases := []struct {
		name, flag, env string
		want            int
		wantErr         string
	}{
		{name: "neither", want: console.DefaultSQLPortMaxRows},
		{name: "flag", flag: "5000", want: 5000},
		{name: "env", env: " 2500 ", want: 2500},
		{name: "flag beats env", flag: "7", env: "9", want: 7},
		{name: "flag zero", flag: "0", wantErr: "--sql-port-max-rows 0"},
		{name: "flag negative", flag: "-3", wantErr: "--sql-port-max-rows -3"},
		{name: "env zero", env: "0", wantErr: "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS"},
		{name: "env words", env: "many", wantErr: "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS"},
		{name: "env blank", env: "   ", want: console.DefaultSQLPortMaxRows},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prev := upSQLPortMaxRows
			t.Cleanup(func() { upSQLPortMaxRows = prev })
			upSQLPortMaxRows = console.DefaultSQLPortMaxRows
			t.Setenv("BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS", c.env)
			cmd := &cobra.Command{}
			cmd.Flags().IntVar(&upSQLPortMaxRows, "sql-port-max-rows", console.DefaultSQLPortMaxRows, "")
			if c.flag != "" {
				if err := cmd.Flags().Set("sql-port-max-rows", c.flag); err != nil {
					t.Fatal(err)
				}
			}
			err := resolveSQLPortMaxRows(cmd)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want one naming %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if upSQLPortMaxRows != c.want {
				t.Errorf("cap = %d, want %d", upSQLPortMaxRows, c.want)
			}
		})
	}
}

// The resolved cap reaches the console's configuration, and the real watch
// command registers the flag.
func TestUpConsoleConfig_sqlPortMaxRowsReachesTheConsole(t *testing.T) {
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/idx",
		consoleOpts{Listen: "127.0.0.1:8090", Token: "tok", SQLPortMaxRows: 4321}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SQLPortMaxRows != 4321 {
		t.Errorf("Config.SQLPortMaxRows = %d, want 4321", cfg.SQLPortMaxRows)
	}
	if got := upConsoleOpts().SQLPortMaxRows; got != upSQLPortMaxRows {
		t.Errorf("upConsoleOpts().SQLPortMaxRows = %d, want the resolved %d", got, upSQLPortMaxRows)
	}
	if f := watchCmd.Flags().Lookup("sql-port-max-rows"); f == nil || f.DefValue != "10000" {
		t.Errorf("watch --sql-port-max-rows = %+v, want it registered with default 10000", f)
	}
}
