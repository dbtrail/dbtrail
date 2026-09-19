package cliapp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// ─── Parent command ───────────────────────────────────────────────────────────

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage bintrail configuration",
}

// ─── config init ──────────────────────────────────────────────────────────────

var configInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Generate a .bintrail.env configuration file",
	Long: `Generates a .bintrail.env file in the current directory with all available
configuration variables. Use --global to write to ~/.config/bintrail/config.env
instead.

Values already set in the environment (from the shell or an existing env file)
are written uncommented; all others are commented out as templates.

The env file is loaded automatically by all bintrail commands with precedence:
  CLI flag > environment variable > default value`,
	RunE: runConfigInit,
}

var cfgGlobal bool

func init() {
	configInitCmd.Flags().BoolVar(&cfgGlobal, "global", false, "Write to ~/.config/bintrail/config.env instead of .bintrail.env")
	configCmd.AddCommand(configInitCmd)
	rootCmd.AddCommand(configCmd)
}

func runConfigInit(cmd *cobra.Command, args []string) error {
	path := ".bintrail.env"
	if cfgGlobal {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("cannot determine home directory: %w", err)
		}
		dir := filepath.Join(home, ".config", "bintrail")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("cannot create config directory: %w", err)
		}
		path = filepath.Join(dir, "config.env")
	}

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("file already exists: %s\nRemove it first or edit it directly.", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot check %s: %w", path, err)
	}

	content := generateEnvTemplate()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	fmt.Printf("Configuration written to %s\n", path)
	fmt.Println("Edit the file to set your values, then uncomment the lines you want to use.")
	return nil
}

// envSection groups env bindings under a section header for the template.
type envSection struct {
	Header   string
	Bindings []envTemplateEntry
}

// envTemplateEntry is a single line in the env template.
type envTemplateEntry struct {
	EnvVar      string
	Placeholder string // shown when no value is set (e.g. "user:pass@tcp(host:3306)/binlog_index")
}

// envSections defines the same env vars as cli.EnvBindings (in internal/cli),
// grouped by category for template generation. Keep in sync with cli.EnvBindings.
var envSections = []envSection{
	{
		Header: "Database connections",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_INDEX_DSN", "user:pass@tcp(host:3306)/binlog_index"},
			{"BINTRAIL_SOURCE_DSN", "user:pass@tcp(host:3306)/myapp"},
			{"BINTRAIL_SOURCE_FLAVOR", "mysql"},
		},
	},
	{
		Header: "Filters",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_SCHEMAS", ""},
			{"BINTRAIL_TABLES", ""},
			{"BINTRAIL_COLUMN_EQ", ""},
		},
	},
	{
		Header: "Server identity",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_ID", ""},
			// Optional: UUID of a pre-registered BYOS server returned by
			// POST /api/v1/servers. When set, the agent's WebSocket connect
			// is reconciled to that server record; when unset, the SaaS
			// auto-creates a new byos-<server-id> record (back-compat).
			// See issue #317.
			{"BINTRAIL_SERVER_UUID", ""},
		},
	},
	{
		Header: "Archives",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_ARCHIVE_DIR", ""},
			{"BINTRAIL_ARCHIVE_S3", "s3://my-bucket/archives/"},
			{"BINTRAIL_ARCHIVE_S3_REGION", ""},
		},
	},
	{
		Header: "S3 bucket (used by bintrail init)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_S3_BUCKET", ""},
			{"BINTRAIL_S3_REGION", ""},
			{"BINTRAIL_S3_ARN", ""},
		},
	},
	{
		Header: "Stream settings",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_SERVER_ID", ""},
			{"BINTRAIL_BATCH_SIZE", "1000"},
			{"BINTRAIL_METRICS_ADDR", ""},
			{"BINTRAIL_METRICS_SCRAPE_INTERVAL", "60"},
			{"BINTRAIL_STREAM_GAP_TIMEOUT", "30"},
		},
	},
	{
		Header: "TLS (used by bintrail stream)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_SSL_MODE", "preferred"},
			{"BINTRAIL_SSL_CA", ""},
			{"BINTRAIL_SSL_CERT", ""},
			{"BINTRAIL_SSL_KEY", ""},
		},
	},
	{
		Header: "Agent (used by bintrail agent)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_API_KEY", ""},
			{"BINTRAIL_AGENT_ENDPOINT", "wss://api.dbtrail.io/v1/agent"},
			{"BINTRAIL_AGENT_MAX_RECONNECT_ATTEMPTS", "10"},
		},
	},
	{
		Header: "Local event buffer (BYOS mode)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_BUFFER_RETAIN", "6h"},
			{"BINTRAIL_BUFFER_MAX_EVENTS", "0"},
			{"BINTRAIL_BUFFER_MAX_BYTES", "0"},
			{"BINTRAIL_START_GTID", ""},
		},
	},
	{
		Header: "BYOS flush pipeline",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_S3_PREFIX", "bintrail/"},
			{"BINTRAIL_FLUSH_INTERVAL", "5s"},
		},
	},
	{
		Header: "Shim auth (used by bintrail shim)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_AUTH_METHOD", ""},
		},
	},
	{
		Header: "Shim limits (used by bintrail shim; see docs/time-travel-sql.md)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_SHIM_QUERY_TIMEOUT", "5m"},
			{"BINTRAIL_SHIM_MAX_CONNECTIONS", "100"},
			{"BINTRAIL_SHIM_MAX_FULLTABLE_QUERIES", "4"},
		},
	},
	{
		Header: "Built-in rotation (used by bintrail up)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_ROTATE_RETAIN", indexer.DefaultRotateRetain},
			{"BINTRAIL_ROTATE_INTERVAL", "1h"},
			{"BINTRAIL_ROTATE_ADD_FUTURE", "3"},
		},
	},
	{
		Header: "Baseline retention (used by bintrail baseline; prunes local snapshots once a durable S3 copy exists)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_BASELINE_RETAIN", ""},
		},
	},
	{
		Header: "DuckDB tuning (query/recover/reconstruct: trade memory-safety for speed)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_ULTRAFAST", ""},
			{"BINTRAIL_DUCKDB_THREADS", ""},
			{"BINTRAIL_DUCKDB_MEMORY_LIMIT", ""},
		},
	},
	{
		Header: "Iceberg export (bintrail export iceberg)",
		Bindings: []envTemplateEntry{
			{"BINTRAIL_ICEBERG_WAREHOUSE", ""},
		},
	},
	{
		Header: "Memory guards at scale (recover refuses oversized scripts; reconstruct warns on large windows, #654)",
		Bindings: []envTemplateEntry{
			// recover: refuse a reversal script whose row payload exceeds this (0 = unlimited).
			{"BINTRAIL_RECOVER_MAX_BYTES", "2GB"},
			// reconstruct: warn when a full-table window exceeds this many events (0 disables).
			{"BINTRAIL_RECONSTRUCT_WARN_EVENTS", "5000000"},
			// reconstruct: events fetched per page when streaming a full-table
			// window (0 = built-in default). Trades peak memory against round trips.
			{"BINTRAIL_RECONSTRUCT_FETCH_BATCH", ""},
		},
	},
	// The BINTRAIL_CONSOLE_* vars moved with the web console to the standalone
	// bintrail-console binary (serve/watch), which reads the same env file —
	// they are no longer advertised in the core CLI's template.
}

// generateEnvTemplate builds the .bintrail.env file content. Variables
// that are already set in the current environment are written uncommented
// with their current value; all others are commented out.
func generateEnvTemplate() string {
	var sb strings.Builder
	sb.WriteString("# Bintrail configuration\n")
	sb.WriteString("# Generated by bintrail config init\n")
	sb.WriteString("#\n")
	sb.WriteString("# Uncomment and set values below. These are loaded automatically\n")
	sb.WriteString("# by all bintrail commands. CLI flags take precedence over env vars.\n")
	sb.WriteString("#\n")
	sb.WriteString("# Precedence: CLI flag > environment variable > default value\n")
	sb.WriteString("#\n")
	sb.WriteString("# Only variables that back a CLI flag are listed here. Process-wide ones\n")
	sb.WriteString("# have no flag to override them and are documented instead: AWS_* for S3\n")
	sb.WriteString("# credentials, BINTRAIL_S3_ENDPOINT / BINTRAIL_S3_PATH_STYLE for an\n")
	sb.WriteString("# S3-compatible store (docs/upload.md).\n")

	for _, sec := range envSections {
		fmt.Fprintf(&sb, "\n# ── %s ──\n", sec.Header)
		for _, entry := range sec.Bindings {
			if v, ok := os.LookupEnv(entry.EnvVar); ok && v != "" {
				fmt.Fprintf(&sb, "%s=%s\n", entry.EnvVar, v)
			} else if entry.Placeholder != "" {
				fmt.Fprintf(&sb, "# %s=%s\n", entry.EnvVar, entry.Placeholder)
			} else {
				fmt.Fprintf(&sb, "# %s=\n", entry.EnvVar)
			}
		}
	}

	return sb.String()
}
