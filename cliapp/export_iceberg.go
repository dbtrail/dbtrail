package cliapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/icebergexport"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The export commands live in cliapp, not internal/cli, on purpose: internal/cli
// is linked by the console daemon and bintrail-pg, and the Iceberg library
// must reach neither (cliapp/icebergfree_test.go). cliapp is the root that
// only cmd/bintrail links.

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Write table state to an external table format",
	Long: `Export the current state of indexed tables to a format other tools read.
bintrail's own storage does not change: the archives, baselines and index stay
what they are, and an export reads through them and writes somewhere new.`,
}

var exportIcebergCmd = &cobra.Command{
	Use:   "iceberg",
	Short: "Export table state to Apache Iceberg tables, incrementally",
	Long: `Write each table's current state as an Apache Iceberg table under --warehouse,
one table per <warehouse>/<schema>/<table>/, readable by DuckDB, Spark, Trino
and Athena.

CATALOGS

  hadoop (default) The tables live in a local directory (--warehouse). One
                   export at a time per directory: a second one is refused.
  glue             The tables are registered in the AWS Glue Data Catalog and
                   written under --warehouse s3://bucket/prefix, so Athena,
                   EMR Spark, Trino and Snowflake read them by name. The
                   database is the source schema. Credentials and region come
                   from the AWS default chain (AWS_REGION, a profile, an
                   instance role). Names must be lowercase letters, digits and
                   underscores, the only names Glue keeps as given.
  rest             An Iceberg REST catalog at --catalog-uri. --catalog-warehouse
                   is the identifier the server expects (a catalog name, an AWS
                   account id, an S3 Tables bucket ARN); --warehouse is an
                   optional location prefix. Authenticate with --catalog-sigv4,
                   or an OAuth client credential, or a bearer token. Secrets are
                   never flag values: set ` + envCatalogCredential + ` or
                   ` + envCatalogToken + `, or point --catalog-credential-file or
                   --catalog-token-file at a file holding the secret.

With glue or rest, two exports of one table may run at once: the one that
commits second is refused for that table ("refused-conflict") and commits
nothing; the next run continues from the winner's cursor. A table the catalog
already holds that bintrail did not create, or created for another source
server, is refused, never adopted. Glue and REST need an index that registers
its source (bintrail stream does; bintrail index from binlog files does not).
On every catalog, two tables whose names differ only in case are refused.

The first run loads the newest baseline snapshot. Every run after that appends
only what changed: the events between the table's own cursor and the run's
binlog cut are folded to the net change per primary key and committed as ONE
Iceberg snapshot (equality deletes for every touched key, plus the rows that
still exist). The table is never rewritten; compaction is the reader's
concern and any engine can do it.

The cursor lives in the table's own properties, in the same commit as the
data, so nothing is written to the index and a run that dies before committing
leaves the previous snapshot in place and resumes from it. To reload a table
from a fresh baseline, remove its directory (with glue or rest, drop the table
from the catalog).

WHAT IT REFUSES

  capture gap     The window spans events the index permanently lost, or hours
                  rotated out without an archive. The table does not advance.
  schema change   The table's columns changed since it was exported (or since
                  its baseline was taken). Remove the table directory to reload
                  it from a fresh baseline.
  destructive DDL A TRUNCATE / DROP / RENAME (or MariaDB's CREATE OR REPLACE) in
                  the window emits no row events, so folding over it would
                  resurrect rows that no longer exist.
  no primary key  Equality deletes name rows by key; a table without one, or
                  with a FLOAT, DOUBLE, TIME, BIT, JSON or spatial key, cannot
                  be exported.
  BIT columns     Stored differently by the baseline and the row events; not
                  reconciled yet.
  skipped events  The capture daemon dropped events for the table inside the
                  window (stream_state.capture_skips), so the index is not
                  complete for it.

Those refusals are per table. A refused table does not advance and is retried
on the next run; the other tables commit. The exit status is non-zero when any
table did not end current.

One refusal is for the whole run, before any table is touched: an index that
holds more than one source in bintrail_servers, or has no such table. Events
cannot be attributed to one source, so two sources with the same schema.table
would interleave in one Iceberg table. A file-mode index with no registered
source is accepted.

This is a one-shot command for the operator's scheduler. It never runs inside
the capture daemon.

Examples:
  # First run: load every table of the newest snapshot, then fold the deltas
  bintrail export iceberg --index-dsn "..." --baseline-dir /data/baselines \
    --warehouse /data/iceberg

  # Hourly, from cron: the same command line appends only what changed
  bintrail export iceberg --index-dsn "..." --baseline-dir /data/baselines \
    --warehouse /data/iceberg --tables shop.orders,shop.customers

  # Register the tables in AWS Glue, data on S3 (Athena reads them by name)
  AWS_REGION=us-west-2 bintrail export iceberg --index-dsn "..." \
    --baseline-dir /data/baselines --catalog glue --warehouse s3://my-bucket/iceberg

  # Glue's Iceberg REST endpoint, signed with SigV4
  bintrail export iceberg --index-dsn "..." --baseline-dir /data/baselines \
    --catalog rest --catalog-uri https://glue.us-west-2.amazonaws.com/iceberg \
    --catalog-warehouse 123456789012 --catalog-sigv4 --warehouse s3://my-bucket/iceberg

  # Read it back (DuckDB 1.4 needs the key columns selected, or a
  # materialized copy, for equality deletes to apply; 1.5 does not)
  duckdb -c "INSTALL iceberg; LOAD iceberg; SELECT id, status FROM iceberg_scan('/data/iceberg/shop/orders') LIMIT 10;"`,
	RunE: runExportIceberg,
}

var (
	eiIndexDSN    string
	eiBaselineDir string
	eiBaselineS3  string
	eiWarehouse   string
	eiTables      string
	eiAt          string
	eiFetchBatch  int
	eiFormat      string

	eiCatalog          string
	eiCatalogURI       string
	eiCatalogWarehouse string
	eiSigV4            bool
	eiSigV4Region      string
	eiSigV4Service     string
	eiCredentialFile   string
	eiTokenFile        string
)

// The REST catalog's secrets are read from these variables or from the
// --catalog-*-file flags, never from a flag value: a command line is
// readable by every local user (ps, /proc/<pid>/cmdline) and lands in shell
// history. The env file bindCommandEnv loads (.bintrail.env,
// ~/.config/bintrail/config.env) sets them too.
const (
	envCatalogCredential = "BINTRAIL_ICEBERG_CATALOG_CREDENTIAL"
	envCatalogToken      = "BINTRAIL_ICEBERG_CATALOG_TOKEN"
)

func init() {
	f := exportIcebergCmd.Flags()
	f.StringVar(&eiIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	f.StringVar(&eiBaselineDir, "baseline-dir", "", "Local directory of baseline snapshots (one of --baseline-dir/--baseline-s3 is required)")
	f.StringVar(&eiBaselineS3, "baseline-s3", "", "S3 URL prefix of baseline snapshots")
	// --warehouse stays the documented name here: it is the Iceberg word for
	// this place and BINTRAIL_ICEBERG_WAREHOUSE binds it. --output is accepted
	// so the name learned on the other commands is not refused (#1793).
	cli.AddPathFlag(exportIcebergCmd, &eiWarehouse, "warehouse", "", "Local directory to write the Iceberg tables under (required)", cli.OutputFlag, cli.OutputDirAlias)
	f.StringVar(&eiTables, "tables", "", "Comma-separated schema.table list (default: every table in the newest baseline snapshot)")
	f.StringVar(&eiAt, "at", "", "Point-in-time to export up to (default: now)")
	f.IntVar(&eiFetchBatch, "fetch-batch-size", 0, "Event page size for the delta fold (0 = default)")
	f.StringVar(&eiFormat, "format", "text", "Output format: text or json")
	f.StringVar(&eiCatalog, "catalog", "hadoop", "Catalog to register the tables in: hadoop (a local directory), glue (AWS Glue, data on S3) or rest (an Iceberg REST catalog)")
	f.StringVar(&eiCatalogURI, "catalog-uri", "", "REST catalog base URL (--catalog rest)")
	f.StringVar(&eiCatalogWarehouse, "catalog-warehouse", "", "REST catalog warehouse identifier: a catalog name, an AWS account id, an S3 Tables bucket ARN (--catalog rest)")
	f.BoolVar(&eiSigV4, "catalog-sigv4", false, "Sign REST catalog requests with AWS credentials from the default chain (--catalog rest)")
	f.StringVar(&eiSigV4Region, "catalog-sigv4-region", "", "Region to sign for (default: from a Glue or S3 Tables --catalog-uri, else the AWS chain)")
	f.StringVar(&eiSigV4Service, "catalog-sigv4-service", "", "Service to sign for (default: glue or s3tables from the --catalog-uri host, else execute-api)")
	f.StringVar(&eiCredentialFile, "catalog-credential-file", "", "File holding the REST catalog OAuth client credential (client_id:client_secret); or set "+envCatalogCredential)
	f.StringVar(&eiTokenFile, "catalog-token-file", "", "File holding the REST catalog bearer token; or set "+envCatalogToken)
	cli.AddDuckDBTuningFlags(exportIcebergCmd)
	bindCommandEnv(exportIcebergCmd)

	exportCmd.AddCommand(exportIcebergCmd)
	rootCmd.AddCommand(exportCmd)
}

func runExportIceberg(cmd *cobra.Command, _ []string) error {
	if eiIndexDSN == "" {
		return fmt.Errorf("--index-dsn is required")
	}
	if eiBaselineDir == "" && eiBaselineS3 == "" {
		return fmt.Errorf("one of --baseline-dir or --baseline-s3 is required")
	}
	if eiBaselineDir != "" && eiBaselineS3 != "" {
		return fmt.Errorf("--baseline-dir and --baseline-s3 are mutually exclusive")
	}
	catCfg, err := exportCatalogConfig()
	if err != nil {
		return err
	}
	if catCfg.Kind == icebergexport.CatalogHadoop {
		if strings.TrimSpace(eiWarehouse) == "" {
			return fmt.Errorf("--warehouse is required")
		}
		if err := icebergexport.CheckWarehouse(eiWarehouse); err != nil {
			return fmt.Errorf("--warehouse: %w", err)
		}
	}
	location, err := icebergexport.CheckCatalog(catCfg, eiWarehouse)
	if err != nil {
		return err
	}
	if !cliutil.IsValidOutputFormat(eiFormat) {
		return fmt.Errorf("--format must be text or json")
	}
	if eiFetchBatch < 0 {
		return fmt.Errorf("--fetch-batch-size must be >= 0 (0 = default)")
	}
	source := eiBaselineDir
	if source == "" {
		source = eiBaselineS3
	}

	at := time.Now().UTC()
	if eiAt != "" {
		parsed, err := cliutil.ParseTime(eiAt)
		if err != nil {
			return fmt.Errorf("--at: %w", err)
		}
		if parsed != nil {
			at = *parsed
		}
	}

	tables, err := resolveExportTables(cmd, source)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("no tables to export: no baseline snapshot was discovered under %s "+
			"(take one with `bintrail dump` + `bintrail baseline` first)", source)
	}

	tuning, err := cli.DuckDBTuningFromFlags(cmd)
	if err != nil {
		return err
	}

	outcomes, err := icebergexport.Run(cmd.Context(), icebergexport.Config{
		IndexDSN:       eiIndexDSN,
		BaselineSrc:    source,
		Warehouse:      eiWarehouse,
		Tables:         tables,
		At:             at,
		Catalog:        catCfg,
		FetchBatchSize: eiFetchBatch,
		ArchiveFetcher: cli.TunedArchiveFetcher(tuning),
		DuckDBTuning:   tuning,
		OnCommit:       func(c icebergexport.Commit) { auditIcebergCommit(cmd.Context(), c) },
	})
	if err != nil {
		return err
	}

	failed := 0
	for _, o := range outcomes {
		if !o.Verdict.OK() {
			failed++
		}
	}
	target := exportTarget(catCfg, location)
	if eiFormat == "json" {
		err = writeExportJSON(cmd.OutOrStdout(), outcomes, string(catCfg.Kind), eiWarehouse, at)
	} else {
		err = writeExportSummary(cmd.OutOrStdout(), outcomes, target, at)
	}
	if err != nil {
		return fmt.Errorf("write the export report: %w", err)
	}
	if failed > 0 {
		return fmt.Errorf("iceberg export: %d of %d table(s) did not end current", failed, len(outcomes))
	}
	return nil
}

// exportCatalogConfig builds the catalog settings from the flags. The REST
// secrets are read only for --catalog rest, so a token exported for another
// tool does not break a local run; a secret FILE flag given with another
// catalog is refused, because the operator meant something by it.
func exportCatalogConfig() (icebergexport.CatalogConfig, error) {
	c := icebergexport.CatalogConfig{
		Kind:         icebergexport.CatalogKind(strings.ToLower(strings.TrimSpace(eiCatalog))),
		URI:          eiCatalogURI,
		Warehouse:    eiCatalogWarehouse,
		SigV4:        eiSigV4,
		SigV4Region:  eiSigV4Region,
		SigV4Service: eiSigV4Service,
	}
	if c.Kind == "" {
		c.Kind = icebergexport.CatalogHadoop
	}
	if c.Kind != icebergexport.CatalogREST {
		for _, f := range []struct{ name, v string }{
			{"--catalog-credential-file", eiCredentialFile},
			{"--catalog-token-file", eiTokenFile},
		} {
			if f.v != "" {
				return c, fmt.Errorf("%s only applies to --catalog rest", f.name)
			}
		}
		return c, nil
	}
	var err error
	if c.Credential, err = readSecret(envCatalogCredential, "--catalog-credential-file", eiCredentialFile); err != nil {
		return c, err
	}
	if c.Token, err = readSecret(envCatalogToken, "--catalog-token-file", eiTokenFile); err != nil {
		return c, err
	}
	return c, nil
}

// readSecret returns the secret from the environment variable or from the
// file the flag names, trimmed of surrounding blanks (a file usually ends
// in a newline). Both at once is refused rather than ranked. An unreadable
// file is reported WITHOUT the value given: a secret pasted where the path
// belongs is the likely mistake, and echoing it would print it.
func readSecret(env, flag, path string) (string, error) {
	fromEnv := strings.TrimSpace(os.Getenv(env))
	if path == "" {
		return fromEnv, nil
	}
	if fromEnv != "" {
		return "", fmt.Errorf("both %s and %s are set; use one", env, flag)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err
		}
		return "", fmt.Errorf("%s: the file cannot be read (%v); the flag takes the PATH of a file holding the secret", flag, err)
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", fmt.Errorf("%s is empty", flag)
	}
	return v, nil
}

// exportTarget says where a run wrote, for the summary's first line.
func exportTarget(c icebergexport.CatalogConfig, warehouse string) string {
	switch c.Kind {
	case icebergexport.CatalogGlue:
		return "the Glue catalog, " + strings.TrimSpace(warehouse)
	case icebergexport.CatalogREST:
		t := "the REST catalog at " + icebergexport.RedactURI(c.URI)
		if c.Warehouse != "" {
			t += " (warehouse " + c.Warehouse + ")"
		}
		if strings.TrimSpace(warehouse) != "" {
			t += ", " + strings.TrimSpace(warehouse)
		}
		return t
	}
	return warehouse
}

// resolveExportTables returns --tables when given, otherwise every table in
// the newest discoverable baseline snapshot: the set the first run can seed,
// which keeps later runs a strict continuation of it.
func resolveExportTables(cmd *cobra.Command, source string) ([]string, error) {
	if eiTables != "" {
		var out []string
		for _, entry := range strings.Split(eiTables, ",") {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			if !strings.Contains(entry, ".") {
				return nil, fmt.Errorf("--tables entry %q must be schema.table", entry)
			}
			out = append(out, entry)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("--tables: no entries after trimming")
		}
		return out, nil
	}
	return reconstruct.NewestSnapshotTables(cmd.Context(), source)
}

// auditIcebergCommit records one audit event per commit, called by the
// export right after that commit is durable and before the run moves on. A
// run killed between two tables has still written the first, and only a
// per-commit emission says so; an unchanged table commits no rows and is not
// an event.
//
// snapshot_id is present only when the commit left the table at an Iceberg
// snapshot. A first load of a zero-row baseline commits the table and its
// cursor and no data file, so there is no snapshot to name; writing "0"
// there would send a reader to iceberg_snapshots() for an id that does not
// exist (#1509).
func auditIcebergCommit(ctx context.Context, c icebergexport.Commit) {
	detail := map[string]string{
		"commit":   string(c.Kind),
		"rows":     strconv.FormatInt(c.Rows, 10),
		"deletes":  strconv.FormatInt(c.Deletes, 10),
		"events":   strconv.FormatInt(c.Events, 10),
		"cursor":   c.Cursor,
		"location": c.Location,
	}
	if c.SnapshotID != nil {
		detail["snapshot_id"] = strconv.FormatInt(*c.SnapshotID, 10)
	}
	ext.Record(ctx, ext.AuditEvent{
		Surface: "cli",
		Action:  "export.iceberg",
		Actor:   ext.ProcessActor(""),
		Schema:  c.Schema,
		Table:   c.Table,
		Detail:  detail,
	})
}

func writeExportSummary(w io.Writer, outcomes []icebergexport.Outcome, warehouse string, at time.Time) error {
	var b strings.Builder
	fmt.Fprintf(&b, "iceberg export to %s at %s\n\n", warehouse, at.UTC().Format(time.RFC3339))
	width := 0
	for _, o := range outcomes {
		if n := len(o.Schema) + 1 + len(o.Table); n > width {
			width = n
		}
	}
	for _, o := range outcomes {
		name := o.Schema + "." + o.Table
		fmt.Fprintf(&b, "  %-*s  %s\n", width, name, o.Verdict)
		if o.Detail != "" {
			fmt.Fprintf(&b, "  %-*s  └─ %s\n", width, "", o.Detail)
		}
	}
	b.WriteString("\n")
	_, err := io.WriteString(w, b.String())
	return err
}

// exportJSON is the --format json shape.
type exportJSON struct {
	Catalog   string            `json:"catalog"`
	Warehouse string            `json:"warehouse"`
	At        string            `json:"at"`
	Tables    []exportTableJSON `json:"tables"`
}

type exportTableJSON struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	Verdict    string `json:"verdict"`
	Detail     string `json:"detail,omitempty"`
	RowsLoaded int64  `json:"rows_loaded"`
	Events     int64  `json:"events"`
	Upserts    int64  `json:"upserts"`
	Deletes    int64  `json:"deletes"`
	SnapshotID *int64 `json:"snapshot_id,omitempty"` // absent when the table has no snapshot (a zero-row load)
	Cursor     string `json:"cursor,omitempty"`
	Location   string `json:"location,omitempty"`
}

func writeExportJSON(w io.Writer, outcomes []icebergexport.Outcome, catalogKind, warehouse string, at time.Time) error {
	out := exportJSON{Catalog: catalogKind, Warehouse: warehouse, At: at.UTC().Format(time.RFC3339)}
	for _, o := range outcomes {
		out.Tables = append(out.Tables, exportTableJSON{
			Schema: o.Schema, Table: o.Table, Verdict: string(o.Verdict), Detail: o.Detail,
			RowsLoaded: o.RowsLoaded, Events: o.Events, Upserts: o.Upserts, Deletes: o.Deletes,
			SnapshotID: o.SnapshotID, Cursor: o.Cursor, Location: o.Location,
		})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}
