package consoleapp

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
)

// sql-compare is the read router's measuring tool (#2038): it plays SELECTs
// against the MySQL source and against the copy (through the port, routing
// off) and reports which ones the copy answers DIFFERENTLY. It is a
// read-only operator command, outside watch; it never touches the source
// beyond SELECT and EXPLAIN.

var (
	scSourceDSN     string
	scCopyDSN       string
	scSchema        string
	scStatements    string
	scSample        int
	scMaxRows       int
	scNoRerun       bool
	scFormat        string
	scCostThreshold float64
	scScanRows      int64
)

var sqlCompareCmd = &cobra.Command{
	Use:   "sql-compare",
	Short: "Run SELECTs against the MySQL source and the copy and report which ones the copy answers differently",
	Long: `Plays read statements through both sides of the read router and compares
the answers row by row: the MySQL source (--source-dsn) and the server's
copy, reached through the console's MySQL-protocol port with read routing
OFF (--copy-dsn, user = server id or name, password = the access token).

Statements come from a file (--statements; one or more lines each, ending
in ';') or are sampled from what the source recently ran (--sample N, from
performance_schema.events_statements_history_long).

Per statement the report says EQUAL, DIFFERENT (and how: rows, order, case,
null, precision, text), NOT_ON_COPY (the copy refused it), SOURCE_ERROR,
INCONCLUSIVE (cut at the port's cap, over --max-rows, or the source changed
between two runs) or SKIPPED (not a plain read: writes, WITH ... DELETE,
SELECT ... INTO, locking reads, GET_LOCK/SLEEP/BENCHMARK, two statements in
one; a stored function with side effects cannot be screened by text), plus
what the read router would do with it. The run refuses to start when the
copy port has read routing on (it would compare MySQL with MySQL), and
exits 1 when nothing reached a comparison. The exit status is 1 when a statement the router would send to
the copy answers differently: that is a veto to add or a difference to
document before turning routing on for that workload.

The copy answers from its last snapshot: compare on a quiet source or right
after a snapshot, since the tool does not model the port's copy-age check (a
copy behind the source shows up as DIFFERENT); a table written during the run
shows up as INCONCLUSIVE "source changed". Same rows in a different order are
counted apart and never fail the run (ties resolve differently per engine).`,
	RunE: runSQLCompare,
}

func init() {
	f := sqlCompareCmd.Flags()
	f.StringVar(&scSourceDSN, "source-dsn", "", "MySQL source DSN, user:pass@tcp(host:port)/db (env BINTRAIL_SOURCE_DSN)")
	f.StringVar(&scCopyDSN, "copy-dsn", "", "The console's MySQL-protocol port for the same server: <server>:<token>@tcp(host:port)/ (read routing must be off on that port)")
	f.StringVar(&scSchema, "schema", "", "USE this schema on both sides first (default: the DSNs' own)")
	f.StringVar(&scStatements, "statements", "", "File of statements to compare, ';'-terminated")
	f.IntVar(&scSample, "sample", 0, "Instead of a file, compare the N most recent distinct SELECTs the source ran (performance_schema.events_statements_history_long)")
	f.IntVar(&scMaxRows, "max-rows", 1000, "Rows read from the source per statement; more is INCONCLUSIVE")
	f.BoolVar(&scNoRerun, "no-rerun", false, "Do not re-read the source on a DIFFERENT statement (the rerun tells live writes from real differences)")
	f.StringVar(&scFormat, "format", "text", "Output format: text or json")
	f.Float64Var(&scCostThreshold, "route-cost-threshold", readrouter.DefaultPolicy().CostThreshold, "The router's cost threshold, for the 'what would the router do' column")
	f.Int64Var(&scScanRows, "route-scan-rows", readrouter.DefaultPolicy().ScanRows, "The router's full-scan row threshold, for the same column")
	rootCmd.AddCommand(sqlCompareCmd)
}

func runSQLCompare(cmd *cobra.Command, args []string) error {
	if scSourceDSN == "" {
		scSourceDSN = os.Getenv("BINTRAIL_SOURCE_DSN")
	}
	switch {
	case scSourceDSN == "":
		return fmt.Errorf("--source-dsn is required (or BINTRAIL_SOURCE_DSN)")
	case scCopyDSN == "":
		return fmt.Errorf("--copy-dsn is required: the console's MySQL-protocol port, <server>:<token>@tcp(host:port)/")
	case (scStatements == "") == (scSample == 0):
		return fmt.Errorf("give exactly one of --statements <file> or --sample <n>")
	case scFormat != "text" && scFormat != "json":
		return fmt.Errorf("invalid format %q: must be text or json", scFormat)
	}
	ctx := cmd.Context()
	var statements []string
	if scStatements != "" {
		fh, err := os.Open(scStatements)
		if err != nil {
			return err
		}
		statements, err = sqlcompare.ParseStatements(fh)
		fh.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", scStatements, err)
		}
	} else {
		var err error
		statements, err = sqlcompare.SampleSource(ctx, scSourceDSN, scSample, scSchema)
		if err != nil {
			return err
		}
	}
	rep, err := sqlcompare.Run(ctx, sqlcompare.Options{
		SourceDSN: scSourceDSN,
		CopyDSN:   scCopyDSN,
		Schema:    scSchema,
		MaxRows:   scMaxRows,
		NoRerun:   scNoRerun,
		Policy:    readrouter.Policy{CostThreshold: scCostThreshold, ScanRows: scScanRows},
	}, statements)
	if err != nil {
		return err
	}
	if scFormat == "json" {
		if err := sqlcompare.WriteJSON(cmd.OutOrStdout(), rep); err != nil {
			return err
		}
	} else {
		sqlcompare.WriteText(cmd.OutOrStdout(), rep)
	}
	if rep.NothingCompared() {
		return errSQLCompareNothing
	}
	if rep.Failed() {
		return errSQLCompareDiffers
	}
	return nil
}

// errSQLCompareNothing: a run that compared nothing must not exit 0.
var errSQLCompareNothing = fmt.Errorf("nothing was compared: no statement ran on both sides (see the report)")

// errSQLCompareDiffers is the exit-1 signal; the report above it already
// said which statements.
var errSQLCompareDiffers = fmt.Errorf("the copy answers at least one copy-routed statement differently from MySQL")
