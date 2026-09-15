package reconstruct

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ParquetWriterCompression / ParquetWriterRowGroupSize are the codec and row
// group size a reconstructed baseline snapshot is written with. They are pinned
// to `bintrail baseline`'s own defaults rather than exposed as flags: the whole
// point of #1169 is that the emitted snapshot is indistinguishable from a
// mydumper-sourced one, and a snapshot chain whose files alternate codecs run to
// run is a needless source of "why is this one different".
const (
	ParquetWriterCompression  = "zstd"
	ParquetWriterRowGroupSize = 500_000
)

// parquetTableWriter writes one table of a reconstructed baseline snapshot.
//
// It is the Parquet-mode sibling of MydumperWriter and deliberately mirrors its
// lifecycle — Close finalizes, Discard unlinks (#1162) — so ReconstructTable's
// error paths behave identically in both output formats. The difference that
// matters is what a partial file means: a truncated Parquet file has no footer
// and does not parse at all, so a crash leaves something visibly broken rather
// than a loadable prefix. Discard still removes it, because a snapshot directory
// containing an unreadable file for one table is a worse diagnostic than one
// where that table is simply absent.
type parquetTableWriter struct {
	w    *baseline.Writer
	cols []baseline.Column
	path string

	rows      int64
	closed    bool
	finalized bool

	// missingWarned tracks columns already warned about being absent from an
	// emitted row, so a systematic mismatch warns once per column instead of once
	// per row.
	missingWarned map[string]bool
}

// newParquetTableWriter creates the table's Parquet file under snapshotDir,
// laid out exactly where baseline.Run puts it (<snapshot>/<db>/<table>.parquet)
// so reconstruct.FindBaseline discovers it by the same glob.
func newParquetTableWriter(path string, cols []baseline.Column, md map[string]string) (*parquetTableWriter, error) {
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  ParquetWriterCompression,
		RowGroupSize: ParquetWriterRowGroupSize,
		Metadata:     md,
	})
	if err != nil {
		return nil, fmt.Errorf("create baseline parquet writer %s: %w", path, err)
	}
	return &parquetTableWriter{w: w, cols: cols, path: path, missingWarned: map[string]bool{}}, nil
}

// WriteRow renders one reconstructed row — keyed by column name, as
// mergeBaselineImages emits it — into the text form baseline.Writer converts,
// and appends it to the Parquet file.
func (w *parquetTableWriter) WriteRow(row map[string]any, schema, table string) error {
	if w.closed {
		return ErrWriterClosed
	}
	values := make([]string, len(w.cols))
	nulls := make([]bool, len(w.cols))
	for i, col := range w.cols {
		v, ok := row[col.Name]
		if !ok {
			// Same defensive stance as rowAfterOrdered on the mydumper path: the
			// #602/#843 guards already refused every drift shape that could get
			// here, so this is a bug-catcher, not a supported layout.
			if !w.missingWarned[col.Name] {
				w.missingWarned[col.Name] = true
				slog.Warn("reconstructed row missing column present in the baseline schema; emitting NULL",
					"schema", schema, "table", table, "column", col.Name)
			}
			nulls[i] = true
			continue
		}
		text, isNull, err := renderBaselineValue(col, v)
		if err != nil {
			return fmt.Errorf("%s.%s column %q: %w", schema, table, col.Name, err)
		}
		values[i], nulls[i] = text, isNull
	}
	if err := w.w.WriteRow(values, nulls); err != nil {
		return err
	}
	w.rows++
	return nil
}

// Rows returns how many rows have been written.
func (w *parquetTableWriter) Rows() int64 { return w.rows }

// Close flushes the Parquet footer. Only a successful Close finalizes the file;
// see MydumperWriter.finalized for why that distinction is load-bearing.
func (w *parquetTableWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.w.Close(); err != nil {
		return err
	}
	w.finalized = true
	return nil
}

// Discard aborts the writer and unlinks the file it created. A no-op after a
// successful Close, so a completed table's output is structurally undeletable by
// a caller's deferred error path (#1162).
func (w *parquetTableWriter) Discard() error {
	if w.finalized {
		return nil
	}
	if !w.closed {
		w.closed = true
		// Ignore the close error: the file is unlinked immediately below, so
		// whatever failed to flush changes nothing.
		_ = w.w.Close()
	}
	if err := os.Remove(w.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// baselineMeta identifies the snapshot a reconstruction was derived from.
type baselineMeta struct {
	Path     string
	Time     time.Time
	Metadata baseline.DumpMetadata
}

// Parquet file-metadata keys unique to a RECONSTRUCTED snapshot.
//
// The definitions moved to internal/baseline in #1545, where the rest of the
// footer vocabulary lives and where the dump writers and every reader can reach
// them. These aliases stay so existing call sites and tests keep compiling, and
// because this is the package whose writer stamps them.
//
// Written since #1169 as pure provenance — nothing looked them up, so their
// presence could not change how a consumer treated the snapshot. #1545 gave
// them readers; the string values did not move, because every snapshot already
// on disk carries them.
const (
	MetaKeySnapshotProducer = baseline.MetaKeySnapshotProducer
	MetaKeyDerivedFrom      = baseline.MetaKeyDerivedFrom
	MetaKeyDerivedFromPath  = baseline.MetaKeyDerivedFromPath
	// SnapshotProducerReconstruct is the MetaKeySnapshotProducer value this
	// package stamps. A dump stamps baseline.ProducerDump.
	SnapshotProducerReconstruct = baseline.ProducerReconstruct
)

// mergeBaselineIntoParquet is the OutputFormatParquet sibling of
// mergeBaselineIntoWriter: it runs the identical merge and writes the result as
// a baseline snapshot Parquet file instead of a mydumper SQL dump.
//
// # What makes the output a baseline rather than just a Parquet file
//
//   - Layout: <snapshot>/<db>/<table>.parquet, byte-for-byte where baseline.Run
//     puts it, so FindBaseline's glob discovers it unchanged.
//   - Schema: the columns come from the SOURCE baseline's own
//     MetaKeyCreateTableSQL, parsed by the same ParseSchema code path that read
//     the mydumper schema file originally — so the emitted Parquet has the same
//     column names, the same MySQL→Parquet type mapping, and (via
//     baseline.Writer's alphabetical sort) the same physical column order.
//   - Anchor: MetaKeyBinlogFile/Pos carry the run's cut for every table this
//     path WRITES, the coordinate where
//     the next fold resumes. This is the load-bearing field; see
//     ResolveSnapshotCut.
//   - MetaKeyCreateTableSQL is propagated verbatim, so a reconstruct anchored on
//     THIS snapshot can emit a schema file (or a further snapshot) exactly as it
//     could from the original.
//
// # What is deliberately NOT stamped
//
//   - MetaKeyGTIDSet: binlog_events.gtid is per event, not an accumulated
//     executed-set, and there is no correct way to synthesize one from it. No
//     consumer needs it (the delta fetch anchors on file/pos), and operators read
//     it — so absent beats wrong.
//   - MetaKeyContentDigest / MetaKeyRowCount: the digest certifies that a dump
//     captured the same rows as the SOURCE (#633), and `verify` compares it
//     against a live source. A reconstructed snapshot never touched the source,
//     so any digest here would be a fingerprint of our own arithmetic presented
//     as source fidelity — it would manufacture a mismatch, or worse, a false
//     match. Absent is the honest state, and readers already treat an absent
//     digest as "not verifiable this way" rather than as an error.
func mergeBaselineIntoParquet(ctx context.Context, in mergeInput, rep *TableReport) (retErr error) {
	colNames, err := prepareMerge(ctx, in)
	if err != nil {
		return err
	}

	// The emitted snapshot's schema is the SOURCE snapshot's schema. Parsing the
	// embedded CREATE TABLE (rather than inferring types from the Parquet file
	// we are about to read) is what keeps the types identical across a refresh
	// chain: Parquet's own types are lossy in the other direction — a DECIMAL, a
	// TIME and a JSON column are all "string" once written.
	cols, err := baseline.ParseSchemaText(in.CreateTableSQL)
	if err != nil {
		return fmt.Errorf("parse the baseline's embedded CREATE TABLE for %s.%s: %w", in.Schema, in.Table, err)
	}
	if err := checkSchemaMatchesBaseline(cols, colNames, in.Schema, in.Table); err != nil {
		return err
	}

	md := map[string]string{
		baseline.MetaKeySnapshotTimestamp: in.SnapshotAt.UTC().Format(time.RFC3339),
		"bintrail.source_database":        in.Schema,
		"bintrail.source_table":           in.Table,
		"bintrail.bintrail_version":       baseline.Version,
		baseline.MetaKeyCreateTableSQL:    in.CreateTableSQL,
		MetaKeySnapshotProducer:           SnapshotProducerReconstruct,
		MetaKeyDerivedFrom:                in.SourceBaseline.Time.UTC().Format(time.RFC3339),
		MetaKeyDerivedFromPath:            in.SourceBaseline.Path,
		baseline.MetaKeyCreateTableAsOf:   createTableAsOf(in.SourceBaseline).UTC().Format(time.RFC3339),
	}
	if line := captureGapLines(in); line != "" {
		md[baseline.MetaKeyCaptureGap] = line
	}
	switch {
	case in.Cut != nil:
		md[baseline.MetaKeyBinlogFile] = in.Cut.File
		md[baseline.MetaKeyBinlogPos] = strconv.FormatUint(in.Cut.Pos, 10)
	case in.SourceBaseline.Metadata.BinlogFile != "":
		// No cut means the index holds no events, so nothing was folded and the
		// source's anchor is still exactly where deltas resume. Carrying it over
		// keeps the chain unbroken; omitting it would strand the new snapshot with
		// no anchor at all and force the next fetch back onto the imprecise
		// timestamp bound (#797).
		md[baseline.MetaKeyBinlogFile] = in.SourceBaseline.Metadata.BinlogFile
		md[baseline.MetaKeyBinlogPos] = strconv.FormatInt(in.SourceBaseline.Metadata.BinlogPos, 10)
	}

	// The disk check (#1614) runs here, not before the fold: a table carried
	// forward unchanged never gets this far, so only a table that is really
	// rewritten counts, sized on the file it is rebuilt from, already local.
	if in.SpaceCheck != nil {
		fi, err := os.Stat(in.LocalBaselinePath)
		if err != nil {
			return fmt.Errorf("size the backup file %s.%s is rebuilt from: %w", in.Schema, in.Table, err)
		}
		if err := in.SpaceCheck(in.SnapshotDir, fi.Size()); err != nil {
			return err
		}
	}

	path := filepath.Join(in.SnapshotDir, in.Schema, in.Table+".parquet")
	w, err := newParquetTableWriter(path, cols, md)
	if err != nil {
		return err
	}
	// Same #1162 stance as the mydumper path: any error return unlinks this
	// table's file. A removal failure is logged, never returned — it must not
	// shadow the error that triggered the abort.
	defer func() {
		if retErr == nil {
			return
		}
		if derr := w.Discard(); derr != nil {
			slog.Warn("could not remove partial snapshot file for failed table",
				"schema", in.Schema, "table", in.Table, "path", path, "error", derr)
		}
	}()

	stats, err := mergeBaselineImages(ctx, mergeCore{
		LocalBaselinePath: in.LocalBaselinePath,
		Schema:            in.Schema,
		Table:             in.Table,
		PKCols:            in.PKCols,
		Changes:           in.Changes,
		Spill:             in.Spill,
		CheckPass: func(m map[string]*query.ResultRow) error {
			return checkPostBaselineColumns(in, m, colNames)
		},
		DuckDBTuning: in.DuckDBTuning,
	}, func(rowMap map[string]any) error {
		return w.WriteRow(rowMap, in.Schema, in.Table)
	})
	if err != nil {
		return err
	}
	rep.BaselineRows = stats.BaselineRows
	rep.UpdatesApplied = stats.UpdatesApplied
	rep.InsertsEmitted = stats.InsertsEmitted
	rep.DeletesSkipped = stats.DeletesSkipped

	if err := w.Close(); err != nil {
		return fmt.Errorf("close snapshot writer for %s.%s: %w", in.Schema, in.Table, err)
	}
	rep.Files = []string{filepath.Join(in.Schema, in.Table+".parquet")}
	rep.RowsWritten = w.Rows()
	return nil
}

// captureGapLines builds the snapshot's MetaKeyCaptureGap value: what it
// inherited from the snapshot it was derived from, plus what this run itself
// overrode.
//
// Inheritance is the load-bearing half. A refresh chain folds forward, so the
// events a gapped ancestor never captured are absent from every descendant —
// dropping the ancestor's line at the first refresh would silently launder a
// knowingly-incomplete baseline into a clean-looking one, which is exactly the
// state #1170 exists to make impossible.
func captureGapLines(in mergeInput) string {
	lines := strings.TrimSpace(in.SourceBaseline.Metadata.CaptureGap)
	if in.CaptureGap == nil {
		return lines
	}
	own := in.SnapshotAt.UTC().Format(time.RFC3339) + ": " + in.CaptureGap.Reason()
	if lines == "" {
		return own
	}
	return lines + "\n" + own
}

// checkBaselineSchemaCurrent refuses when the CREATE TABLE embedded in the
// source baseline no longer describes the table's current columns.
//
// The comparison is against the schema SNAPSHOT (the resolver), not the live
// table — that is the newest statement of the schema this index has, and it is
// what every other reconstruct decision is already made against. Generated
// columns are excluded on both sides: mydumper never dumps their values, so
// ParseSchema drops them, and the snapshot marks them explicitly.
//
// Why refuse rather than warn: the emitted snapshot carries the OLD CREATE
// TABLE forward as its own schema. Publish it and every later reconstruct
// anchored on it declares a table shape the source stopped having, with rows
// projected onto the old column set — a dump that loads and is wrong. A real
// re-dump is the only correct answer, so the message says so instead of
// offering a flag.
//
// Column TYPES (#1651) and NULL-ness (#1665) are compared against typesTM, not tm, and only when the
// caller has one: the schema snapshot in effect at the fold's target instant,
// and only when that snapshot was taken after the baseline. An older snapshot
// says nothing about the baseline's types (the baseline's own CREATE TABLE,
// dumped from the live table, is newer), and a snapshot taken after the target
// describes a change the fold does not reach, which a restore to an earlier
// moment must not refuse over. nil compares names only.
//
// Names are compared against tm, which the fold also picks by target (#1667):
// nil when no snapshot describes the table at the target, which skips names.
func checkBaselineSchemaCurrent(createSQL string, tm, typesTM *metadata.TableMeta, schema, table string) error {
	return checkBaselineSchema(createSQL, tm, typesTM, schema, table)
}

// checkBaselineSchema compares column names against tm (nil skips them) and, when typesTM is
// not nil, declared types and NULL-ness (#1665) against typesTM. The Iceberg export passes nil: it
// never publishes the carried CREATE TABLE, and it compares the types it maps
// to Iceberg itself (icebergexport.sameTableTypes), so a type change that does
// not move the exported value (an ENUM relabel, a longer VARCHAR) must not
// refuse there.
func checkBaselineSchema(createSQL string, tm, typesTM *metadata.TableMeta, schema, table string) error {
	if strings.TrimSpace(createSQL) == "" || (tm == nil && typesTM == nil) {
		// A missing CREATE TABLE is already refused upstream with its own
		// message, and with no snapshot on either side there is nothing to
		// compare. Neither is this check's story to tell.
		return nil
	}
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		return fmt.Errorf("parse the baseline's embedded CREATE TABLE for %s.%s: %w", schema, table, err)
	}
	inBaseline := make(map[string]baseline.Column, len(cols))
	for _, c := range cols {
		inBaseline[strings.ToLower(c.Name)] = c
	}
	// retyped is in typesTM's ordinal order, which is already stable.
	var retyped []string
	if typesTM != nil {
		for _, c := range typesTM.Columns {
			if c.IsGenerated {
				continue
			}
			if b, ok := inBaseline[strings.ToLower(c.Name)]; ok {
				if change, changed := columnDefinitionChanged(b, c); changed {
					retyped = append(retyped, fmt.Sprintf("%s (%s)", c.Name, change))
				}
			}
		}
	}
	var added, dropped []string
	if tm != nil {
		current := make(map[string]bool, len(tm.Columns))
		for _, c := range tm.Columns {
			if !c.IsGenerated {
				current[strings.ToLower(c.Name)] = true
			}
		}
		for name := range current {
			if _, ok := inBaseline[name]; !ok {
				added = append(added, name)
			}
		}
		for name := range inBaseline {
			if !current[name] {
				dropped = append(dropped, name)
			}
		}
	}
	if len(added) == 0 && len(dropped) == 0 && len(retyped) == 0 {
		return nil
	}
	sort.Strings(added)
	sort.Strings(dropped)
	return fmt.Errorf(
		"%s.%s changed shape since its baseline was taken (added since: %s; gone since: %s; type changed since: %s) — "+
			"a snapshot emitted from it would carry the OLD CREATE TABLE forward and project every row onto the old columns and types, "+
			"so every reconstruct anchored on it would be wrong. Take a real snapshot instead: `bintrail dump` + `bintrail baseline`. "+
			"(If the schema snapshot is what is stale, run `bintrail snapshot` first and retry.): %w",
		schema, table, strings.Join(orNone(added), ", "), strings.Join(orNone(dropped), ", "),
		strings.Join(orNone(retyped), ", "), ErrSchemaChanged)
}

// columnTypeChanged reports whether a column's type in the schema snapshot is
// no longer the type its baseline's CREATE TABLE declares (#1651). The emitted
// snapshot carries that CREATE TABLE forward, and it is what a restore loads:
// reconstruct's mydumper output, the SQL export and drill all write it as the
// table definition. So any real change refuses, not only one that moves the
// Parquet value. An INT widened to BIGINT fails the write past INT's range
// (#1652); a VARCHAR(32) widened to VARCHAR(64), or an ENUM given a new label,
// publishes and then refuses the row at load ("Data too long", "Data
// truncated"); a DATETIME given fractional seconds loads and silently rounds
// them. Only spelling is ignored (baseline.ComparableColumnType), so a display
// width a server upgrade dropped is not a change.
//
// The check runs in Parquet mode only, so it keeps a refresh from carrying a
// wrong definition forward; a mydumper-mode reconstruct or SQL export writes
// the source baseline's CREATE TABLE as it is and is not refused here.
//
// The cost of refusing is bounded: a scheduled backup that refuses falls back
// to a full backup where the daemon may take one, which dumps the current
// CREATE TABLE and so compares equal from then on. The daemon-wide refresh
// interval and a restore have no such fallback; they refuse until a full
// backup exists. A type change the stream saw no DDL for (TRUNCATE is skipped,
// and so is a statement parseDDL does not recognize) leaves no newer snapshot,
// and then nothing is compared at all.
//
// When the snapshot lacks COLUMN_TYPE (taken before column_type was captured)
// only the DATA_TYPE token is compared: INT to BIGINT is seen, a length, a
// scale or a sign change is not. With neither (a PostgreSQL snapshot, which
// carries no MySQL types) nothing is compared, because a missing type is not a
// change.
func columnTypeChanged(declared string, c metadata.ColumnMeta) (was, now string, changed bool) {
	if strings.TrimSpace(declared) == "" {
		return "", "", false
	}
	switch {
	case strings.TrimSpace(c.ColumnType) != "":
		was, now = baseline.ComparableColumnType(declared), baseline.ComparableColumnType(c.ColumnType)
	case strings.TrimSpace(c.DataType) != "":
		was, now = typeToken(baseline.ComparableColumnType(declared)), typeToken(baseline.ComparableColumnType(c.DataType))
	default:
		return "", "", false
	}
	return strings.TrimSpace(declared), strings.TrimSpace(firstNonEmpty(c.ColumnType, c.DataType)), was != now
}

// columnDefinitionChanged is one column's entry in the type-change refusal:
// its declared type (columnTypeChanged) and whether it allows NULL
// (columnNullabilityChanged, #1665), as "was -> now". When only the NULL-ness
// moved, the type is shown unchanged on both sides as each side spells it, or
// left out when the snapshot carries no type.
func columnDefinitionChanged(b baseline.Column, c metadata.ColumnMeta) (string, bool) {
	was, now, typeChanged := columnTypeChanged(b.DeclaredType, c)
	nullWas, nullNow, nullChanged := columnNullabilityChanged(b.NotNull, c)
	if !typeChanged && !nullChanged {
		return "", false
	}
	if !typeChanged {
		was, now = strings.TrimSpace(b.DeclaredType), strings.TrimSpace(firstNonEmpty(c.ColumnType, c.DataType))
		if now == "" {
			was = ""
		}
	}
	if nullChanged {
		was, now = strings.TrimSpace(was+" "+nullWas), strings.TrimSpace(now+" "+nullNow)
	}
	return was + " -> " + now, true
}

// columnNullabilityChanged reports whether a column's IS_NULLABLE in the
// schema snapshot disagrees with the NOT NULL its baseline's CREATE TABLE
// declares (#1665). Same reason as a type change: the carried CREATE TABLE is
// what a restore loads, so a column made nullable and then set to NULL
// publishes a backup that fails at load ("Column cannot be null"), and one made
// NOT NULL publishes a definition the source no longer has. A snapshot that
// does not know (an empty value, as a PostgreSQL snapshot stores) is not a
// change.
func columnNullabilityChanged(notNull bool, c metadata.ColumnMeta) (was, now string, changed bool) {
	switch strings.ToUpper(strings.TrimSpace(c.IsNullable)) {
	case "YES":
		if notNull {
			return "NOT NULL", "NULL", true
		}
	case "NO":
		if !notNull {
			return "NULL", "NOT NULL", true
		}
	}
	return "", "", false
}

// typeToken is the leading type word of a ComparableColumnType spelling.
func typeToken(comparable string) string {
	if n := strings.IndexAny(comparable, "( "); n >= 0 {
		return comparable[:n]
	}
	return comparable
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

// checkSchemaMatchesBaseline refuses when the CREATE TABLE embedded in the
// source snapshot does not describe the columns that snapshot actually holds.
//
// The two are written together by baseline.Run and normally agree exactly. When
// they don't — a hand-assembled snapshot, a CREATE TABLE whose DDL shape
// ParseSchema reads differently than it did at dump time (an unusual generated
// column, #767) — the mismatch is silent and directional: a column in the DDL
// but not in the file is written as all-NULL, and a column in the file but not
// in the DDL is dropped. Both produce a snapshot that loads and is wrong, which
// is the one outcome this whole path exists to avoid.
func checkSchemaMatchesBaseline(cols []baseline.Column, colNames []string, schema, table string) error {
	inDDL := make(map[string]bool, len(cols))
	for _, c := range cols {
		inDDL[c.Name] = true
	}
	inFile := make(map[string]bool, len(colNames))
	for _, n := range colNames {
		inFile[n] = true
	}
	var onlyDDL, onlyFile []string
	for _, c := range cols {
		if !inFile[c.Name] {
			onlyDDL = append(onlyDDL, c.Name)
		}
	}
	for _, n := range colNames {
		if !inDDL[n] {
			onlyFile = append(onlyFile, n)
		}
	}
	if len(onlyDDL) == 0 && len(onlyFile) == 0 {
		return nil
	}
	return fmt.Errorf(
		"baseline for %s.%s disagrees with its own embedded CREATE TABLE "+
			"(only in the DDL: %s; only in the Parquet file: %s) — emitting a snapshot from it would NULL-fill or "+
			"drop those columns; re-run `bintrail dump` + `bintrail baseline` to take a consistent snapshot",
		schema, table,
		strings.Join(orNone(onlyDDL), ", "), strings.Join(orNone(onlyFile), ", "))
}

func orNone(s []string) []string {
	if len(s) == 0 {
		return []string{"none"}
	}
	return s
}

// renderBaselineValue converts one reconstructed value into the text form
// baseline.Writer's convertValue parses, plus a NULL flag.
//
// Going through text rather than building parquet.Values directly is
// deliberate: convertValue is the single, test-pinned authority on how a MySQL
// value becomes a Parquet value (UNSIGNED widening #506, zero-date pseudo-NULL,
// hex-blob decoding #503), and every other producer feeding baseline.Writer —
// the mydumper readers, internal/archive, internal/byos — reaches it the same
// way. A parallel typed path would be a second conversion authority free to
// drift from it.
//
// The values arriving here have two provenances and both must round-trip:
//
//   - Baseline pass-through rows carry DuckDB scan types read back out of the
//     PREVIOUS snapshot's Parquet (int32/int64/uint64/float/string/[]byte/
//     time.Time). These are re-rendered into the exact text that produced them,
//     so a row untouched by any delta is byte-identical across a refresh chain —
//     which is what makes repeated refreshes stable rather than slowly drifting.
//   - Delta rows carry a binlog event's row_after image, JSON-decoded upstream
//     (#668/#475), so every number is a float64 regardless of the column's real
//     type. That is why the integer families re-render through a truncation
//     check instead of a bare float format: `1.234568e+06` in an INT column
//     would fail convertValue's ParseInt and abort the run.
//
// An unrecognised Go type falls through to %v rather than failing here.
// convertValue is the fail-loud gate: a value it cannot parse aborts the run
// with the column, type and text in the message, which is a better diagnostic
// than a Go type name from this layer.
func renderBaselineValue(col baseline.Column, v any) (text string, isNull bool, err error) {
	if v == nil {
		return "", true, nil
	}
	switch t := v.(type) {
	case string:
		return t, false, nil

	case []byte:
		if baseline.IsBinaryType(col.MySQLType) {
			// The 0x<hex> form convertValue's decodeBinaryLiteral decodes. Always
			// hex — never the raw bytes — so bytes that happen to spell "0x1234"
			// round-trip as themselves rather than being decoded as a literal.
			return "0x" + hex.EncodeToString(t), false, nil
		}
		return string(t), false, nil

	case time.Time:
		if strings.EqualFold(strings.TrimSpace(col.MySQLType), "date") {
			return t.UTC().Format("2006-01-02"), false, nil
		}
		// The fractional part is elided when zero, which is the form
		// parseDatetimeToMicros's no-dot branch expects; a value with sub-second
		// precision keeps it and takes the other branch. UTC because the Parquet
		// timestamp is microseconds since the Unix epoch and convertValue parses
		// the text back in UTC.
		return t.UTC().Format("2006-01-02 15:04:05.999999"), false, nil

	case bool:
		// MySQL's BOOLEAN is TINYINT(1); a JSON row image can carry it as a real
		// bool, which ParseInt would reject as "true".
		if t {
			return "1", false, nil
		}
		return "0", false, nil

	case float32:
		return formatFloatForColumn(float64(t), col, 32), false, nil
	case float64:
		return formatFloatForColumn(t, col, 64), false, nil

	case json.Number:
		return t.String(), false, nil

	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", t), false, nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", t), false, nil

	default:
		return fmt.Sprintf("%v", t), false, nil
	}
}

// integerTypeTokens names the MySQL types convertValue parses with ParseInt /
// ParseUint, i.e. the ones for which a float rendering must not carry a decimal
// point or an exponent.
var integerTypeTokens = map[string]bool{
	"tinyint": true, "smallint": true, "mediumint": true,
	"int": true, "integer": true, "bigint": true, "year": true,
}

// formatFloatForColumn renders a float value as the text convertValue will parse
// for this column's type. bitSize is the float's own precision, so a float32 is
// not widened into the noise of its float64 expansion (0.1 → 0.10000000149…).
func formatFloatForColumn(f float64, col baseline.Column, bitSize int) string {
	typ := strings.ToLower(strings.TrimSpace(col.MySQLType))
	if integerTypeTokens[typ] {
		if !math.IsInf(f, 0) && !math.IsNaN(f) && f == math.Trunc(f) {
			// 'f' with -1 precision never emits an exponent, so ParseInt sees a
			// plain integer for any magnitude.
			return strconv.FormatFloat(f, 'f', -1, 64)
		}
		// A non-integral value in an integer column is a real inconsistency;
		// render it faithfully and let convertValue refuse it rather than
		// silently truncating data here.
		return strconv.FormatFloat(f, 'f', -1, bitSize)
	}
	if typ == "float" || typ == "double" || typ == "real" {
		// ParseFloat accepts exponent notation, so 'g' is safe here and keeps the
		// shortest exact representation.
		return strconv.FormatFloat(f, 'g', -1, bitSize)
	}
	// Everything else (DECIMAL, and any column whose value arrived as a float
	// through JSON) is stored as text verbatim; avoid an exponent so the stored
	// string still looks like the number MySQL would print.
	return strconv.FormatFloat(f, 'f', -1, bitSize)
}

// createTableAsOf is when the source baseline's CREATE TABLE was read from the
// live table: its own footer value when a fold carried one, else the source
// snapshot's time, which is exact for a dump and the best a pre-#1651 fold
// can offer. The column-type check compares the schema snapshot against it,
// not against the folded snapshot's directory time: a fold copies the CREATE
// TABLE unchanged, so its directory time says nothing about the definition's
// age.
func createTableAsOf(src baselineMeta) time.Time {
	if !src.Metadata.CreateTableAsOf.IsZero() {
		return src.Metadata.CreateTableAsOf
	}
	return src.Time
}
